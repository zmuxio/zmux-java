package io.zmux;

import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import io.zmux.seal.ClientConfig;
import io.zmux.seal.Gate;
import io.zmux.seal.HandlePsk;
import io.zmux.seal.KeyAlg;
import io.zmux.seal.KeyId;
import io.zmux.seal.PinAnchor;
import io.zmux.seal.Profile;
import io.zmux.seal.Seal;
import io.zmux.seal.ServerConfig;
import io.zmux.seal.StaticKey;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * The TLS and secure-carrier modes of the scripted test peer; the contract is that of zmux-rs/interop/peer.
 *
 * <ul>
 *   <li>{@code tls-server}, {@code tls-client addr}, {@code tls-split-client addr}: sessions over TLS 1.3 whose join
 *       secret is the exporter alone, a Bound join on a further connection. The first byte of each connection says what
 *       it is: {@code B} bootstrap, {@code J} join, {@code U} uplink and {@code D} downlink of a split bootstrap. Both
 *       print {@code KEYS <hex>}, the exporter value of label {@code EXPORTER-interop-app}, length 16 (twice that for a
 *       split bootstrap).
 *   <li>{@code seal-server profiles [addr]}, {@code seal-client addr profile}: the secure carrier's Establish on TCP
 *       before the session. The node's static key is 32 bytes of {@code 0x5a}, pinned by the client; Handle and
 *       pre-shared key derive from the test PSK with domain {@code interop}. The client prints {@code PROFILE <n>}; both
 *       print {@code KEYS <hex>} after {@code ESTABLISHED}.
 * </ul>
 */
final class CarrierInteropPeer {
    private static final byte[] PSK = "zmux interop pre-shared key 0123".getBytes(StandardCharsets.US_ASCII);
    private static final String APP_LABEL = "EXPORTER-interop-app";
    /** The pairing token of a split bootstrap's halves. */
    private static final String PAIR = Base64.getUrlEncoder().withoutPadding().encodeToString("zmux split pair!".getBytes(StandardCharsets.US_ASCII));

    private CarrierInteropPeer() {}

    static void run(String[] args) throws Exception {
        try (var loops = EventLoops.create(2)) {
            switch (args[0]) {
                case "tls-server" -> tlsServer(loops);
                case "tls-client", "tls-split-client" -> tlsClient(loops, args[1], args[0].equals("tls-split-client"));
                case "seal-server" -> sealServer(loops, args[1], args.length > 2 ? args[2] : "127.0.0.1:0");
                case "seal-client" -> sealClient(loops, args[1], Long.parseLong(args[2]));
                default -> throw new IllegalArgumentException("unknown mode");
            }
        }
    }

    private static InetSocketAddress address(String text) {
        int split = text.lastIndexOf(':');
        return new InetSocketAddress(text.substring(0, split), Integer.parseInt(text.substring(split + 1)));
    }

    private static void printKeys(ZmuxSession session) {
        try {
            System.out.println("KEYS " + HexFormat.of().formatHex(session.exportKeyingMaterial(APP_LABEL, new byte[0], 16)));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    // ---- TLS ----

    /** Multipath whose join secret is the exporter; a join may be Bound. */
    private static ZmuxConfig tlsConfig(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).datagrams(true).incomingUniStreams(16).maxPipes(4)
                .pipeAuth(PipeAuth.exporter().withEndToEnd()).allowRecordMode((carrier, mode, peer) -> true).build();
    }

    private static void tlsServer(EventLoops loops) throws Exception {
        TestTls.init();
        try (var listener = ZmuxListener.of(ListenerConfig.of(tlsConfig(loops)));
             var server = (SSLServerSocket) TestTls.server().getServerSocketFactory().createServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))) {
            server.setEnabledProtocols(new String[] {"TLSv1.3"});
            System.out.println("LISTEN 127.0.0.1:" + server.getLocalPort());
            Thread.ofVirtual().start(() -> {
                try {
                    while (true) {
                        var socket = (SSLSocket) server.accept();
                        Thread.ofVirtual().start(() -> admit(listener, socket));
                    }
                } catch (IOException closed) { /* the listener socket closed */ }
            });
            ZmuxSession session = listener.accept(Duration.ofSeconds(20));
            if (session == null) throw new IOException("no session");
            printKeys(session);
            InteropPeer.serve(session, null);
        }
    }

    /** Reads the connection's kind byte and hands the carrier to the listener as that kind. */
    private static void admit(ZmuxListener listener, SSLSocket socket) {
        try {
            socket.startHandshake();
            int kind = socket.getInputStream().read();
            var peer = (InetSocketAddress) socket.getRemoteSocketAddress();
            switch (kind) {
                case 'U' -> listener.adapter().handle(pipe(socket, true, false), JoinHint.pair(PAIR, true, peer));
                case 'D' -> listener.adapter().handle(pipe(socket, false, true), JoinHint.pair(PAIR, false, peer));
                default -> listener.adapter().handle(pipe(socket, true, true), JoinHint.inBand(peer));
            }
        } catch (IOException | RuntimeException e) {
            try { socket.close(); } catch (IOException ignored) { /* gone */ }
        }
    }

    private static void tlsClient(EventLoops loops, String addr, boolean split) throws Exception {
        var config = tlsConfig(loops);
        PipeIo boot;
        if (split) {
            var up = dial(addr, 'U');
            var down = dial(addr, 'D');
            boot = PipeIo.split(pipe(up, false, true), pipe(down, true, false));
        } else {
            boot = pipe(dial(addr, 'B'), true, true);
        }
        ZmuxSession session = Zmux.open(boot, Role.CLIENT, config);
        printKeys(session);
        var join = session.addPipe(pipe(dial(addr, 'J'), true, true), PipeOptions.builder().recordMode(RecordMode.BOUND).build());
        if (join.state().isEmpty()) throw new IOException("the join has no state");
        InteropPeer.serve(session, null);
    }

    private static SSLSocket dial(String addr, char kind) throws Exception {
        var ctx = SSLContext.getInstance("TLSv1.3");
        ctx.init(null, new TrustManager[] {new TrustAll()}, null);
        var at = address(addr);
        var socket = (SSLSocket) ctx.getSocketFactory().createSocket(at.getHostString(), at.getPort());
        socket.startHandshake();
        socket.getOutputStream().write(kind);
        socket.getOutputStream().flush();
        return socket;
    }

    /** The self-signed test peers are not verified. */
    private static final class TrustAll extends X509ExtendedTrustManager {
        @Override public void checkClientTrusted(X509Certificate[] chain, String type, Socket socket) { }
        @Override public void checkServerTrusted(X509Certificate[] chain, String type, Socket socket) { }
        @Override public void checkClientTrusted(X509Certificate[] chain, String type, javax.net.ssl.SSLEngine engine) { }
        @Override public void checkServerTrusted(X509Certificate[] chain, String type, javax.net.ssl.SSLEngine engine) { }
        @Override public void checkClientTrusted(X509Certificate[] chain, String type) { }
        @Override public void checkServerTrusted(X509Certificate[] chain, String type) { }
        @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
    }

    /**
     * A carrier over a blocking TLS socket, with the connection's exporter, declared Bound-capable. A half that is
     * the whole of its connection's use ends the socket with it.
     */
    private static PipeIo pipe(SSLSocket socket, boolean receive, boolean send) throws IOException {
        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream();
        PipeReader reader = new PipeReader() {
            volatile boolean closed;
            @Override public void start(Sink sink) {
                Thread.ofVirtual().start(() -> {
                    try {
                        byte[] bytes = new byte[16384];
                        for (int n; !closed && (n = in.read(bytes)) >= 0;) {
                            var done = new CountDownLatch(1);
                            sink.onData(ByteBuffer.wrap(bytes.clone(), 0, n), done::countDown);
                            done.await();
                        }
                        if (!closed) sink.onEnd();
                    } catch (Exception e) {
                        if (!closed) sink.onError(e);
                    }
                });
            }
            @Override public void close() {
                closed = true;
                if (!send) try { socket.close(); } catch (IOException ignored) { /* gone */ }
            }
        };
        PipeWriter writer = new PipeWriter() {
            @Override public synchronized void write(OutboundBatch data, Completion done) {
                try {
                    for (ByteBuffer b : data.segments(new ByteBuffer[data.segmentCount()])) {
                        byte[] a = new byte[b.remaining()];
                        b.duplicate().get(a);
                        out.write(a);
                    }
                    out.flush();
                    done.completed();
                } catch (IOException e) { done.failed(e); }
            }
            @Override public void close() { try { socket.close(); } catch (IOException ignored) { /* gone */ } }
        };
        PipeIo.Exporter exporter = (label, context, length) -> {
            try {
                return ((ExtendedSSLSession) socket.getSession()).exportKeyingMaterialData(label, context == null ? new byte[0] : context, length);
            } catch (IOException | RuntimeException e) {
                return null;
            }
        };
        return PipeIo.of(receive ? reader : null, send ? writer : null).withExporter(exporter).withSecure(true);
    }

    // ---- secure carrier ----

    private static final StaticKey NODE;
    private static final HandlePsk KEYS;

    static {
        try {
            byte[] scalar = new byte[32];
            Arrays.fill(scalar, (byte) 0x5a);
            NODE = StaticKey.fromScalar(scalar);
            KEYS = HandlePsk.derive(PSK, "interop".getBytes(StandardCharsets.US_ASCII), null);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** The secure carrier gives the session its secret: no pipe authentication. */
    private static ZmuxConfig sealConfig(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).datagrams(true).incomingUniStreams(16).build();
    }

    private static void sealServer(EventLoops loops, String profiles, String addr) throws Exception {
        Gate gate = new Gate() {
            @Override public byte[] resolve(byte[] handle, SocketAddress src) { return Arrays.equals(handle, KEYS.handle()) ? KEYS.psk() : null; }
            @Override public io.zmux.seal.Credential credential(byte[] handle, Profile profile, SocketAddress src) { return null; }
            @Override public Object commit(io.zmux.seal.CommitRequest a, SocketAddress src) { return "committed"; }
        };
        Profile[] accepted = Arrays.stream(profiles.split(",")).map(p -> Profile.of(Long.parseLong(p))).toArray(Profile[]::new);
        var server = ServerConfig.builder(gate, NODE).acceptedProfiles(accepted).build();
        var lc = ListenerConfig.builder().session(sealConfig(loops)).inboundTransform(Seal.server(server)).build();
        try (var listener = Zmux.listen(address(addr), lc)) {
            var bound = listener.localAddress();
            System.out.println("LISTEN " + bound.getHostString() + ":" + bound.getPort());
            ZmuxSession session = listener.accept(Duration.ofSeconds(20));
            if (session == null) throw new IOException("no session");
            InteropPeer.serve(session, CarrierInteropPeer::printKeys);
        }
    }

    private static void sealClient(EventLoops loops, String addr, long value) throws Exception {
        Profile profile = Profile.of(value);
        var cfg = ClientConfig.builder().anchor(new PinAnchor(KeyId.of(KeyAlg.X25519, NODE.publicKey()))).allowedProfiles(profile);
        if (profile.isPskBased()) cfg.psk(KEYS.handle(), KEYS.psk());
        SocketChannel channel = SocketChannel.open(address(addr));
        PipeIo carrier = io.zmux.carrier.PipeChannel.pipe(channel, channel, io.zmux.carrier.Framing.BYTE_STREAM, 1 << 16);
        PipeIo established = Seal.establish(carrier, cfg.build()).toCompletableFuture().get(10, TimeUnit.SECONDS);
        System.out.println("PROFILE " + Seal.peerOf(established).orElseThrow().profile().value());
        InteropPeer.serve(Zmux.open(established, Role.CLIENT, sealConfig(loops)), CarrierInteropPeer::printKeys);
    }
}
