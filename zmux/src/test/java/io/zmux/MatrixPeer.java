package io.zmux;

import io.zmux.carrier.Framing;
import io.zmux.carrier.PipeChannel;
import io.zmux.carrier.PipeIo;
import io.zmux.seal.AppData;
import io.zmux.seal.AppDataSource;
import io.zmux.seal.ClientConfig;
import io.zmux.seal.Gate;
import io.zmux.seal.HandlePsk;
import io.zmux.seal.KeyAlg;
import io.zmux.seal.KeyId;
import io.zmux.seal.Peer;
import io.zmux.seal.PinAnchor;
import io.zmux.seal.Profile;
import io.zmux.seal.Seal;
import io.zmux.seal.ServerConfig;
import io.zmux.seal.SigningKey;
import io.zmux.seal.StaticKey;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * The scripted peer of the secure-carrier matrix ({@code zmux-spec/interop/seal-matrix/matrix.py}, whose
 * docstring is the contract): {@code matrix-server <scenario>} and
 * {@code matrix-client <scenario> <addr> [<tls-addr>] [<proof-key-id-hex>]}. Run it with
 * {@code interop/matrix-peer.sh}, or from the zmux module directory (where the test certificate is kept, under
 * {@code target/tls}) with {@code java -cp target/classes:target/test-classes io.zmux.MatrixPeer ...} on JDK 25.
 */
public final class MatrixPeer {
    private static final Duration WAIT = Duration.ofSeconds(25);
    private static final int A_LEN = 1 << 20, B_LEN = 64 << 10;
    private static final AllowRecordMode ANY_MODE = (c, m, p) -> true;

    private static final StaticKey SERVER_KEY = key(0x5a);
    private static final StaticKey CLIENT_KEY = key(0x6b);
    private static final HandlePsk HP;

    static {
        try {
            HP = HandlePsk.derive("zmux interop pre-shared key 0123".getBytes(StandardCharsets.UTF_8),
                    "interop".getBytes(StandardCharsets.UTF_8), new byte[0]);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private MatrixPeer() {}

    public static void main(String[] args) {
        int rc;
        try {
            if (args.length < 2) throw new IllegalArgumentException("usage: matrix-server <scenario> | matrix-client <scenario> <addr> ...");
            rc = switch (args[0]) {
                case "matrix-server" -> server(args[1]);
                case "matrix-client" -> client(args[1], Arrays.copyOfRange(args, 2, args.length));
                default -> throw new IllegalArgumentException("unknown role " + args[0]);
            };
        } catch (Throwable e) {
            e.printStackTrace();
            String why = String.valueOf(e).replace('\n', ' ');
            System.out.println("FAIL " + why);
            rc = 1;
        }
        System.out.flush();
        Runtime.getRuntime().halt(rc);
    }

    // --- server ---

    private static int server(String sc) throws Exception {
        Scenario s = Scenario.of(sc);
        ZmuxConfig cfg = sessionConfig(s);
        Gate gate = new Gate() {
            @Override public byte[] resolve(byte[] handle, SocketAddress src) { return Arrays.equals(handle, HP.handle()) ? HP.psk() : null; }
            @Override public io.zmux.seal.Credential credential(byte[] handle, Profile profile, SocketAddress src) { return null; }
            @Override public Object commit(io.zmux.seal.CommitRequest a, SocketAddress src) { return "matrix"; }
        };
        var sc0 = ServerConfig.builder(gate, SERVER_KEY).acceptedProfiles(Profile.XXHFS_PSK0_CHACHA, Profile.XXHFS_PSK0_AES, Profile.XXHFS_CHACHA, Profile.XXHFS_AES).allowRecordMode(s.multipath ? ANY_MODE : null).build();
        if (s == Scenario.APPDATA) {
            sc0 = sc0.toBuilder().anchor(new PinAnchor(KeyId.of(KeyAlg.X25519, CLIENT_KEY.publicKey())))
                    .appData(only(2, "srv-msg2")).build();
        }
        if (s == Scenario.PROOF) {
            SigningKey proof = SigningKey.generate(KeyAlg.ML_DSA_44);
            sc0 = sc0.toBuilder().signer(proof).build();
            System.out.println("PROOFKEY " + HexFormat.of().formatHex(proof.keyId()));
        }
        var lb = ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(sc0));
        InetSocketAddress loopback = new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), 0);
        if (s != Scenario.JOIN) {
            try (ZmuxListener l = Zmux.listen(loopback, lb.build())) {
                System.out.println("LISTEN " + hostPort(l.localAddress()));
                System.out.flush();
                return serve(s, l);
            }
        }
        // join: the listener's own socket is TLS (Bound joins); the plain TCP port hands its carriers to it.
        try (ZmuxListener l = Zmux.listen(loopback, lb.tls(TlsOptions.server(TestTls.server()).withSecure()).build());
             ServerSocketChannel tcp = ServerSocketChannel.open().bind(loopback)) {
            Thread.ofVirtual().start(() -> {
                while (true) {
                    try {
                        SocketChannel ch = tcp.accept();
                        var peer = (InetSocketAddress) ch.getRemoteAddress();
                        l.adapter().handle(PipeChannel.pipe(ch, ch, Framing.BYTE_STREAM, 1 << 16), JoinHint.inBand(peer));
                    } catch (IOException closed) {
                        return;
                    }
                }
            });
            System.out.println("LISTEN " + hostPort((InetSocketAddress) tcp.getLocalAddress()));
            System.out.println("LISTENTLS " + hostPort(l.localAddress()));
            System.out.flush();
            return serve(s, l);
        }
    }

    private static int serve(Scenario s, ZmuxListener l) throws Exception {
        ZmuxSession session = l.accept(WAIT);
        if (session == null) throw new IOException("no session accepted");
        Peer peer = (Peer) session.admitted().orElseThrow();
        if (s == Scenario.APPDATA) printAppData(peer);
        if (s == Scenario.CLOSE) {
            ZmuxStream st = session.acceptStream(WAIT);
            st.inputStream().readNBytes(B_LEN);
            session.abort(85, "matrix-close");
            try { session.closed().get(3, TimeUnit.SECONDS); } catch (Exception ended) { /* the abort hands over in the background */ }
            System.out.println("CLOSING");
            return 0;
        }
        try (ZmuxStream a = session.acceptStream(WAIT)) {
            a.write(a.inputStream().readAllBytes());
            a.finish();
        }
        try (ZmuxStream b = session.openStream()) {
            b.write(pattern(B_LEN));
            b.finish();
            check("B", b.inputStream().readAllBytes(), B_LEN);
        }
        if (s == Scenario.EPOCH) System.out.println("EPOCH " + session.epoch());
        if (s == Scenario.CLOSE_IDLE) {
            session.abort(85, "matrix-close");
            try { session.closed().get(3, TimeUnit.SECONDS); } catch (Exception ended) { /* the abort hands over in the background */ }
            System.out.println("CLOSING");
            return 0;
        }
        var end = session.closed().get(WAIT.toMillis(), TimeUnit.MILLISECONDS);
        if (end instanceof SessionError.ApplicationClosed c && c.code() == 0) {
            System.out.println("OK");
            return 0;
        }
        System.out.println("FAIL session ended with " + end);
        return 1;
    }

    // --- client ---

    private static int client(String sc, String[] rest) throws Exception {
        Scenario s = Scenario.of(sc);
        InetSocketAddress at = address(rest[0]);
        var cc = ClientConfig.builder();
        byte[] node = KeyId.of(KeyAlg.X25519, SERVER_KEY.publicKey());
        cc.anchor(new PinAnchor(node)).allowedProfiles(s.profile);
        if (s.profile.isPskBased()) cc.psk(HP.handle(), HP.psk());
        if (s == Scenario.APPDATA) cc.key(CLIENT_KEY).appData(only(3, "cli-msg3"));
        if (s == Scenario.PROOF) cc.anchor(new PinAnchor(node, HexFormat.of().parseHex(rest[1]))).requirePqAuth(true);
        PipeIo p = Seal.establish(tcp(at), cc.build())
                .toCompletableFuture().get(WAIT.toMillis(), TimeUnit.MILLISECONDS);
        Peer peer = Seal.peerOf(p).orElseThrow();
        if (s == Scenario.APPDATA) printAppData(peer);
        if (s == Scenario.PROOF) {
            System.out.println("PQ confidential=" + peer.pqConfidential() + " authenticated=" + peer.pqAuthenticated());
            if (!peer.pqAuthenticated()) throw new IllegalStateException("server not PQ authenticated");
        }
        try (ZmuxSession session = Zmux.open(p, Role.CLIENT, sessionConfig(s))) {
            if (s == Scenario.JOIN) {
                var tls = TlsOptions.client(trustAll(), address(rest[1]).getHostString()).withSecure();
                session.addTlsPipe(address(rest[1]), tls, PipeOptions.builder().recordMode(RecordMode.BOUND).build());
                int n = session.pipes().size();
                System.out.println("PIPES " + n);
                if (n < 2) throw new IllegalStateException("pipes " + n);
            }
            if (s == Scenario.EPOCH) {
                session.advanceEpoch();
                long deadline = System.nanoTime() + WAIT.toNanos();
                while (!(session.nextEvent(Duration.ofNanos(Math.max(1, deadline - System.nanoTime()))) instanceof SessionEvent.EpochCommitted)) {
                    if (System.nanoTime() - deadline >= 0) throw new IOException("no EpochCommitted");
                }
                session.addPipe(at, PipeOptions.builder().recordMode(RecordMode.SEALED).build());
            }
            if (s == Scenario.CLOSE) {
                ZmuxStream st = session.openStream();
                byte[] chunk = pattern(16 << 10);
                try {
                    while (true) st.write(chunk);
                } catch (IOException expected) { /* The session ended. */ }
                return reportClose(session);
            }
            try (ZmuxStream a = session.openStream()) {
                a.write(pattern(A_LEN));
                a.finish();
                check("A", a.inputStream().readAllBytes(), A_LEN);
            }
            try (ZmuxStream b = session.acceptStream(WAIT)) {
                b.write(b.inputStream().readAllBytes());
                b.finish();
            }
            if (s == Scenario.CLOSE_IDLE) return reportClose(session);
            if (s == Scenario.EPOCH) System.out.println("EPOCH " + session.epoch());
            session.shutdown(0, "", Duration.ofSeconds(10)); // Code 0, after the echo of B is delivered.
            System.out.println("OK");
            return 0;
        }
    }

    private static int reportClose(ZmuxSession session) throws Exception {
        SessionError end;
        try {
            end = session.closed().get(WAIT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            System.out.println("CLOSE lost timeout");
            return 3;
        }
        if (end instanceof SessionError.ApplicationClosed c && c.remote()) {
            System.out.println("CLOSE code=" + c.code() + " reason=" + c.reason());
            if (c.code() == 85 && c.reason().equals("matrix-close")) {
                System.out.println("OK");
                return 0;
            }
            return 1;
        }
        System.out.println("CLOSE lost " + String.valueOf(end).replace('\n', ' '));
        return 3;
    }

    // --- helpers ---

    private enum Scenario {
        PSK(Profile.XXHFS_PSK0_CHACHA, false), XX(Profile.XXHFS_CHACHA, false), JOIN(Profile.XXHFS_PSK0_CHACHA, true),
        EPOCH(Profile.XXHFS_PSK0_CHACHA, true), APPDATA(Profile.XXHFS_CHACHA, false), PROOF(Profile.XXHFS_CHACHA, false),
        CLOSE(Profile.XXHFS_PSK0_CHACHA, false), CLOSE_IDLE(Profile.XXHFS_PSK0_CHACHA, false);

        final Profile profile;
        final boolean multipath;

        Scenario(Profile profile, boolean multipath) { this.profile = profile; this.multipath = multipath; }

        static Scenario of(String name) { return valueOf(name.toUpperCase(java.util.Locale.ROOT).replace('-', '_')); }
    }

    private static ZmuxConfig sessionConfig(Scenario s) {
        var b = ZmuxConfig.builder();
        if (s.multipath) b.maxPipes(4).allowRecordMode(ANY_MODE);
        return b.build();
    }

    private static AppDataSource only(int message, String data) {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        return (msg, known) -> CompletableFuture.completedFuture(msg == message ? Optional.of(bytes) : Optional.empty());
    }

    private static void printAppData(Peer peer) {
        for (AppData d : peer.appData()) {
            System.out.println("APPDATA " + d.message() + " " + new String(d.data(), StandardCharsets.UTF_8)
                    + " fs=" + d.forwardSecret() + " auth=" + d.authenticated());
        }
    }

    private static PipeIo tcp(InetSocketAddress at) throws IOException {
        SocketChannel ch = SocketChannel.open(at);
        return PipeChannel.pipe(ch, ch, Framing.BYTE_STREAM, 1 << 16);
    }

    private static byte[] pattern(int n) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) b[i] = (byte) (i * 31 + 7);
        return b;
    }

    private static void check(String name, byte[] got, int n) {
        if (!Arrays.equals(got, pattern(n))) throw new IllegalStateException("stream " + name + ": " + got.length + " bytes, not the pattern of " + n);
    }

    private static StaticKey key(int fill) {
        byte[] s = new byte[32];
        Arrays.fill(s, (byte) fill);
        try {
            return StaticKey.fromScalar(s);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static InetSocketAddress address(String hostPort) {
        int i = hostPort.lastIndexOf(':');
        String host = hostPort.substring(0, i);
        if (host.startsWith("[") && host.endsWith("]")) host = host.substring(1, host.length() - 1);
        return new InetSocketAddress(host, Integer.parseInt(hostPort.substring(i + 1)));
    }

    private static String hostPort(InetSocketAddress a) {
        String h = a.getAddress().getHostAddress();
        return (h.indexOf(':') >= 0 ? "[" + h + "]" : h) + ":" + a.getPort();
    }

    /** A TLS 1.3 client context that checks nothing: the matrix server's certificate is self-signed. */
    private static SSLContext trustAll() throws Exception {
        var tm = new X509ExtendedTrustManager() {
            @Override public void checkClientTrusted(X509Certificate[] c, String a, Socket s) {}
            @Override public void checkServerTrusted(X509Certificate[] c, String a, Socket s) {}
            @Override public void checkClientTrusted(X509Certificate[] c, String a, SSLEngine e) {}
            @Override public void checkServerTrusted(X509Certificate[] c, String a, SSLEngine e) {}
            @Override public void checkClientTrusted(X509Certificate[] c, String a) {}
            @Override public void checkServerTrusted(X509Certificate[] c, String a) {}
            @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        };
        SSLContext ctx = SSLContext.getInstance("TLSv1.3");
        ctx.init(null, new javax.net.ssl.TrustManager[] {tm}, null);
        return ctx;
    }
}
