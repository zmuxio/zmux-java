package io.zmux.quic;

import io.zmux.ZmuxListener;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.zmux.EarlyData;
import io.zmux.EventLoops;
import io.zmux.ListenerConfig;
import io.zmux.PipeAuth;
import io.zmux.Preamble;
import io.zmux.Zmux;
import io.zmux.ZmuxConfig;
import io.zmux.ZmuxSession;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * The QUIC modes of the scripted test peer: {@code quic-server [n]} serves n sessions, {@code
 * quic-client address} a session, then a second one whose first stream has OPEN_INFO {@code
 * early}. The adapter offers no 0-RTT, so that stream goes after the handshake and the
 * {@code EARLY} line says {@code sent=false}.
 */
public final class QuicInteropPeer {
    private static final byte[] PSK = "zmux interop pre-shared key 0123".getBytes(StandardCharsets.US_ASCII);
    private static final QuicOptions OPTIONS = new QuicOptions(2 << 20, 4, 16, Duration.ofSeconds(30));

    private QuicInteropPeer() {}

    public static void run(String[] args, Consumer<ZmuxSession> script) throws Exception {
        // A last argument "exporter": the join secret from the exporter alone, and KEYS <hex> for each session.
        boolean exporter = args[args.length - 1].equals("exporter");
        try (var loops = EventLoops.create(2)) {
            var builder = ZmuxConfig.builder().eventLoops(loops).datagrams(true).earlyData(EarlyData.ZMUX_PEER).maxPipes(4);
            if (exporter) builder.pipeAuth(PipeAuth.exporter().withEndToEnd()).allowRecordMode((carrier, mode, peer) -> true);
            else builder.pipeAuth(PipeAuth.psk(PSK).withEndToEnd());
            var config = builder.build();
            Consumer<ZmuxSession> serve = exporter ? session -> { printKeysWhenReady(session); script.accept(session); } : script;
            switch (args[0]) {
                case "quic-server" -> server(args.length > 1 && !args[1].equals("exporter") ? Integer.parseInt(args[1]) : 1, config, serve);
                case "quic-client" -> client(args[1], config, serve);
                default -> throw new IllegalArgumentException("unknown mode");
            }
        }
    }

    private static void server(int sessions, ZmuxConfig config, Consumer<ZmuxSession> script) throws Exception {
        TestTls.init();
        try (var listener = ZmuxListener.of(ListenerConfig.of(config));
             var server = new QuicServer(new InetSocketAddress("127.0.0.1", 0), TestTls.server, OPTIONS, listener)) {
            System.out.println("LISTEN 127.0.0.1:" + server.address().getPort());
            for (int i = 0; i < sessions; i++) {
                ZmuxSession session = listener.accept(Duration.ofSeconds(30));
                if (session == null) throw new IOException("no session");
                script.accept(session);
            }
            linger();
        }
    }

    private static void client(String address, ZmuxConfig config, Consumer<ZmuxSession> script) throws Exception {
        int split = address.lastIndexOf(':');
        var remote = new InetSocketAddress(address.substring(0, split), Integer.parseInt(address.substring(split + 1)));
        var tls = QuicSslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).applicationProtocols("qmux-02");
        try (var client = new QuicClient(tls, OPTIONS)) {
            script.accept(client.connectAsync(remote, "localhost", config).get(10, TimeUnit.SECONDS));
            ZmuxSession session = client.connectAsync(remote, "localhost", config).get(10, TimeUnit.SECONDS);
            try (var stream = session.openStream(Preamble.openInfo("early".getBytes(StandardCharsets.UTF_8)))) {
                stream.write("early-data".getBytes(StandardCharsets.UTF_8));
                stream.finish();
                var reply = new ByteArrayOutputStream();
                var bytes = ByteBuffer.allocate(4096);
                for (int n; (n = stream.read(bytes.clear())) >= 0;) reply.write(bytes.array(), 0, n);
                System.out.println("EARLY sent=false accepted=false reply=" + reply.toString(StandardCharsets.UTF_8));
            }
            script.accept(session);
            linger();
        }
    }

    /** Prints the session's exporter value once the carrier's handshake gives it (a session on early data starts before). */
    private static void printKeysWhenReady(ZmuxSession session) {
        Thread.ofVirtual().start(() -> {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() - end < 0) {
                try {
                    System.out.println("KEYS " + java.util.HexFormat.of().formatHex(session.exportKeyingMaterial("EXPORTER-interop-app", new byte[0], 16)));
                    return;
                } catch (IOException notYet) {
                    try { Thread.sleep(5); } catch (InterruptedException e) { return; }
                }
            }
        });
    }

    /** Closing the endpoint at once would overtake the last session's close. */
    private static void linger() throws InterruptedException { Thread.sleep(300); }
}
