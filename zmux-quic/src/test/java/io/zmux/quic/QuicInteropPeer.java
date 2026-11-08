package io.zmux.quic;

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
        try (var loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).earlyData(EarlyData.ZMUX_PEER)
                    .maxPipes(4).pipeAuth(PipeAuth.psk(PSK).withEndToEnd()).build();
            switch (args[0]) {
                case "quic-server" -> server(args.length > 1 ? Integer.parseInt(args[1]) : 1, config, script);
                case "quic-client" -> client(args[1], config, script);
                default -> throw new IllegalArgumentException("unknown mode");
            }
        }
    }

    private static void server(int sessions, ZmuxConfig config, Consumer<ZmuxSession> script) throws Exception {
        TestTls.init();
        try (var listener = Zmux.listener(ListenerConfig.of(config));
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
            script.accept(client.connect(remote, "localhost", config).toCompletableFuture().get(10, TimeUnit.SECONDS));
            ZmuxSession session = client.connect(remote, "localhost", config).toCompletableFuture().get(10, TimeUnit.SECONDS);
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

    /** Closing the endpoint at once would overtake the last session's close. */
    private static void linger() throws InterruptedException { Thread.sleep(300); }
}
