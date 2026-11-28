package io.zmux.http;

import io.zmux.CapacityLimits;
import io.zmux.SessionPolicy;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.zmux.EventLoops;
import io.zmux.ListenerConfig;
import io.zmux.PipeAuth;
import io.zmux.PipeOptions;
import io.zmux.RecordMode;
import io.zmux.Role;
import io.zmux.Zmux;
import io.zmux.ZmuxConfig;
import io.zmux.ZmuxSession;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The WebSocket modes of the scripted test peer, over TLS 1.3 ({@code wss://<addr>/zmux}, ALPN {@code http/1.1}):
 * {@code wss-server} and {@code wss-client addr}. The bootstrap carrier and a Bound join are each a WebSocket whose
 * exporter is its connection's, and the session's join secret is that exporter alone. Both print
 * {@code KEYS <hex>}, the exporter value of label {@code EXPORTER-interop-app} (length 16), before the script's
 * {@code ESTABLISHED}. The contract is that of zmux-rs/interop/peer.
 */
public final class WssInteropPeer {
    private WssInteropPeer() {}

    public static void run(String[] args, Consumer<ZmuxSession> script) throws Exception {
        try (var loops = EventLoops.create(2); var group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory())) {
            var config = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).maxIncomingUniStreams(16).sessionPolicy(new SessionPolicy(new CapacityLimits(4)))
                    .pipeAuth(PipeAuth.exporter().withEndToEnd()).allowRecordMode((carrier, mode, peer) -> true).build();
            switch (args[0]) {
                case "wss-server" -> server(config, group, script);
                case "wss-client" -> client(args[1], config, group, script);
                default -> throw new IllegalArgumentException("unknown mode");
            }
        }
    }

    private static void server(ZmuxConfig config, MultiThreadIoEventLoopGroup group, Consumer<ZmuxSession> script) throws Exception {
        try (var listener = Zmux.listener(ListenerConfig.builder().session(config)
                     .onReject(info -> System.err.println("rejected: " + info)).build());
             var endpoint = HttpEndpoint.builder(listener).webSocket().build()) {
            var ssl = HttpEndpoint.serverSsl(SslContextBuilder.forServer(Fixture.kmf()).sslProvider(SslProvider.JDK).protocols("TLSv1.3"));
            var server = new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
                    .childHandler(endpoint.initializer(ssl, null)).bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
            try {
                System.out.println("LISTEN 127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort());
                ZmuxSession session = listener.accept(Duration.ofSeconds(20));
                if (session == null) throw new IOException("no session");
                printKeys(session);
                script.accept(session);
            } finally {
                server.close().sync();
            }
        }
    }

    private static void client(String addr, ZmuxConfig config, MultiThreadIoEventLoopGroup group, Consumer<ZmuxSession> script) throws Exception {
        var tls = SslContextBuilder.forClient().sslProvider(SslProvider.JDK).protocols("TLSv1.3").trustManager(InsecureTrustManagerFactory.INSTANCE);
        try (var http = HttpCarrierClient.builder().group(group).tls(tls).timeout(Duration.ofSeconds(10)).build()) {
            URI url = URI.create("wss://" + addr + "/zmux");
            ZmuxSession session = Zmux.open(http.webSocketPipe(url, Map.of(), List.of(), null), Role.CLIENT, config);
            printKeys(session);
            session.addPipe(http.webSocketPipe(url, Map.of(), List.of(), null), PipeOptions.builder().recordMode(RecordMode.BOUND).build());
            script.accept(session);
            // The close frames go out before the event loops stop.
            Thread.sleep(300);
        }
    }

    private static void printKeys(ZmuxSession session) throws IOException {
        System.out.println("KEYS " + HexFormat.of().formatHex(session.exportKeyingMaterial("EXPORTER-interop-app", new byte[0], 16)));
    }
}
