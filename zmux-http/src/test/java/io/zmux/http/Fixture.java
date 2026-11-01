package io.zmux.http;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.zmux.EventLoops;
import io.zmux.ListenerConfig;
import io.zmux.Zmux;
import io.zmux.ZmuxConfig;
import io.zmux.ZmuxListener;
import io.zmux.ZmuxSession;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import static org.junit.jupiter.api.Assertions.*;

/** A listener behind an {@link HttpEndpoint} on a Netty server, and a client. */
final class Fixture implements AutoCloseable {
    final EventLoops loops = EventLoops.create(2);
    final EventLoopGroup group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    final ZmuxConfig config;
    final ZmuxListener listener;
    final HttpEndpoint endpoint;
    final Channel server;
    final URI base;

    Fixture(Function<ZmuxConfig.Builder, ZmuxConfig.Builder> cfg, Function<ListenerConfig.Builder, ListenerConfig.Builder> lcfg,
            Function<HttpEndpoint.Builder, HttpEndpoint.Builder> ep, boolean tls, Supplier<ChannelHandler> host) throws Exception {
        config = cfg.apply(ZmuxConfig.builder().eventLoops(loops)).build();
        listener = Zmux.listener(lcfg.apply(ListenerConfig.builder().session(config)).build());
        endpoint = ep.apply(HttpEndpoint.builder(listener)).build();
        SslContext ssl = tls ? HttpEndpoint.serverSsl(SslContextBuilder.forServer(kmf()).sslProvider(SslProvider.JDK)) : null;
        server = new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
                .childHandler(endpoint.initializer(ssl, host)).bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
        int port = ((InetSocketAddress) server.localAddress()).getPort();
        base = URI.create((tls ? "https" : "http") + "://localhost:" + port + "/z");
    }

    static Fixture plain(Function<HttpEndpoint.Builder, HttpEndpoint.Builder> ep) throws Exception {
        return new Fixture(b -> b, b -> b, ep, false, null);
    }

    HttpCarrierClient client(Function<HttpCarrierClient.Builder, HttpCarrierClient.Builder> b) throws Exception {
        var x = HttpCarrierClient.builder().group(group).timeout(Duration.ofSeconds(5));
        if (base.getScheme().equals("https")) {
            x.tls(SslContextBuilder.forClient().sslProvider(SslProvider.JDK).trustManager(tmf()));
        }
        return b.apply(x).build();
    }

    /** One stream each way between client and the session the listener accepted. */
    void echo(ZmuxSession client) throws Exception {
        try (ZmuxSession s = listener.accept(Duration.ofSeconds(5))) {
            assertNotNull(s);
            exchange(client, s, 200_000);
            exchange(s, client, 300_000);
        }
    }

    static void exchange(ZmuxSession from, ZmuxSession to, int n) throws Exception {
        byte[] data = new byte[n];
        new java.util.Random(n).nextBytes(data);
        try (var ex = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var st = from.openStream();
            var w = ex.submit(() -> { st.write(data); st.finish(); return null; });
            try (var in = to.acceptStream(Duration.ofSeconds(10))) {
                assertNotNull(in);
                assertArrayEquals(data, in.readAll(1 << 24));
            }
            w.get(10, TimeUnit.SECONDS);
            st.close();
        }
    }

    @Override public void close() throws Exception {
        endpoint.close();
        listener.close();
        server.close().sync();
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
        loops.close();
    }

    private static final char[] PASS = "changeit".toCharArray();
    private static KeyStore store;

    static synchronized KeyStore store() throws Exception {
        if (store != null) return store;
        Path dir = Path.of("target", "tls");
        Files.createDirectories(dir);
        Path ks = dir.resolve("localhost.p12");
        if (!Files.exists(ks)) {
            Path keytool = Path.of(System.getProperty("java.home"), "bin", "keytool");
            Process p = new ProcessBuilder(keytool.toString(), "-genkeypair", "-alias", "localhost", "-keyalg", "EC",
                    "-groupname", "secp256r1", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1",
                    "-validity", "3650", "-storetype", "PKCS12", "-keystore", ks.toString(), "-storepass", "changeit",
                    "-keypass", "changeit").redirectErrorStream(true).start();
            if (!p.waitFor(60, TimeUnit.SECONDS) || p.exitValue() != 0) throw new IllegalStateException("keytool failed");
        }
        KeyStore s = KeyStore.getInstance("PKCS12");
        try (FileInputStream in = new FileInputStream(ks.toFile())) { s.load(in, PASS); }
        return store = s;
    }

    static KeyManagerFactory kmf() throws Exception {
        var k = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        k.init(store(), PASS);
        return k;
    }

    static TrustManagerFactory tmf() throws Exception {
        var t = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        t.init(store());
        return t;
    }
}
