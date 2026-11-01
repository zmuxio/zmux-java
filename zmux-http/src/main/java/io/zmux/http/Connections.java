package io.zmux.http;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The client's HTTP connections: a channel for one request at a time, with an HTTP object codec
 * in its pipeline: an HTTP/1.1 connection (fresh, or idle from a pool for bounded requests) or a
 * stream of a shared HTTP/2 connection. Safe for concurrent use.
 */
final class Connections {
    /** The HTTP versions a client uses. */
    enum Version { HTTP_1_1, HTTP_2, AUTO }

    private final EventLoopGroup group;
    private final SslContext ssl, ssl1;
    private final Version version;
    private final Duration connectTimeout;
    /** Shared HTTP/2 connections by origin. */
    private final Map<String, CompletableFuture<Channel>> h2 = new HashMap<>();
    /** Idle HTTP/1.1 connections by origin. */
    private final Map<String, ArrayDeque<Channel>> idle = new HashMap<>();

    /** ssl offers h2 and http/1.1, ssl1 http/1.1 only. */
    Connections(EventLoopGroup group, SslContext ssl, SslContext ssl1, Version version, Duration connectTimeout) {
        this.group = group;
        this.ssl = ssl;
        this.ssl1 = ssl1;
        this.version = version;
        this.connectTimeout = connectTimeout;
    }

    static boolean secure(URI u) {
        String s = u.getScheme();
        return "https".equalsIgnoreCase(s) || "wss".equalsIgnoreCase(s);
    }

    /** The server's address of u, unresolved: a client credential's source. */
    static InetSocketAddress address(URI u) {
        return InetSocketAddress.createUnresolved(u.getHost(), port(u));
    }

    static int port(URI u) {
        return u.getPort() > 0 ? u.getPort() : secure(u) ? 443 : 80;
    }

    private static String origin(URI u) {
        return (secure(u) ? "s:" : "c:") + u.getHost() + ":" + port(u);
    }

    /**
     * A channel for one request to u. pooled: an HTTP/1.1 connection may come from (and go back
     * to, {@link #release}) the pool; otherwise each request has a connection of its own. http1:
     * HTTP/1.1 in any case (a WebSocket upgrade).
     */
    CompletableFuture<Channel> open(URI u, boolean pooled, boolean http1) {
        String o = origin(u);
        boolean wantH2 = !http1 && (version == Version.HTTP_2 || version == Version.AUTO && secure(u));
        if (wantH2) {
            CompletableFuture<Channel> conn;
            synchronized (this) {
                conn = h2.get(o);
                if (conn == null || conn.isCompletedExceptionally() || conn.isDone() && !conn.join().isActive()) {
                    conn = connect(u, false);
                    h2.put(o, conn);
                }
            }
            return conn.thenCompose(c -> c.pipeline().get(Http2MultiplexHandler.class) == null
                    ? CompletableFuture.completedFuture(c) // ALPN chose HTTP/1.1
                    : stream(c));
        }
        if (pooled) {
            synchronized (this) {
                ArrayDeque<Channel> q = idle.get(o);
                while (q != null && !q.isEmpty()) {
                    Channel c = q.pollFirst();
                    if (c.isActive()) return CompletableFuture.completedFuture(c);
                }
            }
        }
        return connect(u, true);
    }

    /** Gives an HTTP/1.1 connection back after a complete exchange; one that cannot be reused is closed. */
    void release(URI u, Channel c, boolean reusable) {
        if (c instanceof Http2StreamChannel) { c.close(); return; }
        if (!reusable || !c.isActive()) { c.close(); return; }
        synchronized (this) {
            idle.computeIfAbsent(origin(u), k -> new ArrayDeque<>()).addLast(c);
        }
    }

    private CompletableFuture<Channel> stream(Channel conn) {
        CompletableFuture<Channel> f = new CompletableFuture<>();
        new Http2StreamChannelBootstrap(conn).handler(new ChannelInitializer<>() {
            @Override protected void initChannel(Channel ch) { ch.pipeline().addLast(new Http2StreamFrameToHttpObjectCodec(false)); }
        }).open().addListener(r -> {
            if (r.isSuccess()) f.complete((Channel) r.getNow());
            else f.completeExceptionally(r.cause());
        });
        return f;
    }

    /** A new connection; http1: HTTP/1.1 only. Complete once its protocol is known. */
    private CompletableFuture<Channel> connect(URI u, boolean http1) {
        CompletableFuture<Channel> f = new CompletableFuture<>();
        boolean tls = secure(u);
        boolean priorKnowledge = !tls && !http1 && version == Version.HTTP_2;
        Bootstrap b = new Bootstrap().group(group).channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) Math.min(Integer.MAX_VALUE, connectTimeout.toMillis()))
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<>() {
                    @Override protected void initChannel(Channel ch) {
                        if (tls) {
                            SslHandler s = (http1 || version == Version.HTTP_1_1 ? ssl1 : ssl).newHandler(ch.alloc(), u.getHost(), port(u));
                            ch.pipeline().addLast(s);
                            ch.pipeline().addLast(new ApplicationProtocolNegotiationHandler(ApplicationProtocolNames.HTTP_1_1) {
                                @Override protected void configurePipeline(ChannelHandlerContext ctx, String protocol) {
                                    if (!http1 && ApplicationProtocolNames.HTTP_2.equals(protocol)) h2(ctx.channel());
                                    else ctx.pipeline().addLast(new HttpClientCodec());
                                    f.complete(ctx.channel());
                                }

                                @Override protected void handshakeFailure(ChannelHandlerContext ctx, Throwable cause) {
                                    f.completeExceptionally(cause);
                                    ctx.close();
                                }
                            });
                        } else if (priorKnowledge) {
                            h2(ch);
                        } else {
                            ch.pipeline().addLast(new HttpClientCodec());
                        }
                    }
                });
        ChannelFuture cf = b.connect(new InetSocketAddress(u.getHost(), port(u)));
        cf.addListener(r -> {
            if (!r.isSuccess()) f.completeExceptionally(r.cause());
            else if (!tls) f.complete(cf.channel());
        });
        return f;
    }

    private static void h2(Channel ch) {
        ch.pipeline().addLast(Http2FrameCodecBuilder.forClient().build());
        ch.pipeline().addLast(new Http2MultiplexHandler(new ChannelInboundHandlerAdapter()));
    }

    /** Closes the pooled and shared connections. */
    synchronized void close() {
        for (var q : idle.values()) for (Channel c : q) c.close();
        idle.clear();
        for (var f : h2.values()) f.thenAccept(Channel::close);
        h2.clear();
    }
}
