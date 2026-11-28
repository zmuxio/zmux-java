package io.zmux.http;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.zmux.http.internal.HttpTransport;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The client's HTTP connections: a channel for one request at a time, with an HTTP object codec
 * in its pipeline: an HTTP/1.1 connection (fresh, or idle from a pool for bounded requests) or a
 * stream of a shared HTTP/2 or HTTP/3 connection. Safe for concurrent use.
 */
final class Connections {
    /** A request-only operation that can be cancelled without affecting a shared connection. */
    static final class RequestOpening extends CompletableFuture<Channel> {}

    /** The HTTP versions a client uses. */
    enum Version { HTTP_1_1, HTTP_2, HTTP_3, AUTO }

    private final EventLoopGroup group;
    private final SslContext ssl, ssl1;
    private final Version version;
    private final Duration connectTimeout;
    final HttpTransport.Client h3;
    /** Shared HTTP/2 or HTTP/3 connections by origin, according to the client version. */
    private final Map<String, CompletableFuture<Channel>> shared = new HashMap<>();
    /** Idle HTTP/1.1 connections by origin. */
    private final Map<String, ArrayDeque<Channel>> idle = new HashMap<>();
    /** Every connection opened and not yet closed, those still connecting too: {@link #close} closes them all. */
    private final ChannelGroup channels = new DefaultChannelGroup("zmux-http-client", GlobalEventExecutor.INSTANCE);
    /** No connection opens or goes back to the pool any more; set under this. */
    private volatile boolean closed;

    /** ssl offers h2 and http/1.1, ssl1 http/1.1 only. */
    Connections(EventLoopGroup group, SslContext ssl, SslContext ssl1, Version version, Duration connectTimeout) {
        this(group, ssl, ssl1, version, connectTimeout, null);
    }

    Connections(EventLoopGroup group, SslContext ssl, SslContext ssl1, Version version, Duration connectTimeout,
                HttpTransport.Client h3) {
        this.h3 = h3;
        this.group = group;
        this.ssl = ssl;
        this.ssl1 = ssl1;
        this.version = version;
        this.connectTimeout = connectTimeout;
    }

    boolean http3() { return version == Version.HTTP_3; }
    boolean onBorrowedLoop() { return http3() && h3.onBorrowedLoop(); }

    void requireRequestScheme(URI u) {
        if (http3() && (!"https".equalsIgnoreCase(u.getScheme()) || u.getHost() == null)) {
            throw new IllegalArgumentException("HTTP/3 requires an HTTPS URL");
        }
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
        synchronized (this) {
            if (closed) return CompletableFuture.failedFuture(HttpCarrierClient.closedFailure());
        }
        String o = origin(u);
        boolean wantH2 = !http1 && (version == Version.HTTP_2 || (version == Version.AUTO && secure(u)));
        boolean wantH3 = !http1 && http3();
        if (http1 && http3() && h3.borrowsConnection()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("the client is bound to an HTTP/3 connection"));
        }
        if (wantH3) {
            try {
                requireRequestScheme(u);
            } catch (IllegalArgumentException failure) {
                return CompletableFuture.failedFuture(failure);
            }
            var opened = new RequestOpening();
            openH3(u, o, opened, !h3.borrowsConnection());
            return opened;
        }
        if (wantH2) {
            var conn = shared(u, o, false);
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

    // The HTTP/3 transport comes from the zmux-http3 module, outside this module's analysis.
    @SuppressWarnings("ConstantValue")
    private void openH3(URI u, String origin, CompletableFuture<Channel> opened, boolean mayRetry) {
        shared(u, origin, true).whenComplete((connection, failure) -> {
            if (opened.isDone()) return;
            if (failure != null) { opened.completeExceptionally(failure); return; }
            // Leave part of the caller's opening budget for one replacement after credit expires.
            var request = h3.stream(connection, h3.borrowsConnection() ? connectTimeout : connectTimeout.dividedBy(2));
            opened.whenComplete((channel, error) -> { if (opened.isCancelled()) request.cancel(false); });
            request.whenComplete((channel, error) -> {
                if (error instanceof HttpTransport.RetryOpen && mayRetry && !opened.isDone()) {
                    openH3(u, origin, opened, false);
                } else if (error != null) {
                    opened.completeExceptionally(error);
                } else {
                    if (h3.borrowsConnection()) track(channel);
                    if (!opened.complete(channel)) HttpTransport.abort(channel);
                }
            });
        });
    }

    private synchronized CompletableFuture<Channel> shared(URI u, String origin, boolean h3) {
        if (closed) return CompletableFuture.failedFuture(HttpCarrierClient.closedFailure());
        var conn = shared.get(origin);
        if (conn == null || conn.isCompletedExceptionally() || (conn.isDone() && !reusable(conn.join(), h3))) {
            try {
                conn = h3 ? this.h3.connect(group, u, connectTimeout, this::track) : connect(u, false);
            } catch (RuntimeException failure) {
                return CompletableFuture.failedFuture(failure);
            }
            shared.put(origin, conn);
        }
        return conn;
    }

    /** Gives an HTTP/1.1 connection back after a complete exchange; one that cannot be reused is closed. */
    void release(URI u, Channel c, boolean reusable) {
        if (Requests.multiplexed(c)) { c.close(); return; }
        if (!reusable || !c.isActive()) { c.close(); return; }
        synchronized (this) {
            if (!closed) {
                idle.computeIfAbsent(origin(u), k -> new ArrayDeque<>()).addLast(c);
                return;
            }
        }
        c.close();
    }

    private boolean reusable(Channel c, boolean h3) {
        return h3 ? this.h3.reusable(c) : c.isActive();
    }

    private static CompletableFuture<Channel> stream(Channel conn) {
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
                                    else ctx.pipeline().addLast(new HttpClientCodec(), TransportErrors.INSTANCE);
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
                            ch.pipeline().addLast(new HttpClientCodec(), TransportErrors.INSTANCE);
                        }
                    }
                });
        ChannelFuture cf = b.connect(new InetSocketAddress(u.getHost(), port(u)));
        track(cf.channel());
        cf.addListener(r -> {
            if (!r.isSuccess()) f.completeExceptionally(r.cause());
            else if (!tls) f.complete(cf.channel());
        });
        return f;
    }

    private void track(Channel channel) {
        // Adding before reading closed means a racing close is handled by one of the two.
        channels.add(channel);
        if (closed) channel.close();
    }

    private static void h2(Channel ch) {
        ch.pipeline().addLast(Http2FrameCodecBuilder.forClient().build());
        ch.pipeline().addLast(new Http2MultiplexHandler(new ChannelInboundHandlerAdapter()), TransportErrors.INSTANCE);
    }

    /**
     * Closes every connection: pooled, shared, in use (a carrier's, an attempt's) and still
     * connecting. Complete once they are closed.
     */
    ChannelGroupFuture close() {
        List<CompletableFuture<Channel>> quic;
        synchronized (this) {
            closed = true;
            idle.clear();
            quic = http3() ? List.copyOf(shared.values()) : List.of();
            shared.clear();
        }
        for (var opening : quic) opening.completeExceptionally(HttpCarrierClient.closedFailure());
        return channels.close();
    }

    /** The connections open or connecting, for tests. */
    int open() { return channels.size(); }
}
