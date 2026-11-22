package io.zmux.http3;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.Http3ConnectionHandler;
import io.netty.handler.codec.http3.Http3ErrorCode;
import io.netty.handler.codec.http3.Http3Exception;
import io.netty.handler.codec.http3.Http3ServerConnectionHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicServerCodecBuilder;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.ReferenceCountUtil;
import io.zmux.CarrierFailedException;
import io.zmux.ZmuxFuture;
import io.zmux.http.HttpEndpoint;
import io.zmux.http.internal.HttpTransport;
import io.zmux.http.internal.HttpTransport.Route;
import io.zmux.internal.spi.Loops;
import io.zmux.internal.spi.Spi;
import io.zmux.quic.QuicExporter;
import io.zmux.quic.QuicOptions;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Serves an {@link HttpEndpoint} on HTTP/3. Requests are routed independently on their streams,
 * with a host handler for non-carriers and for HTTP fallback. The endpoint and listener belong
 * to the application.
 */
public final class Http3Server implements AutoCloseable {
    private final EventLoopGroup group;
    private final Channel datagram;
    private final AtomicBoolean closed = new AtomicBoolean();

    private Http3Server(EventLoopGroup group, Channel datagram) {
        this.group = group;
        this.datagram = datagram;
    }

    /**
     * Binds a UDP socket for HTTP/3, with ALPN {@code h3} and early data disabled. The server owns
     * its socket, connections and event loop. A null host selects a 404 handler.
     */
    public static Http3Server bind(InetSocketAddress address, QuicSslContextBuilder tls, QuicOptions options,
                                   HttpEndpoint endpoint, Supplier<ChannelHandler> host) throws IOException {
        Objects.requireNonNull(address, "address");
        Objects.requireNonNull(options, "options");
        var connections = initializer(endpoint, host, true);
        var context = Objects.requireNonNull(tls, "tls").applicationProtocols("h3").earlyData(false)
                .keylog(QuicExporter.KEYLOG).build();
        if (context.isClient()) throw new IllegalArgumentException("server TLS builder required");
        var codec = Http3Streams.configure(options, new QuicServerCodecBuilder())
                .sslContext(context).handler(connections).build();
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        boolean success = false;
        try {
            var binding = new Bootstrap().group(group).channel(NioDatagramChannel.class).handler(codec).bind(address);
            try {
                binding.sync();
            } catch (InterruptedException interrupted) {
                binding.channel().close();
                Thread.currentThread().interrupt();
                throw new CarrierFailedException("could not bind HTTP/3 listener", interrupted);
            }
            var server = new Http3Server(group, binding.channel());
            success = true;
            return server;
        } catch (RuntimeException failed) {
            throw new CarrierFailedException("could not bind HTTP/3 listener", failed);
        } finally {
            if (!success) group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
        }
    }

    /** The bound UDP address. */
    public InetSocketAddress address() { return (InetSocketAddress) datagram.localAddress(); }

    /**
     * Initializes HTTP/3 on each of a host QUIC codec's connections, before it receives the first
     * stream. Install as the codec's connection handler, with incoming streams in byte-buffer
     * mode with auto-read and half-closure enabled. The codec should use server TLS with ALPN
     * {@code h3}, early data disabled, and {@link QuicExporter#KEYLOG} for channel-bound
     * credentials. Reserve receive credit for the peer's control and two QPACK streams.
     */
    public static ChannelInitializer<QuicChannel> initializer(HttpEndpoint endpoint, Supplier<ChannelHandler> host) {
        return initializer(endpoint, host, false);
    }

    private static ChannelInitializer<QuicChannel> initializer(
            HttpEndpoint endpoint, Supplier<ChannelHandler> host, boolean ownsConnection) {
        Objects.requireNonNull(endpoint, "endpoint");
        return new ChannelInitializer<>() {
            @Override protected void initChannel(QuicChannel channel) {
                new Binding(endpoint, host, ownsConnection).install(channel);
            }
        };
    }

    /**
     * Adds HTTP/3 to a host-owned connection on its loop. Call before any incoming streams,
     * using the codec configuration described in {@link #initializer}. A peer connection close
     * completes the observation. A connection error reported by the codec fails it.
     * Cancelling it withdraws endpoint requests awaiting handoff and
     * routes future requests directly to the host. The connection and handed-off carriers
     * continue to belong to their current owners.
     */
    public static ZmuxFuture<Void> serve(QuicChannel connection, HttpEndpoint endpoint, Supplier<ChannelHandler> host) {
        Objects.requireNonNull(connection, "connection");
        Objects.requireNonNull(endpoint, "endpoint");
        Binding binding = new Binding(endpoint, host, false);
        var loop = connection.eventLoop();
        Loops.dispatch(loop, loop.inEventLoop(), () -> {
            if (!connection.isOpen()) {
                binding.result.complete(null);
                return;
            }
            try {
                binding.install(connection);
            } catch (RuntimeException failed) {
                binding.result.completeExceptionally(failed);
            }
        }, () -> binding.result.complete(null));
        return Spi.future(binding.result, () -> {
            if (binding.result.cancel(false)) Loops.dispatch(loop, loop.inEventLoop(), binding::stop, () -> {});
        });
    }

    /**
     * Mounts the endpoint on a request stream of a host's own Netty HTTP/3 connection handler.
     * Use as the {@link Http3ServerConnectionHandler} request initializer. The QUIC codec requirements
     * are the same as for {@link #initializer}.
     */
    public static ChannelInitializer<QuicStreamChannel> requestInitializer(
            HttpEndpoint endpoint, Supplier<ChannelHandler> host) {
        Objects.requireNonNull(endpoint, "endpoint");
        return requestInitializer(endpoint, host, null);
    }

    private static ChannelInitializer<QuicStreamChannel> requestInitializer(
            HttpEndpoint endpoint, Supplier<ChannelHandler> host, Binding binding) {
        return new ChannelInitializer<>() {
            @Override protected void initChannel(QuicStreamChannel stream) {
                var pipeline = stream.pipeline();
                if (binding != null) pipeline.addLast(binding.requestErrors);
                stream.config().setAllowHalfClosure(true);
                Http3Streams.install(pipeline, true);
                if (binding == null || !binding.result.isCancelled()) {
                    Route route = HttpTransport.route(endpoint, pipeline);
                    if (binding != null) {
                        binding.routes.add(route);
                        stream.closeFuture().addListener(ignored ->
                                route.afterSetup(() -> binding.routes.remove(route)));
                    }
                }
                pipeline.addLast(host == null ? HttpTransport.notFound() : host.get());
            }
        };
    }

    /** All state except the result belongs to the one connection loop. */
    private static final class Binding extends ChannelInboundHandlerAdapter {
        private final HttpEndpoint endpoint;
        private final Supplier<ChannelHandler> host;
        private final Set<Route> routes = new HashSet<>();
        private final boolean ownsConnection;
        private final CompletableFuture<Void> result = new CompletableFuture<>();
        private final RequestErrors requestErrors = new RequestErrors(result);

        Binding(HttpEndpoint endpoint, Supplier<ChannelHandler> host, boolean ownsConnection) {
            this.endpoint = endpoint;
            this.host = host;
            this.ownsConnection = ownsConnection;
        }

        void install(QuicChannel connection) {
            var pipeline = connection.pipeline();
            if (pipeline.get(Http3ConnectionHandler.class) != null) {
                throw new IllegalStateException("connection already has an HTTP/3 handler");
            }
            var control = new ChannelInboundHandlerAdapter() {
                @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
                    ReferenceCountUtil.release(msg);
                }

                @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                    result.completeExceptionally(cause);
                    ctx.channel().parent().pipeline().fireExceptionCaught(cause);
                }

                @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
                    if (event == ChannelInputShutdownEvent.INSTANCE) {
                        // Netty closes the connection before forwarding the control stream's FIN.
                        result.completeExceptionally(new Http3Exception(Http3ErrorCode.H3_CLOSED_CRITICAL_STREAM,
                                "HTTP/3 peer ended the connection control stream"));
                    }
                    ctx.fireUserEventTriggered(event);
                }
            };
            pipeline.addLast(this);
            // Netty declares these parameters @Nullable and passes null itself (default unknown streams and
            // settings); the inspection misreads its class-file annotations.
            //noinspection DataFlowIssue
            pipeline.addBefore(pipeline.context(this).name(), null,
                    new Http3ServerConnectionHandler(requestInitializer(endpoint, host, this), control, null, null, true));
            connection.closeFuture().addListener(ignored -> {
                // A protocol error on a child can be reported later in the same read turn.
                Runnable ended = () -> result.complete(null);
                Loops.run(connection.eventLoop(), ended, ended);
            });
        }

        void stop() {
            for (Route route : List.copyOf(routes)) route.stop();
        }

        @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
            if (event instanceof SslHandshakeCompletionEvent handshake && !handshake.isSuccess()) {
                result.completeExceptionally(handshake.cause());
            }
            ctx.fireUserEventTriggered(event);
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            result.completeExceptionally(cause);
            if (ownsConnection) ctx.close();
        }
    }

    @ChannelHandler.Sharable
    private static final class RequestErrors extends ChannelInboundHandlerAdapter {
        private final CompletableFuture<Void> result;

        RequestErrors(CompletableFuture<Void> result) { this.result = result; }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // The Netty request codec reports framing/QPACK errors just before it closes the
            // connection. Message validation (for example Content-Length) resets only this stream.
            if (cause instanceof Http3Exception error && switch (error.errorCode()) {
                case H3_FRAME_UNEXPECTED, H3_FRAME_ERROR, H3_EXCESSIVE_LOAD, QPACK_DECOMPRESSION_FAILED -> true;
                default -> false;
            }) {
                result.completeExceptionally(cause);
            }
            ctx.fireExceptionCaught(cause);
        }
    }

    /** Closes the socket and the connections and event loop it owns. */
    @Override public void close() {
        if (closed.compareAndSet(false, true)) {
            datagram.closeFuture().addListener(ignored -> group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS));
            datagram.close();
        }
        //noinspection resource: borrowed: the channel's event loop belongs to group, shut down above
        if (!datagram.eventLoop().inEventLoop()) {
            group.terminationFuture().awaitUninterruptibly(2, TimeUnit.SECONDS);
        }
    }
}
