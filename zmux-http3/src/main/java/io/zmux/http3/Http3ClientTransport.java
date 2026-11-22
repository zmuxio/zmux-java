package io.zmux.http3;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3ClientConnectionHandler;
import io.netty.handler.codec.http3.Http3ControlStreamFrame;
import io.netty.handler.codec.http3.Http3GoAwayFrame;
import io.netty.handler.codec.http3.Http3SettingsFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicClientCodecBuilder;
import io.netty.handler.codec.quic.QuicException;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamLimitChangedEvent;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.handler.codec.quic.QuicTransportError;
import io.netty.util.AttributeKey;
import io.zmux.http.internal.HttpTransport;
import io.zmux.internal.spi.Loops;
import io.zmux.quic.QuicExporter;
import io.zmux.quic.QuicOptions;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/** QUIC integration for Connections. Each pooled H3 connection owns a UDP channel. */
final class Http3ClientTransport implements HttpTransport.Client {
    private static final AttributeKey<State> STATE = AttributeKey.valueOf(Http3ClientTransport.class, "state");
    private final QuicSslContext tls;
    private final QuicOptions options;

    Http3ClientTransport(QuicSslContextBuilder builder, QuicOptions options) {
        tls = builder.applicationProtocols("h3").earlyData(false).endpointIdentificationAlgorithm("HTTPS")
                .keylog(QuicExporter.KEYLOG).build();
        if (!tls.isClient()) throw new IllegalArgumentException("client TLS builder required");
        this.options = options;
    }

    @Override public boolean reusable(Channel channel) {
        return canOpen(channel);
    }

    private static boolean canOpen(Channel channel) {
        return channel.isActive() && !channel.attr(STATE).get().draining;
    }

    private static final class Pending {
        final CompletableFuture<Channel> result;
        ScheduledFuture<?> expiration;

        Pending(CompletableFuture<Channel> result) {
            this.result = result;
        }

        void complete(Channel stream, Throwable failure) {
            expiration.cancel(false);
            if (failure != null) result.completeExceptionally(failure);
            else if (!result.complete(stream)) HttpTransport.abort(stream);
        }
    }

    /** All queue state belongs to the QUIC connection loop. The pool reads the drain flag. */
    private static final class State extends ChannelInboundHandlerAdapter {
        volatile boolean draining;
        private final ArrayDeque<Pending> waiting = new ArrayDeque<>();
        private boolean allocating;
        private boolean blocked;
        private int requests;

        void enqueue(QuicChannel connection, CompletableFuture<Channel> result, Duration timeout) {
            if (result.isDone()) return;
            if (!canOpen(connection)) {
                result.completeExceptionally(new HttpTransport.RetryOpen(null));
                return;
            }
            Pending pending = new Pending(result);
            pending.expiration = Loops.schedule(connection.eventLoop(), () -> {
                if (waiting.contains(pending)) drain(connection, new TimeoutException("H3 stream credit wait expired"));
            }, timeout.toNanos(), TimeUnit.NANOSECONDS, () -> result.completeExceptionally(new ClosedChannelException()));
            if (pending.expiration == null) return;
            waiting.addLast(pending);
            result.whenComplete((stream, error) -> {
                if (result.isCancelled()) Loops.run(connection.eventLoop(), () -> {
                    if (waiting.remove(pending)) pending.expiration.cancel(false);
                    pump(connection);
                }, () -> { });
            });
            pump(connection);
        }

        void drain(QuicChannel connection, Throwable cause) {
            if (draining) return;
            draining = true;
            failWaiting(new HttpTransport.RetryOpen(cause));
            closeIfDrained(connection);
        }

        private void failWaiting(Throwable cause) {
            Pending next;
            while ((next = waiting.pollFirst()) != null) next.complete(null, cause);
        }

        private void requestClosed(QuicChannel connection) {
            requests--;
            closeIfDrained(connection);
        }

        private void closeIfDrained(QuicChannel connection) {
            if (draining && !allocating && requests == 0) connection.close();
        }

        @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
            if (event instanceof QuicStreamLimitChangedEvent) {
                blocked = false;
                pump((QuicChannel) ctx.channel());
            }
            ctx.fireUserEventTriggered(event);
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            failWaiting(new ClosedChannelException());
            ctx.fireChannelInactive();
        }

        private void pump(QuicChannel connection) {
            Pending pending = waiting.peekFirst();
            if (pending == null || allocating || draining || blocked
                    || connection.peerAllowedStreams(QuicStreamType.BIDIRECTIONAL) <= 0) return;
            allocating = true;
            try {
                Http3.newRequestStream(connection, new ChannelInitializer<QuicStreamChannel>() {
                    @Override protected void initChannel(QuicStreamChannel ch) {
                        ch.config().setAllowHalfClosure(true);
                        Http3Streams.install(ch.pipeline(), false);
                    }
                }).addListener(open -> allocated(connection, pending, (Channel) open.getNow(), open.cause()));
            } catch (RuntimeException failure) {
                allocated(connection, pending, null, failure);
            }
        }

        private void allocated(QuicChannel connection, Pending pending, Channel stream, Throwable cause) {
            allocating = false;
            if (stream != null) {
                requests++;
                stream.closeFuture().addListener(closed -> requestClosed(connection));
                if (waiting.remove(pending)) pending.complete(stream, null);
                else HttpTransport.abort(stream);
            } else if (cause instanceof QuicException error && error.error() == QuicTransportError.STREAM_LIMIT_ERROR) {
                // The transport can disagree with its cached count. Wait for a fresh credit event.
                blocked = true;
            } else if (waiting.remove(pending)) {
                pending.complete(null, cause);
            }
            closeIfDrained(connection);
            if (!waiting.isEmpty() && !blocked) {
                Loops.run(connection.eventLoop(), () -> pump(connection),
                        () -> failWaiting(new ClosedChannelException()));
            }
        }
    }

    @Override public CompletableFuture<Channel> connect(EventLoopGroup group, URI uri, Duration timeout, Consumer<Channel> track) {
        var connected = new CompletableFuture<Channel>();
        String host = uri.getHost();
        int port = uri.getPort() > 0 ? uri.getPort() : 443;
        // JDK name resolution can block. The pool reserves this future immediately while a
        // short-lived virtual thread resolves the address outside the reservation and I/O loop.
        Thread resolver = Thread.ofVirtual().name("zmux-h3-resolve").unstarted(() -> {
            if (connected.isDone()) return;
            try {
                InetAddress address = InetAddress.getByName(host);
                if (!connected.isDone()) bind(group, host, new InetSocketAddress(address, port), track, connected);
            } catch (UnknownHostException | RuntimeException failure) {
                connected.completeExceptionally(failure);
            }
        });
        connected.orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS).whenComplete((channel, failure) -> {
            if (failure != null) resolver.interrupt();
        });
        resolver.start();
        return connected;
    }

    private void bind(EventLoopGroup group, String host, InetSocketAddress peer, Consumer<Channel> track,
                      CompletableFuture<Channel> connected) {
        SocketProtocolFamily family = peer.getAddress() instanceof Inet6Address
                ? SocketProtocolFamily.INET6 : SocketProtocolFamily.INET;
        var codec = Http3Streams.configure(options, new QuicClientCodecBuilder())
                .sslEngineProvider(channel -> tls.newEngine(channel.alloc(), host, peer.getPort()))
                .build();
        ChannelFuture bind = new Bootstrap().group(group).channelFactory(() -> new NioDatagramChannel(family))
                .handler(codec).bind(0);
        Channel udp = bind.channel();
        track.accept(udp);
        connected.whenComplete((channel, failure) -> {
            if (failure != null) udp.close();
        });
        udp.closeFuture().addListener(ignored -> connected.completeExceptionally(new ClosedChannelException()));
        bind.addListener(bound -> {
            if (!bound.isSuccess()) connected.completeExceptionally(bound.cause());
            else if (!connected.isDone()) handshake(udp, peer, connected);
        });
    }

    private static void handshake(Channel udp, InetSocketAddress peer, CompletableFuture<Channel> result) {
        var settings = new CompletableFuture<Void>();
        var state = new State();
        var controls = new SimpleChannelInboundHandler<Http3ControlStreamFrame>() {
            @Override protected void channelRead0(ChannelHandlerContext ctx, Http3ControlStreamFrame frame) {
                if (frame instanceof Http3SettingsFrame) settings.complete(null);
                if (frame instanceof Http3GoAwayFrame goAway) {
                    var connection = (QuicChannel) ctx.channel().parent();
                    state.drain(connection, null);
                    connection.pipeline().fireUserEventTriggered(goAway);
                }
            }

            @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable failure) {
                settings.completeExceptionally(failure);
                ctx.fireExceptionCaught(failure);
            }
        };
        try {
            QuicChannel.newBootstrap(udp)
                    .attr(STATE, state)
                    .handler(new ChannelInitializer<QuicChannel>() {
                        @Override protected void initChannel(QuicChannel connection) {
                            // Netty declares these parameters @Nullable and passes null itself (no push, default
                            // unknown streams and settings); the inspection misreads its class-file annotations.
                            //noinspection DataFlowIssue
                            connection.pipeline().addLast(new Http3ClientConnectionHandler(controls, null, null, null, true), state);
                        }
                    })
                    .remoteAddress(peer).connect().addListener(connected -> {
                        if (!connected.isSuccess()) {
                            result.completeExceptionally(connected.cause());
                            return;
                        }
                        var connection = (QuicChannel) connected.getNow();
                        connection.closeFuture().addListener(ignored -> {
                            result.completeExceptionally(new ClosedChannelException());
                            udp.close();
                        });
                        settings.whenComplete((ignored, failure) -> {
                            if (failure != null) result.completeExceptionally(failure);
                            else if (!result.complete(connection)) connection.close();
                        });
                    });
        } catch (RuntimeException failure) {
            result.completeExceptionally(failure);
        }
    }

    @Override public CompletableFuture<Channel> stream(Channel channel, Duration timeout) {
        if (!(channel instanceof QuicChannel connection)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("HTTP/3 requires a QUIC connection"));
        }
        var result = new CompletableFuture<Channel>();
        Loops.run(connection.eventLoop(), () -> connection.attr(STATE).get().enqueue(connection, result, timeout),
                () -> result.completeExceptionally(new ClosedChannelException()));
        return result;
    }
}
