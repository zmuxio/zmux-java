package io.zmux.http3;

import static io.zmux.quic.internal.NettyLoops.getBorrowedEventLoop;

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
import io.netty.handler.codec.http3.Http3ConnectionHandler;
import io.netty.handler.codec.http3.Http3ControlStreamFrame;
import io.netty.handler.codec.http3.Http3GoAwayFrame;
import io.netty.handler.codec.http3.Http3SettingsFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicClientCodecBuilder;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamLimitChangedEvent;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.AttributeKey;
import io.zmux.http.internal.HttpTransport;
import io.zmux.internal.spi.Loops;
import io.zmux.quic.QuicExporter;
import io.zmux.quic.QuicOptions;
import io.zmux.quic.internal.QuicDatagrams;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/** QUIC integration for Connections, either dialing its own endpoint or borrowing a connection. */
final class Http3ClientTransport implements HttpTransport.Client {
    private static final AttributeKey<State> STATE = AttributeKey.valueOf(Http3ClientTransport.class, "state");
    private record Dial(QuicSslContext tls, QuicOptions options) {}
    private record Binding(QuicChannel connection, H3Authority authority, CompletableFuture<Channel> ready) {}
    final WebTransportProvider provider;
    private final Dial dial;
    private final Binding binding;

    Http3ClientTransport(QuicSslContextBuilder builder, QuicOptions options) {
        this(builder, options, WebTransportProvider.linked(options));
    }

    Http3ClientTransport(QuicSslContextBuilder builder, QuicOptions options, WebTransportProvider provider) {
        this.provider = provider;
        var tls = builder.applicationProtocols("h3").earlyData(false).endpointIdentificationAlgorithm("HTTPS")
                .keylog(QuicExporter.KEYLOG).build();
        if (!tls.isClient()) throw new IllegalArgumentException("client TLS builder required");
        dial = new Dial(tls, options);
        binding = null;
    }

    Http3ClientTransport(QuicChannel connection, URI origin, boolean requestDatagrams) {
        this(connection, origin, requestDatagrams, null);
    }

    Http3ClientTransport(QuicChannel connection, URI origin, boolean requestDatagrams, QuicOptions options) {
        provider = options == null ? null : WebTransportProvider.linked(options);
        dial = null;
        var authority = H3Authority.origin(origin);
        var ready = new CompletableFuture<Channel>();
        binding = new Binding(Objects.requireNonNull(connection, "connection"), authority, ready);
        var loop = getBorrowedEventLoop(connection);
        Loops.dispatch(loop, loop.inEventLoop(), () -> {
            try {
                var engine = connection.sslEngine();
                if (!connection.isActive() || engine == null || !engine.getUseClientMode()
                        || !"h3".equals(engine.getApplicationProtocol())) {
                    throw new IllegalArgumentException("expected an established HTTP/3 client connection");
                }
                var capability = QuicDatagrams.get(connection);
                var state = State.install(connection, authority, provider, null, null, requestDatagrams && capability != null && capability.capacity() > 0);
                state.ready.whenComplete((channel, failure) -> {
                    if (failure == null) ready.complete(channel);
                    else ready.completeExceptionally(failure);
                });
            } catch (RuntimeException failure) {
                ready.completeExceptionally(failure);
            }
        }, () -> ready.completeExceptionally(new ClosedChannelException()));
    }

    io.zmux.ZmuxFuture<WebTransportSession> openSession(io.zmux.http.HttpCarrierClient client, URI target,
            io.netty.handler.codec.http3.Http3HeadersFrame head, io.zmux.CarrierCredential credential, Duration timeout) {
        var result = new CompletableFuture<WebTransportSession>();
        result.orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
        var opening = HttpTransport.openRequest(client, target);
        result.whenComplete((session, error) -> {
            if (error != null) opening.cancel(false);
        });
        opening.whenComplete((channel, failure) -> {
            if (failure != null) {
                result.completeExceptionally(failure);
                return;
            }
            result.whenComplete((session, error) -> {
                if (result.isCancelled() || error instanceof java.util.concurrent.TimeoutException) HttpTransport.abort(channel);
            });
            var stream = (QuicStreamChannel) channel;
            Loops.dispatch(getBorrowedEventLoop(stream), getBorrowedEventLoop(stream).inEventLoop(),
                    () -> WebTransportConnect.start(stream, target, head, credential, result),
                    () -> result.completeExceptionally(new ClosedChannelException()));
        });
        return io.zmux.internal.spi.Spi.future(result, () -> result.cancel(false));
    }

    @Override public boolean borrowsConnection() { return binding != null; }
    @Override public boolean onBorrowedLoop() { return binding != null && getBorrowedEventLoop(binding.connection).inEventLoop(); }

    @Override public boolean reusable(Channel channel) {
        return canOpen(channel);
    }

    private static boolean canOpen(Channel channel) {
        var state = channel.attr(STATE).get();
        var wt = channel instanceof QuicChannel quic ? WebTransportConnection.get(quic) : null;
        return channel.isActive() && state != null && !state.draining && (wt == null || !wt.draining());
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
        final H3Authority authority;
        final CompletableFuture<Channel> ready = new CompletableFuture<>();
        volatile boolean draining;
        private final ArrayDeque<Pending> waiting = new ArrayDeque<>();
        private boolean allocating;
        private final QuicStreamCredit credit = new QuicStreamCredit(QuicStreamType.BIDIRECTIONAL);
        private int requests;

        private State(H3Authority authority) { this.authority = authority; }

        static State install(QuicChannel connection, H3Authority authority, WebTransportProvider provider, QuicOptions options, Duration setupTimeout, boolean datagramEnabled) {
            var attribute = connection.attr(STATE);
            var existing = attribute.get();
            if (existing != null) {
                if (!Objects.equals(existing.authority, authority)) {
                    throw new IllegalArgumentException("HTTP/3 connection already bound to a different authority or owner");
                }
                return existing;
            }
            var pipeline = connection.pipeline();
            if (pipeline.get(Http3ConnectionHandler.class) != null) {
                throw new IllegalStateException("connection already has an HTTP/3 handler");
            }
            var state = new State(authority);
            var datagrams = datagramEnabled ? H3Datagrams.install(connection) : null;
            if (provider != null && options == null) options = provider.receiveLimits(connection);
            var wt = provider == null ? null : new WebTransportConnection(connection, options, false, provider,
                    io.zmux.internal.spi.CarrierBudget.standalone(Math.addExact(provider.receiveCapacity(connection), options.connectionWindow())), setupTimeout != null ? setupTimeout : provider.reorderTimeout(connection));
            var controls = new SimpleChannelInboundHandler<Http3ControlStreamFrame>() {
                @Override protected void channelRead0(ChannelHandlerContext ctx, Http3ControlStreamFrame frame) {
                    if (frame instanceof Http3SettingsFrame settings) {
                        if (datagrams != null) datagrams.peerSettings(settings);
                        if (wt != null) wt.settings(settings.settings());
                        state.ready.complete(connection);
                    }
                    if (frame instanceof Http3GoAwayFrame goAway) {
                        if (wt != null) wt.goAway(goAway.id());
                        state.drain(connection);
                        connection.pipeline().fireUserEventTriggered(goAway);
                    }
                }

                @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable failure) {
                    if (wt != null) WebTransportSettings.controlFailure(ctx, failure);
                    state.ready.completeExceptionally(failure);
                    ctx.fireExceptionCaught(failure);
                }
            };
            attribute.set(state);
            if (wt == null) {
                // Netty accepts null for handlers and settings supplied by its defaults.
                //noinspection DataFlowIssue
                pipeline.addLast(new Http3ClientConnectionHandler(controls, null, null,
                        datagrams == null ? null : datagrams.settings(), true));
            } else {
                var local = new io.netty.handler.codec.http3.DefaultHttp3SettingsFrame(WebTransportSettings.advertise(false));
                // Netty accepts null to select the default stream handlers.
                //noinspection DataFlowIssue
                wt.install(new Http3ClientConnectionHandler(controls, null, null, local,
                        true, WebTransportSettings::validate));
            }
            pipeline.addLast(state);
            connection.closeFuture().addListener(ignored -> state.ready.completeExceptionally(new ClosedChannelException()));
            return state;
        }

        void enqueue(QuicChannel connection, CompletableFuture<Channel> result, Duration timeout) {
            if (result.isDone()) return;
            if (!canOpen(connection)) {
                result.completeExceptionally(new HttpTransport.RetryOpen(null));
                return;
            }
            Pending pending = new Pending(result);
            pending.expiration = Loops.schedule(getBorrowedEventLoop(connection), () -> expire(connection, pending),
                    timeout.toNanos(), TimeUnit.NANOSECONDS, () -> result.completeExceptionally(new ClosedChannelException()));
            if (pending.expiration == null) return;
            waiting.addLast(pending);
            result.whenComplete((stream, error) -> {
                if (result.isCancelled()) Loops.run(getBorrowedEventLoop(connection), () -> {
                    if (waiting.remove(pending)) pending.expiration.cancel(false);
                    pump(connection);
                }, () -> { });
            });
            pump(connection);
        }

        private void expire(QuicChannel connection, Pending pending) {
            if (!waiting.contains(pending)) return;
            var failure = new TimeoutException("H3 stream credit wait expired");
            if (authority == null) {
                stopOpening(connection, failure);
            } else if (waiting.remove(pending)) {
                pending.complete(null, failure);
                pump(connection);
            }
        }

        void drain(QuicChannel connection) { stopOpening(connection, null); }

        private void stopOpening(QuicChannel connection, Throwable cause) {
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
            if (authority == null && draining && !allocating && requests == 0) connection.close();
        }

        @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
            if (event instanceof QuicStreamLimitChangedEvent) {
                credit.changed();
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
            if (pending == null || allocating || draining || !credit.available(connection)) return;
            allocating = true;
            try {
                Http3.newRequestStream(connection, new ChannelInitializer<QuicStreamChannel>() {
                    @Override protected void initChannel(QuicStreamChannel ch) {
                        ch.config().setAllowHalfClosure(true);
                        Http3Streams.install(ch.pipeline(), false);
                        if (authority != null) ch.pipeline().addLast(authority.guard());
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
            } else if (!credit.waitForCredit(cause) && waiting.remove(pending)) {
                pending.complete(null, cause);
            }
            closeIfDrained(connection);
            if (!waiting.isEmpty() && !credit.blocked()) {
                Loops.run(getBorrowedEventLoop(connection), () -> pump(connection),
                        () -> failWaiting(new ClosedChannelException()));
            }
        }
    }

    @Override public CompletableFuture<Channel> connect(EventLoopGroup group, URI uri, Duration timeout, Consumer<Channel> track) {
        if (binding != null) {
            if (!binding.authority.matches(uri)) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("HTTP/3 request does not match the connection authority"));
            }
            if (!binding.connection.isActive()) return CompletableFuture.failedFuture(new ClosedChannelException());
            return binding.ready.copy().orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        var connected = new CompletableFuture<Channel>();
        String host = uri.getHost();
        int port = uri.getPort() > 0 ? uri.getPort() : 443;
        // JDK name resolution can block. The pool reserves this future immediately while a
        // short-lived virtual thread resolves the address outside the reservation and I/O loop.
        Thread resolver = Thread.ofVirtual().name("zmux-h3-resolve").unstarted(() -> {
            if (connected.isDone()) return;
            try {
                InetAddress address = InetAddress.getByName(host);
                if (!connected.isDone()) bind(group, host, new InetSocketAddress(address, port), track, connected, timeout);
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
                      CompletableFuture<Channel> connected, Duration timeout) {
        SocketProtocolFamily family = peer.getAddress() instanceof Inet6Address
                ? SocketProtocolFamily.INET6 : SocketProtocolFamily.INET;
        var codec = Http3Streams.configure(dial.options, new QuicClientCodecBuilder())
                .sslEngineProvider(channel -> dial.tls.newEngine(channel.alloc(), host, peer.getPort()))
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
            else if (!connected.isDone()) handshake(udp, peer, connected, timeout);
        });
    }

    private void handshake(Channel udp, InetSocketAddress peer, CompletableFuture<Channel> result, Duration timeout) {
        try {
            QuicChannel.newBootstrap(udp)
                    .handler(new ChannelInitializer<QuicChannel>() {
                        @Override protected void initChannel(QuicChannel connection) {
                            State.install(connection, null, provider, dial.options(), timeout, false);
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
                        connection.attr(STATE).get().ready.whenComplete((ignored, failure) -> {
                            if (failure != null) result.completeExceptionally(failure);
                            else if (!result.complete(connection)) connection.close();
                        });
                    });
        } catch (RuntimeException failure) {
            result.completeExceptionally(failure);
        }
    }

    @Override public CompletableFuture<Channel> stream(Channel channel, Duration timeout) {
        if (!(channel instanceof QuicChannel connection) || (binding != null && channel != binding.connection)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("HTTP/3 requires a QUIC connection"));
        }
        var result = new CompletableFuture<Channel>();
        Loops.run(getBorrowedEventLoop(connection), () -> {
            var state = connection.attr(STATE).get();
            if (state == null) result.completeExceptionally(new IllegalStateException("HTTP/3 connection has no client state"));
            else state.enqueue(connection, result, timeout);
        },
                () -> result.completeExceptionally(new ClosedChannelException()));
        return result;
    }
}
