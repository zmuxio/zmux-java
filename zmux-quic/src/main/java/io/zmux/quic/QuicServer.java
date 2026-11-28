package io.zmux.quic;

import static io.zmux.quic.internal.NettyLoops.getBorrowedEventLoop;

import io.zmux.internal.spi.Loops;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.*;
import io.zmux.ZmuxListener;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Supplies QUIC request and lane carriers to an ordinary zmux listener. The listener remains application-owned.
 *
 * <p>A connection closes after the zmux close of its last pipe was delivered or lingered, with
 * an application close carrying the last session's code. Netty's QUIC close takes a 32-bit
 * code: a session code above 2<sup>31</sup> - 1 closes the connection with 0.
 */
public final class QuicServer implements AutoCloseable {
    private final EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    /** The sessions of this endpoint's connections live on its one loop. */
    private final NettyHost host = new NettyHost(group.next());
    private final io.zmux.internal.loop.SerialCallbacks completions = new io.zmux.internal.loop.SerialCallbacks();
    private final Set<QuicChannel> connections = ConcurrentHashMap.newKeySet();
    private final Channel datagram;
    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();
    private final java.util.concurrent.atomic.AtomicBoolean admissionStopped = new java.util.concurrent.atomic.AtomicBoolean();

    /**
     * A server whose connections are admitted by their first bidirectional stream with the
     * listener's admission (0-RTT is refused); a connection given back goes with that stream to
     * the listener's {@link io.zmux.ListenerConfig#fallback()} ({@link QuicTakeover#connection}).
     */
    public QuicServer(InetSocketAddress address, QuicSslContextBuilder tlsBuilder, QuicOptions options, ZmuxListener listener) throws IOException {
        try {
            QuicSslContext tls = tlsBuilder.earlyData(false).keylog(QuicExporter.KEYLOG).build();
            if (tls.isClient()) throw new IllegalArgumentException("server TLS builder required");
            ChannelHandler codec = options.apply(new QuicServerCodecBuilder()).sslContext(tls)
                    .handler(new ChannelInitializer<QuicChannel>() {
                        @Override protected void initChannel(QuicChannel channel) {
                            connections.add(channel);
                            if (closed.get() || admissionStopped.get()) { connections.remove(channel); channel.close(); return; }
                            channel.closeFuture().addListener(ignored -> connections.remove(channel));
                            var carrier = new ConnectionCarrier(options, listener, host, completions);
                            channel.pipeline().addLast(carrier);
                            if (admissionStopped.get()) carrier.stop(channel);
                        }
                    }).streamHandler(STREAMS).build();
            datagram = new Bootstrap().group(group).channel(NioDatagramChannel.class).handler(codec).bind(address).sync().channel();
        } catch (InterruptedException | RuntimeException error) {
            host.close();
            group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new io.zmux.CarrierFailedException("could not bind QUIC listener", error);
        }
    }
    /**
     * The hosts of the connections served by {@link #serve}: one per Netty loop, for the loop's lifetime.
     * A loop's entry goes when it terminates (a weak map would keep it: the host references its loop).
     */
    private static final java.util.concurrent.ConcurrentHashMap<EventLoop, NettyHost> HOSTS = new java.util.concurrent.ConcurrentHashMap<>();

    static NettyHost hostOf(EventLoop loop) {
        boolean[] made = {false};
        NettyHost host = HOSTS.computeIfAbsent(loop, l -> { made[0] = true; return new NettyHost(l); });
        // Outside the map's update: a loop already terminated runs the listener at once.
        if (made[0]) loop.terminationFuture().addListener(f -> HOSTS.remove(loop, host));
        return host;
    }

    /**
     * Serves one established QUIC connection with listener, as a {@link QuicServer} serves each
     * connection it accepts: a slot of its source (its remote address), the admission of its
     * first bidirectional stream, the inbound transform, then the classification of every
     * stream; a connection given back goes with that stream to the listener's fallback
     * ({@link QuicTakeover#connection}), and is closed once the fallback is done with it.
     * The connection's codec must hand its streams on with {@link #streamHandler()}, and a
     * stream that arrives before serve is called is closed: call it on the connection's loop
     * when its handshake completes. Netty
     * offers no query for whether a connection used 0-RTT: a host whose listener has an
     * admission gives connections whose TLS refused early data.
     *
     * <p>The service can stop new admission independently of lane acceptance. An explicitly
     * stopped service leaves the external connection to its owner when it drains. A connection
     * refused at a limit (no source slot) is closed and
     * counted, and the stage completes: the connection ended. A listener already closed closes
     * the connection, and the stage fails with {@link io.zmux.ListenerClosedException}. These
     * outcomes belong to this adapter, which hands the connection to the listener as a whole.
     */
    public static QuicConnectionService serve(QuicChannel connection, QuicOptions options, ZmuxListener listener) {
        java.util.Objects.requireNonNull(connection, "connection");
        java.util.Objects.requireNonNull(options, "options");
        java.util.Objects.requireNonNull(listener, "listener");
        var result = new java.util.concurrent.CompletableFuture<Void>();
        NettyHost host = hostOf(getBorrowedEventLoop(connection));
        var callbacks = new io.zmux.internal.loop.SerialCallbacks();
        var carrier = ConnectionCarrier.established(options, listener, host, callbacks);
        connection.closeFuture().addListener(ignored -> result.complete(null));
        Runnable add = () -> {
            if (!connection.isActive()) { result.complete(null); return; }
            if (connection.attr(ConnectionCarrier.KEY).get() != null || Boolean.TRUE.equals(connection.attr(ConnectionCarrier.SERVED_ONCE).get())) {
                result.completeExceptionally(new IllegalStateException("QUIC connection already served"));
                return;
            }
            if (listener.isClosed()) {
                result.completeExceptionally(new io.zmux.ListenerClosedException());
                connection.close();
                return;
            }
            connection.pipeline().addLast(carrier);
            carrier.whenDrained().whenComplete((value, failure) -> result.complete(null));
        };
        // From the connection's loop (its handshake event, say) at once, so no stream comes first.
        //noinspection resource: borrowed: the channel owns its event loop
        Loops.dispatch(getBorrowedEventLoop(connection), getBorrowedEventLoop(connection).inEventLoop(), add, () -> result.complete(null));
        var published = ConnectionCarrier.offLoop(result, callbacks, ignored -> {});
        return new QuicConnectionService(() -> { if (!result.isDone()) carrier.stop(connection); },
                () -> { if (!result.isDone()) carrier.stopLanes(connection); },
                published.minimalCompletionStage());
    }

    /**
     * The stream handler of a codec whose connections are given to {@link #serve}: it hands
     * each stream the peer opens to the zmux carrier of its connection, and closes the streams
     * of a connection that has none.
     */
    public static ChannelHandler streamHandler() { return STREAMS; }

    /**
     * Observes the negotiated native datagram size of a host-owned connection. Call on its
     * event loop from the codec's connection initializer, before Netty activates the channel.
     */
    public static void observeDatagramCapacity(QuicChannel connection) {
        if (!getBorrowedEventLoop(connection).inEventLoop()) throw new IllegalStateException("QUIC connection loop required");
        io.zmux.quic.internal.QuicDatagrams.observe(connection);
    }

    private static final ChannelHandler STREAMS = streams(null);

    /** Supplies the handler for streams opened after the host detached a connection in its fallback. */
    public static ChannelHandler streamHandler(java.util.function.Supplier<? extends ChannelHandler> handback) {
        return streams(java.util.Objects.requireNonNull(handback, "handback"));
    }

    private static ChannelHandler streams(java.util.function.Supplier<? extends ChannelHandler> handback) {
        return new ChannelInitializer<QuicStreamChannel>() {
            @Override protected void initChannel(QuicStreamChannel stream) {
                var connection = stream.parent();
                var carrier = connection.attr(ConnectionCarrier.KEY).get();
                if (carrier != null && !carrier.hostOwnsConnection()) {
                    carrier.incoming(stream);
                } else if (handback != null && Boolean.TRUE.equals(connection.attr(ConnectionCarrier.HOST_OWNED).get())) {
                    stream.pipeline().addLast(handback.get());
                } else {
                    stream.close();
                }
            }
        };
    }

    public InetSocketAddress address() { return (InetSocketAddress) datagram.localAddress(); }
    NettyHost host() { return host; }
    /** A snapshot of this endpoint's accepted native connections. */
    public java.util.List<QuicChannel> connections() { return java.util.List.copyOf(connections); }

    /** Stops new connections and pipe admission. Existing attachments drain on their connections. */
    public void stopAdmission() {
        if (!admissionStopped.compareAndSet(false, true)) return;
        for (QuicChannel connection : connections) {
            var carrier = connection.attr(ConnectionCarrier.KEY).get();
            if (carrier != null) carrier.stop(connection);
        }
    }

    /** Opens a bidi carrier on an owned connection; cancellation returns only that stream, not the connection. */
    public io.zmux.ZmuxFuture<io.zmux.carrier.PipeIo> openPipeAsync(QuicChannel connection) {
        if (closed.get() || admissionStopped.get()) return QuicClient.future(java.util.concurrent.CompletableFuture.failedFuture(new io.zmux.CarrierFailedException("QUIC admission stopped")));
        if (connection == null || !connections.contains(connection))
            return QuicClient.future(java.util.concurrent.CompletableFuture.failedFuture(new IllegalArgumentException("connection is not owned by this endpoint")));
        ConnectionCarrier carrier = connection.attr(ConnectionCarrier.KEY).get();
        if (carrier == null) return QuicClient.future(java.util.concurrent.CompletableFuture.failedFuture(new io.zmux.CarrierFailedException("QUIC connection not ready")));
        return QuicClient.future(carrier.openApplicationPipe());
    }

    /** {@link #openPipeAsync}, waiting for the pipe input. */
    public io.zmux.carrier.PipeIo openPipe(QuicChannel connection) throws IOException {
        return QuicClient.await(openPipeAsync(connection));
    }

    /**
     * Closes the endpoint and every connection on its socket, a connection whose taken-over stream the host detached
     * included: they share the endpoint's socket and event loops, which end here.
     */
    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        for (QuicChannel connection : connections) ConnectionCarrier.close(connection);
        datagram.close();
        host.close();
        var termination = group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
        //noinspection resource: borrowed: the group owns its event loop, shut down above
        if (!getBorrowedEventLoop(datagram).inEventLoop()) termination.awaitUninterruptibly();
    }
}
