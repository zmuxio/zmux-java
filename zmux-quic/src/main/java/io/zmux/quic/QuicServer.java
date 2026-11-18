package io.zmux.quic;

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

    /**
     * The target of a connection whose first bidirectional stream the listener's admission
     * did not accept: it takes the connection and that stream (every byte read replayed), as
     * {@link io.zmux.Admission.Fallback} takes a carrier; the connection's other streams are
     * closed. serve runs off the event loops.
     */
    @FunctionalInterface
    public interface Fallback {
        void serve(QuicChannel connection, io.zmux.carrier.PipeIo stream);
    }

    public QuicServer(InetSocketAddress address, QuicSslContextBuilder tlsBuilder, QuicOptions options, ZmuxListener listener) throws IOException {
        this(address, tlsBuilder, options, listener, null);
    }

    /**
     * A server whose connections are admitted by their first bidirectional stream with the
     * listener's admission (0-RTT is refused); fallback (or null: a fallback stalls) takes a
     * connection that is not accepted.
     */
    public QuicServer(InetSocketAddress address, QuicSslContextBuilder tlsBuilder, QuicOptions options, ZmuxListener listener,
                      Fallback fallback) throws IOException {
        try {
            QuicSslContext tls = tlsBuilder.earlyData(false).keylog(QuicExporter.KEYLOG).build();
            if (tls.isClient()) throw new IllegalArgumentException("server TLS builder required");
            ChannelHandler codec = options.apply(new QuicServerCodecBuilder()).sslContext(tls)
                    .handler(new ChannelInitializer<QuicChannel>() {
                        @Override protected void initChannel(QuicChannel channel) {
                            connections.add(channel);
                            if (closed.get()) { connections.remove(channel); channel.close(); return; }
                            channel.closeFuture().addListener(ignored -> connections.remove(channel));
                            channel.pipeline().addLast(new ConnectionCarrier(options, listener, fallback, host, completions));
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

    private static NettyHost hostOf(EventLoop loop) {
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
     * stream; fallback (or null: a fallback stalls) takes a connection that is not accepted.
     * The connection's codec must hand its streams on with {@link #streamHandler()}, and a
     * stream that arrives before serve is called is closed: call it on the connection's loop
     * when its handshake completes. Netty
     * offers no query for whether a connection used 0-RTT: a host whose listener has an
     * admission gives connections whose TLS refused early data.
     *
     * <p>The stage completes when the connection ends; cancelling it stops handing the
     * connection's new streams to the listener (they are closed), and neither the connection
     * nor its sessions are closed. A connection refused at a limit (no source slot) is closed and
     * counted, and the stage completes: the connection ended. A listener already closed closes
     * the connection, and the stage fails with {@link io.zmux.ListenerClosedException}. These
     * outcomes belong to this adapter, which hands the connection to the listener as a whole.
     */
    public static io.zmux.ZmuxFuture<Void> serve(QuicChannel connection, QuicOptions options, ZmuxListener listener,
                                                                  Fallback fallback) {
        java.util.Objects.requireNonNull(connection, "connection");
        java.util.Objects.requireNonNull(options, "options");
        java.util.Objects.requireNonNull(listener, "listener");
        var result = new java.util.concurrent.CompletableFuture<Void>();
        if (listener.isClosed()) {
            connection.close();
            result.completeExceptionally(new io.zmux.ListenerClosedException());
            return QuicClient.future(result);
        }
        NettyHost host = hostOf(connection.eventLoop());
        var carrier = ConnectionCarrier.established(options, listener, fallback, host, new io.zmux.internal.loop.SerialCallbacks());
        connection.closeFuture().addListener(ignored -> result.complete(null));
        Runnable add = () -> {
            if (!connection.isActive()) { result.complete(null); return; }
            connection.pipeline().addLast(carrier);
        };
        // From the connection's loop (its handshake event, say) at once, so no stream comes first.
        //noinspection resource: borrowed: the channel owns its event loop
        Loops.dispatch(connection.eventLoop(), connection.eventLoop().inEventLoop(), add, () -> result.complete(null));
        result.whenComplete((v, e) -> { if (result.isCancelled()) carrier.stop(); });
        return QuicClient.future(result);
    }

    /**
     * The stream handler of a codec whose connections are given to {@link #serve}: it hands
     * each stream the peer opens to the zmux carrier of its connection, and closes the streams
     * of a connection that has none.
     */
    public static ChannelHandler streamHandler() { return STREAMS; }

    private static final ChannelHandler STREAMS = new ChannelInitializer<QuicStreamChannel>() {
        @Override public boolean isSharable() { return true; }
        @Override protected void initChannel(QuicStreamChannel stream) {
            ConnectionCarrier c = stream.parent().attr(ConnectionCarrier.KEY).get();
            if (c == null) stream.close(); else c.incoming(stream);
        }
    };

    public InetSocketAddress address() { return (InetSocketAddress) datagram.localAddress(); }
    NettyHost host() { return host; }
    /** A snapshot of this endpoint's accepted native connections. */
    public java.util.List<QuicChannel> connections() { return java.util.List.copyOf(connections); }

    /** Opens a bidi carrier on an owned connection; cancellation returns only that stream, not the connection. */
    public io.zmux.ZmuxFuture<io.zmux.carrier.PipeIo> openPipeAsync(QuicChannel connection) {
        if (closed.get()) return QuicClient.future(java.util.concurrent.CompletableFuture.failedFuture(new io.zmux.CarrierFailedException("QUIC endpoint closed")));
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

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        for (QuicChannel connection : connections) ConnectionCarrier.close(connection);
        datagram.close();
        host.close();
        var termination = group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
        //noinspection resource: borrowed: the group owns its event loop, shut down above
        if (!datagram.eventLoop().inEventLoop()) termination.awaitUninterruptibly();
    }
}
