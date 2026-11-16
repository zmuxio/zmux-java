package io.zmux.quic;

import io.zmux.internal.spi.Loops;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.SocketProtocolFamily;
import io.netty.handler.codec.quic.*;
import io.netty.util.AttributeKey;
import io.zmux.Zmux;
import io.zmux.ZmuxConfig;
import io.zmux.ZmuxListener;
import io.zmux.Role;
import io.zmux.ZmuxFuture;
import io.zmux.ZmuxSession;
import io.zmux.carrier.PipeIo;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Owns family-specific UDP endpoints and their QUIC connections. Close it after the sessions that use it.
 *
 * <p>A connection closes after the zmux close of its last pipe was delivered or lingered, with
 * an application close carrying the last session's code. Netty's QUIC close takes a 32-bit
 * code: a session code above 2<sup>31</sup> - 1 closes the connection with 0.
 */
public final class QuicClient implements AutoCloseable {
    private static final AttributeKey<String> HOST = AttributeKey.valueOf(QuicClient.class, "host");
    private final EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    /** The sessions of this endpoint's connections live on its one loop. */
    private final NettyHost host = new NettyHost(group.next());
    private final io.zmux.internal.loop.SerialCallbacks completions = new io.zmux.internal.loop.SerialCallbacks();
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final Set<QuicChannel> connections = ConcurrentHashMap.newKeySet();
    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();
    private final Set<CompletableFuture<?>> pending = ConcurrentHashMap.newKeySet();
    private final QuicOptions options;
    private final ZmuxListener listener;
    private final QuicSslContext tls;
    /** One owned endpoint per address family; a pending future is the sole bind reservation. */
    private final ConcurrentHashMap<SocketProtocolFamily, CompletableFuture<Channel>> datagrams = new ConcurrentHashMap<>();

    /**
     * The TLS builder supplies trust and ALPN. Host verification is required; transport early
     * data is disabled, so a credential always travels in 1-RTT data.
     */
    public QuicClient(QuicSslContextBuilder tlsBuilder, QuicOptions options) throws IOException {
        this(tlsBuilder, options, null);
    }

    /** Binds incoming join/lane routing and session configuration to an application-owned listener. */
    public QuicClient(QuicSslContextBuilder tlsBuilder, QuicOptions options, ZmuxListener listener) throws IOException {
        this.options = options; this.listener = listener;
        try {
            tls = tlsBuilder.earlyData(false).endpointIdentificationAlgorithm("HTTPS").keylog(QuicExporter.KEYLOG).build();
            if (!tls.isClient()) throw new IllegalArgumentException("client TLS builder required");
            Channel ipv4 = bindDatagram(SocketProtocolFamily.INET).sync().channel();
            datagrams.put(SocketProtocolFamily.INET, CompletableFuture.completedFuture(ipv4));
        } catch (InterruptedException | RuntimeException error) {
            host.close(); group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS); workers.shutdownNow();
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new io.zmux.CarrierFailedException("could not bind QUIC endpoint", error);
        }
    }

    /**
     * Starts a zmux session on a fresh QUIC connection and its first bidirectional stream,
     * which starts with the prefix of {@link ZmuxConfig#carrierCredential()}.
     */
    public ZmuxFuture<ZmuxSession> connectAsync(InetSocketAddress address, String serverName, ZmuxConfig config) {
        if (listener != null) return future(CompletableFuture.failedFuture(new IllegalStateException("listener-backed clients use connect(address, serverName)")));
        java.util.Objects.requireNonNull(config);
        return future(start(address, serverName, config.carrierCredential().orElse(null), pipe -> {
            if (!config.alpn().contains(pipe.capabilities().applicationProtocol())) throw new javax.net.ssl.SSLHandshakeException("unexpected ALPN");
            return Zmux.open(pipe, Role.CLIENT, config);
        }));
    }

    /**
     * Starts a joinable session using the bound listener's configuration and routing; the
     * connection presents that configuration's {@link ZmuxConfig#carrierCredential()}.
     */
    public ZmuxFuture<ZmuxSession> connectAsync(InetSocketAddress address, String serverName) {
        if (listener == null) return future(CompletableFuture.failedFuture(new IllegalStateException("no listener bound to QUIC client")));
        return future(start(address, serverName, listener.config().carrierCredential().orElse(null), pipe -> listener.open(pipe, Role.CLIENT)));
    }

    @FunctionalInterface private interface SessionFactory { ZmuxSession start(PipeIo pipe) throws IOException; }
    /**
     * As {@link #connect(InetSocketAddress, String, ZmuxConfig)} with a layer that runs on the first stream before the
     * session does, such as an Establish ({@code p -> Seal.establish(p, cfg)}); the layer bounds its own time.
     */
    public ZmuxFuture<ZmuxSession> connectAsync(InetSocketAddress address, String serverName, ZmuxConfig config,
                                                java.util.function.Function<PipeIo, CompletionStage<PipeIo>> layer) {
        if (listener != null) return future(CompletableFuture.failedFuture(new IllegalStateException("listener-backed clients use connect(address, serverName)")));
        java.util.Objects.requireNonNull(config);
        java.util.Objects.requireNonNull(layer);
        return future(start(address, serverName, config.carrierCredential().orElse(null), pipe -> {
            try {
                return Zmux.open(layer.apply(pipe).toCompletableFuture().get(), Role.CLIENT, config);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new java.io.InterruptedIOException();
            } catch (java.util.concurrent.ExecutionException e) {
                throw e.getCause() instanceof io.zmux.ZmuxException z ? z : new io.zmux.CarrierFailedException("QUIC session setup failed", e.getCause());
            }
        }));
    }

    private CompletionStage<ZmuxSession> start(InetSocketAddress address, String serverName, io.zmux.CarrierCredential credential,
                                                 SessionFactory factory) {
        CompletableFuture<ZmuxSession> result = track(new CompletableFuture<>());
        CompletableFuture<ZmuxSession> notified = ConnectionCarrier.offLoop(result, completions, ZmuxSession::close);
        if (result.isDone()) return notified;
        CompletionStage<ConnectionCarrier> establishing = connectCarrier(address, serverName, credential, ConnectionCarrier.DEDICATED);
        result.whenComplete((value, failure) -> { if (failure != null) establishing.toCompletableFuture().cancel(false); });
        establishing.whenComplete((carrier, error) -> {
            if (error != null) { result.completeExceptionally(error); return; }
            result.whenComplete((value, failure) -> { if (failure != null) carrier.channel().close(); });
            if (result.isDone()) { carrier.channel().close(); return; }
            carrier.openPipe().whenComplete((pipe, failure) -> {
                if (failure != null) { carrier.channel().close(); result.completeExceptionally(failure); return; }
                Loops.run(workers, () -> {
                    ZmuxSession session = null;
                    try {
                        if (result.isDone()) { carrier.channel().close(); return; }
                        session = factory.start(pipe);
                        carrier.bind(session);
                        if (!result.complete(session)) session.close();
                    } catch (IOException | RuntimeException cause) {
                        if (session != null) session.close();
                        carrier.channel().close(); result.completeExceptionally(cause);
                    }
                }, () -> { carrier.channel().close(); result.completeExceptionally(new io.zmux.CarrierClientClosedException("QUIC endpoint closed")); });
            });
        });
        return notified;
    }

    /** Opens a pipe on a fresh connection for an existing session's addPipe call, without a credential. */
    public ZmuxFuture<PipeIo> dialAsync(InetSocketAddress address, String serverName, ZmuxSession owner) {
        return dialAsync(address, serverName, owner, null);
    }

    /**
     * Opens a pipe on a fresh connection for an existing session's addPipe call; the pipe, the
     * connection's first stream, starts with credential's prefix (null: none).
     */
    public ZmuxFuture<PipeIo> dialAsync(InetSocketAddress address, String serverName, ZmuxSession owner,
                                            io.zmux.CarrierCredential credential) {
        CompletableFuture<PipeIo> result = track(new CompletableFuture<>());
        CompletableFuture<PipeIo> notified = ConnectionCarrier.offLoop(result, completions, pipe -> {
            if (pipe.reader() != null) pipe.reader().close();
            if (pipe.writer() != null) pipe.writer().abort(new IOException("QUIC operation cancelled"));
        });
        if (result.isDone()) return future(notified);
        CompletionStage<ConnectionCarrier> establishing = connectCarrier(address, serverName, credential, ConnectionCarrier.DEDICATED);
        result.whenComplete((value, failure) -> { if (failure != null) establishing.toCompletableFuture().cancel(false); });
        establishing.whenComplete((carrier, failure) -> {
            if (failure != null) { result.completeExceptionally(failure); return; }
            result.whenComplete((value, error) -> { if (error != null) carrier.channel().close(); });
            if (result.isDone()) { carrier.channel().close(); return; }
            try { carrier.bind(java.util.Objects.requireNonNull(owner, "owner")); }
            catch (RuntimeException error) { carrier.channel().close(); result.completeExceptionally(error); return; }
            carrier.openPipe().whenComplete((pipe, error) -> {
                if (error != null) result.completeExceptionally(error);
                else if (!result.complete(pipe)) carrier.channel().close();
            });
        });
        return future(notified);
    }

    @SuppressWarnings("CollectionUndefinedEquality") // pending holds futures, which compare by identity
    private <T> CompletableFuture<T> track(CompletableFuture<T> result) {
        pending.add(result);
        result.whenComplete((value, error) -> pending.remove(result));
        if (closed.get()) result.completeExceptionally(new io.zmux.CarrierClientClosedException("QUIC endpoint closed"));
        return result;
    }

    /**
     * A new connection to address; its first bidirectional stream starts with credential's prefix
     * (null: none); closing tells who closes it once its last pipe ended.
     */
    CompletionStage<ConnectionCarrier> connectCarrier(InetSocketAddress address, String host, io.zmux.CarrierCredential credential, int closing) {
        CompletableFuture<ConnectionCarrier> result = track(new CompletableFuture<>());
        if (result.isDone()) return result;
        InetSocketAddress target;
        try { target = nativeAddress(address); }
        catch (RuntimeException error) { result.completeExceptionally(error); return result; }
        ConnectionCarrier carrier = new ConnectionCarrier(options, listener, null, this.host, completions, credential,
                literal(host) ? "" : host);
        carrier.closing(closing);
        result.whenComplete((value, error) -> { if (error != null) carrier.dispose(); });
        SocketProtocolFamily family = target.getAddress() instanceof java.net.Inet6Address
                ? SocketProtocolFamily.INET6 : SocketProtocolFamily.INET;
        datagram(family).whenComplete((endpoint, bindError) -> {
            if (bindError != null) { result.completeExceptionally(bindError); return; }
            if (result.isDone()) return;
            try { QuicChannel.newBootstrap(endpoint).attr(HOST, host).handler(carrier)
                    .streamHandler(new ChannelInitializer<QuicStreamChannel>() {
                        @Override protected void initChannel(QuicStreamChannel stream) { carrier.incoming(stream); }
                    }).remoteAddress(target).connect().addListener(done -> {
                        if (!done.isSuccess()) result.completeExceptionally(done.cause());
                        else {
                            QuicChannel connection = (QuicChannel) done.getNow(); connections.add(connection);
                            connection.closeFuture().addListener(ignored -> connections.remove(connection));
                            if (closed.get() || !result.complete(carrier)) connection.close();
                        }
                    }); } catch (RuntimeException error) { result.completeExceptionally(error); }
        });
        return result;
    }

    /** Whether name is an IP address literal, which TLS does not send as a server name. */
    private static boolean literal(String name) {
        return name.indexOf(':') >= 0 || name.chars().allMatch(c -> c == '.' || (c >= '0' && c <= '9'));
    }

    /** A mapped IPv4 address is still an IPv4 route; preserve genuine IPv6 scope IDs. */
    private static InetSocketAddress nativeAddress(InetSocketAddress address) {
        if (!(address.getAddress() instanceof java.net.Inet6Address ipv6)) return address;
        byte[] bytes = ipv6.getAddress();
        if (bytes[10] != (byte)0xff || bytes[11] != (byte)0xff) return address;
        for (int i=0;i<10;i++) if (bytes[i] != 0) return address;
        try { return new InetSocketAddress(java.net.InetAddress.getByAddress(java.util.Arrays.copyOfRange(bytes,12,16)),address.getPort()); }
        catch (java.net.UnknownHostException impossible) { throw new AssertionError(impossible); }
    }

    private ChannelFuture bindDatagram(SocketProtocolFamily family) {
        ChannelHandler codec = options.apply(new QuicClientCodecBuilder())
                .sslEngineProvider(connection -> {
                    InetSocketAddress remote = (InetSocketAddress) connection.remoteSocketAddress();
                    return tls.newEngine(connection.alloc(), connection.attr(HOST).get(), remote == null ? -1 : remote.getPort()); // -1: no port hint
                }).build();
        return new Bootstrap().group(group).channelFactory(() -> new NioDatagramChannel(family))
                .handler(codec).bind(0);
    }

    private CompletableFuture<Channel> datagram(SocketProtocolFamily family) {
        if (closed.get()) return CompletableFuture.failedFuture(new io.zmux.CarrierClientClosedException("QUIC endpoint closed"));
        var existing = datagrams.get(family);
        if (existing != null) return existing;
        var created = new CompletableFuture<Channel>();
        existing = datagrams.putIfAbsent(family, created);
        if (existing != null) return existing;
        if (closed.get()) { bindFailed(family,created,new io.zmux.CarrierClientClosedException("QUIC endpoint closed")); return created; }
        // Binding is outside the map publication; callbacks and close never wait under a lock.
        try { bindDatagram(family).addListener(done -> {
            if (!done.isSuccess()) bindFailed(family,created,done.cause());
            else {
                Channel channel = ((ChannelFuture)done).channel();
                if (closed.get()) { channel.close(); bindFailed(family,created,new io.zmux.CarrierClientClosedException("QUIC endpoint closed")); }
                else if (!created.complete(channel)) channel.close();
            }
        }); } catch (RuntimeException error) { bindFailed(family,created,error); }
        return created;
    }

    private void bindFailed(SocketProtocolFamily family, CompletableFuture<Channel> bind, Throwable error) {
        datagrams.remove(family,bind); // Failed creation must not poison subsequent dial attempts.
        bind.completeExceptionally(error);
    }

    boolean listenerBacked() { return listener != null; }

    EventLoop nativeLoop() { return group.next(); }

    NettyHost host() { return host; }

    /** The completion context of this endpoint's operations. */
    io.zmux.internal.loop.SerialCallbacks completions() { return completions; }

    /** A snapshot of native connections for transport-specific statistics and control. */
    public java.util.List<QuicChannel> connections() { return java.util.List.copyOf(connections); }

    /** Closes owned connections and cancels pending handshakes. Native callbacks never wait on their own loop. */
    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        for (CompletableFuture<?> operation : pending) operation.completeExceptionally(new io.zmux.CarrierClientClosedException("QUIC endpoint closed"));
        workers.shutdownNow();
        for (QuicChannel connection : connections) ConnectionCarrier.close(connection);
        for (var endpoint : datagrams.values()) endpoint.whenComplete((channel,error) -> { if (channel != null) channel.close(); });
        host.close();
        var termination = group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
        if (!nativeLoop().inEventLoop()) termination.awaitUninterruptibly();
    }

    /** stage as a ZmuxFuture; cancelling it cancels stage. */
    static <T> ZmuxFuture<T> future(CompletionStage<T> stage) {
        return io.zmux.internal.spi.Spi.future(stage, () -> stage.toCompletableFuture().cancel(false));
    }

    /** f's result, waiting for it. */
    static <T> T await(ZmuxFuture<T> f) throws IOException {
        try {
            return f.get();
        } catch (InterruptedException e) {
            f.cancel(false);
            Thread.currentThread().interrupt();
            throw new java.io.InterruptedIOException("interrupted");
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable c = e.getCause();
            if (c instanceof io.zmux.ZmuxException z) throw z;
            if (c instanceof RuntimeException r) throw r;
            throw new io.zmux.CarrierFailedException("QUIC operation failed", c);
        }
    }

    /** {@link #connectAsync(InetSocketAddress, String, ZmuxConfig)}, waiting for the session. */
    public ZmuxSession connect(InetSocketAddress address, String serverName, ZmuxConfig config) throws IOException {
        return await(connectAsync(address, serverName, config));
    }

    /** {@link #connectAsync(InetSocketAddress, String)}, waiting for the session. */
    public ZmuxSession connect(InetSocketAddress address, String serverName) throws IOException {
        return await(connectAsync(address, serverName));
    }

    /** {@link #connectAsync(InetSocketAddress, String, ZmuxConfig, java.util.function.Function)}, waiting for the session. */
    public ZmuxSession connect(InetSocketAddress address, String serverName, ZmuxConfig config,
                               java.util.function.Function<PipeIo, CompletionStage<PipeIo>> layer) throws IOException {
        return await(connectAsync(address, serverName, config, layer));
    }

    /** {@link #dialAsync(InetSocketAddress, String, ZmuxSession)}, waiting for the pipe input. */
    public PipeIo dial(InetSocketAddress address, String serverName, ZmuxSession owner) throws IOException {
        return await(dialAsync(address, serverName, owner));
    }

    /** {@link #dialAsync(InetSocketAddress, String, ZmuxSession, io.zmux.CarrierCredential)}, waiting for the pipe input. */
    public PipeIo dial(InetSocketAddress address, String serverName, ZmuxSession owner, io.zmux.CarrierCredential credential) throws IOException {
        return await(dialAsync(address, serverName, owner, credential));
    }
}
