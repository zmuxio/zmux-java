package io.zmux.quic;

import io.netty.channel.*;
import io.netty.handler.codec.quic.*;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.zmux.ZmuxSession;
import io.zmux.ZmuxListener;
import io.zmux.carrier.PipeIo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Transport bookkeeping only. First-record classification and authentication belong to the zmux listener. */
final class ConnectionCarrier extends ChannelInboundHandlerAdapter {
    static final io.netty.util.AttributeKey<ConnectionCarrier> KEY = io.netty.util.AttributeKey.valueOf(ConnectionCarrier.class, "owner");
    private final QuicOptions options;
    private final ZmuxListener listener;
    private final QuicServer.Fallback fallback;
    /** The host of the sessions on this connection's streams: the connection's own loop. */
    private final NettyHost host;
    private final io.zmux.internal.loop.SerialCallbacks completions;
    private final ArrayList<StreamCarrier> pending = new ArrayList<>();
    /** Streams that wait for the admission of the connection's first bidirectional stream. */
    private final ArrayList<StreamCarrier> held = new ArrayList<>();
    /** The admission of the connection: none yet, under way, accepted, or refused. */
    private int admission;
    private static final int UNDECIDED = 0, ADMITTING = 1, ACCEPTED = 2, REFUSED = 3;
    private volatile QuicChannel channel;
    private final java.util.concurrent.atomic.AtomicBoolean disposed = new java.util.concurrent.atomic.AtomicBoolean();
    private ZmuxSession session;
    private boolean handshake;
    /** A server connection's source slot, taken when it is accepted, before its handshake; used by its first stream. */
    private ZmuxListener.Slot slot;
    /** An accepted (server) connection. */
    private final boolean server;
    /** The handover of the admitted first stream, through which the connection's further streams go. */
    private ZmuxListener.Handled first;
    /** A client's credential, presented on the connection's first bidirectional stream; null: none. */
    private final io.zmux.Credential credential;
    /** A client's server name, as it sent it; "" when none. */
    private final String serverName;
    /** The largest QUIC datagram payload the connection sends; 0 until both negotiated datagrams. */
    private volatile int maxDatagram;
    /** The pipes of the connection's datagram channel by Pipe Stream ID. */
    private final io.zmux.internal.loop.ChannelDemux demux = new io.zmux.internal.loop.ChannelDemux();

    /** A server connection's carrier. */
    ConnectionCarrier(QuicOptions options, ZmuxListener listener, QuicServer.Fallback fallback, NettyHost host,
                      io.zmux.internal.loop.SerialCallbacks completions) {
        this(options, listener, fallback, host, completions, null, "", true);
    }

    /** A server connection whose handshake completed before it was handed over ({@link QuicServer#serve}). */
    static ConnectionCarrier established(QuicOptions options, ZmuxListener listener, QuicServer.Fallback fallback, NettyHost host,
                                         io.zmux.internal.loop.SerialCallbacks completions) {
        ConnectionCarrier c = new ConnectionCarrier(options, listener, fallback, host, completions);
        c.established = true;
        return c;
    }

    /** The handshake was done before the carrier was added. */
    private boolean established;
    /** No new stream goes to the listener any more ({@link QuicServer#serve} cancelled); loop only. */
    private boolean stopped;

    /** Stops handing the connection's new streams to the listener; they are closed. Any thread. */
    void stop() { ChannelLoops.executor(channel).execute(() -> stopped = true); }

    /** A client connection's carrier: its first bidirectional stream starts with credential's prefix (null: none). */
    ConnectionCarrier(QuicOptions options, ZmuxListener listener, QuicServer.Fallback fallback, NettyHost host,
                      io.zmux.internal.loop.SerialCallbacks completions, io.zmux.Credential credential, String serverName) {
        this(options, listener, fallback, host, completions, credential, serverName, false);
    }

    private ConnectionCarrier(QuicOptions options, ZmuxListener listener, QuicServer.Fallback fallback, NettyHost host,
                              io.zmux.internal.loop.SerialCallbacks completions, io.zmux.Credential credential, String serverName,
                              boolean server) {
        this.server = server;
        this.options = options; this.listener = listener; this.fallback = fallback; this.host = host; this.completions = completions;
        this.credential = credential; this.serverName = serverName;
    }
    QuicChannel channel() { return channel; }
    QuicOptions options() { return options; }
    NettyHost host() { return host; }

    @Override public void handlerAdded(ChannelHandlerContext context) {
        channel = (QuicChannel) context.channel(); channel.attr(KEY).set(this);
        if (disposed.get()) { channel.close(); return; }
        if (server && listener != null) {
            // A server connection: its source's slot comes first, before the handshake.
            var peer = (java.net.InetSocketAddress) channel.remoteSocketAddress();
            slot = listener.reserve(peer, null);
            if (slot == null) { channel.close(); return; }
        }
        if (established) { handshake = true; routePending(); }
    }
    @Override public void userEventTriggered(ChannelHandlerContext context, Object event) {
        if (event instanceof QuicDatagramExtensionEvent datagrams && options.datagrams()) maxDatagram = Math.max(0, datagrams.maxLength());
        if (event instanceof SslHandshakeCompletionEvent completed) {
            if (completed.isSuccess()) { handshake = true; routePending(); }
            else { handshakeFailed(); context.close(); }
        }
        context.fireUserEventTriggered(event);
    }
    @Override public void channelInactive(ChannelHandlerContext context) {
        if (!handshake) handshakeFailed();
        else if (slot != null) { slot.release(); slot = null; } // no first stream came: the slot goes
        for (StreamCarrier stream : pending) stream.dispose();
        for (StreamCarrier stream : held) stream.dispose();
        pending.clear(); held.clear(); context.fireChannelInactive();
    }
    @Override public void exceptionCaught(ChannelHandlerContext context, Throwable error) { context.close(); }

    /** The handshake of a server connection failed, timed out, or the connection ended before it completed. */
    private void handshakeFailed() {
        ZmuxListener.Slot s = slot;
        slot = null;
        if (s != null) s.handshakeFailed();
    }

    // ---- the datagram channel (EXTENSIONS.md Section 6.8) ----

    /**
     * A QUIC datagram of the connection: a channel datagram, routed by its Pipe Stream ID to
     * the pipe on this connection it names, whichever session that pipe belongs to.
     */
    @Override public void channelRead(ChannelHandlerContext context, Object message) {
        if (!(message instanceof io.netty.buffer.ByteBuf datagram)) { context.fireChannelRead(message); return; }
        try {
            java.nio.ByteBuffer b = datagram.nioBuffer();
            demux.deliver(b, b.position(), b.limit());
        } finally {
            datagram.release();
        }
    }

    /** The connection's lost bytes as last collected (-1: not yet), and when a collection was last asked for. */
    private volatile long lostBytes = -1;
    private final java.util.concurrent.atomic.AtomicLong lostAskedAt = new java.util.concurrent.atomic.AtomicLong(Long.MIN_VALUE);

    /**
     * The connection's lost bytes (the QUIC stack's retransmission count), as last collected,
     * -1 before the first; asks for a fresh count at most once a second, without waiting.
     */
    long lostBytes() {
        QuicChannel current = channel;
        long now = System.nanoTime(), at = lostAskedAt.get();
        if (current != null && current.isActive() && (at == Long.MIN_VALUE || now - at >= 1_000_000_000L)
                && lostAskedAt.compareAndSet(at, now)) {
            current.collectStats().addListener(done -> {
                if (done.isSuccess()) lostBytes = ((QuicConnectionStats) done.getNow()).lostBytes();
            });
        }
        return lostBytes;
    }

    /** The largest channel datagram the connection sends now, 0 while datagrams are not negotiated both ways. */
    int maxDatagram() { return maxDatagram; }

    /** Sends a channel datagram; refused runs on the loop when the connection refuses it. */
    void sendDatagram(byte[] datagram, Runnable refused) {
        QuicChannel current = channel;
        if (current == null || !current.isActive()) { refused.run(); return; }
        current.writeAndFlush(io.netty.buffer.Unpooled.wrappedBuffer(datagram))
                .addListener(done -> { if (!done.isSuccess()) refused.run(); });
    }

    /** Routes the channel datagrams for Pipe Stream ID id to sink until the returned stop runs; on the loop. */
    Runnable receiveDatagrams(long id, io.zmux.internal.loop.CarrierChannel.Sink sink) {
        return demux.register(id, sink);
    }

    /** The channel datagrams received on this connection that named no pipe attached to a session. */
    long channelDropped() { return demux.dropped(); }

    void dispose() {
        disposed.set(true);
        QuicChannel current = channel;
        if (current != null) current.close();
    }

    void bind(ZmuxSession owner) {
        ChannelLoops.executor(channel).execute(() -> { if (listener == null) session = owner; handshake = true; routePending(); });
    }
    void incoming(QuicStreamChannel stream) {
        boolean bidi = stream.type() == QuicStreamType.BIDIRECTIONAL;
        StreamCarrier carrier = new StreamCarrier(stream, host, true, bidi);
        stream.pipeline().addLast(carrier);
        if (!handshake || listener == null && session == null) pending.add(carrier);
        else route(carrier);
    }
    private void routePending() {
        if (!handshake || listener == null && session == null) return;
        for (StreamCarrier carrier : pending) route(carrier);
        pending.clear();
    }
    private void route(StreamCarrier carrier) {
        if (stopped) { carrier.dispose(); return; }
        if (listener == null) {
            if (carrier.unidirectional()) session.handleLane(carrier.io(this));
            else carrier.dispose();
            return;
        }
        switch (admission) {
            case ACCEPTED -> first.sibling(carrier.io(this), io.zmux.JoinHint.inBand(peer()));
            case REFUSED -> carrier.dispose();
            case UNDECIDED -> {
                // The admission reads the peer's first bidirectional stream (ID 0 from a client, 1 from a
                // server), whichever of the peer's streams arrives first.
                if (carrier.unidirectional() || carrier.stream().streamId() > 1) held.add(carrier);
                else admit(carrier);
            }
            default -> held.add(carrier);
        }
    }

    private java.net.InetSocketAddress peer() {
        return (java.net.InetSocketAddress) channel.remoteSocketAddress(); // null once the connection closed
    }

    /**
     * Admits the connection by the peer's first bidirectional stream, stream 0 of a client:
     * only once it is accepted are the connection's other
     * streams handed to the listener, without a new admission. A connection that is not accepted is closed (after a stall, when the stalled
     * stream ends); a fallback target takes it with that stream, and its other streams are
     * closed.
     */
    private void admit(StreamCarrier first) {
        admission = ADMITTING;
        QuicChannel connection = channel;
        QuicServer.Fallback target = fallback;
        ZmuxListener.Slot s = slot;
        slot = null;
        io.zmux.Admission.Fallback fb = target == null ? null : (stream, info) -> target.serve(connection, stream);
        var admitted = s != null ? s.handle(first.io(this), io.zmux.JoinHint.inBand(s.peer()), fb)
                : listener.handle(first.io(this), io.zmux.JoinHint.inBand(peer()), fb);
        this.first = admitted;
        admitted.verdict().whenComplete((verdict, error) -> ChannelLoops.executor(connection).execute(() -> {
            if (verdict == io.zmux.Admission.Verdict.ACCEPT) {
                admission = ACCEPTED;
                var waiting = new ArrayList<>(held);
                held.clear();
                for (StreamCarrier stream : waiting) route(stream);
                return;
            }
            admission = REFUSED;
            for (StreamCarrier stream : held) stream.dispose();
            held.clear();
            if (verdict == io.zmux.Admission.Verdict.STALL) admitted.closed().whenComplete((v, e) -> connection.close());
            else if (verdict != io.zmux.Admission.Verdict.FALLBACK) connection.close();
        }));
    }
    /**
     * The application's view of an endpoint operation: completed in the endpoint's serial
     * completion context, never on the loop that hosts its sessions (callbacks may block).
     * A result nobody takes any more (the view was cancelled) is discarded.
     */
    static <T> CompletableFuture<T> offLoop(CompletableFuture<T> operation, io.zmux.internal.loop.SerialCallbacks completions,
                                            java.util.function.Consumer<T> discard) {
        CompletableFuture<T> notification = new CompletableFuture<>();
        operation.whenComplete((value, error) -> completions.execute(() -> {
            if (error != null) notification.completeExceptionally(error);
            else if (!notification.complete(value)) discard.accept(value);
        }));
        notification.whenComplete((value, error) -> { if (notification.isCancelled()) operation.cancel(false); });
        return notification;
    }

    CompletionStage<PipeIo> openApplicationPipe() {
        return offLoop(open(QuicStreamType.BIDIRECTIONAL).toCompletableFuture(), completions, pipe -> {
            try { if (pipe.reader() != null) pipe.reader().close(); }
            finally { if (pipe.writer() != null) pipe.writer().abort(new IOException("QUIC operation cancelled")); }
        });
    }

    CompletionStage<PipeIo> openPipe() { return open(QuicStreamType.BIDIRECTIONAL); }

    /** The client's view of this connection for its credential: no exporter (the QUIC handshake offers none here). */
    private io.zmux.Admission.CarrierInfo clientInfo() {
        var remote = (java.net.InetSocketAddress) channel.remoteSocketAddress();
        String protocol = applicationProtocol();
        return new io.zmux.Admission.CarrierInfo(remote, remote, protocol == null ? "" : protocol, serverName, null, io.zmux.RequestHead.NONE);
    }
    /** The negotiated ALPN protocol; null without an engine or a negotiated protocol. */
    String applicationProtocol() {
        javax.net.ssl.SSLEngine engine = channel.sslEngine();
        return engine == null ? null : engine.getApplicationProtocol();
    }
    CompletionStage<PipeIo> openLane() { return open(QuicStreamType.UNIDIRECTIONAL); }
    interface StreamObserver {
        void created(QuicStreamChannel stream);
        void failed(Throwable cause);
    }
    CompletionStage<PipeIo> openManagedPipe(StreamObserver observer) { return open(QuicStreamType.BIDIRECTIONAL, observer); }
    private CompletionStage<PipeIo> open(QuicStreamType type) { return open(type, null); }
    private CompletionStage<PipeIo> open(QuicStreamType type, StreamObserver observer) {
        CompletableFuture<PipeIo> result = new CompletableFuture<>();
        try { channel.createStream(type, new ChannelInitializer<QuicStreamChannel>() {
            @Override protected void initChannel(QuicStreamChannel stream) {
                stream.pipeline().addLast(new StreamCarrier(stream, host, type == QuicStreamType.BIDIRECTIONAL, true));
                if (observer != null) observer.created(stream);
            }
        }).addListener(done -> {
            if (!done.isSuccess()) { if (observer != null) observer.failed(done.cause()); result.completeExceptionally(done.cause()); }
            else {
                QuicStreamChannel stream = (QuicStreamChannel) done.getNow();
                StreamCarrier handler = stream.pipeline().get(StreamCarrier.class);
                if (handler == null || !stream.isActive()) {
                    IOException error = new IOException("QUIC stream initialization failed");
                    if (observer != null) observer.failed(error);
                    stream.close(); result.completeExceptionally(error);
                    return;
                }
                if (credential != null && type == QuicStreamType.BIDIRECTIONAL && stream.streamId() == 0) {
                    // The credential goes first on the stream the server's admission reads, in 1-RTT data.
                    byte[] prefix;
                    try { prefix = credential.prefix(clientInfo()); }
                    catch (IOException | RuntimeException error) {
                        stream.close(); result.completeExceptionally(error);
                        return;
                    }
                    if (prefix != null && prefix.length > 0)
                        handler.write(io.zmux.carrier.OutboundBatch.of(java.nio.ByteBuffer.wrap(prefix)), cause -> {
                            if (cause != null) { stream.close(); result.completeExceptionally(cause); }
                        });
                }
                if (!result.complete(handler.io(this))) stream.close();
            }
        }); } catch (RuntimeException error) { if (observer != null) observer.failed(error); result.completeExceptionally(error); }
        return result;
    }
}
