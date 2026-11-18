package io.zmux.quic;

import io.zmux.internal.spi.Loops;
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
    private io.zmux.adapter.Slot slot;
    /** An accepted (server) connection. */
    private final boolean server;
    /** The handover of the admitted first stream, through which the connection's further streams go. */
    private io.zmux.adapter.Handled first;
    /** A client's credential, presented on the connection's first bidirectional stream; null: none. */
    private final io.zmux.CarrierCredential credential;
    /** A client's server name, as it sent it; "" when none. */
    private final String serverName;
    /** The largest QUIC datagram payload the connection sends; 0 until both negotiated datagrams. */
    private volatile int maxDatagram;
    /** The connection's streams that a session took and that have not closed yet; loop only. */
    private int pipes;
    /** The application code of the last session that ended on the connection (0 for an end without one); loop only. */
    private volatile long lastCode;
    /** Whether that session ended locally (the side that closed it closes the connection); loop only. */
    private boolean lastLocal;
    /**
     * What the connection is, for its close once its last pipe ended: {@link #SERVED} (a
     * server connection), {@link #DEDICATED} (a client connection of its own) or
     * {@link #POOLED} (a pool's, which the pool closes).
     */
    private volatile int closing;
    static final int SERVED = 0, DEDICATED = 1, POOLED = 2;
    /** How long a dedicated connection whose session the peer ended waits for the peer's QUIC close. */
    private static final long PEER_CLOSE_WAIT_MILLIS = 2_000;

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
    void stop() { Loops.run(channel.eventLoop(), () -> stopped = true, () -> {}); } // a refused stop: the loop closes the connection

    /** A client connection's carrier: its first bidirectional stream starts with credential's prefix (null: none). */
    ConnectionCarrier(QuicOptions options, ZmuxListener listener, QuicServer.Fallback fallback, NettyHost host,
                      io.zmux.internal.loop.SerialCallbacks completions, io.zmux.CarrierCredential credential, String serverName) {
        this(options, listener, fallback, host, completions, credential, serverName, false);
    }

    private ConnectionCarrier(QuicOptions options, ZmuxListener listener, QuicServer.Fallback fallback, NettyHost host,
                              io.zmux.internal.loop.SerialCallbacks completions, io.zmux.CarrierCredential credential, String serverName,
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
            slot = listener.adapter().reserve(peer, null);
            if (slot == null) { channel.close(); return; }
        }
        if (established) { handshake = true; routePending(); }
    }
    @Override public void userEventTriggered(ChannelHandlerContext context, Object event) {
        if (event instanceof QuicDatagramExtensionEvent datagrams && options.isDatagrams()) maxDatagram = Math.max(0, datagrams.maxLength());
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
        io.zmux.adapter.Slot s = slot;
        slot = null;
        if (s != null) s.handshakeFailed();
    }

    // ---- the datagram channel ----

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
        if (current != null) close(current);
    }

    // ---- the connection's end ----

    /** Sets who closes the connection once its last pipe ended ({@link #DEDICATED}, {@link #POOLED}). */
    void closing(int who) { closing = who; }

    /** A session took stream: one more pipe until it closes. On the loop. */
    void pipeStarted(QuicStreamChannel stream) {
        pipes++;
        stream.closeFuture().addListener(done -> pipeEnded());
    }

    /**
     * A pipe's stream closed, which its transport does only once the session's CONNECTION_CLOSE
     * was delivered (the inbound side ended) or its linger expired. With the last one the side
     * that ended the session closes the connection, so that its QUIC close, with the session's
     * code, reaches a peer that is still open; a pool's connection stays for the pool. A
     * dedicated connection whose session the peer ended, or that carried no session to its end,
     * waits a little for the peer's close, then closes itself.
     */
    private void pipeEnded() {
        if (--pipes > 0) return;
        int who = closing;
        if (who == POOLED) return;
        if (lastLocal) { close(channel); return; }
        if (who == DEDICATED) {
            Loops.schedule(channel.eventLoop(), () -> { if (pipes == 0) close(channel); },
                    PEER_CLOSE_WAIT_MILLIS, java.util.concurrent.TimeUnit.MILLISECONDS, () -> close(channel));
        }
    }

    /** A session on one of the connection's pipes ended with e. On the loop. */
    void sessionEnded(io.zmux.internal.core.SessionError e) {
        lastCode = e != null && e.kind() == io.zmux.internal.core.SessionError.Kind.APPLICATION ? e.code() : 0;
        lastLocal = e != null && !e.remote();
    }

    /**
     * Closes a connection with the application code of the last session it carried (0 when it
     * carried none, or that one ended without an application code). The QUIC API takes a
     * 32-bit code: a code above 2<sup>31</sup> - 1 goes out as 0.
     */
    static void close(QuicChannel connection) {
        ConnectionCarrier c = connection.attr(KEY).get();
        long code = c == null ? 0 : c.lastCode;
        connection.close(true, code <= Integer.MAX_VALUE ? (int) code : 0, io.netty.buffer.Unpooled.EMPTY_BUFFER);
    }

    void bind(ZmuxSession owner) {
        // Refused: the loop is shutting down and closes the connection, which fails the session's pipe.
        Loops.run(channel.eventLoop(), () -> { if (listener == null) session = owner; handshake = true; routePending(); }, () -> {});
    }
    void incoming(QuicStreamChannel stream) {
        boolean bidi = stream.type() == QuicStreamType.BIDIRECTIONAL;
        StreamCarrier carrier = new StreamCarrier(stream, host, true, bidi);
        stream.pipeline().addLast(carrier);
        if (!handshake || (listener == null && session == null)) pending.add(carrier);
        else route(carrier);
    }
    private void routePending() {
        if (!handshake || (listener == null && session == null)) return;
        for (StreamCarrier carrier : pending) route(carrier);
        pending.clear();
    }
    private void route(StreamCarrier carrier) {
        if (stopped) { carrier.dispose(); return; }
        if (listener == null) {
            if (carrier.unidirectional()) io.zmux.Zmux.handleLane(carrier.io(this));
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
    // The handler comes from the listener adapter in another module, where the analyzer takes its result for null.
    @SuppressWarnings("ConstantValue")
    private void admit(StreamCarrier first) {
        admission = ADMITTING;
        QuicChannel connection = channel;
        QuicServer.Fallback target = fallback;
        io.zmux.adapter.Slot s = slot;
        slot = null;
        io.zmux.Admission.Fallback fb = target == null ? null : (stream, info) -> target.serve(connection, stream);
        var admitted = s != null ? s.handle(first.io(this), io.zmux.JoinHint.inBand(s.peer()), fb)
                : listener.adapter().handle(first.io(this), io.zmux.JoinHint.inBand(peer()), fb);
        this.first = admitted;
        var decision = admitted.verdict();
        // A loop that refuses the verdict closes the connection and its held streams as it shuts down.
        decision.whenComplete((verdict, error) -> Loops.run(connection.eventLoop(), () -> {
            if (verdict == io.zmux.AdmissionVerdict.ACCEPT) {
                admission = ACCEPTED;
                var waiting = new ArrayList<>(held);
                held.clear();
                for (StreamCarrier stream : waiting) route(stream);
                return;
            }
            admission = REFUSED;
            for (StreamCarrier stream : held) stream.dispose();
            held.clear();
            // A connection that carried no session closes with code 0.
            if (verdict == io.zmux.AdmissionVerdict.STALL) admitted.closed().whenComplete((v, e) -> close(connection));
            else if (verdict != io.zmux.AdmissionVerdict.FALLBACK) close(connection);
        }, () -> {}));
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

    /** The exporter of this connection's handshake; null until it completes. */
    byte[] export(String label, byte[] context, int length) { return QuicExporter.export(channel.sslEngine(), label, context, length); }

    /** The client's view of this connection for its credential. */
    private io.zmux.Admission.CarrierInfo clientInfo() {
        var remote = (java.net.InetSocketAddress) channel.remoteSocketAddress();
        String protocol = applicationProtocol();
        return new io.zmux.Admission.CarrierInfo(remote, remote, protocol == null ? "" : protocol, serverName, this::export, io.zmux.RequestHead.NONE);
    }
    /** The negotiated ALPN protocol; null without an engine or a negotiated protocol. */
    String applicationProtocol() {
        javax.net.ssl.SSLEngine engine = channel.sslEngine();
        return engine == null ? null : engine.getApplicationProtocol();
    }
    CompletionStage<io.zmux.carrier.PipeWriter> openLane() { return open(QuicStreamType.UNIDIRECTIONAL).thenApply(PipeIo::writer); }
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
