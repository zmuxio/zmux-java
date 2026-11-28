package io.zmux.quic;

import static io.zmux.quic.internal.NettyLoops.getBorrowedEventLoop;

import io.zmux.internal.spi.Loops;
import io.netty.channel.*;
import io.netty.handler.codec.quic.*;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.zmux.ZmuxSession;
import io.zmux.ZmuxListener;
import io.zmux.carrier.PipeIo;
import io.zmux.internal.core.Time;
import io.zmux.quic.internal.QuicDatagrams;
import io.zmux.quic.internal.QuicHandshake;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Transport bookkeeping only. First-record classification and authentication belong to the zmux listener. */
final class ConnectionCarrier extends ChannelInboundHandlerAdapter {
    static final io.netty.util.AttributeKey<ConnectionCarrier> KEY = io.netty.util.AttributeKey.valueOf(ConnectionCarrier.class, "owner");
    static final io.netty.util.AttributeKey<Boolean> SERVED_ONCE = io.netty.util.AttributeKey.valueOf(ConnectionCarrier.class, "served");
    static final io.netty.util.AttributeKey<Boolean> HOST_OWNED = io.netty.util.AttributeKey.valueOf(ConnectionCarrier.class, "hostOwned");
    private final QuicOptions options;
    private final ZmuxListener listener;
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
    private QuicDatagrams datagrams;
    private Runnable releaseDatagrams;
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

    /** A server connection's carrier. */
    ConnectionCarrier(QuicOptions options, ZmuxListener listener, NettyHost host,
                      io.zmux.internal.loop.SerialCallbacks completions) {
        this(options, listener, host, completions, null, "", true);
    }

    /** A server connection whose handshake completed before it was handed over ({@link QuicServer#serve}). */
    static ConnectionCarrier established(QuicOptions options, ZmuxListener listener, NettyHost host,
                                         io.zmux.internal.loop.SerialCallbacks completions) {
        ConnectionCarrier c = new ConnectionCarrier(options, listener, host, completions);
        c.established = true;
        return c;
    }

    /** The handshake was done before the carrier was added. */
    private boolean established;
    /** Published before withdrawing setups, including those on a different session loop. */
    private volatile boolean stopped;
    private volatile boolean lanesStopped;
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private int bidirectionalPipes;
    private boolean admissionFinished;
    private boolean handingBack;
    private boolean ownershipReleased;
    private final java.util.Set<StreamCarrier> laneHeaders = new java.util.HashSet<>();

    private final java.util.Set<io.zmux.internal.spi.AdapterPorts.Setup> settingUp = java.util.concurrent.ConcurrentHashMap.newKeySet();

    private void track(StreamCarrier stream, io.zmux.adapter.Handled carrier) {
        var setup = io.zmux.internal.spi.AdapterPorts.setup(carrier);
        settingUp.add(setup);
        // route registered on the stream's loop before it can run the first-record task.
        // A stop racing registration either sees this setup or is visible here.
        if (stopped) setup.interrupt();
        withdrawOnClose(stream, setup);
        setup.onFinished(() -> {
            settingUp.remove(setup);
            if (stopped) Loops.run(getBorrowedEventLoop(channel), this::finishAdmission, () -> {});
        });
    }

    private static void withdrawOnClose(StreamCarrier stream, io.zmux.internal.spi.AdapterPorts.Setup setup) {
        var close = stream.stream().closeFuture();
        ChannelFutureListener withdrawn = ignored -> setup.interrupt();
        close.addListener(withdrawn);
        setup.onFinished(() -> close.removeListener(withdrawn));
    }

    /** Withdraws new pipes while the connection continues to serve attached pipes and lanes. */
    void stop(QuicChannel connection) {
        stopSetups();
        Loops.dispatch(getBorrowedEventLoop(connection), getBorrowedEventLoop(connection).inEventLoop(), () -> {
            releaseSlot();
            if (lanesStopped) {
                discardWaiting();
                discardLaneHeaders();
            } else {
                rejectHeldPipes(held);
                rejectHeldPipes(pending);
            }
            finishAdmission();
        }, () -> {}); // A stopped loop closes its channels.
    }

    void stopLanes(QuicChannel connection) {
        lanesStopped = true;
        stop(connection);
    }

    CompletionStage<Void> whenDrained() { return drained.minimalCompletionStage(); }

    private void releaseSlot() {
        var reserved = slot;
        slot = null;
        if (reserved != null) reserved.release();
    }

    private static void rejectHeldPipes(ArrayList<StreamCarrier> streams) {
        streams.removeIf(stream -> {
            if (stream.unidirectional()) return false;
            stream.dispose();
            return true;
        });
    }

    private void finishAdmission() {
        if (admissionFinished || !stopped || handingBack || pipes != 0 || !settingUp.isEmpty() || admission == ADMITTING) return;
        admissionFinished = true;
        discardWaiting();
        discardLaneHeaders();
        if (established) releaseConnection();
        else close(channel);
        drained.complete(null);
    }

    private void discardWaiting() {
        for (StreamCarrier stream : pending) stream.dispose();
        for (StreamCarrier stream : held) stream.dispose();
        pending.clear();
        held.clear();
    }

    private void discardLaneHeaders() {
        for (var lane : java.util.List.copyOf(laneHeaders)) {
            if (!lane.attached()) lane.dispose();
        }
    }

    /** A client connection's carrier: its first bidirectional stream starts with credential's prefix (null: none). */
    ConnectionCarrier(QuicOptions options, ZmuxListener listener, NettyHost host,
                      io.zmux.internal.loop.SerialCallbacks completions, io.zmux.CarrierCredential credential, String serverName) {
        this(options, listener, host, completions, credential, serverName, false);
    }

    private ConnectionCarrier(QuicOptions options, ZmuxListener listener, NettyHost host,
                              io.zmux.internal.loop.SerialCallbacks completions, io.zmux.CarrierCredential credential, String serverName,
                              boolean server) {
        this.server = server;
        this.options = options; this.listener = listener; this.host = host; this.completions = completions;
        this.credential = credential; this.serverName = serverName;
    }
    QuicChannel channel() { return channel; }
    QuicOptions options() { return options; }
    NettyHost host() { return host; }

    @Override public void handlerAdded(ChannelHandlerContext context) {
        channel = (QuicChannel) context.channel();
        if (!channel.attr(KEY).compareAndSet(null, this)) throw new IllegalStateException("QUIC connection already served");
        if (server) channel.attr(SERVED_ONCE).set(true);
        datagrams = QuicDatagrams.observe(channel);
        releaseDatagrams = datagrams.claim();
        if (disposed.get()) { channel.close(); return; }
        if (server && listener != null && !stopped) {
            // A server connection: its source's slot comes first, before the handshake.
            var peer = (java.net.InetSocketAddress) channel.remoteSocketAddress();
            slot = listener.adapter().reserve(peer, null);
            if (slot == null) { channel.close(); return; }
            awaitFirstStream(slot);
        }
        if (established && QuicHandshake.security(channel) == io.zmux.Admission.OuterSecurity.QUIC) {
            handshake = true;
            routePending();
        }
    }
    /** A reservation belongs to this connection only until it supplies the first admission stream. */
    private void awaitFirstStream(io.zmux.adapter.Slot reserved) {
        long limit = Time.nanos(listener.adapter().firstRecordTimeout());
        long handshake = listener.config().handshakeTimeout().map(Time::nanos).orElse(Time.NEVER);
        long delay = Math.min(limit, handshake);
        //noinspection resource: borrowed: the channel owns its event loop
        var timer = delay == Time.NEVER ? null : getBorrowedEventLoop(channel).schedule(() -> {
            if (slot == reserved) {
                handshakeFailed();
                channel.close();
            }
        }, delay, java.util.concurrent.TimeUnit.NANOSECONDS);
        io.zmux.internal.spi.AdapterPorts.reservation(reserved).onFinished(() ->
                Loops.run(getBorrowedEventLoop(channel), () -> {
                    Loops.cancel(timer);
                    if (slot == reserved) channel.close();
                }, () -> Loops.cancel(timer)));
    }

    @Override public void userEventTriggered(ChannelHandlerContext context, Object event) {
        if (event instanceof SslHandshakeCompletionEvent completed) {
            if (completed.isSuccess()) { handshake = true; routePending(); }
            else { handshakeFailed(); context.close(); }
        }
        context.fireUserEventTriggered(event);
    }
    private void stopSetups() {
        stopped = true;
        for (var setup : settingUp) setup.interrupt();
    }

    @Override public void channelInactive(ChannelHandlerContext context) {
        stopSetups();
        if (!handshake) handshakeFailed();
        else releaseSlot();
        admissionFinished = true;
        discardWaiting();
        drained.complete(null);
        context.fireChannelInactive();
    }
    @Override public void exceptionCaught(ChannelHandlerContext context, Throwable error) { context.close(); }

    /** The handshake of a server connection failed, timed out, or the connection ended before it completed. */
    private void handshakeFailed() {
        io.zmux.adapter.Slot s = slot;
        slot = null;
        if (s != null) s.handshakeFailed();
    }

    // ---- the datagram channel ----

    /** The largest channel datagram the connection sends now, 0 while datagrams are not negotiated both ways. */
    int maxDatagram() { return options.isDatagrams() && datagrams != null ? datagrams.capacity() : 0; }
    int datagramRoom() { return options.isDatagrams() && datagrams != null ? datagrams.room() : 0; }

    /** Sends one channel datagram and reports the carrier's acceptance. */
    void sendDatagram(byte[] datagram, long expiry, java.util.function.Consumer<io.zmux.carrier.CarrierChannel.Outcome> completed) {
        if (maxDatagram() == 0) { completed.accept(io.zmux.carrier.CarrierChannel.Outcome.REJECTED); return; }
        datagrams.send(datagram, -1, expiry, completed);
    }

    Runnable watchDatagrams(Runnable changed) { return datagrams.watch(changed); }

    /** Routes the channel datagrams for Pipe Stream ID id to sink until the returned stop runs; on the loop. */
    Runnable receiveDatagrams(long id, io.zmux.carrier.CarrierChannel.Sink sink) {
        return datagrams.register(id, sink::datagram);
    }

    /** The channel datagrams received on this connection that named no pipe attached to a session. */
    long channelDropped() { return datagrams.dropped(); }

    private void releaseConnection() {
        if (ownershipReleased) return;
        ownershipReleased = true;
        releaseDatagrams.run();
        channel.attr(KEY).compareAndSet(this, null);
        channel.pipeline().remove(this);
    }

    private void releaseHandback() {
        admissionFinished = true;
        handingBack = false;
        channel.attr(HOST_OWNED).set(true);
        releaseConnection();
        drained.complete(null);
    }

    boolean hostOwnsConnection() {
        if (admission == ACCEPTED || first == null || !first.detached()) return false;
        releaseHandback();
        return true;
    }

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
        if (stream.closeFuture().isDone()) return;
        pipes++;
        boolean bidi = stream.type() == QuicStreamType.BIDIRECTIONAL;
        if (bidi) bidirectionalPipes++;
        stream.closeFuture().addListener(done -> pipeEnded(bidi));
        if (bidi && listener != null && admission == ACCEPTED) routeHeld();
    }

    /**
     * A pipe's stream closed, which its transport does only once the session's CONNECTION_CLOSE
     * was delivered (the inbound side ended) or its linger expired. With the last one the side
     * that ended the session closes the connection, so that its QUIC close, with the session's
     * code, reaches a peer that is still open; a pool's connection stays for the pool. A
     * dedicated connection whose session the peer ended, or that carried no session to its end,
     * waits a little for the peer's close, then closes itself.
     */
    private void pipeEnded(boolean bidi) {
        if (bidi && --bidirectionalPipes == 0 && settingUp.isEmpty()) discardLaneHeaders();
        if (--pipes > 0) return;
        if (stopped) { finishAdmission(); return; }
        int who = closing;
        if (who == POOLED) return;
        if (lastLocal) { close(channel); return; }
        if (who == DEDICATED) {
            Loops.schedule(getBorrowedEventLoop(channel), () -> { if (pipes == 0) close(channel); },
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
        Loops.run(getBorrowedEventLoop(channel), () -> { if (listener == null) session = owner; handshake = true; routePending(); }, () -> {});
    }
    void incoming(QuicStreamChannel stream) {
        boolean bidi = stream.type() == QuicStreamType.BIDIRECTIONAL;
        if (admissionFinished || admission == REFUSED || (bidi ? stopped : lanesStopped)) { stream.close(); return; }
        var security = handshake ? QuicHandshake.observe(stream) : io.zmux.Admission.OuterSecurity.EARLY_DATA;
        StreamCarrier carrier = new StreamCarrier(stream, host, true, bidi, security);
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
        if (admissionFinished || admission == REFUSED) { carrier.dispose(); return; }
        if (carrier.unidirectional()) {
            if (lanesStopped) { carrier.dispose(); return; }
            if (listener == null || (admission == ACCEPTED && bidirectionalPipes != 0)) {
                routeLane(carrier);
            } else if (admission != ACCEPTED || !settingUp.isEmpty()) {
                held.add(carrier);
            } else {
                carrier.dispose();
            }
            return;
        }
        if (stopped || listener == null) { carrier.dispose(); return; }
        switch (admission) {
            case ACCEPTED -> track(carrier, first.sibling(carrier.io(this), io.zmux.JoinHint.inBand(peer())));
            case UNDECIDED -> {
                // The admission reads the peer's first bidirectional stream (ID 0 from a client, 1 from a
                // server), whichever of the peer's streams arrives first.
                if (carrier.stream().streamId() > 1) held.add(carrier);
                else admit(carrier);
            }
            default -> held.add(carrier);
        }
    }

    private void routeLane(StreamCarrier carrier) {
        if (listener == null) {
            io.zmux.Zmux.handleLane(carrier.io(this));
            return;
        }
        laneHeaders.add(carrier);
        var stream = carrier.stream();
        stream.closeFuture().addListener(closed -> laneHeaders.remove(carrier));
        carrier.taken.stage().whenComplete((taken, failed) ->
                Loops.dispatch(getBorrowedEventLoop(channel), getBorrowedEventLoop(channel).inEventLoop(), () -> laneHeaders.remove(carrier), () -> {}));
        // The listener still owns the first-record limit, authorization and routing. This
        // setup is independent of the set of bidirectional admissions that stop withdraws.
        var handled = first.sibling(carrier.io(this), io.zmux.JoinHint.inBand(peer()));
        withdrawOnClose(carrier, io.zmux.internal.spi.AdapterPorts.setup(handled));
    }

    private void routeHeld() {
        if (held.isEmpty()) return;
        var waiting = new ArrayList<>(held);
        held.clear();
        for (StreamCarrier stream : waiting) route(stream);
    }

    private java.net.InetSocketAddress peer() {
        return (java.net.InetSocketAddress) channel.remoteSocketAddress(); // null once the connection closed
    }

    /**
     * Admits the connection by the peer's first bidirectional stream, stream 0 of a client:
     * only once it is accepted are the connection's other
     * streams handed to the listener, without a new admission. A connection that is not accepted is closed (after a stall, when the stalled
     * stream ends); one given back goes with that stream to the listener's fallback ({@link QuicTakeover#connection}),
     * its other streams closed, and is closed once the fallback is done with the stream, unless the host detached it
     * ({@link io.zmux.Takeover#detach()}): the connection is then the host's.
     */
    // The handler comes from the listener adapter in another module, where the analyzer takes its result for null.
    @SuppressWarnings("ConstantValue")
    private void admit(StreamCarrier first) {
        admission = ADMITTING;
        QuicChannel connection = channel;
        io.zmux.adapter.Slot s = slot;
        slot = null;
        var admitted = s != null ? s.handle(first.io(this), io.zmux.JoinHint.inBand(s.peer()))
                : listener.adapter().handle(first.io(this), io.zmux.JoinHint.inBand(peer()));
        this.first = admitted;
        track(first, admitted);
        var decision = admitted.verdict();
        // A loop that refuses the verdict closes the connection and its held streams as it shuts down.
        decision.whenComplete((verdict, error) -> Loops.run(getBorrowedEventLoop(connection), () -> {
            if (verdict == io.zmux.AdmissionVerdict.ACCEPT) {
                admission = ACCEPTED;
                routeHeld();
                finishAdmission();
                return;
            }
            admission = REFUSED;
            discardWaiting();
            // A connection that carried no session closes with code 0.
            // A stalled stream, and one taken over, end before the connection does; a stream the host detached
            // leaves its connection to the host.
            if (verdict == io.zmux.AdmissionVerdict.STALL || verdict == io.zmux.AdmissionVerdict.FALLBACK || verdict == io.zmux.AdmissionVerdict.TAKEOVER) {
                handingBack = true;
                // A detached stream remains alive under the host, so waiting for its physical
                // close would keep the adapter attached to a connection it has already returned.
                io.zmux.internal.spi.AdapterPorts.setup(admitted).onFinished(() -> {
                    if (admitted.detached()) {
                        Loops.run(getBorrowedEventLoop(connection), this::releaseHandback, () -> {});
                    } else {
                        admitted.closed().whenComplete((v, e) -> Loops.run(getBorrowedEventLoop(connection), () -> {
                            handingBack = false;
                            if (!stopped) close(connection);
                            else finishAdmission();
                        }, () -> {}));
                    }
                });
            } else if (!stopped) close(connection);
            else finishAdmission();
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
        return new io.zmux.Admission.CarrierInfo(remote, remote, protocol == null ? "" : protocol, serverName, this::export, io.zmux.RequestHead.NONE)
                .withOuterSecurity(QuicHandshake.security(channel));
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
        if (stopped && type == QuicStreamType.BIDIRECTIONAL) {
            openingFailed(result, observer, new IOException("QUIC admission stopped"));
            return result;
        }
        try { channel.createStream(type, new ChannelInitializer<QuicStreamChannel>() {
            @Override protected void initChannel(QuicStreamChannel stream) {
                stream.pipeline().addLast(new StreamCarrier(stream, host, type == QuicStreamType.BIDIRECTIONAL, true));
                if (observer != null) observer.created(stream);
            }
        }).addListener(done -> {
            if (!done.isSuccess()) { openingFailed(result, observer, done.cause()); }
            else {
                QuicStreamChannel stream = (QuicStreamChannel) done.getNow();
                StreamCarrier handler = stream.pipeline().get(StreamCarrier.class);
                if (handler == null || !stream.isActive() || (stopped && type == QuicStreamType.BIDIRECTIONAL)) {
                    IOException error = new IOException("QUIC stream initialization failed");
                    stream.close(); openingFailed(result, observer, error);
                    return;
                }
                if (credential != null && type == QuicStreamType.BIDIRECTIONAL && stream.streamId() == 0) {
                    // The credential goes first on the stream the server's admission reads, in 1-RTT data.
                    byte[] prefix;
                    try { prefix = credential.prefix(clientInfo()); }
                    catch (IOException | RuntimeException error) {
                        stream.close(); openingFailed(result, observer, error);
                        return;
                    }
                    if (prefix != null && prefix.length > 0) {
                        result.whenComplete((io, failure) -> { if (result.isCancelled()) stream.close(); });
                        handler.write(io.zmux.carrier.OutboundBatch.of(java.nio.ByteBuffer.wrap(prefix)), cause -> {
                            if (cause != null) { stream.close(); openingFailed(result, observer, cause); }
                            else if (!result.complete(handler.io(this))) stream.close();
                        });
                        return;
                    }
                }
                if (!result.complete(handler.io(this))) stream.close();
            }
        }); } catch (RuntimeException error) { openingFailed(result, observer, error); }
        return result;
    }

    private static void openingFailed(CompletableFuture<PipeIo> result, StreamObserver observer, Throwable error) {
        if (observer != null) observer.failed(error);
        result.completeExceptionally(error);
    }
}
