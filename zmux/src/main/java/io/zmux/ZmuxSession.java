package io.zmux;

import io.zmux.carrier.PipeIo;
import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.Event;
import io.zmux.internal.core.Session;
import io.zmux.internal.core.SessionState;
import io.zmux.internal.core.Stream;
import io.zmux.internal.loop.Bootstrap;
import io.zmux.internal.loop.Operation;
import io.zmux.internal.loop.LaneTable;
import io.zmux.internal.loop.SessionRuntime;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import javax.net.ssl.SSLEngine;

/**
 * A zmux session: streams and datagrams multiplexed over its pipes. All methods are
 * thread-safe. Blocking methods park the calling thread (virtual threads are a good fit)
 * and must not be called on a zmux event-loop thread; {@link #close()} blocks at most one
 * second, for the hand-off of CONNECTION_CLOSE, and there only starts the close.
 *
 * <p>The futures of a session's asynchronous operations complete one at a time, in the order
 * the session settled them, on one callback thread of the session (never an event loop).
 * A dependent stage that blocks there delays the session's later completions; never wait
 * there for another future of the same session, and use the {@code *Async} stage variants
 * with an executor of your own for work that blocks. {@code cancel} never waits: it returns
 * true when no attempt of the operation can run any more, false when an outcome was already
 * committed.
 */
public final class ZmuxSession implements AutoCloseable {
    final SessionRuntime driver;
    final Session core;
    private final String applicationProtocol;
    /** Presented by the TCP and TLS pipes this session dials ({@link ZmuxConfig#credential()}); null: none. */
    private final Credential credential;
    private volatile Boolean designatedDialer;
    /** The carrier selection report ({@link #reportCarrier}); guarded by itself. */
    private final java.util.ArrayList<CandidateResult> carrierTries = new java.util.ArrayList<>(0);
    private CarrierKind carrier;
    private String carrierVariant = "";

    /** The application's designation of the bootstrap dialer, or null. */
    Boolean designatedDialer() { return designatedDialer; }

    /**
     * Supplies the {@link PipeAuth#deferred() deferred PSK}, at least 16 bytes, once: the join keys
     * are derived from it (with the exporter when both are selected) and the peer's held joins go
     * on. Before establishment it is kept for the negotiation, and dropped and cleared when that
     * does not select it. The caller keeps psk.
     *
     * @throws IllegalArgumentException without a deferred PSK or for a shorter PSK
     * @throws JoinSecretSetException when it was supplied already
     * @throws NotNegotiatedException after an establishment that did not select the PSK
     */
    public void setJoinSecret(byte[] psk) throws IOException {
        if (psk == null || psk.length < 16) throw new IllegalArgumentException("PSK shorter than 16 bytes");
        if (!core.config().deferredPsk()) throw new IllegalArgumentException("setJoinSecret without a deferred PSK");
        byte[] copy = psk.clone();
        try {
            run(op -> {
                driver.setJoinSecret(copy);
                return Boolean.TRUE;
            });
        } finally {
            java.util.Arrays.fill(copy, (byte) 0);
        }
    }

    /**
     * Keying material (RFC 5705, RFC 8446 Section 7.5) of the innermost end-to-end handshake of
     * the bootstrap carrier, its {@link io.zmux.carrier.PipeIo.Capabilities#exporter() exporter};
     * for a split bootstrap over two connections the uplink's value followed by the downlink's,
     * 2 × length bytes. Both ends obtain the same value.
     *
     * @throws IllegalArgumentException for a negative length or a label of zmux's own ({@code EXPORTER-zmux-})
     * @throws NoExporterException when the carrier has none
     * @throws IOException while its handshake is incomplete
     */
    public byte[] exportKeyingMaterial(String label, byte[] context, int length) throws IOException {
        java.util.Objects.requireNonNull(label, "label");
        if (label.startsWith("EXPORTER-zmux-")) throw new IllegalArgumentException("exporter label " + label + " is reserved");
        if (length < 0) throw new IllegalArgumentException("negative exporter length");
        if (!driver.bootHasExporter()) throw new NoExporterException();
        byte[] v = driver.exportKeyingMaterial(label, context, length);
        if (v == null) throw new IOException("the carrier's handshake is incomplete");
        return v;
    }

    /**
     * A 32-byte value that identifies this session, the same at both ends, to include in an
     * authentication transcript; public, no secret. Empty before
     * establishment and when multipath is not in use.
     */
    public Optional<byte[]> sessionBinding() throws IOException {
        return Optional.ofNullable((byte[]) run(op -> core.sessionBinding()));
    }

    /** The session's liveness settings in effect: setting them changes nothing. */
    public Liveness liveness() throws IOException {
        return (Liveness) run(op -> {
            var l = core.config().live();
            return Liveness.of(l.keepAlive() < 0 ? null : Duration.ofNanos(l.keepAlive()), Duration.ofNanos(l.suspectFloor()),
                    l.deadTimeout() <= 0 ? null : Duration.ofNanos(l.deadTimeout()),
                    l.standbyProbe() <= 0 ? null : Duration.ofNanos(l.standbyProbe()), l.probeInterval() <= 0 ? null : Duration.ofNanos(l.probeInterval()),
                    l.probeJitter());
        });
    }

    /**
     * Replaces the session's liveness settings as a whole, under the rules of the configuration:
     * a setting not written is its default, not the value the session started with. The timers rearm at once; a
     * pipe already suspect keeps the floor and dead timeout it turned suspect under; a pipe's own
     * settings keep winning. Nothing is sent.
     */
    public void setLiveness(Liveness l) throws IOException {
        java.util.Objects.requireNonNull(l, "l");
        var v = new io.zmux.internal.core.CoreConfig.Live(
                l.keepAlive().map(ZmuxConfig::nanos).orElse(io.zmux.internal.core.CoreConfig.OFF),
                ZmuxConfig.nanos(l.suspectFloor()),
                l.deadTimeout().map(ZmuxConfig::nanos).orElse(0L),
                l.standbyProbe().map(ZmuxConfig::nanos).orElse(0L),
                l.probeInterval().map(ZmuxConfig::nanos).orElse(0L),
                l.probeJitter());
        run(op -> { core.setLiveness(v); return Boolean.TRUE; });
    }

    /**
     * Raises the bidirectional and unidirectional streams the peer may have open while the
     * session runs, and sends the credit. It only grows: a value at or below the
     * current one leaves that type as it is, so 0 raises nothing. While unidirectional credit is
     * staged, uni raises the value the first accept moves it to. After GOAWAY no credit is sent.
     *
     * @throws IllegalArgumentException outside [0, 2^60]
     */
    public void raiseIncomingStreams(long bidi, long uni) throws IOException {
        if (bidi < 0 || uni < 0 || bidi > 1L << 60 || uni > 1L << 60) throw new IllegalArgumentException("raiseIncomingStreams outside [0, 2^60]");
        run(op -> {
            core.raiseIncomingStreams(true, bidi);
            core.raiseIncomingStreams(false, uni);
            return Boolean.TRUE;
        });
    }

    /** The session's shaping readings. */
    public io.zmux.shaping.ShapingMeters shapingMeters() throws IOException {
        return (io.zmux.shaping.ShapingMeters) run(op -> core.shapingMeters());
    }

    /** The session's shaping limits. */
    public io.zmux.shaping.ShapingLimits shapingLimits() throws IOException {
        return (io.zmux.shaping.ShapingLimits) run(op -> core.shapingLimits());
    }

    /**
     * Replaces the session's shaping limits, tightened or loosened; they are never sent. What was
     * spent stays spent.
     */
    public void setShapingLimits(io.zmux.shaping.ShapingLimits l) throws IOException {
        java.util.Objects.requireNonNull(l, "l");
        run(op -> { core.setShapingLimits(l); return Boolean.TRUE; });
    }

    /** Identifies the bootstrap dialer for a symmetric carrier; a known designation cannot be reversed. */
    public void designateDialer(boolean local) throws IOException {
        var role = (io.zmux.internal.core.CoreConfig.Role) run(op -> core.transportRole());
        if (role != io.zmux.internal.core.CoreConfig.Role.AUTO) {
            if (local != (role == io.zmux.internal.core.CoreConfig.Role.INITIATOR))
                throw new IllegalStateException("bootstrap dialer is already known");
            return;
        }
        synchronized (this) {
            if (designatedDialer != null && designatedDialer != local) throw new IllegalStateException("bootstrap dialer is already known");
            designatedDialer = local;
        }
    }


    /**
     * The value of the admission that accepted the session's bootstrap carrier ({@link
     * Admission.Decision#value()}; for a split bootstrap its uplink half's); empty without one
     * and on the dialing side. It does not change for the session's lifetime, resumptions
     * included; {@link ZmuxPipe#admitted()} is each pipe's.
     */
    public java.util.Optional<Object> admitted() { return java.util.Optional.ofNullable(driver.admitted()); }

    ZmuxSession(SessionRuntime driver, Session core, String applicationProtocol, Credential credential) {
        this.driver = driver;
        this.core = core;
        this.applicationProtocol = applicationProtocol;
        this.credential = credential;
        driver.host(this);
        var redial = driver.hooks().redial();
        if (redial != null) Redialer.start(this, (ZmuxConfig.Redial) redial.hook(), redial.minBackoff(), redial.maxBackoff());
    }

    /**
     * States that the host has proven pipe 0 end to end: local, sends nothing. Until
     * then a session without a secure first pipe has no join keys: local joins fail with
     * {@link JoinPendingException} and the peer's are held. With a secure first pipe, a no-op.
     */
    public void declareEndToEnd() throws IOException {
        run(op -> { driver.declareEndToEnd(); return null; });
    }

    /**
     * Starts a session epoch exchange at once when the session is
     * Stable, or right after the one in progress; {@link SessionEvent.EpochCommitted} and
     * {@link SessionEvent.EpochDeleted} report it. Ordinary rotation also follows the policy
     * ({@link ZmuxConfig.Builder#epochPeriod}, {@link ZmuxConfig.Builder#epochJoins},
     * {@link ZmuxConfig.Builder#epochAfterFirstPipe}, {@link ZmuxConfig.Builder#epochAfterResume}).
     *
     * @throws NotNegotiatedException without multipath
     * @throws JoinPendingException before the session has its join keys
     */
    public void advanceEpoch() throws IOException {
        run(op -> { core.advanceEpoch(); return null; });
    }

    /** The epoch new local joins use. */
    public long epoch() throws IOException {
        return (Long) run(op -> core.epoch());
    }

    /**
     * Advances the epoch as {@link #advanceEpoch} does and then replaces every carrier the session
     * had, through the {@link ZmuxConfig.Builder#redial redial hook}, within deadline: a new pipe
     * joins with the new epoch and the old one is removed gracefully. It serves only a suspected
     * leak of a session-level secret; ordinary rotation is {@link #advanceEpoch} or the policy.
     *
     * @throws IllegalStateException without a redial hook
     * @throws SocketTimeoutException when deadline passes first
     */
    public void forceEpochAdvance(Duration deadline) throws IOException {
        var redial = driver.hooks().redial();
        if (redial == null) throw new IllegalStateException("forceEpochAdvance without a redial hook");
        long until = deadline(java.util.Objects.requireNonNull(deadline, "deadline"));
        List<ZmuxPipe> old = pipes();
        long next = epoch() + 1;
        advanceEpoch();
        try {
            call(op -> core.waitEpoch(next, op) ? Boolean.TRUE : Operation.PENDING, core::cancelEpochWait, until);
        } catch (SocketTimeoutException e) {
            throw new SocketTimeoutException("forceEpochAdvance: epoch " + next + " not in use");
        }
        ZmuxConfig.Redial hook = (ZmuxConfig.Redial) redial.hook();
        try (Cancellation cancel = new Cancellation()) {
            for (ZmuxPipe p : old) {
                var st = p.stats().orElse(null);
                if (st == null) continue; // gone already: nothing to replace
                PipeOptions.Direction dir = st.inbound() == null ? PipeOptions.Direction.SEND_ONLY
                        : st.outbound() == null ? PipeOptions.Direction.RECEIVE_ONLY : PipeOptions.Direction.BIDIRECTIONAL;
                if (System.nanoTime() - until >= 0) throw new SocketTimeoutException("forceEpochAdvance: deadline");
                io.zmux.carrier.PipeIo io;
                try {
                    io = hook.dial(dir, cancel);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new java.io.InterruptedIOException("forceEpochAdvance interrupted");
                }
                addPipe(io, PipeOptions.builder().direction(dir).build());
                p.close();
            }
        }
    }

    /** The weakest record mode of the session's unretired pipes and pipe 0, which counts for life. */
    public RecordMode weakestRecordMode() throws IOException {
        return (RecordMode) run(op -> io.zmux.internal.join.Mode.record(core.weakestMode()));
    }

    static long deadline(Duration timeout) {
        if (timeout == null) return Long.MAX_VALUE;
        try {
            long d = System.nanoTime() + timeout.toNanos();
            return d < System.nanoTime() ? Long.MAX_VALUE : d;
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    /** Runs step on the loop and waits at most until deadline for its outcome. */
    Object call(Operation.Step step, Operation.Withdraw withdraw, long deadline) throws IOException {
        driver.checkBlocking();
        return Errors.check(new Operation<>(driver, step, withdraw).submit().await(deadline));
    }

    Object run(Operation.Step step) throws IOException {
        return call(step, null, Long.MAX_VALUE);
    }

    /** Maps a decided outcome to the future's value; runs on the completion context. */
    @FunctionalInterface interface Outcome<T> { T map(Object value) throws IOException; }

    /**
     * Runs step on the loop without waiting. discard releases a value produced after the
     * operation was cancelled; deadline (System.nanoTime based) bounds the wait.
     */
    <T> ZmuxFuture<T> async(Operation.Step step, Operation.Withdraw withdraw, java.util.function.Consumer<Object> discard,
                            long deadline, Outcome<T> map) {
        Operation<T> op = new Operation<>(driver, step, withdraw, discard, deadline);
        ZmuxFuture<T> f = future(op, r -> {
            if (r instanceof Throwable) throw Errors.outcome(r, 0);
            return map.map(r);
        });
        op.submit();
        return f;
    }

    /** The future of op: map turns its decided outcome (a value or a Throwable) into the result. */
    static <T> ZmuxFuture<T> future(Operation<?> op, Outcome<T> map) {
        ZmuxFuture<T> f = new ZmuxFuture<>(op::cancel);
        Runnable publish = () -> {
            if (op.isCancelled()) return;
            try {
                f.publish(map.map(op.outcome()));
            } catch (IOException | RuntimeException failure) {
                f.publishFailure(failure);
            }
        };
        if (!op.onDone(publish)) publish.run();
        return f;
    }

    // ---- streams ----

    /** Opens a bidirectional stream, waiting for stream credit (FIFO). Nothing is sent until the first write. */
    public ZmuxStream openStream() throws IOException {
        return open(true, Preamble.EMPTY, true, null);
    }

    /** Waits at most timeout for stream credit; {@link SocketTimeoutException} after it. */
    public ZmuxStream openStream(Duration timeout) throws IOException {
        return open(true, Preamble.EMPTY, true, timeout);
    }

    /** Opens a bidirectional stream with its opening information. */
    public ZmuxStream openStream(Preamble preamble) throws IOException {
        return open(true, preamble, true, null);
    }

    /** Opens a bidirectional stream with its opening information, waiting at most timeout for credit. */
    public ZmuxStream openStream(Preamble preamble, Duration timeout) throws IOException {
        return open(true, preamble, true, timeout);
    }

    /**
     * Opens a bidirectional stream marked PIPE_CARRIER and returns it as a pipe input of a
     * nested session; nothing is sent until the nested session's first record. info is the
     * OPEN_INFO (null: none) and tlvs are application TLVs of the opening preamble, for example
     * the destination a relay routes by; both need the preamble extension, and without it
     * (a plain QMux peer) must be null and empty.
     */
    public io.zmux.carrier.PipeIo openPipeStream(byte[] info, List<Preamble.Tlv> tlvs) throws IOException {
        List<Preamble.Tlv> values = List.copyOf(tlvs);
        boolean preamble = (Boolean) run(op -> core.preambleInUse());
        if (!preamble && (info != null || !values.isEmpty())) throw new IllegalArgumentException("OPEN_INFO and application TLVs require the preamble extension");
        return pipeOf(open(true, preamble ? new Preamble(info == null ? null : info.clone(), values, true) : Preamble.EMPTY, true, null));
    }

    /** {@link #openPipeStream(byte[], List)} without OPEN_INFO or application TLVs. */
    public io.zmux.carrier.PipeIo openPipeStream() throws IOException { return openPipeStream(null, List.of()); }

    /** Opens a bidirectional stream without waiting; fails with {@link StreamLimitException} without credit. */
    public ZmuxStream tryOpenStream(Preamble preamble) throws IOException {
        return open(true, preamble, false, null);
    }

    /** Opens a unidirectional (send-only) stream, waiting for stream credit. */
    public ZmuxSendStream openSendStream() throws IOException {
        return open(false, Preamble.EMPTY, true, null);
    }

    /** Opens a unidirectional (send-only) stream with its opening information, waiting for stream credit. */
    public ZmuxSendStream openSendStream(Preamble preamble) throws IOException {
        return open(false, preamble, true, null);
    }

    /** Opens a unidirectional stream, waiting at most timeout for stream credit. */
    public ZmuxSendStream openSendStream(Preamble preamble, Duration timeout) throws IOException {
        return open(false, preamble, true, timeout);
    }

    private ZmuxStream open(boolean bidi, Preamble pre, boolean wait, Duration timeout) throws IOException {
        var lanes = (LaneTable.StreamLanes) call(openAttempt(bidi, pre, wait),
                op -> core.streams().cancelOpenWait(bidi, op), deadline(timeout));
        return new ZmuxStream(this, lanes, Preamble.EMPTY);
    }

    private Operation.Step openAttempt(boolean bidi, Preamble pre, boolean wait) {
        boolean[] waited = {false};
        return op -> {
            Stream s;
            try {
                s = core.streams().openStream(bidi);
            } catch (CoreException e) {
                if (!wait || e.reason() != CoreException.Reason.STREAM_LIMIT) throw e;
                if (!waited[0]) {
                    waited[0] = true;
                    core.stats().streamCreditWait();
                }
                core.streams().waitOpen(bidi, op);
                return Operation.PENDING;
            }
            try {
                if (pre.openInfo() != null) core.streams().setOpenInfo(s, pre.openInfo());
                for (Preamble.Tlv t : pre.tlvs()) core.streams().addPreambleTlv(s, t.type(), t.value());
                if (pre.pipeCarrier()) core.streams().setPipeCarrier(s);
            } catch (CoreException e) {
                core.streams().release(s);
                throw e;
            }
            return driver.lanes.of(s);
        };
    }

    /** Asynchronously opens a bidirectional stream; cancellation is acknowledged by the core. */
    public ZmuxFuture<ZmuxStream> openStreamAsync() { return openStreamAsync(Preamble.EMPTY); }
    public ZmuxFuture<ZmuxStream> openStreamAsync(Preamble preamble) {
        return openAsync(true, preamble, stream -> stream);
    }
    public ZmuxFuture<ZmuxSendStream> openSendStreamAsync() { return openSendStreamAsync(Preamble.EMPTY); }
    public ZmuxFuture<ZmuxSendStream> openSendStreamAsync(Preamble preamble) {
        return openAsync(false, preamble, stream -> stream);
    }
    private <T> ZmuxFuture<T> openAsync(boolean bidi, Preamble pre,
            Function<ZmuxStream,T> view) {
        Objects.requireNonNull(pre);
        return async(openAttempt(bidi, pre, true), op -> core.streams().cancelOpenWait(bidi, op),
                lanes -> driver.lanes.releaseStream(((LaneTable.StreamLanes) lanes).stream()), Long.MAX_VALUE,
                raw -> view.apply(new ZmuxStream(this, (LaneTable.StreamLanes) raw, Preamble.EMPTY)));
    }

    /** Waits for the next bidirectional stream the peer opens. */
    public ZmuxStream acceptStream() throws IOException {
        return accept(true, null);
    }

    /** Waits at most timeout for the next bidirectional stream; {@link SocketTimeoutException} after it. */
    public ZmuxStream acceptStream(Duration timeout) throws IOException {
        return accept(true, timeout);
    }

    /** Waits for the next unidirectional stream; the first call grants the peer its full credit. */
    public ZmuxReceiveStream acceptReceiveStream() throws IOException {
        return accept(false, null);
    }

    /** Waits at most timeout for the next unidirectional stream; {@link SocketTimeoutException} after it. */
    public ZmuxReceiveStream acceptReceiveStream(Duration timeout) throws IOException {
        return accept(false, timeout);
    }

    private ZmuxStream accept(boolean bidi, Duration timeout) throws IOException {
        var lanes = (LaneTable.StreamLanes) call(acceptAttempt(bidi),
                op -> core.streams().cancelAcceptWait(bidi, op), deadline(timeout));
        return new ZmuxStream(this, lanes, ZmuxStream.preambleOf(lanes.stream()));
    }

    private Operation.Step acceptAttempt(boolean bidi) {
        return op -> {
            Stream s = core.streams().acceptStream(bidi);
            if (s != null) return driver.lanes.of(s);
            if (core.error() != null) throw CoreException.session(core.error());
            core.streams().waitAccept(bidi, op);
            return Operation.PENDING;
        };
    }

    /** Asynchronously accepts a bidirectional stream. Null timeout means no deadline. */
    public ZmuxFuture<ZmuxStream> acceptStreamAsync() { return acceptStreamAsync(null); }
    public ZmuxFuture<ZmuxStream> acceptStreamAsync(Duration timeout) {
        return acceptAsync(true, timeout, stream -> stream);
    }
    public ZmuxFuture<ZmuxReceiveStream> acceptReceiveStreamAsync() { return acceptReceiveStreamAsync(null); }
    public ZmuxFuture<ZmuxReceiveStream> acceptReceiveStreamAsync(Duration timeout) {
        return acceptAsync(false, timeout, stream -> stream);
    }
    private <T> ZmuxFuture<T> acceptAsync(boolean bidi, Duration timeout,
            Function<ZmuxStream,T> view) {
        return async(acceptAttempt(bidi), op -> core.streams().cancelAcceptWait(bidi, op),
                lanes -> core.streams().unaccept(((LaneTable.StreamLanes) lanes).stream()), deadline(timeout), raw -> {
            var lanes = (LaneTable.StreamLanes) raw;
            return view.apply(new ZmuxStream(this, lanes, ZmuxStream.preambleOf(lanes.stream())));
        });
    }

    private final Datagrams datagramView = new Datagrams(this);

    /** Session datagrams; operations throw {@link NotNegotiatedException} unless both sides enabled them. */
    public Datagrams datagrams() { return datagramView; }


    // ---- datagrams ----

    /**
     * Queues a session datagram; never blocks (a full queue drops its oldest datagram).
     * Returns false when the datagram limiter dropped it (counted as {@code datagrams_dropped_limit}).
     */
    boolean sendSessionDatagram(ByteBuffer payload) throws IOException {
        return sendDatagram(null, 0, payload);
    }

    /**
     * Waits only for send-queue space, never for limiter tokens. Returns false when the
     * datagram limiter dropped it.
     */
    boolean sendSessionDatagram(ByteBuffer payload, Duration timeout) throws IOException {
        return sendDatagram(null, 0, payload, true, timeout);
    }

    boolean sendDatagram(Stream stream, long context, ByteBuffer payload) throws IOException {
        return sendDatagram(stream, context, payload, false, null);
    }

    private boolean sendDatagram(Stream stream, long context, ByteBuffer payload, boolean wait, Duration timeout) throws IOException {
        driver.checkBlocking();
        int n = payload.remaining();
        var limiter = driver.datagramSendLimiter();
        if (limiter != null && limiter.tryReady(n) < n) {
            run(op -> { core.datagrams().limitedSend(); return Boolean.TRUE; });
            return false;
        }
        byte[] b = new byte[n];
        payload.get(b);
        call(op -> {
            if (wait && core.datagrams().sendQueueFull()) {
                if (core.error() != null) throw CoreException.session(core.error());
                core.datagrams().waitSend(op); return Operation.PENDING;
            }
            core.sendDatagram(stream, context, b); return Boolean.TRUE;
        }, op -> core.datagrams().cancelSendWait(op), deadline(timeout));
        if (limiter != null) limiter.charge(n);
        return true;
    }

    /** Waits for the next session datagram. */
    byte[] receiveSessionDatagram() throws IOException {
        return receiveSessionDatagram(null);
    }

    /** Waits at most timeout (null: no limit) for the next session datagram; {@link SocketTimeoutException} after it. */
    byte[] receiveSessionDatagram(Duration timeout) throws IOException {
        return (byte[]) call(op -> {
            byte[] d = core.datagrams().receive();
            if (d != null) return d;
            if (core.error() != null) throw CoreException.session(core.error());
            core.datagrams().waitReceive(op);
            return Operation.PENDING;
        }, op -> core.datagrams().cancelReceiveWait(op), deadline(timeout));
    }

    // ---- pipes ----

    /**
     * Joins a new TCP connection to address as a pipe and waits until the peer attached it
     * ({@link ZmuxPipe#awaitPeerAttached}), or with {@link PipeOptions.Builder#noWaitAttached}
     * until the Reply verified; the join timeout ({@link ZmuxConfig.Builder#joinTimeout}) bounds
     * the whole call, from its start. A pipe still unattached then is aborted and
     * {@link SocketTimeoutException} thrown; interruption aborts it as well
     * ({@link InterruptedIOException}); a pipe that fails or is removed first gives
     * {@link PipeGoneException}, a refused join {@link JoinRefusedException}, the session's end the
     * session's exception. A failed call leaves no pipe: its abort waits for the event loop at most one
     * second, as a close does, and a loop that does not answer by then gets the abort queued (it runs when the
     * loop does, or the session's end releases the pipe) while the call throws its failure. A send-only pipe is attached by a PIPE_ACK
     * that arrives on another pipe: without one that receives, the wait lasts until the deadline.
     * A join that the attachment cap or the refusal backoff holds waits for room. The connection
     * starts with the prefix of the session configuration's {@link ZmuxConfig#credential()}.
     * Needs multipath ({@link NotNegotiatedException} otherwise).
     */
    public ZmuxPipe addPipe(InetSocketAddress address, PipeOptions o) throws IOException {
        driver.checkBlocking();
        long deadline = joinDeadline();
        SocketChannel ch = Zmux.dial(address, remaining(deadline));
        Zmux.present(ch, credential, address);
        return attached(joined(driver.addPipe(ch, o.join()), deadline), o, deadline);
    }

    /**
     * Joins a new TLS connection (server name verified, the session's ALPN offered) as a pipe,
     * returning as {@link #addPipe(InetSocketAddress, PipeOptions)} does; after the handshake it
     * presents the session configuration's {@link ZmuxConfig#credential()}.
     */
    public ZmuxPipe addTlsPipe(InetSocketAddress address, TlsOptions tls, PipeOptions o) throws IOException {
        driver.checkBlocking();
        long deadline = joinDeadline();
        SocketChannel ch = Zmux.dial(address, remaining(deadline));
        String alpn = applicationProtocol.isEmpty() ? ZmuxConfig.DEFAULT_ALPN : applicationProtocol;
        SSLEngine engine;
        try {
            engine = tls.clientEngine(address.getPort(), List.of(alpn));
        } catch (RuntimeException e) {
            ch.close();
            throw e;
        }
        return attached(joined(driver.addPipe(ch, engine, remaining(deadline), tls.secure(), o.join(), Zmux.presenter(credential, address)), deadline),
                o, deadline);
    }

    /**
     * Joins a pipe input as a pipe, returning as {@link #addPipe(InetSocketAddress, PipeOptions)}
     * does. The pipe input belongs to zmux from the call on, whatever the outcome: zmux closes it
     * on every failure, the caller never does.
     */
    public ZmuxPipe addPipe(PipeIo io, PipeOptions o) throws IOException {
        try {
            driver.checkBlocking();
        } catch (RuntimeException e) {
            Bootstrap.discard(io, e);
            throw e;
        }
        long deadline = joinDeadline();
        return attached(joined(driver.addPipe(io, o.join()), deadline), o, deadline);
    }

    /** The nanoseconds left until deadline (System.nanoTime), at least 1. */
    private static long remaining(long deadline) {
        return deadline == Long.MAX_VALUE ? io.zmux.internal.core.Time.NEVER : Math.max(1, deadline - System.nanoTime());
    }

    /** The deadline (System.nanoTime) of a join called now: the join timeout from now. */
    private long joinDeadline() {
        long t = System.nanoTime(), d = t + core.joinDeadline();
        return d < t ? Long.MAX_VALUE : d;
    }

    /**
     * Waits until the peer attached a joined pipe, unless o says not to, within the
     * call's deadline. A pipe still unattached at the deadline, or when the wait is interrupted,
     * is aborted (PIPE_ABANDON(ZMUX_PIPE_FAILED)); one that fails or is removed first fails the
     * call with {@link PipeGoneException}, the session's end with the session's exception.
     */
    private ZmuxPipe attached(ZmuxPipe p, PipeOptions o, long deadline) throws IOException {
        if (o.noWaitAttached) return p;
        Object r = new Operation<>(driver, p.attachedStep(), core::cancelPeerAttachedWait).submit().await(deadline);
        // The core fails a join still unattached at the join deadline, which runs from JOINING,
        // after this call's deadline: past it, the pipe is gone because the wait timed out.
        if (r == ZmuxPipe.MISSING && deadline != Long.MAX_VALUE && System.nanoTime() - deadline >= 0) r = new SocketTimeoutException();
        if (r == ZmuxPipe.MISSING) throw new PipeGoneException(p.id());
        if (r instanceof SocketTimeoutException) {
            cancelJoinedPipe(p.id());
            throw new SocketTimeoutException("pipe " + p.id() + " not attached by the peer within the join deadline");
        }
        if (r instanceof InterruptedIOException) cancelJoinedPipe(p.id());
        Errors.check(r);
        return p;
    }

    ZmuxPipe joined(CompletableFuture<Long> f, long deadline) throws IOException {
        long id;
        try {
            id = f.get();
        } catch (InterruptedException e) {
            // The exception cleared the flag, so the cleanup waits below block; it is restored after them.
            try {
                driver.cancelJoinSetup(f);
                Long created = null;
                boolean resolved = false;
                while (!resolved) {
                    try { created = f.get(); resolved = true; }
                    catch (InterruptedException again) { /* Cleanup must acknowledge the original attempt. */ }
                    catch (ExecutionException failed) { resolved = true; }
                }
                if (created != null) cancelJoinedPipe(created);
            } finally { Thread.currentThread().interrupt(); }
            throw new InterruptedIOException("interrupted");
        } catch (ExecutionException e) {
            throw Errors.failure(e.getCause());
        }
        // The core also fails a local join still unconfirmed after its deadline (PathSession
        // timers), which reports GONE through the pipe-state wake-up; the call's deadline, from
        // its start, bounds it here as well.
        Object r = new Operation<>(driver, op -> switch (core.joinProgress(id)) {
            case CONFIRMED -> { driver.refreshPipes(); yield Boolean.TRUE; }
            case GONE -> Boolean.FALSE;
            case PENDING -> {
                if (core.error() != null) throw CoreException.session(core.error());
                driver.waitPipe(op);
                yield Operation.PENDING;
            }
        }, driver::cancelPipeWait).submit().await(deadline);
        if (r instanceof Throwable) {
            cancelJoinedPipe(id);
            Errors.check(r);
        }
        if (!Boolean.TRUE.equals(r)) throw new PipeGoneException(id);
        return new ZmuxPipe(this, id);
    }

    /**
     * Aborts a joined pipe whose call failed, waiting for the loop at most as long as a close
     * does (one second): a loop stuck longer cannot hold the calling thread. The abort is queued
     * on the loop either way and runs when the loop does (or the session's end releases the
     * pipe); the caller's own failure is reported regardless. True when it ran within the bound.
     */
    boolean cancelJoinedPipe(long id) {
        var done = new CompletableFuture<Void>();
        CompletableFuture<?> wait = driver.executeCarrier(() -> {
            try { abandon(id); done.complete(null); } catch (Throwable error) { done.completeExceptionally(error); }
        }) ? done : driver.closedFuture();
        return awaitBounded(wait, closeHandOffLimit);
    }

    /** Waits for f at most nanos (uninterruptibly, keeping the flag); true when it completed. */
    static boolean awaitBounded(CompletableFuture<?> f, long nanos) {
        boolean interrupted = false;
        long end = System.nanoTime() + nanos;
        try {
            while (true) {
                long left = end - System.nanoTime();
                if (left <= 0) return f.isDone();
                try {
                    f.get(left, TimeUnit.NANOSECONDS);
                    return true;
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (ExecutionException e) {
                    return true;
                } catch (TimeoutException e) {
                    return false;
                }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    private void abandon(long id) {
        try {
            core.removePipe(id, false);
        } catch (CoreException ignored) {
            // Already gone.
        }
    }

    /** The attachments as of the loop's last round; no I/O. Works on every session. */
    public List<ZmuxPipe> pipes() {
        return driver.pipeSnapshot().stream().map(i -> new ZmuxPipe(this, i.pipe())).toList();
    }

    /** Maintains one route's joined pipes; closing the handle stops maintenance, preserving existing traffic. */
    public PipeMaintenance maintainPipes(PipePool pool, Cancellation cancellation) throws IOException {
        return PipeMaintenance.start(this, pool, cancellation);
    }

    /** {@link #maintainPipes(PipePool, Cancellation)} without a cancellation. */
    public PipeMaintenance maintainPipes(PipePool pool) throws IOException { return maintainPipes(pool, null); }

    /** Whether the maintenance target and join admission permit dialing another pipe now. */
    public boolean joinReady() throws IOException {
        return (Boolean) run(op -> core.joinReady());
    }

    /** Re-checks every pipe after a suspected path change. */
    public void revalidatePipes() throws IOException {
        run(op -> {
            core.revalidatePipes(0, true);
            return Boolean.TRUE;
        });
    }

    /**
     * Ends {@link ZmuxConfig.Builder#holdFirstFlight}: everything queued so far is built into
     * records and written to the bootstrap carrier in one write, as far as one batch holds it.
     * Calls after the first, and on a session without the hold, do nothing. It never blocks.
     */
    public void releaseFirstFlight() {
        driver.post(() -> core.releaseFirstFlight(io.zmux.internal.loop.Clock.now()));
    }

    /** Changes the session's scheduling policy. */
    public void setScheduler(Scheduler s) {
        Objects.requireNonNull(s);
        driver.post(() -> driver.pickers.install(null, Scheduling.policy(s), Scheduling.picker(s)));
    }

    /** Snapshots as of the loop's last round, including pipe 0. */
    public List<PipeStats> pipeStats() {
        return driver.pipeSnapshot();
    }

    /** The default quality event band of outbound pipes. */
    public void setQualityBand(QualityBand band) throws IOException {
        Objects.requireNonNull(band);
        run(op -> { core.setQualityBand(-1, ZmuxConfig.nanos(band.rtt()), band.rateRatio(),
                ZmuxConfig.nanos(band.minInterval())); return Boolean.TRUE; });
    }

    /** The outbound capacity floor, with hysteresis. */
    public void setCapacityFloor(CapacityFloor floor) throws IOException {
        Objects.requireNonNull(floor);
        run(op -> { core.setCapacityFloor(floor.rate(), floor.recoveryRatio()); return Boolean.TRUE; });
    }

    // ---- record shaping ----

    /**
     * Installs factory on every outbound pipe, replacing earlier instances; null: no shaping.
     * A deadline a shaper sets (a delay, a gate) is met precisely: when it is less than 1 ms
     * away, the session's event loop parks in slices of at most 0.1 ms with a non-blocking
     * check for I/O between them, so a shaper that sets deadlines at a high rate trades CPU
     * time on the loop for timing precision.
     */
    public void setShaper(io.zmux.shaping.ShaperFactory factory) { setShaper(factory, null); }

    /**
     * Installs factory as {@link #setShaper(io.zmux.shaping.ShaperFactory)} does, with a shaping configuration's budget:
     * budget is called once, here, off the session; null: none. The account carries over.
     */
    public void setShaper(io.zmux.shaping.ShaperFactory factory, java.util.function.Supplier<io.zmux.shaping.ShapingBudget> budget) {
        var b = ZmuxConfig.askBudget(budget);
        driver.post(() -> {
            core.setShapingBudget(b);
            core.setShaper(factory);
        });
    }

    /**
     * Installs shaper on one outbound pipe until the next {@link #setShaper}; null: the pipe is
     * not shaped. False when the pipe has no outbound direction.
     */
    public boolean setPipeShaper(long pipe, io.zmux.shaping.Shaper shaper) throws IOException {
        return (Boolean) run(op -> core.setPipeShaper(pipe, shaper));
    }

    /** The shaping counters as of the loop's last round; no I/O. */
    public io.zmux.shaping.ShapingStats shapingStats() {
        var s = driver.shapingStats();
        return s == null ? io.zmux.shaping.ShapingStats.ZERO : s;
    }

    /** Transfers an inbound unidirectional carrier lane of this session's transport connection. */
    public void handleLane(PipeIo io) { driver.handleLane(Objects.requireNonNull(io)); }

    // ---- session ----

    /** Sends a QX_PING request (or joins the outstanding one) and returns the round-trip time. */
    public Duration ping() throws IOException {
        return ping(null);
    }

    /** Like {@link #ping()}, waiting at most timeout (null: no limit); {@link SocketTimeoutException} after it. */
    public Duration ping(Duration timeout) throws IOException {
        return Duration.ofNanos((Long) call(pingStep(), op -> core.keepAlive().cancelWait(op), deadline(timeout)));
    }

    /** Like {@link #ping()} without blocking. */
    public ZmuxFuture<Duration> pingAsync() {
        return async(pingStep(), op -> core.keepAlive().cancelWait(op), null, Long.MAX_VALUE, r -> Duration.ofNanos((Long) r));
    }

    private Operation.Step pingStep() {
        long[] ticket = {-1};
        return op -> {
            if (ticket[0] < 0) ticket[0] = core.ping(op);
            long r = core.pingResult(ticket[0]);
            if (r >= 0) return r;
            core.ping(op);
            return Operation.PENDING;
        };
    }

    /** Waits for the next session event. Concurrent callers each receive different events. */
    public SessionEvent nextEvent() throws IOException {
        return nextEvent(null);
    }

    /** Waits at most timeout (null: no limit) for the next session event; {@link SocketTimeoutException} after it. */
    public SessionEvent nextEvent(Duration timeout) throws IOException {
        return (SessionEvent) call(eventStep(), driver::cancelEventWait, deadline(timeout));
    }

    /** Like {@link #nextEvent()} without blocking. */
    public ZmuxFuture<SessionEvent> nextEventAsync() {
        return async(eventStep(), driver::cancelEventWait, null, Long.MAX_VALUE, r -> (SessionEvent) r);
    }

    private Operation.Step eventStep() {
        return op -> {
            while (true) {
                Object ev = driver.pollEvent(op);
                if (ev == null) break;
                SessionEvent event = ev instanceof SessionRuntime.RetiredPipes(long retired, long failed, long closed, long dropped, List<Long> last)
                        ? new SessionEvent.PipesRetired(retired, failed, closed, dropped, last)
                        : ev instanceof SessionEvent application ? application : Events.publicEvent((Event) ev);
                if (event != null) { driver.cancelEventWait(op); return event; }
            }
            if (core.state() == SessionState.CLOSED) {
                driver.cancelEventWait(op);
                throw CoreException.session(core.error());
            }
            return Operation.PENDING;
        };
    }

    /** The fixed bound of a close's hand-off of CONNECTION_CLOSE to the carriers; tests shorten it. */
    static volatile long closeHandOffLimit = TimeUnit.SECONDS.toNanos(1);

    /**
     * Closes with code 0 at once, discarding data not yet sent; like {@link #close(ApplicationError)}.
     * Unlike a socket's close it does not flush: {@link #shutdown(Duration)} lets the peer read what
     * was written.
     */
    @Override
    public void close() {
        close(ApplicationError.NONE);
    }

    /**
     * Closes with an application error at once, discarding data not yet sent. CONNECTION_CLOSE
     * is written to the carrier of every usable outbound pipe (for a socket, taken into its send
     * buffer), followed by the end of the carrier's sending direction; the session then reads and
     * discards what the peer still sends until the peer ends its direction or the linger timeout
     * (min(2 s, max(250 ms, 3 &times; SRTT))) passes, and only then closes the carriers. Returns
     * once that drain is over, or at the latest after one second in all, the rest of the
     * linger running in the background; at once when no pipe can carry CONNECTION_CLOSE. A
     * carrier that has not taken it within the second is closed with the others and the session
     * ends locally. The process may exit once it returned. A repeated or concurrent call sends nothing more and returns no later than the
     * first; on an ended session it returns at once. Interruption does not cut the wait short
     * (the flag is kept). On the session's event-loop or callback thread it only starts the
     * close. {@link #awaitClosed()} waits for the session's whole end.
     */
    public void close(ApplicationError error) {
        if (!driver.mayBlock()) {
            startClose(error);
            return;
        }
        byte[] r = error.reason().getBytes(StandardCharsets.UTF_8);
        long limit = closeHandOffLimit;
        long deadline = System.nanoTime() + limit;
        // The hand-off yields the linger timeout, read on the loop.
        var op = new Operation<>(driver, o -> {
            core.closeBounded(error.code(), r, limit, io.zmux.internal.loop.Clock.now());
            return core.waitCloseHandedOff(o) ? (Object) core.lingerTimeout() : Operation.PENDING;
        }, core::cancelCloseHandOffWait);
        op.submit();
        if (!op.awaitUninterruptibly(deadline)) {
            // The loop's own bound ends the close as well; this covers a loop that lags behind it.
            driver.post(core::endClose);
            op.awaitUninterruptibly(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50));
            op.cancel();
            return;
        }
        // CONNECTION_CLOSE and FIN are out; the session drains the peer's data until its FIN or
        // the linger timeout. Wait for that within the bound, so that a process exiting on
        // return leaves no unread data behind (which would abort the carrier with a RST and
        // could take the CONNECTION_CLOSE with it); the rest of the linger runs on the loop.
        if (op.outcome() instanceof Long linger) {
            long wait = Math.min(linger, deadline - System.nanoTime());
            if (wait > 0) awaitClosedUninterruptibly(wait);
        }
    }

    private void awaitClosedUninterruptibly(long nanos) {
        boolean interrupted = false;
        long end = System.nanoTime() + nanos;
        try {
            while (true) {
                long left = end - System.nanoTime();
                if (left <= 0) return;
                try {
                    driver.closedFuture().get(left, TimeUnit.NANOSECONDS);
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (TimeoutException | ExecutionException e) {
                    return;
                }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    /** Starts {@link #close(ApplicationError)} without waiting; the loop still bounds the hand-off. */
    private void startClose(ApplicationError error) {
        byte[] r = error.reason().getBytes(StandardCharsets.UTF_8);
        long limit = closeHandOffLimit;
        driver.post(() -> core.closeBounded(error.code(), r, limit, io.zmux.internal.loop.Clock.now()));
    }

    /**
     * Closes gracefully: GOAWAY when in use, then CONNECTION_CLOSE once no stream remains;
     * after timeout it closes at once, through {@link #close()}'s hand-off. Returns when the
     * session is closed. A stream remains until both its parts have ended: the application has
     * read its end (FIN or reset) and the peer has acknowledged the sending part's end
     * ({@link ZmuxStream#finish()} or a reset); its handle need not be closed.
     */
    public void shutdown(Duration timeout) throws IOException {
        driver.checkBlocking();
        Objects.requireNonNull(timeout);
        driver.post(() -> core.shutdown(0, null));
        try {
            driver.closedFuture().get(Math.max(1, timeout.toMillis()), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            close();
            awaitClosed();
        } catch (InterruptedException e) {
            close();
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted");
        } catch (ExecutionException e) {
            throw Errors.failure(e.getCause());
        }
    }

    /** Like {@link #shutdown(Duration)} without blocking: completes once the session is closed. */
    public CompletableFuture<SessionError> shutdownAsync(Duration timeout) {
        Objects.requireNonNull(timeout);
        driver.post(() -> core.shutdown(0, null));
        closed().orTimeout(Math.max(1, timeout.toMillis()), TimeUnit.MILLISECONDS)
                .exceptionally(error -> { startClose(ApplicationError.NONE); return null; });
        return closed();
    }

    /** Completes with the session's error once it is closed, on the session's completion context. */
    public CompletableFuture<SessionError> closed() {
        return driver.closedFuture().thenApply(Errors::session);
    }

    /** Waits until the session is closed and returns why. */
    public SessionError awaitClosed() throws IOException {
        driver.checkBlocking();
        try {
            return Errors.session(driver.closedFuture().get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted");
        } catch (ExecutionException e) {
            throw Errors.failure(e.getCause());
        }
    }

    /** Whether the session is closed or closing. */
    public boolean isClosed() {
        return driver.closedFuture().isDone() || error().isPresent();
    }

    /** The session's error once it ended. */
    public Optional<SessionError> error() {
        io.zmux.internal.core.SessionError e = driver.error();
        return e == null ? Optional.empty() : Optional.of(Errors.session(e));
    }

    /** The largest datagram payload the peer accepts before a flow prefix, as of the loop's last round. */
    int datagramLimit() {
        return driver.info().datagramLimit();
    }

    /** The negotiation results as of the session loop's last round: a snapshot, no I/O. */
    public ConnectionInfo info() {
        var i = driver.info();
        long[] g = i.goAway();
        return new ConnectionInfo(i.established(), i.initiator(), ConnectionInfo.QMUX_VERSION, applicationProtocol,
                Optional.ofNullable(driver.bootstrapTls()), i.peerIsZmux(), i.localDatagrams(), i.peerDatagrams(),
                i.localResetStreamAt(), i.peerResetStreamAt(), i.multipath(), i.pipeAck(), i.resume(),
                i.joinSecurityUnavailable(), i.joinPending(), i.suspended(),
                g == null ? Optional.empty() : Optional.of(new ConnectionInfo.GoAway(g[0], g[1], g[2])),
                Math.max(0, i.datagramLimit() - (i.datagramFlows() ? 1 : 0)),
                i.preamble(), i.priority(), i.datagramFlows(), i.lanesToPeer());
    }

    /**
     * The session's counters as of its loop's last round: a snapshot, no I/O. Bytes a
     * write just put in its stream's lane are counted once the loop took them.
     */
    public SessionStats stats() {
        return SessionStats.of(driver.stats(), carrierKind(), carrierTries(), io.zmux.internal.loop.Clock.now());
    }

    /** The number of the latest carrier tries the statistics keep. */
    static final int CARRIER_TRIES_KEPT = 64;

    /** The credential this session's dialled pipes present; null: none. */
    Credential credential() { return credential; }

    /** The carrier kind a carrier mode chose last; null: none. */
    CarrierKind carrierKind() { synchronized (carrierTries) { return carrier; } }

    /** The variant a carrier mode chose last; "" before a choice or without a label. */
    String carrierVariant() { synchronized (carrierTries) { return carrierVariant; } }

    private java.util.List<CandidateResult> carrierTries() { synchronized (carrierTries) { return java.util.List.copyOf(carrierTries); } }

    /** Records tries of candidates and, when chosen is not null, the choice of kind: in the statistics and as events. */
    void reportCarrier(java.util.List<CandidateResult> results, CarrierKind chosen, String variant) {
        synchronized (carrierTries) {
            carrierTries.addAll(results);
            if (carrierTries.size() > CARRIER_TRIES_KEPT) carrierTries.subList(0, carrierTries.size() - CARRIER_TRIES_KEPT).clear();
            if (chosen != null) { carrier = chosen; carrierVariant = variant; }
        }
        driver.post(() -> {
            for (CandidateResult r : results) driver.addApplicationEvent(r.kind(), new SessionEvent.CarrierCandidate(r));
            if (chosen != null) driver.addApplicationEvent(SessionEvent.CarrierChosen.class, new SessionEvent.CarrierChosen(chosen, variant));
        });
    }

    /**
     * Ends the session without CONNECTION_CLOSE when the peer has not been heard from: its
     * carriers close at once. False when the peer was heard from (the session goes on).
     */
    boolean abandonHandshake() throws IOException {
        try {
            return (Boolean) run(op -> core.abandon());
        } catch (SessionClosedException e) {
            return true;
        }
    }

    /** Waits until the peer's parameters arrived (or the session ended). */
    void awaitEstablished(long deadline) throws IOException {
        call(op -> {
            if (core.established()) return Boolean.TRUE;
            if (core.error() != null) throw CoreException.session(core.error());
            core.waitEstablished(op);
            return Operation.PENDING;
        }, null, deadline);
    }

    /** The pipe view of st, which takes the stream over. */
    private static PipeIo pipeOf(ZmuxStream st) {
        return st.asPipe();
    }
}
