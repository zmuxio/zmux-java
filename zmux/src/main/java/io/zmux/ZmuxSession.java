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
import java.time.Duration;
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
    /**
     * One dial of a further pipe at a time, whoever dials it (the redial hook, a pipe pool): a
     * dial takes it before it decides what is missing and gives it back once its attempt returned
     * or was abandoned (the redial's join deadline, or a pool's run closing or its rotation yielding), so two
     * never restore the same direction. A pool sets no deadline of its own: its dial function bounds its attempt.
     * What an abandoned attempt still creates is released, never kept.
     */
    final java.util.concurrent.Semaphore dialPermit = new java.util.concurrent.Semaphore(1);
    final Session core;
    private final String applicationProtocol;
    /** Presented by the TCP and TLS pipes this session dials ({@link ZmuxConfig#carrierCredential()}); null: none. */
    private final CarrierCredential credential;
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
                return true;
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
        if (!driver.bootExporterPresent()) throw new NoExporterException();
        byte[] v = driver.exportKeyingMaterial(label, context, length);
        if (v == null) throw new NotEstablishedException();
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

    /** The liveness settings the session holds: the last set ({@link #setLiveness}), else the configured. Local state: it never fails or waits. */
    public Liveness liveness() { return liveness; }

    private volatile Liveness liveness;

    private static Liveness livenessOf(io.zmux.internal.core.CoreConfig.Live l) {
        Liveness.Builder b = Liveness.builder().suspectFloor(Duration.ofNanos(l.suspectFloor())).probeJitter(l.probeJitter());
        if (l.keepAlive() < 0) b.noKeepAlivePeriod(); else b.keepAlivePeriod(Duration.ofNanos(l.keepAlive()));
        if (l.deadTimeout() < 0) b.noDeadTimeout(); else if (l.deadTimeout() > 0) b.deadTimeout(Duration.ofNanos(l.deadTimeout()));
        if (l.standbyProbe() <= 0) b.noStandbyProbe(); else b.standbyProbe(Duration.ofNanos(l.standbyProbe()));
        if (l.probeInterval() <= 0) b.noProbeInterval(); else b.probeInterval(Duration.ofNanos(l.probeInterval()));
        return b.build();
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
                l.keepAlivePeriod().map(ZmuxConfig::nanos).orElse(io.zmux.internal.core.CoreConfig.OFF),
                ZmuxConfig.nanos(l.suspectFloor()),
                l.deadTimeoutNanos(),
                l.standbyProbe().map(ZmuxConfig::nanos).orElse(0L),
                l.probeInterval().map(ZmuxConfig::nanos).orElse(0L),
                l.probeJitter());
        run(op -> { core.setLiveness(v); return true; });
        liveness = l;
    }

    /**
     * Raises the bidirectional and unidirectional streams the peer may have open while the
     * session runs, and sends the credit. It only grows: a value at or below the
     * current one leaves that type as it is, so 0 raises nothing. While unidirectional credit is
     * staged, uni raises the value the first accept moves it to. After GOAWAY no credit is sent.
     *
     * @throws IllegalArgumentException outside 0 to {@link Zmux#MAX_STREAM_CREDIT}
     */
    public void raiseIncomingStreams(long bidi, long uni) throws IOException {
        if (bidi < 0 || uni < 0 || bidi > Zmux.MAX_STREAM_CREDIT || uni > Zmux.MAX_STREAM_CREDIT)
            throw new IllegalArgumentException("raiseIncomingStreams outside [0, Zmux.MAX_STREAM_CREDIT]");
        run(op -> {
            core.raiseIncomingStreams(true, bidi);
            core.raiseIncomingStreams(false, uni);
            return true;
        });
    }

    /** The session's shaping readings. */
    public io.zmux.shaping.ShapingMeters shapingMeters() throws IOException {
        return (io.zmux.shaping.ShapingMeters) run(op -> core.shapingMeters());
    }

    /** The shaping limits the session holds: the last set ({@link #setShapingLimits}), else the configured. Local state: it never fails or waits. */
    public io.zmux.shaping.ShapingLimits shapingLimits() { return shapingLimits; }

    private volatile io.zmux.shaping.ShapingLimits shapingLimits;

    /**
     * Replaces the session's shaping limits, tightened or loosened; they are never sent. What was
     * spent stays spent.
     */
    public void setShapingLimits(io.zmux.shaping.ShapingLimits l) throws IOException {
        java.util.Objects.requireNonNull(l, "l");
        run(op -> { core.setShapingLimits(l); return true; });
        shapingLimits = l;
    }

    /**
     * Identifies the bootstrap dialer for a carrier of role {@link Role#AUTO}; a known designation
     * cannot be reversed. For applications that learn the dialer outside zmux.
     *
     * @throws IllegalStateException when the other designation is already known
     */
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

    /** {@link #admitted()} when it is a type; empty otherwise. */
    public <T> java.util.Optional<T> admitted(Class<T> type) { return admitted().filter(type::isInstance).map(type::cast); }

    /**
     * What a live pipe holds as its options: {@code value} is what the pipe was added with and every update applied,
     * null while nothing was held (a pipe the peer joined). A handle that took the holder keeps it after the pipe's end,
     * so it still reads the last held.
     */
    static final class HeldOptions {
        /** Written only on the loop (one writer), read from any thread. */
        private volatile PipeOptions value;

        PipeOptions value() { return value; }

        void set(PipeOptions v) { value = v; }

        /** Applies u to the held options, or to the defaults with the pipe's fixed fields when nothing was held. On the loop. */
        void apply(PipeUpdate u, io.zmux.internal.core.Session.PipeFixed f) {
            PipeOptions cur = value;
            value = (cur != null ? cur : PipeOptions.defaults().fixed(f)).applied(u);
        }
    }

    /**
     * The holders of the live pipes by pipe ID: written on the loop, read from any thread, and a pipe's entry goes with the
     * pipe's end. An entry is made only for a pipe that is there ({@link #heldOf}), so nothing is re-inserted after the end.
     */
    final java.util.concurrent.ConcurrentHashMap<Long, HeldOptions> heldOptions = new java.util.concurrent.ConcurrentHashMap<>();

    /** The holder of live pipe id, made if needed; null when the pipe is gone. On the loop. */
    HeldOptions heldOf(long id) {
        return core.pipeFixed(id) == null ? null : heldOptions.computeIfAbsent(id, k -> new HeldOptions());
    }

    ZmuxSession(SessionRuntime driver, Session core, String applicationProtocol, CarrierCredential credential) {
        this.driver = driver;
        driver.pipeEnded(heldOptions::remove); // on the loop, like heldOf: no entry is made after this
        this.liveness = livenessOf(core.config().live());
        this.shapingLimits = core.shapingLimits();
        this.core = core;
        this.applicationProtocol = applicationProtocol;
        this.credential = credential;
        this.scheduler = driver.hooks().scheduler() instanceof Scheduler s ? s : Scheduler.AFFINITY;
        driver.host(this);
        var redial = driver.hooks().redial();
        if (redial != null) Redialer.start(this, (ZmuxConfig.Redial) redial.hook(), redial.minBackoff(), redial.maxBackoff(), redial.jitter());
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
     * ({@link ZmuxConfig.Builder#epochPeriod(Duration)}, {@link ZmuxConfig.Builder#epochJoins(long)},
     * {@link ZmuxConfig.Builder#epochAfterFirstPipe(boolean)}, {@link ZmuxConfig.Builder#epochAfterResume}).
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
     * Advances the epoch as {@link #advanceEpoch} does, waits until new joins use the new epoch,
     * and then replaces every pipe that existed when it was called: for each, it dials a carrier of
     * the same direction through the {@link ZmuxConfig.Builder#redial(Redial) redial hook}, joins it, and
     * closes the old pipe gracefully once the new one is attached (make before break). It serves
     * only a suspected leak of a session-level secret; ordinary rotation is {@link #advanceEpoch}
     * or the policy.
     *
     * <p>Cancelling the future, or completing it from outside ({@link ZmuxFuture#orTimeout} bounds
     * the call), stops the call: the pipes already replaced stay replaced, and the old pipes not
     * yet replaced are kept as they are. A dial or join that fails ends the call with its error on
     * the same terms. While the previous epoch is still kept, the next offer waits until it is
     * deleted, so with a bound shorter than that wait the call stops before any pipe is replaced.
     *
     * @throws IllegalStateException without a redial hook
     */
    public ZmuxFuture<Void> replacePipes() {
        var redial = driver.hooks().redial();
        if (redial == null) throw new IllegalStateException("replacePipes without a redial hook");
        long until = Long.MAX_VALUE;
        ZmuxConfig.Redial hook = (ZmuxConfig.Redial) redial.hook();
        // Signals the redial hook when the future is cancelled.
        // Closed by the try-with-resources inside the lambda; the future callback only cancels it.
        //noinspection resource
        Cancellation cancel = new Cancellation();
        ZmuxFuture<Void> f = ZmuxFuture.onThread("zmux-replace-pipes", () -> {
            List<ZmuxPipe> old = pipes();
            long next = epoch() + 1;
            advanceEpoch();
            call(op -> core.waitEpoch(next, op) ? true : Operation.PENDING, core::cancelEpochWait, until);
            try (cancel) {
                for (ZmuxPipe p : old) {
                    var st = p.stats().orElse(null);
                    if (st == null) continue; // gone already: nothing to replace
                    PipeOptions.Direction dir = st.inbound() == null ? PipeOptions.Direction.SEND_ONLY
                            : st.outbound() == null ? PipeOptions.Direction.RECEIVE_ONLY : PipeOptions.Direction.BIDIRECTIONAL;
                    io.zmux.carrier.PipeIo io;
                    try {
                        io = hook.dial(dir, cancel);
                    } catch (InterruptedException e) {
                        throw new java.io.InterruptedIOException("replacePipes interrupted");
                    }
                    addPipe(io, PipeOptions.builder().direction(dir).build(), until);
                    p.close();
                }
            }
            return null;
        });
        f.whenComplete((v, e) -> cancel.cancel());
        return f;
    }

    /** The weakest record mode of the session's unretired pipes and pipe 0, which counts for life. */
    public RecordMode weakestRecordMode() throws IOException {
        return (RecordMode) run(op -> io.zmux.internal.join.Mode.record(core.weakestMode()));
    }

    /** The deadline of a wait under a timeout setting: {@link #deadline}, or none while the setting is unset. */
    static long until(Duration setting) {
        return setting == null ? Long.MAX_VALUE : deadline(setting);
    }

    /**
     * The deadline (System.nanoTime) of a wait bounded by timeout from now. Zero or negative: already
     * elapsed (the wait only takes what is ready, else times out); a bound too far to represent: none.
     */
    static long deadline(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        long now = System.nanoTime();
        if (timeout.isNegative() || timeout.isZero()) return now;
        long nanos;
        try {
            nanos = timeout.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
        return nanos >= Long.MAX_VALUE / 2 ? Long.MAX_VALUE : now + nanos;
    }

    /** Runs step on the loop and waits at most until deadline for its outcome. */
    Object call(Operation.Step step, Operation.Withdraw withdraw, long deadline) throws IOException {
        driver.checkBlocking();
        return Errors.check(new Operation<>(driver, step, withdraw, null, deadline).submit().await(deadline));
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
                // The future may have been completed from outside after the outcome was decided:
                // a value nobody receives (a chunk, a stream) is released, not dropped.
                f.publishOrDiscard(map.map(op.outcome()));
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
        return open(true, Preamble.EMPTY, true, Long.MAX_VALUE);
    }

    /** Waits at most timeout for stream credit; {@link SocketTimeoutException} after it. */
    public ZmuxStream openStream(Duration timeout) throws IOException {
        return open(true, Preamble.EMPTY, true, deadline(timeout));
    }

    /** Opens a bidirectional stream with its opening information. */
    public ZmuxStream openStream(Preamble preamble) throws IOException {
        return open(true, preamble, true, Long.MAX_VALUE);
    }

    /** Opens a bidirectional stream with its opening information, waiting at most timeout for credit. */
    public ZmuxStream openStream(Preamble preamble, Duration timeout) throws IOException {
        return open(true, preamble, true, deadline(timeout));
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
        return pipeOf(open(true, preamble ? new Preamble(info, values, true) : Preamble.EMPTY, true, Long.MAX_VALUE));
    }

    /** {@link #openPipeStream(byte[], List)} without OPEN_INFO or application TLVs. */
    public io.zmux.carrier.PipeIo openPipeStream() throws IOException { return openPipeStream(null, List.of()); }

    /** Opens a bidirectional stream without waiting; fails with {@link StreamLimitException} without credit. */
    public ZmuxStream tryOpenStream(Preamble preamble) throws IOException {
        return open(true, preamble, false, Long.MAX_VALUE);
    }

    /** {@link #tryOpenStream(Preamble)} without opening information. */
    public ZmuxStream tryOpenStream() throws IOException {
        return tryOpenStream(Preamble.EMPTY);
    }

    /** Opens a unidirectional (send-only) stream without waiting; fails with {@link StreamLimitException} without credit. */
    public ZmuxSendStream tryOpenSendStream(Preamble preamble) throws IOException {
        return open(false, preamble, false, Long.MAX_VALUE);
    }

    /** {@link #tryOpenSendStream(Preamble)} without opening information. */
    public ZmuxSendStream tryOpenSendStream() throws IOException {
        return tryOpenSendStream(Preamble.EMPTY);
    }

    /** Opens a unidirectional (send-only) stream, waiting for stream credit. */
    public ZmuxSendStream openSendStream() throws IOException {
        return open(false, Preamble.EMPTY, true, Long.MAX_VALUE);
    }

    /** Opens a unidirectional stream, waiting at most timeout for stream credit; {@link SocketTimeoutException} after it. */
    public ZmuxSendStream openSendStream(Duration timeout) throws IOException {
        return open(false, Preamble.EMPTY, true, deadline(timeout));
    }

    /** Opens a unidirectional (send-only) stream with its opening information, waiting for stream credit. */
    public ZmuxSendStream openSendStream(Preamble preamble) throws IOException {
        return open(false, preamble, true, Long.MAX_VALUE);
    }

    /** Opens a unidirectional stream with its opening information, waiting at most timeout for stream credit. */
    public ZmuxSendStream openSendStream(Preamble preamble, Duration timeout) throws IOException {
        return open(false, preamble, true, deadline(timeout));
    }

    private ZmuxStream open(boolean bidi, Preamble pre, boolean wait, long deadline) throws IOException {
        var lanes = (LaneTable.StreamLanes) call(openAttempt(bidi, pre, wait),
                op -> core.streams().cancelOpenWait(bidi, op), deadline);
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
                if (pre.rawOpenInfo() != null) core.streams().setOpenInfo(s, pre.rawOpenInfo());
                for (Preamble.Tlv t : pre.tlvs()) core.streams().addPreambleTlv(s, t.type(), t.raw());
                if (pre.isPipeCarrier()) core.streams().setPipeCarrier(s);
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
        return accept(true, Long.MAX_VALUE);
    }

    /** Waits at most timeout for the next bidirectional stream; {@link SocketTimeoutException} after it. */
    public ZmuxStream acceptStream(Duration timeout) throws IOException {
        return accept(true, deadline(timeout));
    }

    /** Waits for the next unidirectional stream; the first call grants the peer its full credit. */
    public ZmuxReceiveStream acceptReceiveStream() throws IOException {
        return accept(false, Long.MAX_VALUE);
    }

    /** Waits at most timeout for the next unidirectional stream; {@link SocketTimeoutException} after it. */
    public ZmuxReceiveStream acceptReceiveStream(Duration timeout) throws IOException {
        return accept(false, deadline(timeout));
    }

    private ZmuxStream accept(boolean bidi, long deadline) throws IOException {
        var lanes = (LaneTable.StreamLanes) call(acceptAttempt(bidi),
                op -> core.streams().cancelAcceptWait(bidi, op), deadline);
        return new ZmuxStream(this, lanes, ZmuxStream.preambleOf(lanes.stream()));
    }

    private Operation.Step acceptAttempt(boolean bidi) {
        return op -> {
            // A closing session offers nothing: its unaccepted streams were dropped.
            if (core.error() != null) throw CoreException.session(core.error());
            Stream s = core.streams().acceptStream(bidi);
            if (s != null) return driver.lanes.of(s);
            core.streams().waitAccept(bidi, op);
            return Operation.PENDING;
        };
    }

    /**
     * Asynchronously accepts a bidirectional stream; {@link ZmuxFuture#orTimeout} bounds the wait
     * and withdraws it, a stream decided meanwhile staying queued for the next accept.
     */
    public ZmuxFuture<ZmuxStream> acceptStreamAsync() {
        return acceptAsync(true, stream -> stream);
    }

    /** Asynchronously accepts a unidirectional stream, as {@link #acceptStreamAsync()} does. */
    public ZmuxFuture<ZmuxReceiveStream> acceptReceiveStreamAsync() {
        return acceptAsync(false, stream -> stream);
    }

    private <T> ZmuxFuture<T> acceptAsync(boolean bidi, Function<ZmuxStream,T> view) {
        return async(acceptAttempt(bidi), op -> core.streams().cancelAcceptWait(bidi, op),
                lanes -> core.streams().unaccept(((LaneTable.StreamLanes) lanes).stream()), Long.MAX_VALUE, raw -> {
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
    boolean sendSessionDatagram(ByteBuffer payload, long deadline) throws IOException {
        return sendDatagram(null, 0, payload, true, deadline);
    }

    boolean sendDatagram(Stream stream, long context, ByteBuffer payload) throws IOException {
        return sendDatagram(stream, context, payload, false, Long.MAX_VALUE);
    }

    private boolean sendDatagram(Stream stream, long context, ByteBuffer payload, boolean wait, long deadline) throws IOException {
        driver.checkBlocking();
        int n = payload.remaining();
        var limiter = driver.datagramSendLimiter();
        if (limiter != null && !io.zmux.helper.Permit.police(limiter, n)) {
            run(op -> { core.datagrams().limitedSend(); return true; });
            return false;
        }
        // tryAcquire(n, n) before queueing; the grant is refunded unless the datagram is queued.
        boolean queued = false;
        try {
            byte[] b = new byte[n];
            payload.get(b);
            call(op -> {
                if (wait && core.datagrams().sendQueueFull()) {
                    if (core.error() != null) throw CoreException.session(core.error());
                    core.datagrams().waitSend(op); return Operation.PENDING;
                }
                core.sendDatagram(stream, context, b); return true;
            }, op -> core.datagrams().cancelSendWait(op), deadline);
            queued = true;
        } finally {
            if (!queued) io.zmux.helper.Permit.refund(limiter, n);
        }
        return true;
    }

    /**
     * {@link #sendDatagram(Stream, long, ByteBuffer)} without blocking, for loop-driven relays: takes payload over
     * (no copy) and completes, on the completion context, with true once queued or false when the datagram limiter
     * dropped it; a refusal fails it as the blocking send throws.
     */
    ZmuxFuture<Boolean> sendDatagramAsync(Stream stream, long context, byte[] payload) {
        int n = payload.length;
        var limiter = driver.datagramSendLimiter();
        if (limiter != null && !io.zmux.helper.Permit.police(limiter, n)) {
            return async(op -> { core.datagrams().limitedSend(); return false; }, null, null, Long.MAX_VALUE, r -> (Boolean) r);
        }
        ZmuxFuture<Boolean> f = async(op -> { core.sendDatagram(stream, context, payload); return true; }, null, null, Long.MAX_VALUE, r -> (Boolean) r);
        // tryAcquire(n, n) before queueing; the grant is refunded unless the datagram is queued.
        if (limiter != null) f.whenComplete((queued, error) -> { if (error != null) io.zmux.helper.Permit.refund(limiter, n); });
        return f;
    }

    /** Waits until deadline (System.nanoTime; Long.MAX_VALUE: none) for the next session datagram. */
    byte[] receiveSessionDatagram(long deadline) throws IOException {
        return (byte[]) call(op -> {
            byte[] d = core.datagrams().receive();
            if (d != null) return d;
            if (core.error() != null) throw CoreException.session(core.error());
            core.datagrams().waitReceive(op);
            return Operation.PENDING;
        }, op -> core.datagrams().cancelReceiveWait(op), deadline);
    }

    // ---- pipes ----

    /**
     * Joins a new TCP connection to address as a pipe and waits until the peer attached it
     * ({@link ZmuxPipe#awaitAttached}), or with {@link PipeOptions.Builder#returnOnReply(boolean)}
     * until the Reply verified; the join timeout ({@link ZmuxConfig.Builder#joinTimeout(Duration)}) bounds
     * the whole call, from its start. A pipe still unattached then is aborted and
     * {@link SocketTimeoutException} thrown; interruption aborts it as well
     * ({@link InterruptedIOException}); a pipe that fails or is removed first gives
     * {@link PipeGoneException}, a refused join {@link JoinRefusedException}, the session's end the
     * session's exception. A failed call leaves no pipe: its abort waits for the event loop at most one
     * second, as a close does, and a loop that does not answer by then gets the abort queued (it runs when the
     * loop does, or the session's end releases the pipe) while the call throws its failure. A send-only pipe is attached by a PIPE_ACK
     * that arrives on another pipe: without one that receives, the wait lasts until the deadline.
     * A join that the attachment cap or the refusal backoff holds waits for room. The connection
     * starts with the prefix of the session configuration's {@link ZmuxConfig#carrierCredential()}.
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
     * presents the session configuration's {@link ZmuxConfig#carrierCredential()}.
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

    /** {@link #addPipe(PipeIo, PipeOptions)} bounded by deadline (System.nanoTime) as well. */
    private ZmuxPipe addPipe(PipeIo io, PipeOptions o, long until) throws IOException {
        long join = joinDeadline();
        if (join != Long.MAX_VALUE && until - join >= 0) return attached(joined(driver.addPipe(io, o.join()), join), o, join);
        return attached(joined(driver.addPipe(io, o.join()), until, until), o, until);
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
        p.hold(o);
        if (o.settings.qualityBand == PipeUpdate.Change.SET) {
            try {
                var band = o.settings.qualityBandValue;
                p.update((band == null ? PipeUpdate.builder().noQualityBand() : PipeUpdate.builder().qualityBand(band)).build());
            } catch (IOException | RuntimeException e) {
                cancelJoinedPipe(p.rawId());
                throw e;
            }
        }
        if (o.returnOnReply) return p;
        Object r = new Operation<>(driver, p.attachedStep(), core::cancelPeerAttachedWait, null, deadline).submit().await(deadline);
        // The core fails a join still unattached at the join deadline, which runs from JOINING,
        // after this call's deadline: past it, the pipe is gone because the wait timed out.
        if (r == ZmuxPipe.MISSING && deadline != Long.MAX_VALUE && System.nanoTime() - deadline >= 0) r = new SocketTimeoutException();
        if (r == ZmuxPipe.MISSING) throw new PipeGoneException(p.id());
        if (r instanceof SocketTimeoutException) {
            cancelJoinedPipe(p.rawId());
            throw new SocketTimeoutException("pipe " + p.id() + " not attached by the peer within the join deadline");
        }
        if (r instanceof InterruptedIOException) cancelJoinedPipe(p.rawId());
        Errors.check(r);
        return p;
    }

    /** Ends a join setup its caller gave up on, waiting for the attempt to resolve; a pipe it created is cancelled. */
    private void abandonJoinSetup(CompletableFuture<Long> f) {
        driver.cancelJoinSetup(f);
        Long created = null;
        boolean resolved = false;
        boolean interrupted = false;
        while (!resolved) {
            try { created = f.get(); resolved = true; }
            catch (InterruptedException again) { interrupted = true; /* Cleanup must acknowledge the original attempt. */ }
            catch (ExecutionException failed) { resolved = true; }
        }
        if (created != null) cancelJoinedPipe(created);
        if (interrupted) Thread.currentThread().interrupt();
    }

    ZmuxPipe joined(CompletableFuture<Long> f, long deadline) throws IOException {
        return joined(f, deadline, Long.MAX_VALUE);
    }

    /**
     * {@link #joined(CompletableFuture, long)} with the setup bounded by setupDeadline as well; the
     * join timeout alone bounds it otherwise, since a late-reporting setup may still succeed.
     */
    private ZmuxPipe joined(CompletableFuture<Long> f, long deadline, long setupDeadline) throws IOException {
        long id;
        try {
            id = setupDeadline == Long.MAX_VALUE ? f.get() : f.get(remaining(setupDeadline), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            // The exception cleared the flag, so the cleanup waits below block; it is restored after them.
            try { abandonJoinSetup(f); } finally { Thread.currentThread().interrupt(); }
            throw new InterruptedIOException("interrupted");
        } catch (TimeoutException e) {
            // A deadline shorter than the join timeout (replacePipes') passed during the setup.
            abandonJoinSetup(f);
            throw new SocketTimeoutException("join not set up within the call's deadline");
        } catch (ExecutionException e) {
            throw Errors.failure(e.getCause());
        }
        // The core also fails a local join still unconfirmed after its deadline (PathSession
        // timers), which reports GONE through the pipe-state wake-up; the call's deadline, from
        // its start, bounds it here as well.
        Object r = new Operation<>(driver, op -> switch (core.joinProgress(id)) {
            case CONFIRMED -> { driver.refreshPipes(); yield true; }
            case GONE -> false;
            case PENDING -> {
                if (core.error() != null) throw CoreException.session(core.error());
                driver.waitPipe(op);
                yield Operation.PENDING;
            }
        }, driver::cancelPipeWait, null, deadline).submit().await(deadline);
        if (r instanceof Throwable) {
            cancelJoinedPipe(id);
            Errors.check(r);
        }
        if (!Boolean.TRUE.equals(r)) throw new PipeGoneException(PipeId.of(id));
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
        return awaitBounded(wait, io.zmux.internal.core.Session.closeLimit);
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
        return driver.pipeSnapshot().stream().map(i -> new ZmuxPipe(this, i.pipe().value())).toList();
    }

    /** Maintains one route's joined pipes; closing the handle stops maintenance, preserving existing traffic. */
    public PipeMaintenance maintainPipes(PipePool pool, Cancellation cancellation) throws IOException {
        return PipeMaintenance.start(this, pool, cancellation);
    }

    /** {@link #maintainPipes(PipePool, Cancellation)} without a cancellation. */
    public PipeMaintenance maintainPipes(PipePool pool) throws IOException { return maintainPipes(pool, null); }

    /**
     * Whether a local join may start now: the attachments this endpoint joined that have neither failed nor been retired are below
     * maxPipes, the join keys are in place and no join backoff runs; only once the unretired attachments it joined reach
     * maxPipes + pipeHeadroom, the session must also be suspended with an attachment the cap may displace for a join that fills a
     * missing direction.
     */
    boolean joinReady() throws IOException {
        return (Boolean) run(op -> core.joinReady());
    }

    /** Re-checks every pipe after a suspected path change. */
    public void revalidatePipes() throws IOException {
        run(op -> {
            core.revalidatePipes(0, true);
            return true;
        });
    }

    /**
     * Ends {@link ZmuxConfig.Builder#holdFirstFlight(boolean)}: everything queued so far is built into
     * records and written to the bootstrap carrier in one write, as far as one batch holds it.
     * Calls after the first, and on a session without the hold, do nothing. It never blocks.
     * <b>Advanced:</b> only with {@link ZmuxConfig.Builder#holdFirstFlight(boolean)}.
     */
    public void releaseFirstFlight() {
        driver.post(() -> core.releaseFirstFlight(io.zmux.internal.loop.Clock.now()));
    }

    private volatile io.zmux.adapter.SessionAdapter adapterView;

    /**
     * The operations only a carrier adapter needs: join readiness. The end-to-end declaration and the release of a
     * held first flight are the application's, on the session itself.
     */
    public io.zmux.adapter.SessionAdapter adapter() {
        var a = adapterView;
        if (a == null) {
            adapterView = a = io.zmux.internal.spi.AdapterPorts.session(this::joinReady);
        }
        return a;
    }

    // The runtime settings as last set, read back by their getters (the session applies them on its loop).
    private volatile Scheduler scheduler;
    private volatile QualityBand qualityBand;
    private volatile CapacityFloor capacityFloor;

    /** Changes the session's scheduling policy. */
    public void setScheduler(Scheduler s) {
        Objects.requireNonNull(s);
        scheduler = s;
        driver.post(() -> driver.pickers.install(null, Scheduling.policy(s), Scheduling.picker(s)));
    }

    /** The session's scheduling policy: the configured one, or the last {@link #setScheduler}. */
    public Scheduler scheduler() { return scheduler; }

    /** The default quality event band of outbound pipes; empty when cleared or never set. */
    public java.util.Optional<QualityBand> qualityBand() { return java.util.Optional.ofNullable(qualityBand); }

    /** The outbound capacity floor; empty when cleared or never set. */
    public java.util.Optional<CapacityFloor> capacityFloor() { return java.util.Optional.ofNullable(capacityFloor); }

    /** Snapshots as of the loop's last round, including pipe 0. */
    public List<PipeStats> pipeStats() {
        return driver.pipeSnapshot();
    }

    /** Sets the default quality event band of outbound pipes; a pipe's own band ({@link PipeUpdate}) wins. */
    public void setQualityBand(QualityBand band) throws IOException {
        Objects.requireNonNull(band);
        run(op -> { core.setQualityBand(-1, band.rttNanos(), band.rawRateRatio(), band.minIntervalNanos()); return true; });
        qualityBand = band;
    }

    /** Clears the default quality event band: pipes without their own band report nothing. */
    public void clearQualityBand() throws IOException {
        run(op -> { core.clearQualityBand(-1); return true; });
        qualityBand = null;
    }

    /**
     * Sets the outbound capacity floor, with hysteresis; it reports at once when the capacity is below it. The
     * floor's values were checked when it was built; the IOException is the session's: it failed or closed.
     */
    public void setCapacityFloor(CapacityFloor floor) throws IOException {
        Objects.requireNonNull(floor);
        run(op -> { core.setCapacityFloor((double) floor.rate(), floor.recoveryRatio()); return true; });
        capacityFloor = floor;
    }

    /** Clears the capacity floor: no more capacity events. */
    public void clearCapacityFloor() throws IOException {
        run(op -> { core.setCapacityFloor(0, CapacityFloor.DEFAULT_RECOVERY_RATIO); return true; });
        capacityFloor = null;
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

    /** The shaping counters as of the loop's last round; no I/O. */
    public io.zmux.shaping.ShapingStats shapingStats() {
        var s = driver.shapingStats();
        return s == null ? io.zmux.shaping.ShapingStats.ZERO : s;
    }

    // ---- session ----

    /** Sends a QX_PING request (or joins the outstanding one) and returns the round-trip time. */
    public Duration ping() throws IOException {
        return ping(Long.MAX_VALUE);
    }

    /** Like {@link #ping()}, waiting at most timeout; {@link SocketTimeoutException} after it. */
    public Duration ping(Duration timeout) throws IOException {
        return ping(deadline(timeout));
    }

    private Duration ping(long deadline) throws IOException {
        return Duration.ofNanos((Long) call(pingStep(), op -> core.keepAlive().cancelWait(op), deadline));
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
        return nextEvent(Long.MAX_VALUE);
    }

    /** Waits at most timeout for the next session event; {@link SocketTimeoutException} after it. */
    public SessionEvent nextEvent(Duration timeout) throws IOException {
        return nextEvent(deadline(timeout));
    }

    private SessionEvent nextEvent(long deadline) throws IOException {
        return (SessionEvent) call(eventStep(), driver::cancelEventWait, deadline);
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
                SessionEvent event = ev instanceof SessionRuntime.RetiredPipes(long retired, long failed, long closed, List<Long> last)
                        ? new SessionEvent.PipesRetired(retired, failed, closed, last.stream().map(PipeId::of).toList())
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


    /** Completes when the first close or abort returned; null before either. */
    private final java.util.concurrent.atomic.AtomicReference<CompletableFuture<Void>> ending = new java.util.concurrent.atomic.AtomicReference<>();
    /** An abort was called: a close still flushing stops. */
    private volatile boolean aborted;

    /** The largest reason a close or an abort sends, in bytes of UTF-8 (SPEC.md Section 4.3, CONNECTION_CLOSE). */
    static final int MAX_REASON_BYTES = io.zmux.internal.wire.Utf8.MAX_REASON;

    /**
     * Closes gracefully without error: {@link #close(long, String)} with code 0 and an empty
     * reason. try-with-resources calls it on every exit, since {@code AutoCloseable.close} cannot
     * tell whether the block threw; an application that wants an abort on an exception calls
     * {@link #abort} in its {@code catch}.
     */
    @Override
    public void close() {
        closeWith(0, EMPTY_REASON);
    }

    /**
     * Closes gracefully with an application error code and reason, which the peer sees as
     * {@link SessionError.ApplicationClosed}: the data, finishes and resets the streams have
     * written and not yet sent are sent first, as far as flow control and the carriers allow, so that it reaches the peer
     * ahead of the code; then CONNECTION_CLOSE is handed to the carrier of every usable outbound
     * pipe (written to it; for a socket, taken into its send buffer). Open streams are not waited
     * for: they fail locally with the session's error. Returns once CONNECTION_CLOSE has been
     * handed over, at once when no pipe can carry it. Flush and hand-off take at most one second
     * together; in a session where CONNECTION_CLOSE waits for the peer's acknowledgement of earlier records, that
     * wait takes at most the linger timeout (at most two seconds), and the hand-off then takes at
     * most one second from the wait's end: what has not been handed over by then is discarded. Each
     * pipe's write side is then shut down and its inbound side read until the peer ends it or the
     * linger timeout passes, in the background. The process may exit once it returned;
     * {@link #closed()} completes at the session's whole end.
     *
     * <p>close and {@link #abort} are idempotent: the first call of either decides the code, the
     * reason and what is sent; a later call of either sends nothing more and returns no later than
     * the first, except that an abort while a close is still flushing discards what remains
     * unsent and returns at once. On an ended session it returns at once. Interruption does not
     * cut the wait short (the flag is kept). On the session's event-loop or callback thread it
     * returns at once and the same close (flush included, within the same bound) runs in the
     * background.
     *
     * @param code an application error code, from 0 to 2<sup>62</sup> - 1
     * @param reason the reason; beyond 1024 bytes of UTF-8 it is cut at a character boundary
     * @throws IllegalArgumentException when code is outside its range
     */
    public void close(long code, String reason) {
        closeWith(checkCode(code), reasonBytes(reason));
    }

    private static final byte[] EMPTY_REASON = new byte[0];

    private static long checkCode(long code) {
        if (code < 0 || code > (1L << 62) - 1) throw new IllegalArgumentException("application error code outside 0 to 2^62 - 1");
        return code;
    }

    private static byte[] reasonBytes(String reason) {
        return io.zmux.internal.wire.Utf8.truncate(Objects.requireNonNull(reason, "reason"), MAX_REASON_BYTES);
    }

    private void closeWith(long code, byte[] reason) {
        CompletableFuture<Void> mine = new CompletableFuture<>();
        CompletableFuture<Void> first = ending.compareAndExchange(null, mine);
        if (first != null) {
            if (driver.mayBlock()) awaitUninterruptibly(first, io.zmux.internal.core.Session.closeLimit + io.zmux.internal.core.Session.closeCeiling());
            return;
        }
        if (!driver.mayBlock()) {
            // Never blocks the loop or the callbacks: the flush and the hand-off wait elsewhere.
            Thread.ofVirtual().name("zmux-close").start(() -> {
                try { closeGracefully(code, reason); } finally { mine.complete(null); }
            });
            return;
        }
        try {
            closeGracefully(code, reason);
        } finally {
            mine.complete(null);
        }
    }

    private void closeGracefully(long code, byte[] reason) {
        long limit = io.zmux.internal.core.Session.closeLimit;
        long deadline = System.nanoTime() + limit;
        // The flush: until nothing written is unsent, an abort, or the bound less a tenth kept for
        // the hand-off, so that CONNECTION_CLOSE still goes out after data the peer's credit held back.
        long flushEnd = deadline - limit / 10;
        long pause = TimeUnit.MICROSECONDS.toNanos(200);
        while (!aborted && System.nanoTime() - flushEnd < 0) {
            // Not await: it waits for a queued operation's first attempt, which a stalled loop never makes.
            var op = new Operation<>(driver, o -> core.flushable(), null).submit();
            if (!op.awaitUninterruptibly(flushEnd) && op.cancel()) break;
            if (!Boolean.TRUE.equals(op.outcome())) break;
            java.util.concurrent.locks.LockSupport.parkNanos(Math.clamp(flushEnd - System.nanoTime(), 0, pause));
            pause = Math.min(pause * 2, TimeUnit.MILLISECONDS.toNanos(10));
        }
        // Ends the close at once: on a loop that has not run the hand-off yet it closes as well.
        Runnable endNow = () -> {
            core.closeBounded(code, reason, 0, io.zmux.internal.loop.Clock.now());
            core.endClose();
        };
        long left = deadline - System.nanoTime();
        if (left <= 0) {
            driver.post(endNow);
            return;
        }
        // The hand-off; the half-close and the linger then run on the loop.
        var op = new Operation<>(driver, o -> {
            core.closeBounded(code, reason, left, io.zmux.internal.loop.Clock.now());
            return core.waitCloseHandedOff(o) ? true : Operation.PENDING;
        }, core::cancelCloseHandOffWait);
        op.submit();
        // The core bounds the coverage wait and then the hand-off (each from its own start); this
        // ceiling only covers a loop that lags behind those bounds.
        if (!op.awaitUninterruptibly(System.nanoTime() + io.zmux.internal.core.Session.closeCeiling())) {
            // The loop's own bound ends the close as well; this covers a loop that lags behind it.
            driver.post(endNow);
            op.awaitUninterruptibly(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50));
            op.cancel();
        }
    }

    /**
     * Ends the session at once with an application error: CONNECTION_CLOSE with code and reason
     * is written on each usable outbound pipe whose carrier takes it without waiting, data not yet
     * sent is discarded, and open streams fail locally with the session's error. Returns without
     * waiting for any carrier; the carriers are shut down in the background as after a close. Code
     * 0 is allowed; an abort differs from {@link #close(long, String)} with the same code only in
     * what is drained. Idempotent with close: an abort after either sends nothing more and changes
     * neither the code nor the reason, except that one while a close is still flushing ends the
     * flush at once (the close's code and reason still go out).
     *
     * @param code an application error code, from 0 to 2<sup>62</sup> - 1
     * @param reason the reason; beyond 1024 bytes of UTF-8 it is cut at a character boundary
     * @throws IllegalArgumentException when code is outside its range
     */
    public void abort(long code, String reason) {
        checkCode(code);
        byte[] r = reasonBytes(reason);
        aborted = true;
        if (!ending.compareAndSet(null, CompletableFuture.completedFuture(null))) {
            // A close still flushing: it stops and closes; the first close's code stands.
            return;
        }
        startClose(code, r, io.zmux.internal.core.Session.closeLimit);
    }

    private static void awaitUninterruptibly(CompletableFuture<?> f, long nanos) {
        boolean interrupted = false;
        long end = System.nanoTime() + nanos;
        try {
            while (true) {
                long left = end - System.nanoTime();
                if (left <= 0) return;
                try {
                    f.get(left, TimeUnit.NANOSECONDS);
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

    /** Starts a close without waiting; the loop bounds the hand-off. */
    private void startClose(long code, byte[] reason, long limit) {
        driver.post(() -> core.closeBounded(code, reason, limit, io.zmux.internal.loop.Clock.now()));
    }

    /**
     * Closes gracefully with code and reason: GOAWAY when in use, then CONNECTION_CLOSE with them
     * once no stream remains. Returns when the session is closed. A stream remains until both its
     * parts have ended: the application has read its end (FIN or reset) and the peer has
     * acknowledged the sending part's end ({@link ZmuxStream#finish()} or a reset); its handle
     * need not be closed. Interruption closes the session as {@link #close(long, String)} does and
     * throws {@link InterruptedIOException}.
     *
     * @param code an application error code, from 0 to 2<sup>62</sup> - 1
     * @param reason the reason; beyond 1024 bytes of UTF-8 it is cut at a character boundary
     */
    public void shutdown(long code, String reason) throws IOException {
        shutdownWithin(code, reason, Long.MAX_VALUE);
    }

    /**
     * {@link #shutdown(long, String)} bounded by timeout: once it expires the session closes at
     * once, through {@link #close(long, String)}'s hand-off with the same code and reason, and the
     * call returns when it is closed.
     */
    public void shutdown(long code, String reason, Duration timeout) throws IOException {
        shutdownWithin(code, reason, deadline(Objects.requireNonNull(timeout, "timeout")));
    }

    private void shutdownWithin(long code, String reason, long deadline) throws IOException {
        driver.checkBlocking();
        byte[] r = startShutdown(code, reason);
        try {
            if (deadline == Long.MAX_VALUE) driver.closedFuture().get();
            else driver.closedFuture().get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            closeWith(code, r);
            awaitClosed();
        } catch (InterruptedException e) {
            closeWith(code, r);
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted");
        } catch (ExecutionException e) {
            throw Errors.failure(e.getCause());
        }
    }

    /** Validates code and reason and starts the graceful close; returns the reason's bytes. */
    private byte[] startShutdown(long code, String reason) {
        checkCode(code);
        byte[] r = reasonBytes(reason);
        driver.post(() -> core.shutdown(code, r));
        return r;
    }

    /**
     * {@link #shutdown(long, String)} without blocking: completes once the session is closed.
     * Completing the future from outside (cancel, {@link ZmuxFuture#orTimeout},
     * {@link ZmuxFuture#completeOnTimeout}) is the shutdown's bound: the session then closes at
     * once as {@link #close(long, String)} does, in the background.
     */
    public ZmuxFuture<Void> shutdownAsync(long code, String reason) {
        byte[] r = startShutdown(code, reason);
        return ZmuxFuture.of(driver.closedFuture().thenApply(e -> null),
                // Not on the thread that completed the future (the JDK's delay thread for orTimeout): close may wait for a flush.
                () -> Thread.ofVirtual().name("zmux-shutdown").start(() -> closeWith(code, r)));
    }

    /**
     * Completes with the session's error at its whole end, on the session's completion context:
     * once every pipe's inbound side has ended (the peer read CONNECTION_CLOSE and ended its side
     * of the carrier) or the linger timeout has expired, whichever comes first, and the closed
     * carriers were released (their reader and writer ended). Completing before the linger timeout
     * thus confirms that the peer read the close. To wait for "delivered or given up after d", bound
     * the wait itself: {@code closed().get(d, unit)}, or {@code closed().orTimeout(d, unit)} on a
     * stage; such a timeout changes nothing in the session, whose linger goes on and ends the
     * carriers. After the peer's close, the whole end also waits, within the same linger bound,
     * until the application has read what arrived before the close on the streams it opened or
     * accepted.
     */
    public ZmuxFuture<SessionError> closed() {
        return ZmuxFuture.of(driver.closedFuture().thenApply(Errors::session), null);
    }

    /** Waits for the session's whole end, as {@link #closed()} tells it, and returns why. */
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

    /** {@link #awaitClosed()} bounded by timeout: {@link SocketTimeoutException} after it, the session unchanged. */
    public SessionError awaitClosed(Duration timeout) throws IOException {
        driver.checkBlocking();
        long nanos = Math.max(1, Objects.requireNonNull(timeout, "timeout").toNanos());
        try {
            return Errors.session(driver.closedFuture().get(nanos, TimeUnit.NANOSECONDS));
        } catch (TimeoutException e) {
            throw new SocketTimeoutException("session not closed within " + timeout);
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

    /** The session's identity in this process, the one {@link Tracer} callbacks carry. */
    public SessionId id() { return driver.id(); }

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
    CarrierCredential credential() { return credential; }

    /** The carrier kind a carrier choice chose last; null: none. */
    CarrierKind carrierKind() { synchronized (carrierTries) { return carrier; } }

    /** The variant a carrier choice chose last; "" before a choice or without a label. */
    String carrierVariant() { synchronized (carrierTries) { return carrierVariant; } }

    private java.util.List<CandidateResult> carrierTries() { synchronized (carrierTries) { return java.util.List.copyOf(carrierTries); } }

    /** The key of a candidate's event in the event queue (the event key table): its carrier kind and variant. */
    private record CandidateKey(CarrierKind kind, String variant) {}

    /** Records tries of candidates and, when chosen is not null, the choice of kind: in the statistics and as events. */
    void reportCarrier(java.util.List<CandidateResult> results, CarrierKind chosen, String variant) {
        synchronized (carrierTries) {
            carrierTries.addAll(results);
            if (carrierTries.size() > CARRIER_TRIES_KEPT) carrierTries.subList(0, carrierTries.size() - CARRIER_TRIES_KEPT).clear();
            if (chosen != null) { carrier = chosen; carrierVariant = variant; }
        }
        driver.post(() -> {
            for (CandidateResult r : results) driver.addApplicationEvent(new CandidateKey(r.kind(), r.variant()), new SessionEvent.CarrierCandidate(r));
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

    /**
     * Waits until the peer's parameters arrived; throws when the session ended, also when it ended
     * at or after their arrival (a failure processing them, such as an exporter read, ends it in the
     * same step, and an operation submitted after the end fails the same way).
     */
    void awaitEstablished(long deadline) throws IOException {
        call(op -> {
            if (core.error() != null) throw CoreException.session(core.error());
            if (core.established()) return true;
            core.waitEstablished(op);
            return Operation.PENDING;
        }, null, deadline);
    }

    /** The pipe view of st, which takes the stream over. */
    private static PipeIo pipeOf(ZmuxStream st) {
        return st.asPipe();
    }
}
