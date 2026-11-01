package io.zmux;

import io.zmux.carrier.PipeIo;
import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.Event;
import io.zmux.internal.core.Session;
import io.zmux.internal.core.SessionState;
import io.zmux.internal.core.Stream;
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
 * and must not be called on a zmux event-loop thread; {@link #close()} never blocks.
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

    /** The application's designation of the bootstrap dialer, or null. */
    Boolean designatedDialer() { return designatedDialer; }

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
     * nested session (API.md Section 4.6); nothing is sent until the nested session's first record.
     */
    public io.zmux.carrier.PipeIo openPipeStream(List<Preamble.Tlv> tlvs) throws IOException {
        List<Preamble.Tlv> values = List.copyOf(tlvs);
        boolean preamble = (Boolean) run(op -> core.preambleInUse());
        if (!preamble && !values.isEmpty()) throw new IllegalArgumentException("application TLVs require the preamble extension");
        return pipeOf(open(true, preamble ? new Preamble(null, values, true) : Preamble.EMPTY, true, null));
    }

    /** {@link #openPipeStream(List)} without application TLVs. */
    public io.zmux.carrier.PipeIo openPipeStream() throws IOException { return openPipeStream(List.of()); }

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
    private final Paths pathView = new Paths(this);

    /** Session datagrams; operations throw {@link NotNegotiatedException} unless both sides enabled them. */
    public Datagrams datagrams() { return datagramView; }

    /** Multipath: pipes beyond the bootstrap carrier. */
    public Paths paths() { return pathView; }

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

    // ---- pipes (MULTIPATH.md 5) ----

    /**
     * Joins a new TCP connection to address as a pipe and waits until the join is confirmed.
     * A join that the attachment cap holds waits for room.
     */
    ZmuxPipe addPipe(InetSocketAddress address, PipeOptions o) throws IOException {
        driver.checkBlocking();
        if (o.carrierBound) throw new IllegalArgumentException("carrierBound needs a TLS carrier");
        SocketChannel ch = Zmux.dial(address, core.handshakeTimeout());
        Zmux.present(ch, credential, address);
        return joined(driver.addPipe(ch, o.join()));
    }

    /** Joins a new TLS connection (server name verified, the session's ALPN offered) as a pipe. */
    ZmuxPipe addPipeTls(InetSocketAddress address, TlsOptions tls, PipeOptions o) throws IOException {
        driver.checkBlocking();
        SocketChannel ch = Zmux.dial(address, core.handshakeTimeout());
        String alpn = applicationProtocol.isEmpty() ? ZmuxConfig.DEFAULT_ALPN : applicationProtocol;
        SSLEngine engine = tls.clientEngine(address.getPort(), List.of(alpn));
        return joined(driver.addPipe(ch, engine, core.handshakeTimeout(), o.join(), Zmux.presenter(credential, address)));
    }

    /** Reserves a pipe for an out-of-band PIPE_JOIN header, before opening its carrier. */
    PipeReservation reservePipe(PipeOptions options) throws IOException {
        Objects.requireNonNull(options);
        if (options.carrierBound) throw new IllegalArgumentException("a reservation cannot bind to a handshake that does not exist yet");
        var reserved = (io.zmux.internal.core.Joins.Reservation) run(op -> { var r = driver.reservePipe(options.join()); driver.refreshPipes(); return r; });
        return new PipeReservation(this, reserved.id(), Base64.getUrlEncoder().withoutPadding().encodeToString(reserved.frame()));
    }

    /** Joins a pipe input as a pipe. */
    ZmuxPipe addPipe(PipeIo io, PipeOptions o) throws IOException {
        if (io.carrier() == CarrierKind.SPLIT && io.reader() != null && io.writer() != null) {
            // Its pairing makes pipe pair 0 only (SPEC.md 6.1.1): a further pipe joins as an uplink and a downlink.
            throw new IllegalArgumentException("a split bootstrap carrier cannot join a session; join an uplink and a downlink");
        }
        driver.checkBlocking();
        return joined(driver.addPipe(io, o.join()));
    }

    ZmuxPipe joined(CompletableFuture<Long> f) throws IOException {
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
        // No deadline here: the core fails a local join still unconfirmed after handshakeTimeout
        // (PathSession timers), which reports GONE through the pipe-state wake-up.
        Object r = new Operation<>(driver, op -> switch (core.joinProgress(id)) {
            case CONFIRMED -> { driver.refreshPipes(); yield Boolean.TRUE; }
            case GONE -> Boolean.FALSE;
            case PENDING -> {
                if (core.error() != null) throw CoreException.session(core.error());
                driver.waitPipe(op);
                yield Operation.PENDING;
            }
        }, driver::cancelPipeWait).submit().await(Long.MAX_VALUE);
        if (r instanceof Throwable) {
            cancelJoinedPipe(id);
            Errors.check(r);
        }
        if (!Boolean.TRUE.equals(r)) throw new PipeGoneException(id);
        return new ZmuxPipe(this, id);
    }

    private void cancelJoinedPipe(long id) {
        var done = new CompletableFuture<Void>();
        if (driver.executeCarrier(() -> {
            try { abandon(id); done.complete(null); } catch (Throwable error) { done.completeExceptionally(error); }
        })) done.join();
        else driver.closedFuture().join();
    }

    private void abandon(long id) {
        try {
            core.removePipe(id, false);
        } catch (CoreException ignored) {
            // Already gone.
        }
    }

    /** The attachments as of the loop's last round. */
    List<ZmuxPipe> pipes() {
        return driver.pipeSnapshot().stream().map(i -> new ZmuxPipe(this, i.pipe())).toList();
    }

    /** Maintains one route's joined pipes; closing the handle stops maintenance, preserving existing traffic. */
    PipeMaintenance maintainPipes(PipePool pool, Cancellation cancellation) throws IOException {
        return PipeMaintenance.start(this, pool, cancellation);
    }

    /** Whether the maintenance target and join admission permit dialing another pipe now. */
    boolean joinReady() throws IOException {
        return (Boolean) run(op -> core.joinReady());
    }

    /** Re-checks every pipe after a suspected path change. */
    void revalidatePipes() throws IOException {
        run(op -> {
            core.revalidatePipes(0, true);
            return Boolean.TRUE;
        });
    }

    /** Changes the session's scheduling policy. */
    void setScheduler(Scheduler s) {
        Objects.requireNonNull(s);
        driver.post(() -> driver.pickers.install(null, Scheduling.policy(s), Scheduling.picker(s)));
    }

    /** Snapshots as of the loop's last round, including pipe 0. */
    List<PipeStats> pipeStats() {
        return driver.pipeSnapshot();
    }

    /** The default quality event band of outbound pipes. */
    void setQualityBand(QualityBand band) throws IOException {
        Objects.requireNonNull(band);
        run(op -> { core.setQualityBand(-1, ZmuxConfig.nanos(band.rtt()), band.rateRatio(),
                ZmuxConfig.nanos(band.minInterval())); return Boolean.TRUE; });
    }

    /** The outbound capacity floor, with hysteresis. */
    void setCapacityFloor(CapacityFloor floor) throws IOException {
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
    public void setShaper(io.zmux.shaping.ShaperFactory factory) {
        driver.post(() -> core.setShaper(factory));
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

    /**
     * Closes with code 0 at once (CONNECTION_CLOSE, then linger). The close is only queued:
     * this method returns before the session has closed. Use {@link #awaitClosed()} to wait for
     * the end, or {@link #shutdown(Duration)} to close gracefully and wait.
     */
    @Override
    public void close() {
        close(ApplicationError.NONE);
    }

    /** Closes with an application error at once; queued like {@link #close()}, without waiting. */
    public void close(ApplicationError error) {
        byte[] r = error.reason().getBytes(StandardCharsets.UTF_8);
        driver.post(() -> core.close(error.code(), r));
    }

    /**
     * Closes gracefully: GOAWAY when in use, then CONNECTION_CLOSE once no stream remains;
     * after timeout it closes at once. Returns when the session is closed.
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
            Thread.currentThread().interrupt();
            close();
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
                .exceptionally(error -> { close(); return null; });
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

    /** The negotiation results as of the session loop's last round: a snapshot, no I/O. */
    public ConnectionInfo info() {
        var i = driver.info();
        return new ConnectionInfo(i.established(), i.initiator(), i.peerIsZmux(), applicationProtocol, i.goAway(), i.preamble(),
                i.priority(), i.datagramFlows(), i.pipeAck(), i.multipath(), i.lanesToPeer(), i.maxDatagramPayload());
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

    private java.util.List<CandidateResult> carrierTries() { synchronized (carrierTries) { return java.util.List.copyOf(carrierTries); } }

    /** Records tries of candidates and, when chosen is not null, the choice of kind: in the statistics and as events. */
    void reportCarrier(java.util.List<CandidateResult> results, CarrierKind chosen) {
        synchronized (carrierTries) {
            carrierTries.addAll(results);
            if (carrierTries.size() > CARRIER_TRIES_KEPT) carrierTries.subList(0, carrierTries.size() - CARRIER_TRIES_KEPT).clear();
            if (chosen != null) carrier = chosen;
        }
        driver.post(() -> {
            for (CandidateResult r : results) driver.addApplicationEvent(r.kind(), new SessionEvent.CarrierCandidate(r));
            if (chosen != null) driver.addApplicationEvent(SessionEvent.CarrierChosen.class, new SessionEvent.CarrierChosen(chosen));
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
