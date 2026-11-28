package io.zmux;

import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.Tiers;
import io.zmux.internal.core.Session;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * One attachment of a multipath session. Thread-safe. Once the attachment is retired (or the
 * session that ended released its pipes) the reads {@link #state()} and {@link #stats()}
 * report absence and the operations fail with {@link PipeGoneException}; retirement itself
 * is reported by {@link SessionEvent.PipesRetired}.
 */
public final class ZmuxPipe {
    private final ZmuxSession session;
    private final long id;
    private final PipeId pipeId;
    private final CompletableFuture<Void> confirmation;
    private volatile PipeAccounting.Ticket accounting;

    /** An accounting receipt that can outlive this attachment. */
    public Optional<PipeAccounting.Ticket> accounting() {
        var current = accounting;
        if (current == null) {
            current = session.driver.accounting(id);
            if (current != null) accounting = current;
        }
        return Optional.ofNullable(current);
    }
    /** This handle aborted the pipe: its waits then report the abort even after the pipe's retirement. */
    private volatile boolean aborted;

    ZmuxPipe(ZmuxSession session, long id) {
        this(session, id, session.driver.confirmation(id));
    }

    ZmuxPipe(ZmuxSession session, long id, CompletableFuture<Void> confirmation) {
        this.session = session;
        this.id = id;
        this.pipeId = PipeId.of(id);
        this.confirmation = confirmation;
        accounting = session.driver.accounting(id);
        this.held = session.heldOptions.get(id); // a pipe held before this handle was made keeps its options after its end
    }

    /** What the pipe was added with, until the session holds anything for it. */
    private volatile PipeOptions given;
    /** The holder of the pipe's options, once this handle met it; it outlives the pipe's entry in the session, so {@link #options()} reads the last held after the end. */
    private volatile ZmuxSession.HeldOptions held;

    /** Records the options the pipe was added with, if the pipe is still there; before its first update. */
    void hold(PipeOptions o) throws IOException {
        given = o;
        session.run(op -> {
            io.zmux.internal.core.Session.PipeFixed f = session.core.pipeFixed(id);
            ZmuxSession.HeldOptions h = f == null ? null : session.heldOf(id);
            if (h != null) {
                held = h;
                h.set(o.fixed(f));
            }
            return true;
        });
    }

    /**
     * The pipe's options as it holds them: those it was added with, every {@link #update} applied; a setting cleared
     * or never set reads as unset, following the peer or the session. What was fixed when the pipe was added reads as
     * its actual value: the direction (from this endpoint's view), the record mode (also where the record policy chose
     * it) and key group the join used, and the fate group. A pipe the peer joined reads those and the defaults for
     * every other field, with the updates since; {@code returnOnReply} is read as it was given to {@code addPipe}. Local state: it never fails or waits, and answers after the pipe's end with the last held.
     */
    public PipeOptions options() {
        ZmuxSession.HeldOptions h = held;
        if (h == null) {
            h = session.heldOptions.get(id);
            if (h != null) held = h;
        }
        PipeOptions o = h == null ? null : h.value();
        if (o == null) o = given;
        return o != null ? o : initial();
    }

    /** The options of a pipe nothing was held for (the peer added it): the defaults with what its join fixed. */
    private PipeOptions initial() {
        var f = session.driver.pipeFixed().get(id);
        return f != null ? PipeOptions.defaults().fixed(f) : PipeOptions.defaults();
    }

    /** The pipe ID. */
    public PipeId id() {
        return pipeId;
    }

    /** The pipe's directional health as of one turn of the session loop. */
    public Optional<PipeHealth> health() throws IOException {
        return Optional.ofNullable((PipeHealth) session.run(op -> session.core.pipeHealth(id)));
    }

    /** The pipe ID's value, for the session's own calls. */
    long rawId() {
        return id;
    }

    public ZmuxSession session() {
        return session;
    }

    /** Whether this pipe belongs to s. */
    boolean belongsTo(ZmuxSession s) {
        return session == s;
    }

    /**
     * The value of the admission that accepted this pipe's carrier, the one that carried its
     * Hello ({@link Admission.Decision#value()}; for the bootstrap pipe the session's,
     * {@link ZmuxSession#admitted()}); empty without one, on the dialing side, and once the
     * pipe is retired. zmux does not compare it with the session's: a host that requires a
     * join to come from the session's client compares them, in its {@link InboundTransform} or
     * here.
     */
    public Optional<Object> admitted() throws IOException {
        if (id == 0) return session.admitted();
        return Optional.ofNullable(session.run(op -> session.core.pipes().admitted(id)));
    }

    /** {@link #admitted()} when it is a type; empty otherwise. */
    public <T> Optional<T> admitted(Class<T> type) throws IOException { return admitted().filter(type::isInstance).map(type::cast); }

    /** The states of the outbound and inbound directions; empty once retired. */
    public Optional<PipeState> state() throws IOException {
        Session core = session.core;
        Object st = session.run(op -> {
            var states = core.pipeState(id);
            return states == null || states.out() == io.zmux.internal.core.PipeInfo.Out.RETIRED ? MISSING : states;
        });
        if (st == MISSING) return Optional.empty();
        var s = (io.zmux.internal.core.PipeInfo.States) st;
        return Optional.of(new PipeState(s.out().toPublic(), s.in().toPublic()));
    }

    static final Object MISSING = new Object();

    /** A current immutable snapshot; empty once retired. */
    public Optional<PipeStats> stats() throws IOException {
        var snapshot = (io.zmux.internal.loop.SessionRuntime.PipeRead) session.run(op -> session.driver.capturePipeStats(id));
        return Optional.ofNullable(snapshot.read());
    }

    /** The tier settings a {@link PipeUpdate} changes, every field other than the quality band and liveness. */
    private static Tiers.PipeSettings settingsOf(PipeUpdate u) {
        Tiers.PipeSettings.Builder b = Tiers.PipeSettings.builder();
        change(u.sendPriority, () -> b.sendPriority(u.sendPriorityValue), () -> b.clearSendPriority(true));
        change(u.sendStandby, () -> b.sendStandby(u.sendStandbyValue), () -> b.clearSendStandby(true));
        change(u.receivePriority, () -> b.recvPriority(u.receivePriorityValue), () -> b.recvPriority(0L));
        change(u.receiveStandby, () -> b.recvStandby(u.receiveStandbyValue), () -> b.recvStandby(false));
        change(u.weight, () -> b.weight(u.weightValue), () -> b.weight(1L));
        change(u.probation, () -> b.probation(u.probationValue == null ? 0L : ZmuxConfig.nanos(u.probationValue)),
                () -> b.probation(-1L));
        return b.build();
    }

    /** Runs set or clear as c says; KEEP runs neither. */
    private static void change(PipeUpdate.Change c, Runnable set, Runnable clear) {
        switch (c) {
            case SET -> set.run();
            case CLEAR -> clear.run();
            case KEEP -> { }
        }
    }

    /**
     * Changes the pipe's runtime settings together ({@link PipeUpdate}): one call applies all its
     * settings at once, and one that changes both receive settings sends one PIPE_STATUS. A pipe
     * already suspect keeps the suspicion floor and dead timeout it turned suspect under. Changes
     * apply from the next scheduling decision; the settings in force are in {@link #stats()}.
     */
    public void update(PipeUpdate u) throws IOException {
        java.util.Objects.requireNonNull(u, "u");
        Tiers.PipeSettings settings = u.onlyBand() ? null : settingsOf(u);
        PipeLiveness l = switch (u.liveness) {
            case SET -> u.livenessValue;
            case CLEAR -> PipeLiveness.inherit();
            case KEEP -> null;
        };
        Session core = session.core;
        PipeUpdate.Change band = u.qualityBand;
        QualityBand bandValue = u.qualityBandValue;
        boolean[] noOut = new boolean[1];
        gone(() -> {
            // The band first: the only setting refused for the pipe's kind (no outbound direction), before anything changed.
            if (band == PipeUpdate.Change.SET) noOut[0] = bandValue == null ? !core.offQualityBand(id)
                    : !core.setQualityBand(id, bandValue.rttNanos(), bandValue.rawRateRatio(), bandValue.minIntervalNanos());
            else if (band == PipeUpdate.Change.CLEAR) noOut[0] = !core.clearQualityBand(id);
            if (noOut[0]) return;
            if (settings != null) core.setPipe(id, settings);
            if (l != null) core.setPipeLiveness(id, l.suspectFloorNanos(), l.deadTimeoutNanos(), l.probeIntervalNanos(), l.probeJitterRaw());
            var f = core.pipeFixed(id);
            ZmuxSession.HeldOptions h = f == null ? null : session.heldOf(id);
            if (h != null) { // a pipe that is gone gets no entry again
                held = h;
                h.apply(u, f);
            }
        });
        if (noOut[0]) throw new IllegalStateException("pipe " + id + " has no outbound direction for a quality band");
    }

    /**
     * Installs shaper on this pipe's outbound direction until the session's next
     * {@link ZmuxSession#setShaper}; null: the pipe is not shaped. <b>Advanced:</b> most hosts set
     * one factory with {@link ZmuxSession#setShaper}.
     *
     * @throws IllegalStateException when the pipe has no outbound direction
     * @throws PipeGoneException once the pipe is retired
     */
    public void setShaper(io.zmux.shaping.Shaper shaper) throws IOException {
        Session core = session.core;
        boolean[] set = new boolean[1];
        gone(() -> {
            if (core.pipeState(id) == null) throw CoreException.of(CoreException.Reason.INVALID);
            set[0] = core.setPipeShaper(id, shaper);
        });
        if (!set[0]) throw new IllegalStateException("pipe " + id + " has no outbound direction");
    }

    /**
     * Waits until the peer is known to hold this pipe: a pipe this side joined
     * counts once a PIPE_ACK on any lane covered more than zero bytes or, receive-only, once a
     * record arrived; pipe 0 and pipes the peer joined are held from the start. Joining waits for
     * this unless {@link PipeOptions.Builder#returnOnReply(boolean)} is set, when it returns once the Reply
     * verified, before the acceptor read the Confirm: removing the session's other pipes earlier
     * may leave the peer without a live pipe. {@link PipeGoneException} once the pipe is
     * retired, {@link SessionClosedException} once the session ended.
     */
    public void awaitAttached() throws IOException {
        awaitAttached(Long.MAX_VALUE);
    }

    /** {@link #awaitAttached()} bounded by timeout: {@link java.net.SocketTimeoutException} after it. */
    public void awaitAttached(Duration timeout) throws IOException {
        awaitAttached(ZmuxSession.deadline(Objects.requireNonNull(timeout, "timeout")));
    }

    private void awaitAttached(long deadline) throws IOException {
        if (confirmation != null) {
            session.driver.checkBlocking();
            try {
                if (deadline == Long.MAX_VALUE) confirmation.get();
                else confirmation.get(Math.max(0, deadline - System.nanoTime()), java.util.concurrent.TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("interrupted");
            } catch (java.util.concurrent.TimeoutException e) {
                throw new java.net.SocketTimeoutException("wait for attachment timed out");
            } catch (java.util.concurrent.ExecutionException e) {
                throw Errors.failure(e.getCause());
            }
            return;
        }
        Object r = session.call(attachedStep(), session.core::cancelPeerAttachedWait, deadline);
        if (r == MISSING) throw gone();
    }

    /** Like {@link #awaitAttached()} without blocking; cancelling the future withdraws the wait. */
    public ZmuxFuture<Void> attached() {
        if (confirmation != null) return ZmuxFuture.of(confirmation.whenCompleteAsync((v, e) -> {}, session.driver::publishCompletion), null);
        return session.async(attachedStep(), session.core::cancelPeerAttachedWait, null, Long.MAX_VALUE, r -> {
            if (r == MISSING) throw gone();
            return null;
        });
    }

    /** What a wait reports once the pipe is gone: the abort, when this endpoint aborted it, else the pipe's absence. */
    PipeException gone() {
        return aborted ? new PipeAbortedException(pipeId) : new PipeGoneException(pipeId);
    }

    io.zmux.internal.loop.Operation.Step attachedStep() {
        Session core = session.core;
        return op -> {
            try {
                Boolean held = core.waitPeerAttached(id, op);
                return held != null ? held : io.zmux.internal.loop.Operation.PENDING;
            } catch (CoreException e) {
                if (e.reason() == CoreException.Reason.INVALID) return MISSING;
                if (e.reason() == CoreException.Reason.PIPE_ABORTED) { aborted = true; return MISSING; }
                throw e;
            }
        };
    }

    /** Re-checks the pipe after a suspected path change. */
    public void revalidate() throws IOException {
        Session core = session.core;
        gone(() -> core.revalidatePipes(id, false));
    }

    /**
     * Plans a graceful drain when other attachments cover both directions. Its data moves to
     * those attachments and {@link #closed()} completes when removal finishes. Repeated calls,
     * coverage blocks and calls after session termination leave the attachment as it is.
     */
    public void close() {
        remove(true);
    }

    /**
     * Fails the pipe and closes its carrier at once, its unacknowledged data reinjected on the
     * other pipes; protocol bookkeeping may await retirement ({@link #closed()}). Idempotent as
     * {@link #close()} is.
     */
    public void abort() {
        aborted = true;
        remove(false);
    }

    private void remove(boolean graceful) {
        Session core = session.core;
        if (!session.driver.mayBlock()) {
            removeQueued(graceful);
            return;
        }
        try {
            gone(() -> remove(core, graceful));
        } catch (PipeGoneException | SessionClosedException e) {
            // Already removed, or released by the session's end: nothing left to do.
        } catch (InterruptedIOException e) {
            // The interrupt withdrew the waiting operation before it ran: a removal is not
            // shortened by it, so it is queued instead (the flag stays set).
            Thread.currentThread().interrupt();
            removeQueued(graceful);
        } catch (IOException e) {
            // Other failures of a removal leave the pipe to the session's own end.
        }
    }

    private void remove(Session core, boolean graceful) throws CoreException {
        if (graceful) {
            core.drainPipe(id);
        } else {
            strandWarning(core);
            core.removePipe(id, false);
        }
    }

    /**
     * Completes after protocol retirement and actual release of the attachment, including its
     * lanes, outstanding writes and adapter cleanup. If the session ends first, the wait includes
     * its close notification and any remaining attachment cleanup.
     * Cancelling the future withdraws only this wait.
     */
    public ZmuxFuture<Void> closed() {
        Session core = session.core;
        CompletableFuture<Void> done = new CompletableFuture<>();
        var op = new io.zmux.internal.loop.Operation<>(session.driver, o -> core.waitPipeRemoved(id, o) ? true : io.zmux.internal.loop.Operation.PENDING, core::cancelPipeRemovedWait);
        Runnable removed = () -> { if (Boolean.TRUE.equals(op.outcome())) done.complete(null); };
        if (op.onDone(removed)) op.submit(); else removed.run();
        Runnable unsubscribe = session.driver.watchClosed(error ->
                session.driver.closedFuture().thenRun(() -> done.complete(null)));
        Runnable stop = () -> { unsubscribe.run(); op.cancel(); };
        var unsubscribed = done.whenComplete((v, e) -> stop.run());
        return ZmuxFuture.of(unsubscribed.thenCompose(v -> session.driver.attachmentClosed(id)), stop);
    }

    /** Waits for {@link #closed()}. */
    public void awaitClosed() throws IOException {
        session.driver.checkBlocking();
        var f = closed();
        try {
            f.get();
        } catch (InterruptedException e) {
            f.cancel(false);
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted");
        } catch (java.util.concurrent.ExecutionException e) {
            throw Errors.failure(e.getCause());
        }
    }

    /** {@link #awaitClosed()} bounded by timeout: {@link java.net.SocketTimeoutException} after it. */
    public void awaitClosed(Duration timeout) throws IOException {
        session.driver.checkBlocking();
        var f = closed();
        try {
            f.get(Math.max(1, Objects.requireNonNull(timeout, "timeout").toNanos()), java.util.concurrent.TimeUnit.NANOSECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            f.cancel(false);
            throw new java.net.SocketTimeoutException("pipe " + id + " not removed within " + timeout);
        } catch (InterruptedException e) {
            f.cancel(false);
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted");
        } catch (java.util.concurrent.ExecutionException e) {
            throw Errors.failure(e.getCause());
        }
    }

    /** Logs the removal of the last pipe of a direction while streams are open; on the loop. */
    private void strandWarning(Session core) {
        if (core.removalStrands(id)) {
            session.driver.hooks().warn("zmux: removing pipe " + id + ", the last of a direction, while streams are open: the session "
                    + (core.config().resume() > 0 ? "suspends" : "ends with pipes lost"));
        }
    }

    /**
     * Like {@link #abort} without waiting and without being interruptible: queued ahead of
     * every later session command. For reclaiming a pipe its creator rejected.
     */
    void abortQueued() {
        removeQueued(false);
    }

    /** Queues the removal on the loop without waiting; a pipe already retired is left as it is. */
    private void removeQueued(boolean graceful) {
        Session core = session.core;
        session.driver.post(() -> {
            try { remove(core, graceful); } catch (CoreException e) { /* Already retired. */ }
        });
    }

    @FunctionalInterface
    private interface Call {
        void run() throws CoreException;
    }

    private void gone(Call c) throws IOException {
        session.driver.checkBlocking();
        Object r = new io.zmux.internal.loop.Operation<>(session.driver, op -> {
            try {
                c.run();
            } catch (CoreException e) {
                if (e.reason() == CoreException.Reason.INVALID) return MISSING;
                throw e;
            }
            return true;
        }, null).submit().await(Long.MAX_VALUE);
        if (r == MISSING) throw new PipeGoneException(pipeId);
        Errors.check(r);
    }

    @Override
    public String toString() {
        return "ZmuxPipe[" + id + "]";
    }
}
