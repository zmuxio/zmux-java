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

    ZmuxPipe(ZmuxSession session, long id) {
        this.session = session;
        this.id = id;
        this.pipeId = PipeId.of(id);
    }

    /** The pipe ID. */
    public PipeId id() {
        return pipeId;
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
        return Optional.ofNullable(session.run(op -> {
            return session.core.pipes().admitted(id);
        }));
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
        return Optional.ofNullable((PipeStats) session.run(op -> {
            session.driver.refreshUnsent(id);
            return session.core.pipeStats(id, io.zmux.internal.loop.Clock.now(), System.nanoTime());
        }));
    }

    /**
     * Changes the pipe's runtime settings together ({@link PipeUpdate}): one call applies all its
     * settings at once, and one that changes both receive settings sends one PIPE_STATUS. A pipe
     * already suspect keeps the suspicion floor and dead timeout it turned suspect under. Changes
     * apply from the next scheduling decision; the settings in force are in {@link #stats()}.
     */
    public void update(PipeUpdate u) throws IOException {
        java.util.Objects.requireNonNull(u, "u");
        Tiers.PipeSettings.Builder b = Tiers.PipeSettings.builder();
        switch (u.sendPriority) {
            case SET -> b.sendPriority(u.sendPriorityValue);
            case CLEAR -> b.clearSendPriority(true);
            case KEEP -> { }
        }
        switch (u.sendStandby) {
            case SET -> b.sendStandby(u.sendStandbyValue);
            case CLEAR -> b.clearSendStandby(true);
            case KEEP -> { }
        }
        switch (u.receivePriority) {
            case SET -> b.recvPriority(u.receivePriorityValue);
            case CLEAR -> b.recvPriority(0L);
            case KEEP -> { }
        }
        switch (u.receiveStandby) {
            case SET -> b.recvStandby(u.receiveStandbyValue);
            case CLEAR -> b.recvStandby(false);
            case KEEP -> { }
        }
        switch (u.weight) {
            case SET -> b.weight(u.weightValue);
            case CLEAR -> b.weight(1L);
            case KEEP -> { }
        }
        switch (u.probation) {
            case SET -> b.probation(u.probationValue == null ? 0L : ZmuxConfig.nanos(u.probationValue));
            case CLEAR -> b.probation(-1L);
            case KEEP -> { }
        }
        Tiers.PipeSettings settings = u.onlyBand() ? null : b.build();
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
            if (band == PipeUpdate.Change.SET) noOut[0] = !core.setQualityBand(id, ZmuxConfig.nanos(bandValue.rtt()), bandValue.rateRatio(),
                    ZmuxConfig.nanos(bandValue.minInterval()));
            else if (band == PipeUpdate.Change.CLEAR) noOut[0] = !core.clearQualityBand(id);
            if (noOut[0]) return;
            if (settings != null) core.setPipe(id, settings);
            if (l != null) core.setPipeLiveness(id, l.suspectFloorNanos(), l.deadTimeoutNanos(), l.probeIntervalNanos(), l.probeJitterRaw());
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
    public void awaitPeerAttached() throws IOException {
        awaitPeerAttached(Long.MAX_VALUE);
    }

    /** {@link #awaitPeerAttached()} bounded by timeout: {@link java.net.SocketTimeoutException} after it. */
    public void awaitPeerAttached(Duration timeout) throws IOException {
        awaitPeerAttached(ZmuxSession.deadline(Objects.requireNonNull(timeout, "timeout")));
    }

    private void awaitPeerAttached(long deadline) throws IOException {
        Object r = session.call(attachedStep(), session.core::cancelPeerAttachedWait, deadline);
        if (r == MISSING) throw new PipeGoneException(pipeId);
    }

    /** Like {@link #awaitPeerAttached()} without blocking; cancelling the future withdraws the wait. */
    public ZmuxFuture<Void> peerAttachedAsync() {
        return session.async(attachedStep(), session.core::cancelPeerAttachedWait, null, Long.MAX_VALUE, r -> {
            if (r == MISSING) throw new PipeGoneException(pipeId);
            return null;
        });
    }

    io.zmux.internal.loop.Operation.Step attachedStep() {
        Session core = session.core;
        return op -> {
            try {
                Boolean held = core.waitPeerAttached(id, op);
                return held != null ? held : io.zmux.internal.loop.Operation.PENDING;
            } catch (CoreException e) {
                if (e.reason() == CoreException.Reason.INVALID) return MISSING;
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
     * Retires the pipe gracefully: its data moves to the other pipes first. Returns once the
     * removal started; {@link #closed()} completes when it finished. Idempotent: on a pipe already
     * removed, or of an ended session, it does nothing.
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
        remove(false);
    }

    private void remove(boolean graceful) {
        Session core = session.core;
        if (!session.driver.mayBlock()) {
            removeQueued(graceful);
            return;
        }
        try {
            gone(() -> { strandWarning(core); core.removePipe(id, graceful); });
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

    /**
     * Completes when the pipe was removed, its retirement and the session's bookkeeping for it
     * done, or, when the session ends first, at the session's whole end ({@link ZmuxSession#closed()}).
     * Cancelling the future withdraws only this wait.
     */
    public ZmuxFuture<Void> closed() {
        Session core = session.core;
        CompletableFuture<Void> done = new CompletableFuture<>();
        var op = new io.zmux.internal.loop.Operation<>(session.driver, o -> {
            return core.waitPipeRemoved(id, o) ? Boolean.TRUE : io.zmux.internal.loop.Operation.PENDING;
        }, core::cancelPipeRemovedWait);
        Runnable removed = () -> { if (Boolean.TRUE.equals(op.outcome())) done.complete(null); };
        if (op.onDone(removed)) op.submit(); else removed.run();
        session.driver.closedFuture().whenComplete((v, e) -> done.complete(null));
        done.whenComplete((v, e) -> op.cancel());
        return ZmuxFuture.of(done, op::cancel);
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
            try { strandWarning(core); core.removePipe(id, graceful); } catch (CoreException e) { /* Already retired. */ }
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
            return Boolean.TRUE;
        }, null).submit().await(Long.MAX_VALUE);
        if (r == MISSING) throw new PipeGoneException(pipeId);
        Errors.check(r);
    }

    @Override
    public String toString() {
        return "ZmuxPipe[" + id + "]";
    }
}
