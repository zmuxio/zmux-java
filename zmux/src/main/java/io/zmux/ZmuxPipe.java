package io.zmux;

import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.Tiers;
import io.zmux.internal.core.Session;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

/**
 * One attachment of a multipath session. Thread-safe. Once the attachment is retired (or the
 * session that ended released its pipes) the reads {@link #state()} and {@link #stats()}
 * report absence and the operations fail with {@link PipeGoneException}; retirement itself
 * is reported by {@link SessionEvent.PipesRetired}.
 */
public final class ZmuxPipe {
    private final ZmuxSession session;
    private final long id;

    ZmuxPipe(ZmuxSession session, long id) {
        this.session = session;
        this.id = id;
    }

    /** The pipe ID. */
    public long id() {
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
     * PIPE_JOIN ({@link Admission.Decision#value()}; for the bootstrap pipe the session's,
     * {@link ZmuxSession#admitted()}); empty without one, on the dialing side, and once the
     * pipe is retired. zmux does not compare it with the session's: a host that requires a
     * join to come from the session's client compares them, in its {@link InboundTransform} or
     * here.
     */
    public Optional<Object> admitted() throws IOException {
        if (id == 0) return session.admitted();
        return Optional.ofNullable(session.run(op -> {
            Object v = session.core.pipes().admitted(id);
            return v;
        }));
    }

    /** The states of the outbound and inbound directions (STATE_MACHINE.md 3); empty once retired. */
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

    private static final Object MISSING = new Object();

    /** A current immutable snapshot; empty once retired. */
    public Optional<PipeStats> stats() throws IOException {
        return Optional.ofNullable((PipeStats) session.run(op -> {
            session.driver.refreshUnsent(id);
            return session.core.pipeStats(id, io.zmux.internal.loop.Clock.now(), System.nanoTime());
        }));
    }

    /** Overrides the session's quality event band for this outbound pipe. */
    public void setQualityBand(QualityBand band) throws IOException {
        java.util.Objects.requireNonNull(band);
        gone(() -> session.core.setQualityBand(id, ZmuxConfig.nanos(band.rtt()), band.rateRatio(),
                ZmuxConfig.nanos(band.minInterval())));
    }

    private void set(Tiers.PipeSettings o) throws IOException {
        Session core = session.core;
        gone(() -> core.setPipe(id, o));
    }

    public void setSendPriority(long tier) throws IOException {
        if (tier < 0 || tier > io.zmux.carrier.VarInt.MAX) throw new IllegalArgumentException("tier exceeds varint62");
        set(Tiers.PipeSettings.builder().sendPriority(tier).build());
    }

    /** The local outbound tier follows the peer's request again. */
    public void clearSendPriority() throws IOException {
        set(Tiers.PipeSettings.builder().clearSendPriority(true).build());
    }

    public void setReceivePriority(long tier) throws IOException {
        if (tier < 0 || tier > io.zmux.carrier.VarInt.MAX) throw new IllegalArgumentException("tier exceeds varint62");
        set(Tiers.PipeSettings.builder().recvPriority(tier).build());
    }

    public void setSendStandby(boolean on) throws IOException {
        set(Tiers.PipeSettings.builder().sendStandby(on).build());
    }

    /** The local outbound standby follows the peer's request again. */
    public void clearSendStandby() throws IOException {
        set(Tiers.PipeSettings.builder().clearSendStandby(true).build());
    }

    public void setReceiveStandby(boolean on) throws IOException {
        set(Tiers.PipeSettings.builder().recvStandby(on).build());
    }

    public void setWeight(long w) throws IOException {
        if (w < 0) throw new IllegalArgumentException("negative weight");
        set(Tiers.PipeSettings.builder().weight(w).build());
    }

    /** The base of the pipe's probation; zero disables it. */
    public void setProbation(Duration d) throws IOException {
        if (d.isNegative()) throw new IllegalArgumentException("negative probation");
        set(Tiers.PipeSettings.builder().probation(ZmuxConfig.nanos(d)).build());
    }

    /** Re-checks the pipe after a suspected path change (MULTIPATH.md 9.6). */
    public void revalidate() throws IOException {
        Session core = session.core;
        gone(() -> core.revalidatePipes(id, false));
    }

    /** Retires the pipe gracefully: its data moves to the other pipes first. */
    public void close() throws IOException {
        Session core = session.core;
        gone(() -> core.removePipe(id, true));
    }

    /** Fails the pipe and closes its carrier at once; protocol bookkeeping may await retirement. */
    public void abort() throws IOException {
        Session core = session.core;
        gone(() -> core.removePipe(id, false));
    }

    /**
     * Like {@link #abort} without waiting and without being interruptible: queued ahead of
     * every later session command. For reclaiming a pipe its creator rejected.
     */
    void abortQueued() {
        Session core = session.core;
        session.driver.post(() -> {
            try { core.removePipe(id, false); } catch (CoreException e) { /* Already retired. */ }
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
        if (r == MISSING) throw new PipeGoneException(id);
        Errors.check(r);
    }

    @Override
    public String toString() {
        return "ZmuxPipe[" + id + "]";
    }
}
