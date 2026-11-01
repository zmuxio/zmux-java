package io.zmux;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;

/**
 * A maintenance run. Closing stops creation/rotation and closes a replacement still being rotated in;
 * the members' pipes remain owned by the session.
 *
 * <p>A dial attempt that outlives its deadline, a rotation preemption, closing or the session's
 * end is interrupted and abandoned, never awaited: the run may complete while the attempt's
 * {@link PipePool.Dial#open} is still executing. Whatever that attempt then creates (a joined
 * pipe or a reservation) is released as soon as it returns. A dial that ignores interruption
 * therefore delays only its own cleanup, never the run, its completion or {@link #close()}.
 */
public final class PipeMaintenance implements AutoCloseable {
    private static final java.util.logging.Logger LOG = java.util.logging.Logger.getLogger(PipeMaintenance.class.getName());
    public record Result(long joined, long rotations, long failures, List<ZmuxPipe> pipes, IOException cause) {
        public Result { pipes = List.copyOf(pipes); }
    }
    private final ZmuxSession session;
    private final PipePool policy;
    /** The pool's mode ({@link PipePool#mode}); null: none. */
    private final ModeSource mode;
    private final boolean once;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicReference<SessionError> terminal = new AtomicReference<>();
    private final CompletableFuture<Result> completion = new CompletableFuture<>();
    private final ArrayList<Member> members = new ArrayList<>();
    private volatile Thread worker;
    private volatile List<ZmuxPipe> visible = List.of();
    private Cancellation.Registration parent = () -> {};
    private long joined, rotations, failures, backoff, retryDelay;
    /** Consecutive failed dials, the policy's count and dial spacing, and when the last dial started (0: none). */
    private int dialFailures, target;
    private long dialAfter = -1, lastDial; // dialAfter -1: the pool's backoff
    /** When the run started: the reference of the first dial's wait. */
    private final long started;
    /** The policy's latest decision and its recheck delay in nanoseconds (0: none). */
    private long decidedAt, recheck;
    /** The options of the pipes dialled from now on: the pool's, with the policy's probe settings. */
    private PipeOptions dialOptions;
    /** Set by a pipe or session state change; the worker then passes at once (event-driven redial). */
    private final AtomicBoolean changed = new AtomicBoolean();
    private final io.zmux.internal.core.Waiter pipeChange = new io.zmux.internal.core.Waiter() {
        @Override public void wake() {
            changed.set(true);
            Thread t = worker; if (t != null) LockSupport.unpark(t);
        }
    };

    /** A member of the pool: one pipe, or the group of pipes one {@link PipePool.Join} made, kept and replaced whole. */
    private static final class Member {
        final List<ZmuxPipe> pipes;
        /** The mode candidate it was dialled over (-1: no mode) and when it joined (System.nanoTime). */
        final int kind;
        final long bytes, age, added;
        List<ZmuxPipe> replacement;
        /** The replacement's candidate, whether it is an upgrade, when it joined and since when it is usable (0: not). */
        int replacementKind = -1;
        boolean upgrade;
        long replacementAdded, usableSince;
        boolean readySeen, retire;
        long idleSince;
        Member(List<ZmuxPipe> pipes, int kind, long added, long bytes, long age) {
            this.pipes = pipes; this.kind = kind; this.added = added; this.bytes = bytes; this.age = age; idleSince = added;
        }
        /** The member's ID in policy views: that of its first pipe. */
        long id() { return pipes.getFirst().id(); }
    }
    private PipeMaintenance(ZmuxSession session, PipePool policy, boolean once) {
        this.session = session; this.policy = policy; this.once = once; mode = policy.modeSource(); backoff = minimumBackoff(); target = policy.count();
        started = System.nanoTime();
        dialOptions = policy.options();
    }
    static PipeMaintenance start(ZmuxSession session, PipePool policy, Cancellation cancellation) throws IOException {
        return start(session, policy, cancellation, false);
    }
    static PipeMaintenance startOnce(ZmuxSession session, PipePool policy, Cancellation cancellation) throws IOException {
        return start(session, policy.withCount(1).withRotation(PipePool.Rotation.disabled()).withPolicy(null), cancellation, true);
    }
    /**
     * A mode pool that takes first, a pipe of candidate kind chosen at chosen (System.nanoTime),
     * as its first member: the pool of a session dialled in auto mode.
     */
    static PipeMaintenance startWith(ZmuxSession session, PipePool policy, ZmuxPipe first, int kind, long chosen) throws IOException {
        return start(session, policy, null, false, new Member(List.of(first), kind, chosen, Long.MAX_VALUE, Long.MAX_VALUE));
    }
    private static PipeMaintenance start(ZmuxSession session, PipePool policy, Cancellation cancellation, boolean once) throws IOException {
        return start(session, policy, cancellation, once, null);
    }
    private static PipeMaintenance start(ZmuxSession session, PipePool policy, Cancellation cancellation, boolean once, Member first) throws IOException {
        Objects.requireNonNull(policy);
        session.driver.checkBlocking();
        if (!session.info().multipath()) throw new NotNegotiatedException("multipath");
        var run = new PipeMaintenance(session, policy, once);
        if (first != null) { first.readySeen = true; run.members.add(first); run.publish(); }
        if (cancellation != null) run.parent = cancellation.onCancel(run::close);
        try { Thread.ofVirtual().name("zmux-pipe-pool").start(run::run); }
        catch (RuntimeException | Error error) { run.parent.close(); throw error; }
        return run;
    }
    /** Last published membership, including replacements still in probation. */
    public List<ZmuxPipe> pipes() { return visible; }
    public CompletionStage<Result> completion() { return completion.minimalCompletionStage(); }
    @Override public void close() {
        stopped.set(true); Thread t = worker; if (t != null) t.interrupt();
    }
    private void check() throws IOException {
        if (stopped.get()) throw new InterruptedIOException("pipe maintenance cancelled");
        SessionError error = terminal.get(); if (error != null) throw new SessionClosedException(error);
        if (Thread.interrupted()) throw new InterruptedIOException("pipe maintenance interrupted");
    }
    private long minimumBackoff() { return PipePool.nanos(policy.backoff().minimum(), false); }
    private static long jitter(long limit, double fraction) {
        return Math.max(1, (long)(limit * (1 - ThreadLocalRandom.current().nextDouble() * fraction)));
    }
    private Member member(List<ZmuxPipe> pipes, int kind, long added) {
        var rotate = policy.rotation();
        return new Member(pipes, kind, added, jitter(rotate.maxBytes(), rotate.jitter()), jitter(rotate.maxAge().toNanos(), rotate.jitter()));
    }
    /** The member that takes over from m: its replacement. */
    private Member successor(Member m) { return member(m.replacement, m.replacementKind, m.replacementAdded); }
    /** Whether the session is ending: a pipe lost then is no fault of its kind. */
    private boolean ending() { return terminal.get() != null || session.isClosed(); }
    /** Whether an upgrade's replacement is being dialled or waits to take over. */
    private boolean upgrading() {
        for (Member m : members) if (m.replacement != null && m.upgrade) return true;
        return false;
    }
    /** The time a usable replacement still waits before it takes over: the mode's settle time; 0 without a mode. */
    private long settleLeft(Member m, long now) {
        if (mode == null) return 0;
        if (m.usableSince == 0) m.usableSince = now;
        return Math.max(0, mode.settle().toNanos() - (now - m.usableSince));
    }
    /**
     * A dial succeeded: the next one waits the minimum backoff from this one's start (API.md
     * Appendix A, measured from the previous attempt whether it succeeded or failed), its jitter
     * drawn once now. The backoff after a failure restarts from the minimum only once the
     * member is usable.
     */
    private void succeeded() {
        dialFailures = 0;
        retryDelay = jitter(minimumBackoff(), policy.backoff().jitter());
    }
    private void failed() {
        failures++; retryDelay = jitter(backoff, policy.backoff().jitter());
        long maximum = policy.backoff().maximum().toNanos();
        backoff = backoff > maximum / 2 ? maximum : Math.min(maximum, backoff * 2);
    }
    private PipeStats stats(ZmuxPipe pipe) throws IOException {
        return pipe.stats().orElse(null);
    }
    /** Every direction the pipe has is live. */
    private static boolean live(PipeStats s) {
        if (s == null) return false;
        boolean out = s.outbound() == null || switch (s.outbound()) { case ACTIVE, SUSPECT, JOINING -> true; default -> false; };
        boolean in = s.inbound() == null || switch (s.inbound()) { case OPEN, JOINING -> true; default -> false; };
        return out && in;
    }
    private boolean live(List<ZmuxPipe> pipes) throws IOException {
        for (ZmuxPipe p : pipes) if (!live(stats(p))) return false;
        return true;
    }
    private boolean usable(PipeStats s) throws IOException {
        return live(s) && (Boolean)session.run(op -> session.core.pipeUsable(s.pipe()));
    }
    /** A member is usable while all of its pipes are. */
    private boolean usable(List<ZmuxPipe> pipes) throws IOException {
        for (ZmuxPipe p : pipes) if (!usable(stats(p))) return false;
        return true;
    }
    private static boolean usableOnOwner(io.zmux.internal.core.Session core, List<ZmuxPipe> pipes) {
        for (ZmuxPipe p : pipes) if (!core.pipeUsable(p.id())) return false;
        return true;
    }
    private static void abort(List<ZmuxPipe> pipes) {
        for (ZmuxPipe p : pipes) {
            try { p.abort(); } catch (IOException ignored) { /* Gone or its session ended. */ }
        }
    }
    private void publish() {
        ArrayList<ZmuxPipe> pipes = new ArrayList<>();
        for (Member m : members) { pipes.addAll(m.pipes); if (m.replacement != null) pipes.addAll(m.replacement); }
        visible = List.copyOf(pipes);
    }
    /** Called only on the session owner while this worker waits for its command. */
    private boolean rotationReadyOnOwner() {
        var core = session.core; var state = core.state();
        boolean[] live = core.liveDirections();
        if ((state != io.zmux.internal.core.SessionState.ACTIVE && state != io.zmux.internal.core.SessionState.DRAINING)
                || !live[0] || !live[1]) return false;
        for (Member m : members) if (!usableOnOwner(core, m.pipes)) return false;
        return true;
    }
    private boolean canRotate() throws IOException { return (Boolean)session.run(op -> rotationReadyOnOwner()); }
    private boolean rotationUnsafe() throws IOException { return !canRotate(); }
    private static final class RotationPreempted extends IOException {
        RotationPreempted() { super("rotation yielded to recovery"); }
    }
    private void inspect() throws IOException {
        boolean changed = false;
        var settled = new ArrayList<Integer>();
        // Reconcile every loss before committing any rotation: a ready replacement
        // can fill another lost slot without retiring its still-healthy original.
        for (int i = 0; i < members.size();) {
            Member m = members.get(i);
            if (m.replacement != null && !live(m.replacement)) {
                abort(m.replacement); m.replacement = null; failed(); changed = true;
                if (mode != null) mode.record(m.replacementKind, false);
            }
            if (!live(m.pipes)) {
                abort(m.pipes); changed = true;
                if (mode != null && !ending()) mode.dropped(m.kind, m.added);
                if (m.replacement != null) { members.set(i, successor(m)); settled.add(m.replacementKind); failures++; i++; }
                else { members.remove(i); failed(); }
                continue;
            }
            if (!m.readySeen && usable(m.pipes)) { m.readySeen = true; backoff = minimumBackoff(); }
            i++;
        }
        while (members.size() < target) {
            Member donor = null;
            for (Member m : members) if (m.replacement != null) { donor = m; break; }
            if (donor == null) break;
            members.add(successor(donor)); settled.add(donor.replacementKind); donor.replacement = null; changed = true;
        }
        long now = System.nanoTime();
        for (Member m : members) if (m.replacement != null && !usable(m.replacement)) m.usableSince = 0;
        if (members.size() >= target && canRotate()) for (int i = 0; i < members.size(); i++) {
            Member m = members.get(i);
            if (m.replacement != null && usable(m.replacement) && settleLeft(m, now) == 0) {
                List<ZmuxPipe> old = m.pipes, replacement = m.replacement;
                boolean committed = (Boolean)session.run(op -> {
                    if (!rotationReadyOnOwner() || !usableOnOwner(session.core, replacement)) return Boolean.FALSE;
                    for (ZmuxPipe p : old) session.core.removePipe(p.id(), true);
                    return Boolean.TRUE;
                });
                if (committed) { members.set(i, successor(m)); settled.add(m.replacementKind); rotations++; changed = true; }
            }
        }
        if (mode != null) for (int k : settled) mode.established(session, k);
        if (changed) publish();
    }
    /**
     * The wait from the start of the last dial to the next: the policy's DialAfter, which
     * replaces the backoff, else the backoff after a failure; 0: none.
     */
    private long dialWait() { return dialAfter >= 0 ? dialAfter : retryDelay; }

    /** The start of the last dial attempt, successful or not; the start of the run before the first. */
    private long dialRef() { return lastDial == 0 ? started : lastDial; }

    /** Whether the wait since the last dial attempt has passed. */
    private boolean spaced() {
        long wait = dialWait();
        return wait == 0 || System.nanoTime() - dialRef() >= wait;
    }

    /** Asks the policy, if any, and applies its decision within the pool's bounds. */
    private void consult() throws IOException {
        PoolPolicy p = policy.policy();
        if (p == null) return;
        long now = System.nanoTime();
        var view = new ArrayList<PoolPolicy.Member>(members.size());
        for (Member m : members) {
            // A group: its bytes summed, idle since the latest traffic of any of its pipes.
            long sent = 0, received = 0, idle = Long.MAX_VALUE;
            for (ZmuxPipe pipe : m.pipes) {
                PipeStats s = stats(pipe);
                if (s == null) continue;
                sent += s.bytesSent(); received += s.bytesReceived();
                idle = Math.min(idle, s.sinceLastTraffic() == null ? 0 : s.sinceLastTraffic().toNanos());
            }
            if (idle == Long.MAX_VALUE) idle = now - m.idleSince;
            m.idleSince = now - idle;
            view.add(new PoolPolicy.Member(m.id(), java.time.Duration.ofNanos(now - m.added), java.time.Duration.ofNanos(idle),
                    m.pipes.size(), sent, received, usable(m.pipes), m.replacement != null));
        }
        PoolPolicy.Decision d;
        try {
            d = p.decide(new PoolPolicy.View(view, dialFailures, lastDial == 0 ? null : java.time.Duration.ofNanos(now - lastDial)));
            if (d == null) d = PoolPolicy.Decision.KEEP;
        } catch (RuntimeException error) {
            // A rejected decision (such as one Decision refuses to build) keeps the pool as it is,
            // is reported, and the run goes on.
            LOG.log(java.util.logging.Level.WARNING, "zmux pool policy decision rejected; keeping the pool", error);
            d = PoolPolicy.Decision.KEEP;
        }
        target = d.count().orElse(policy.count());
        decidedAt = now; recheck = d.recheckNanos();
        dialAfter = d.dialAfter().isEmpty() ? -1 : Math.min(PipePool.nanos(d.dialAfter().get(), true), policy.backoff().maximum().toNanos());
        dialOptions = policy.options().withProbe(d);
        for (Member m : members) if (d.retire().contains(m.id())) m.retire = true;
        boolean changed = false;
        while (members.size() > target) {
            int k = 0;
            for (int i = 1; i < members.size(); i++) {
                Member m = members.get(i), best = members.get(k);
                if (m.retire && !best.retire || m.retire == best.retire && m.idleSince < best.idleSince) k = i;
            }
            Member m = members.remove(k);
            closeQuietly(m.pipes);
            if (m.replacement != null) closeQuietly(m.replacement);
            changed = true;
        }
        if (changed) publish();
    }

    private static void closeQuietly(List<ZmuxPipe> pipes) {
        for (ZmuxPipe p : pipes) {
            try { p.close(); } catch (IOException ignored) { /* Gone or its session ended. */ }
        }
    }

    private boolean due(Member m) throws IOException {
        if (!usable(m.pipes)) return false;
        if (m.retire) return true;
        var r = policy.rotation();
        long sent = 0, received = 0, attached = Long.MAX_VALUE;
        for (ZmuxPipe pipe : m.pipes) {
            PipeStats s = stats(pipe);
            if (s == null) return false;
            sent += s.bytesSent(); received += s.bytesReceived();
            attached = Math.min(attached, s.attachedFor().toNanos());
        }
        return r.maxBytes() > 0 && Math.max(sent, received) >= m.bytes || !r.maxAge().isZero() && attached >= m.age;
    }
    /** Dials a member; kind: the mode candidate (-1 without a mode). */
    private List<ZmuxPipe> dial(boolean rotating, int kind) throws IOException {
        check();
        // The attempt's cancellation scope: settled by settleAttempt, so the close is a backstop.
        try (var cancellation = new Cancellation()) {
            return dial(rotating, kind, cancellation);
        }
    }

    private List<ZmuxPipe> dial(boolean rotating, int kind, Cancellation cancellation) throws IOException {
        var creation = new AtomicReference<PipePool.Attempt>();
        var outcome = new CompletableFuture<List<ZmuxPipe>>();
        var attemptDone = new CompletableFuture<Void>(); var verdict = new CompletableFuture<Boolean>();
        // The attempt's creation is settled once both the attempt has returned and the owner has
        // decided; whichever comes last runs it, so the owner never waits for a dial that ignores
        // interruption (IMPLEMENTATION.md 2.5).
        attemptDone.thenCombine(verdict, (ignored, ok) -> ok).thenAccept(ok -> {
            PipePool.Attempt scope = creation.get(); if (scope != null) scope.finish(ok);
        });
        Thread attempt = Thread.ofVirtual().name("zmux-pipe-dial").start(() -> {
            List<ZmuxPipe> p = null; Throwable failure = null;
            try {
                if (cancellation.isCancelled()) throw new InterruptedException("dial cancelled");
                var scope = new PipePool.Attempt(session, dialOptions, cancellation, kind); creation.set(scope);
                p = Objects.requireNonNull(policy.join().open(scope), "join returned null");
                if (!scope.adopt(p)) { failure = new IOException("a dial must return the pipes created by its Attempt"); p = null; }
            } catch (Throwable error) { failure = error; }
            // Returned before the owner sees the outcome: a rejected creation is then reclaimed
            // by the owner's verdict itself, before the run reports completion.
            attemptDone.complete(null);
            if (failure == null) outcome.complete(p); else outcome.completeExceptionally(failure);
        });
        List<ZmuxPipe> pipe;
        try {
            pipe = awaitDial(outcome, rotating);
        } catch (Throwable error) {
            Throwable late = settleAttempt(cancellation, attempt, verdict, false);
            if (late != null) error.addSuppressed(late);
            throw error;
        }
        Throwable late = settleAttempt(cancellation, attempt, verdict, true);
        if (late != null) throw new IOException("dial cancellation callback failed", late);
        joined++;
        return pipe;
    }

    /** Waits for the attempt's pipes within the attempt timeout and checks their directions. */
    private List<ZmuxPipe> awaitDial(CompletableFuture<List<ZmuxPipe>> outcome, boolean rotating) throws IOException {
        try {
            long timeout = policy.attemptTimeout() == null ? session.core.handshakeTimeout() : policy.attemptTimeout().toNanos();
            long started = System.nanoTime();
            List<ZmuxPipe> pipe;
            while (true) {
                long remaining = timeout - (System.nanoTime() - started);
                if (remaining <= 0) throw new java.net.SocketTimeoutException("pipe dial deadline");
                try { pipe = outcome.get(Math.min(remaining, policy.inspectionInterval().toNanos()), TimeUnit.NANOSECONDS); break; }
                catch (TimeoutException waiting) { check(); if (rotating && rotationUnsafe()) throw new RotationPreempted(); }
            }
            check();
            boolean out = policy.options().direction != PipeOptions.Direction.RECEIVE_ONLY;
            boolean in = policy.options().direction != PipeOptions.Direction.SEND_ONLY;
            boolean hasOut = false, hasIn = false;
            for (ZmuxPipe one : pipe) { PipeStats s = one.stats().orElseThrow(() -> new PipeGoneException(one.id())); hasOut |= s.outbound() != null; hasIn |= s.inbound() != null; }
            if (hasOut != out || hasIn != in) throw new IOException("dial returned incompatible directions");
            return pipe;
        } catch (InterruptedException error) { check(); throw new InterruptedIOException("pipe dial interrupted"); }
        catch (ExecutionException error) { throw new IOException("pipe dial failed", error.getCause()); }
    }

    /**
     * Ends the attempt's cancellation scope and hands the owner's verdict to the attempt; a
     * failing cancellation callback rejects the pipes and is returned. An abandoned attempt is
     * interrupted and reclaimed when it returns, never awaited.
     */
    private static Throwable settleAttempt(Cancellation cancellation, Thread attempt, CompletableFuture<Boolean> verdict, boolean accepted) {
        Throwable failure = null;
        try { cancellation.cancel(); }
        catch (RuntimeException | Error error) { accepted = false; failure = error; }
        finally {
            if (!accepted) attempt.interrupt();
            verdict.complete(accepted);
        }
        return failure;
    }
    /**
     * The wait before the next pass: the inspection interval, cut short by the policy's recheck
     * and by the next dial falling due while the pool is short of pipes.
     */
    private long pause() {
        long now = System.nanoTime(), wait = policy.inspectionInterval().toNanos();
        if (recheck > 0) wait = Math.clamp(recheck - (now - decidedAt), 0, wait);
        if (members.size() < target) wait = Math.clamp(dialWait() - (now - dialRef()), 0, wait);
        if (mode != null) for (Member m : members) {
            if (m.replacement != null && m.usableSince != 0) wait = Math.clamp(settleLeft(m, now), 0, wait);
        }
        return wait;
    }

    /** Waits for the next pass: until a pipe or session state change, at most nanos. */
    private void await(long nanos) throws IOException {
        long until = System.nanoTime() + nanos;
        while (!changed.get()) {
            check();
            long left = until - System.nanoTime();
            if (left <= 0) break;
            LockSupport.parkNanos(this, left);
        }
        changed.set(false);
    }
    private void run() {
        worker = Thread.currentThread(); IOException cause = null;
        Runnable removeClose = session.driver.watchClosed(error -> { terminal.compareAndSet(null, Errors.session(error)); Thread t = worker; if (t != null) t.interrupt(); });
        try {
            while (true) {
                // Registered before the pass looks: a change during the pass ends the next wait at once.
                session.driver.executeCarrier(() -> session.driver.waitPipe(pipeChange));
                check(); inspect(); consult();
                if (once && !session.joinReady()) break;
                if (session.joinReady()) {
                    if (members.size() < target && spaced()) {
                        lastDial = System.nanoTime();
                        int kind = mode == null ? -1 : mode.pick();
                        try {
                            members.add(member(dial(false, kind), kind, System.nanoTime())); publish(); succeeded();
                            if (mode != null) mode.established(session, kind);
                            if (once) break;
                        }
                        catch (IOException error) { check(); dialFailures++; if (once) { failures++; throw error; } failed(); }
                        continue;
                    }
                    if (canRotate()) for (Member m : members) {
                        if (m.replacement != null || !spaced()) continue;
                        // An upgrade: a more preferred kind not backing off, one per pool at a time.
                        int better = mode != null && !upgrading() && usable(m.pipes) ? mode.better(m.kind) : -1;
                        if (better >= 0 || due(m)) {
                            lastDial = System.nanoTime();
                            int kind = better >= 0 ? better : mode == null ? -1 : mode.pick();
                            try {
                                m.replacement = dial(true, kind); m.replacementKind = kind; m.upgrade = better >= 0;
                                m.replacementAdded = System.nanoTime(); m.usableSince = 0;
                                publish(); succeeded();
                            }
                            catch (RotationPreempted yielded) { retryDelay = 0; }
                            catch (IOException error) { check(); dialFailures++; failed(); }
                            break; // At most one replacement is dialled in this pass.
                        }
                    }
                }
                check(); await(pause());
            }
        } catch (IOException error) { cause = error; }
        catch (RuntimeException error) { cause = new IOException("pipe maintenance failed", error); }
        finally {
            // A session that failed soon after its members' kinds were chosen counts against them.
            if (mode != null && terminal.get() != null && !(terminal.get() instanceof SessionError.ApplicationClosed)) {
                for (Member m : members) mode.dropped(m.kind, m.added);
            }
            Errors.clearInternalWake(); parent.close(); removeClose.run();
            session.driver.executeCarrier(() -> session.driver.cancelPipeWait(pipeChange));
            var done = new CompletableFuture<Void>();
            if (session.driver.executeCarrier(() -> done.complete(null))) done.join(); else session.driver.closedFuture().join();
            worker = null;
            // A replacement still being rotated in is the run's own: it goes when the run is
            // closed; the members' pipes stay.
            if (stopped.get()) for (Member m : members) {
                if (m.replacement != null) { closeQuietly(m.replacement); m.replacement = null; }
            }
            ArrayList<ZmuxPipe> retained = new ArrayList<>();
            for (Member m : members) { retained.addAll(m.pipes); if (m.replacement != null) retained.addAll(m.replacement); }
            completion.complete(new Result(joined, rotations, failures, retained, cause));
        }
    }
}
