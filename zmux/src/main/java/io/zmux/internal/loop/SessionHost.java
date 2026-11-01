package io.zmux.internal.loop;

import io.zmux.internal.core.Time;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The thread a session lives on, with what every host keeps for its sessions: one timer
 * heap, the sessions marked for the end of the round, shutdown hooks and a read-buffer pool.
 * Everything but {@link #tryExecute} and {@link #inLoop} runs on the host thread.
 * {@link EventLoop} hosts sessions on a NIO selector; another module hosts them on the
 * thread of its own event loop by implementing the three hooks.
 */
public abstract class SessionHost implements Executor {
    private static final Logger LOG = Logger.getLogger(SessionHost.class.getName());
    private static final Set<Thread> THREADS = ConcurrentHashMap.newKeySet();

    /** Work that runs once at the end of the round in which it was marked. */
    public interface RoundEnd {
        void roundEnd();
    }

    private final TimerHeap timers = new TimerHeap();
    private final ArrayList<RoundEnd> dirty = new ArrayList<>();
    private final ArrayList<RoundEnd> dirtyRun = new ArrayList<>();
    private final IdentityHashMap<RoundEnd, Boolean> dirtySet = new IdentityHashMap<>();
    private final ArrayList<Runnable> shutdownHooks = new ArrayList<>();
    final BufferPool pool;
    private boolean ending;
    /** The deadline the native wake-up is armed for. */
    private long armed = Time.NEVER;

    protected SessionHost(long idleBufferBytes) {
        pool = new BufferPool(idleBufferBytes);
    }

    /** Whether the calling thread is this host's. */
    public abstract boolean inLoop();

    /** Queues a task for the host thread; false (dropped) once the host is closing. */
    public abstract boolean tryExecute(Runnable r);

    /** Queues a task; throws {@link RejectedExecutionException} once the host is closing. */
    @Override
    public final void execute(Runnable r) {
        if (!tryExecute(r)) throw new RejectedExecutionException("event loop closed");
    }

    /** Arranges {@link #endRound} on the host thread after the current iteration of its loop. */
    protected abstract void requestRoundEnd();

    /** Arranges {@link #runTimers} on the host thread at deadline; an earlier call supersedes a later one. */
    protected abstract void wakeAt(long deadline);

    /** Whether the calling thread hosts sessions (blocking zmux calls are refused there). */
    public static boolean onHostThread() {
        return THREADS.contains(Thread.currentThread());
    }

    /** Registers (or forgets) a thread that hosts sessions. */
    protected static void hostThread(Thread t, boolean hosting) {
        if (hosting) THREADS.add(t);
        else THREADS.remove(t);
    }

    /** Read buffers taken and not yet returned (tests: leak checks). */
    public final int outstandingBuffers() {
        return pool.outstanding();
    }

    // Diagnostics of the transports on this host: how their input reached the sessions.
    private final java.util.concurrent.atomic.LongAdder copiedInput = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder inPlaceInput = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder externals = new java.util.concurrent.atomic.LongAdder();

    /** Input bytes transports copied into a read buffer (a record cut at a lent buffer's end, or lent input). */
    public final long copiedInputBytes() { return copiedInput.sum(); }

    /** Input bytes sessions parsed where their carrier put them. */
    public final long inPlaceInputBytes() { return inPlaceInput.sum(); }

    private final java.util.concurrent.atomic.LongAdder retainedInput = new java.util.concurrent.atomic.LongAdder();

    /** Bytes of owned input buffers sessions still keep references to (a gauge). */
    public final long retainedInputBytes() { return retainedInput.sum(); }

    final void retainedInput(long n) { retainedInput.add(n); }

    /** Transports created on this host for carriers whose callbacks come from other threads. */
    public final long externalTransports() { return externals.sum(); }

    final void copiedInput(int n) { copiedInput.add(n); }
    final void inPlaceInput(int n) { inPlaceInput.add(n); }
    final void externalTransport() { externals.increment(); }

    // ---- host thread ----

    /** Marks work to run at the end of this round. */
    final void markDirty(RoundEnd r) {
        if (dirtySet.put(r, Boolean.TRUE) != null) return;
        dirty.add(r);
        if (dirty.size() == 1 && !ending) requestRoundEnd();
    }

    final void schedule(TimerHeap.Timer t, long deadline) {
        timers.schedule(t, deadline);
        long e = timers.earliest();
        if (e < armed) {
            armed = e;
            wakeAt(e);
        }
    }

    /** Registers work to run on the host when it closes (sessions fail themselves). */
    final void onShutdown(Runnable r) {
        shutdownHooks.add(r);
    }

    final void removeShutdownHook(Runnable r) {
        shutdownHooks.remove(r);
    }

    final int timerCount() {
        return timers.size();
    }

    protected final long earliestTimer() {
        return timers.earliest();
    }

    /** Fires the timers due at now, then arms the wake-up for the next one. */
    protected final void runTimers(long now) {
        armed = Time.NEVER;
        for (TimerHeap.Timer t; (t = timers.pollDue(now)) != null; ) {
            TimerHeap.Timer ft = t;
            safe(() -> ft.fire(now));
        }
        long e = timers.earliest();
        if (e < armed) { // armed is at most NEVER, so e is a real deadline
            armed = e;
            wakeAt(e);
        }
    }

    /**
     * Runs the round end of everything marked so far, once. Work marked meanwhile (a session
     * whose wake-ups produced more output) runs in the next round, after the next I/O, so
     * one busy session cannot starve the host's others.
     */
    protected final void endRound() {
        ending = true;
        try {
            dirtyRun.addAll(dirty);
            dirty.clear();
            dirtySet.clear();
            for (RoundEnd r : dirtyRun) safe(r::roundEnd);
            dirtyRun.clear();
        } finally {
            ending = false;
        }
        if (!dirty.isEmpty()) requestRoundEnd();
    }

    protected final boolean roundPending() {
        return !dirty.isEmpty();
    }

    /** On the host thread as it closes: runs the shutdown hooks and the round ends they cause. */
    protected final void runShutdownHooks() {
        for (Runnable h : new ArrayList<>(shutdownHooks)) safe(h);
        shutdownHooks.clear();
        while (!dirty.isEmpty()) endRound();
    }

    protected static void safe(Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            rethrowFatal(t);
            LOG.log(Level.WARNING, "zmux task failure", t);
        }
    }

    static void rethrowFatal(Throwable t) {
        if (t instanceof VirtualMachineError e) throw e;
    }
}
