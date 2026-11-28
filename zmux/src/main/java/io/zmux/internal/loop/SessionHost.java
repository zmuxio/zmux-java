package io.zmux.internal.loop;

import io.zmux.internal.core.TimerHeap;
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
    private boolean shuttingDown;
    private final ArrayList<Runnable> shutdownHooks = new ArrayList<>();
    final BufferPool pool;
    private boolean ending;
    private ArrayList<Runnable> roundObserversForTests;
    /** The deadline the native wake-up is armed for. */
    private long armed = Time.NEVER;
    private boolean runningTimers;

    protected SessionHost(long idleBufferBytes) {
        this(BufferPool.forHost(idleBufferBytes));
    }

    SessionHost(BufferPool pool) {
        this.pool = pool;
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

    /** Replaces the host wake-up, or cancels it when the deadline is {@link Time#NEVER}. */
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

    /** Tracks provider backing capacity through the last reference. */
    public final void retainedInput(long n) { retainedInput.add(n); }

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

    /** The monotonic time base used by this host to fire its timer heap. */
    protected long timerNow() { return Clock.now(); }

    final void schedule(TimerHeap.Timer t, long deadline) {
        timers.schedule(t, deadline);
        if (!runningTimers) updateWake();
    }

    private void updateWake() {
        long earliest = timers.earliest();
        if (earliest != armed) {
            armed = earliest;
            wakeAt(earliest);
        }
    }

    /** Registers work to run on the host when it closes (sessions fail themselves). */
    public final void onShutdown(Runnable r) {
        shutdownHooks.add(r);
    }

    final void removeShutdownHook(Runnable r) {
        shutdownHooks.remove(r);
    }

    final boolean stopping() { return shuttingDown; }

    final int timerCount() {
        return timers.size();
    }

    protected final long earliestTimer() {
        return timers.earliest();
    }

    /** Runs one synthetic timer turn on an isolated test fixture's owner. */
    public final java.util.concurrent.CompletableFuture<Void> advanceTimersForTests(long elapsedNanos) {
        var done = new java.util.concurrent.CompletableFuture<Void>();
        if (!tryExecute(() -> {
            runTimers(Time.add(timerNow(), elapsedNanos));
            done.complete(null);
        })) done.completeExceptionally(new IllegalStateException("test owner is closed"));
        return done;
    }

    /** A native wake-up stopped being armed; an early callback can rearm the same deadline. */
    protected final void nativeTimerFired(long now) {
        armed = Time.NEVER;
        runTimers(now);
    }

    /** Fires the due timers and publishes the new heap top once after the batch. */
    protected final void runTimers(long now) {
        runningTimers = true;
        try {
            for (TimerHeap.Timer t; (t = timers.pollDue(now)) != null; ) {
                TimerHeap.Timer ft = t;
                safe(() -> ft.fire(now));
            }
        } finally {
            runningTimers = false;
            updateWake();
        }
    }

    /** Observes completed owner rounds on an isolated test fixture. */
    public final Runnable observeRoundsForTests(Runnable observer) {
        execute(() -> {
            if (roundObserversForTests == null) roundObserversForTests = new ArrayList<>();
            roundObserversForTests.add(observer);
        });
        return () -> removeRoundObserverForTests(observer);
    }

    final void removeRoundObserverForTests(Runnable observer) {
        tryExecute(() -> roundObserversForTests.remove(observer));
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
        if (roundObserversForTests != null)
            for (Runnable observer : roundObserversForTests) safe(observer);
        if (!dirty.isEmpty()) requestRoundEnd();
    }

    protected final boolean roundPending() {
        return !dirty.isEmpty();
    }

    private final java.util.concurrent.CompletableFuture<Void> stopped = new java.util.concurrent.CompletableFuture<>();

    /** A consumed close right must still run when ordinary submission races host shutdown. */
    final void release(Runnable cleanup) {
        if (inLoop()) cleanup.run();
        else enqueueRelease(cleanup);
    }

    /** Terminal session work runs after the current protocol round, including during shutdown. */
    final void enqueueRelease(Runnable cleanup) {
        if (!tryExecute(cleanup)) stopped.thenRun(cleanup);
    }

    // A stopped host's committed write receipts still have one owner. Carrier callbacks can arrive
    // concurrently, but must settle against the same ledgers as the last live owner turn.
    private final SerialCallbacks stoppedWrites = new SerialCallbacks();

    final void releaseWriteWhenStopped(Runnable receipt) {
        stopped.thenRun(() -> stoppedWrites.execute(receipt));
    }

    /** On the host thread as it closes: runs the shutdown hooks and the round ends they cause. */
    protected final void runShutdownHooks() {
        shuttingDown = true;
        try {
            for (Runnable h : new ArrayList<>(shutdownHooks)) safe(h);
            shutdownHooks.clear();
            while (!dirty.isEmpty()) endRound();
        } finally { stopped.complete(null); }
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
