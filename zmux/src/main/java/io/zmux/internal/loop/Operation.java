package io.zmux.internal.loop;

import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.Time;
import io.zmux.internal.core.Waiter;
import java.io.InterruptedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.SocketTimeoutException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.locks.LockSupport;

/**
 * One application operation, shared by the blocking and the asynchronous API: a blocking
 * call is the same operation plus a bounded {@link #await}. The outcome is decided by one
 * CAS on {@link #state}: completion, cancellation, timeout and the session's end all go
 * through it and the first wins; the losers have no effect.
 *
 * <p>States: QUEUED (submitted) → RUNNING (an attempt on the session's loop) → DONE, or
 * → WAITING (registered with the core) ⇄ RUNNING, or QUEUED/WAITING → CANCELLED. A cancel
 * that meets RUNNING marks it when the operation has a discard hook; the attempt then ends
 * as CANCELLED and hands any value it produced to the hook. Without a hook the attempt's
 * effect cannot be undone, so the cancel lets that one nonblocking attempt end. Attempts
 * run only on the loop and never touch application buffers.
 */
public class Operation<T> extends Waiter implements Mailbox.Command {
    /** Returned by an attempt that registered with the core and must wait. */
    public static final Object PENDING = new Object();

    /** One nonblocking attempt on the loop: the value, or {@link #PENDING} after registering self. */
    @FunctionalInterface
    public interface Step {
        Object attempt(Operation<?> self) throws CoreException;
    }

    /** Withdraws the operation's registration from the core, on the loop. */
    @FunctionalInterface
    public interface Withdraw {
        void withdraw(Operation<?> self);
    }

    private static final int QUEUED = 0, WAITING = 1, RUNNING = 2, RUNNING_CANCEL = 3, DONE = 4, CANCELLED = 5;
    private static final VarHandle STATE, LISTENER;
    static {
        try {
            STATE = MethodHandles.lookup().findVarHandle(Operation.class, "state", int.class);
            LISTENER = MethodHandles.lookup().findVarHandle(Operation.class, "listener", Object.class);
        }
        catch (ReflectiveOperationException e) { throw new ExceptionInInitializerError(e); }
    }
    private static final CancellationException CANCELLED_OUTCOME = new CancellationException("operation cancelled");

    private final SessionRuntime driver;
    private final Step step;
    private final Withdraw withdraw;
    private final java.util.function.Consumer<Object> discard;
    private final long deadline;
    /** Run by the loop (commands) or by the holder of a stream direction's turn (lane I/O). */
    private final boolean onLoop;
    @SuppressWarnings("unused") private volatile int state;
    /** Written before the state becomes DONE; read after observing DONE. */
    private Object outcome;
    private volatile Thread joiner;
    @SuppressWarnings("unused") private volatile Object listener;
    private static final Object RELEASED = new Object();
    // Loop-only.
    private boolean registered;
    private TimerHeap.Timer timer;
    /** A lane operation's deadline timer; set and removed on the loop. */
    private volatile TimerHeap.Timer laneTimer;
    Operation<?> prevLive, nextLive;
    boolean live;

    /**
     * An operation of driver's session. withdraw (may be null) undoes a registration; discard
     * (may be null) releases a value an attempt produced after the operation was cancelled.
     * deadline is System.nanoTime based, Long.MAX_VALUE for none; it bounds asynchronous
     * waiting on the loop (a blocking caller bounds its own {@link #await}).
     */
    public Operation(SessionRuntime driver, Step step, Withdraw withdraw, java.util.function.Consumer<Object> discard, long deadline) {
        this.driver = driver;
        this.step = step;
        this.withdraw = withdraw;
        this.discard = discard;
        this.deadline = deadline;
        this.onLoop = true;
    }

    /**
     * A lane operation: attempted by the holder of a stream direction's turn (an
     * application thread or the completion context) through {@link #begin}, {@link #pause}
     * and {@link #finish}. withdraw runs on the thread that cancels or times it out.
     */
    Operation(SessionRuntime driver, Withdraw withdraw, long deadline) {
        this.driver = driver;
        this.step = null;
        this.withdraw = withdraw;
        this.discard = null;
        this.deadline = deadline;
        this.onLoop = false;
    }

    public Operation(SessionRuntime driver, Step step, Withdraw withdraw) {
        this(driver, step, withdraw, null, Long.MAX_VALUE);
    }

    /** Submits the operation to its session's loop; returns this. */
    public Operation<T> submit() {
        if (!driver.mailbox().offer(this)) settle(CoreException.session(driver.terminal()));
        return this;
    }

    // ---- loop side ----

    /** First attempt: the mailbox delivers it on the loop. */
    @Override
    public void run() {
        if (deadline != Long.MAX_VALUE && (int) STATE.getVolatile(this) == QUEUED) {
            long left = deadline - System.nanoTime();
            if (left <= 0) { settle(new SocketTimeoutException("timed out")); return; }
            timer = new TimerHeap.Timer() {
                @Override void fire(long now) { timer = null; if (settle(new SocketTimeoutException("timed out"))) withdrawNow(); }
            };
            driver.loop.schedule(timer, Time.add(Clock.now(), left));
        }
        attempt();
    }

    /** The core woke the operation, at the end of a loop round. */
    @Override
    public void wake() {
        attempt();
    }

    /** The session ended before the mailbox delivered the operation. */
    @Override
    public void abandon(Throwable terminal) {
        settle(terminal);
    }

    private void attempt() {
        int s = (int) STATE.getVolatile(this);
        if (s >= DONE) { withdrawNow(); return; }
        if (s == RUNNING || s == RUNNING_CANCEL || !STATE.compareAndSet(this, s, RUNNING)) {
            if ((int) STATE.getVolatile(this) >= DONE) withdrawNow();
            return;
        }
        if (registered) { registered = false; if (withdraw != null) withdraw.withdraw(this); }
        Step body = Objects.requireNonNull(step, "a lane operation is never attempted on the loop");
        Object r;
        try {
            r = body.attempt(this);
        } catch (CoreException e) {
            r = e;
        } catch (RuntimeException e) {
            driver.internalError(e);
            r = e;
        }
        if (r == PENDING) {
            registered = true;
            driver.track(this);
            if (!STATE.compareAndSet(this, RUNNING, WAITING)) { // Cancelled during the attempt.
                withdrawNow();
                finishCancelled();
            }
            return;
        }
        outcome = r;
        driver.publishInfo(); // Happens-before the caller's next info().
        if (STATE.compareAndSet(this, RUNNING, DONE)) {
            released();
            return;
        }
        if (!(r instanceof Throwable) && discard != null) discard.accept(r);
        finishCancelled();
    }

    private void finishCancelled() {
        outcome = CANCELLED_OUTCOME;
        STATE.setVolatile(this, CANCELLED);
        released();
    }

    /** Withdraws a registration and the timer; on the loop. */
    void withdrawNow() {
        if (registered) {
            registered = false;
            if (withdraw != null) {
                try { withdraw.withdraw(this); } catch (RuntimeException e) { driver.internalError(e); }
            }
        }
        if (timer != null) { driver.loop.schedule(timer, Time.NEVER); timer = null; }
        driver.untrack(this);
    }

    /**
     * Completes with r unless an outcome was already decided; any thread. Not for a running
     * attempt (the loop settles those itself). True when r won.
     */
    boolean settle(Object r) {
        while (true) {
            int s = (int) STATE.getVolatile(this);
            if (s != QUEUED && s != WAITING) return false;
            outcome = r;
            if (STATE.compareAndSet(this, s, DONE)) {
                if (!onLoop) withdraw.withdraw(this);
                else if (driver.loop.inLoop()) withdrawNow();
                released();
                return true;
            }
        }
    }

    // ---- lane side ----

    /** Starts an attempt by the turn holder: false when the operation was cancelled or decided. */
    boolean begin() {
        while (true) {
            int s = (int) STATE.getVolatile(this);
            if (s == RUNNING) { Thread.onSpinWait(); continue; } // The previous attempt is about to pause.
            if (s != QUEUED && s != WAITING) return false;
            if (STATE.compareAndSet(this, s, RUNNING)) return true;
        }
    }

    /** Hands the operation a direction's turn: false when it was cancelled or decided meanwhile. */
    boolean grant() {
        return STATE.compareAndSet(this, QUEUED, WAITING);
    }

    /** The attempt must wait; the operation becomes cancellable again. */
    void pause() {
        STATE.setVolatile(this, WAITING);
    }

    /** Ends the attempt with r. */
    void finish(Object r) {
        outcome = r;
        STATE.setVolatile(this, DONE);
        released();
    }

    /** Arms the asynchronous deadline on the loop: on expiry the operation is timed out. */
    void armDeadline() {
        if (deadline == Long.MAX_VALUE) return;
        driver.post(() -> {
            if (isDone()) return;
            long left = deadline - System.nanoTime();
            if (left <= 0) { settle(new SocketTimeoutException("timed out")); return; }
            TimerHeap.Timer t = new TimerHeap.Timer() {
                @Override void fire(long now) { laneTimer = null; settle(new SocketTimeoutException("timed out")); }
            };
            driver.loop.schedule(t, Time.add(Clock.now(), left));
            laneTimer = t;
            if (isDone()) disarm(); // Decided meanwhile: released() may have missed the timer.
        });
    }

    /** Removes an armed lane deadline; on the loop. */
    private void disarm() {
        TimerHeap.Timer t = laneTimer;
        if (t != null) { laneTimer = null; driver.loop.schedule(t, Time.NEVER); }
    }

    /** Unparks a blocked caller; the caller resumes its own attempt. */
    void unpark() {
        Thread t = joiner;
        if (t != null) LockSupport.unpark(t);
    }

    /** Parks the blocked caller once, at most until deadline. */
    void park(long deadline) {
        joiner = Thread.currentThread();
        if (deadline == Long.MAX_VALUE) LockSupport.park(this);
        else LockSupport.parkNanos(this, deadline - System.nanoTime());
    }

    // ---- application side ----

    /**
     * Cancels without waiting for the loop: true when no outcome was decided yet (none will be),
     * false once one was. Withdrawing the registration happens later on the loop.
     */
    public boolean cancel() {
        while (true) {
            int s = (int) STATE.getVolatile(this);
            switch (s) {
                case QUEUED, WAITING -> {
                    outcome = CANCELLED_OUTCOME;
                    if (!STATE.compareAndSet(this, s, CANCELLED)) continue;
                    if (!onLoop) withdraw.withdraw(this);
                    else if (s == WAITING || deadline != Long.MAX_VALUE) driver.post(this::withdrawNow);
                    released();
                    return true;
                }
                case RUNNING -> {
                    // Without a discard hook the attempt's effect cannot be undone: let the
                    // nonblocking attempt end and report its outcome instead.
                    if (discard == null) Thread.onSpinWait();
                    else if (STATE.compareAndSet(this, RUNNING, RUNNING_CANCEL)) return true;
                }
                case DONE -> { return false; }
                default -> { return true; }
            }
        }
    }

    public boolean isDone() {
        return (int) STATE.getVolatile(this) >= DONE;
    }

    public boolean isCancelled() {
        return (int) STATE.getVolatile(this) == CANCELLED;
    }

    /** The decided outcome (a value or a Throwable); only after {@link #isDone}. */
    public Object outcome() {
        return outcome;
    }

    /**
     * Runs listener once the outcome is decided, on the session's completion context (never
     * the loop). False when the outcome was decided already: the caller publishes it itself.
     */
    public boolean onDone(Runnable listener) {
        return LISTENER.compareAndSet(this, null, listener);
    }

    private void released() {
        Thread t = joiner;
        if (t != null) LockSupport.unpark(t);
        if (laneTimer != null) driver.post(this::disarm);
        Object l = LISTENER.getAndSet(this, RELEASED);
        if (l instanceof Runnable r) driver.completions().execute(r);
    }

    /**
     * Waits for the outcome until deadline (System.nanoTime based; Long.MAX_VALUE: none).
     * On timeout or interruption the operation is cancelled; when that cancellation wins the
     * result is SocketTimeoutException or InterruptedIOException (the interrupt flag stays
     * set), otherwise the decided outcome. Never on a loop or completion thread.
     */
    public Object await(long deadline) {
        driver.checkBlocking();
        if (isDone()) return outcome;
        joiner = Thread.currentThread();
        while (!isDone()) {
            if (!Thread.currentThread().isInterrupted()) {
                if (deadline == Long.MAX_VALUE) { LockSupport.park(this); continue; }
                long left = deadline - System.nanoTime();
                if (left > 0) { LockSupport.parkNanos(this, left); continue; }
            }
            // Interrupted or expired: give up unless the outcome won the race.
            if (cancel()) {
                return Thread.currentThread().isInterrupted() ? new InterruptedIOException("interrupted")
                        : new SocketTimeoutException("timed out");
            }
        }
        return outcome;
    }
}
