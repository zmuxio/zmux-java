package io.zmux.quic;

import io.zmux.internal.core.Time;

import io.zmux.internal.spi.Loops;
import io.netty.channel.EventLoop;
import io.zmux.internal.loop.SessionHost;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Hosts zmux sessions on a Netty event loop: the loop of the QUIC connections whose streams
 * carry them, so their records are parsed and written without leaving the loop. A round
 * ends after the loop's iteration (a tail task); the host's timers share one Netty timer
 * armed for the earliest deadline.
 */
final class NettyHost extends SessionHost {
    private final EventLoop loop;
    private volatile boolean closing;
    // Loop only.
    private boolean roundQueued;
    private boolean inRoundTask;
    private java.util.concurrent.ScheduledFuture<?> wake;
    private final Runnable roundTask = this::roundTask;
    private final Runnable timerTask = this::timerTask;

    NettyHost(EventLoop loop) {
        super(io.zmux.EventLoops.DEFAULT_IDLE_BUFFER_BYTES);
        this.loop = loop;
        Loops.run(loop, () -> hostThread(Thread.currentThread(), true), () -> {}); // a stopped loop hosts nothing
        // A loop that stops without close (its group shut down first) fails the sessions once it terminated.
        loop.terminationFuture().addListener(f -> shutDownTerminated());
    }

    /** The thread running the shutdown hooks after the loop terminated: the host's thread meanwhile. */
    private volatile Thread terminator;

    /**
     * Runs the shutdown hooks on the thread that reports the loop's termination. Nothing runs on
     * the loop any more, so that thread stands in for it: it is the host's thread for the hooks
     * ({@link #inLoop}), which then run their loop-side work inline instead of posting it to the
     * stopped loop, where it would be refused and lost.
     */
    private void shutDownTerminated() {
        closing = true;
        Thread t = Thread.currentThread();
        terminator = t;
        hostThread(t, true);
        try {
            shutDown();
        } finally {
            hostThread(t, false);
            terminator = null;
        }
    }

    private final java.util.concurrent.atomic.AtomicBoolean shut = new java.util.concurrent.atomic.AtomicBoolean();

    /** Runs the shutdown hooks once: on the loop at close, or after the loop terminated, when nothing else runs them. */
    private void shutDown() {
        if (shut.compareAndSet(false, true)) runShutdownHooks();
    }

    @Override public boolean inLoop() { return loop.inEventLoop() || Thread.currentThread() == terminator; }

    @Override
    public boolean tryExecute(Runnable r) {
        return !closing && Loops.run(loop, () -> safe(r), () -> {});
    }

    @Override
    protected void requestRoundEnd() {
        if (roundQueued) return;
        roundQueued = true;
        Runnable refused = () -> roundQueued = false;
        // From inside a round, the next one waits for the loop's next iteration.
        if (inRoundTask) Loops.run(loop, roundTask, refused);
        else Loops.run(afterIteration(loop), roundTask, refused);
    }

    /**
     * Ends the pending round now, after a stream's reads were parsed: the sessions' wake-ups
     * and writes go out before the loop goes on with the rest of the packet's processing
     * (acknowledgements, other connections). Not from inside a round.
     */
    void endRoundNow() {
        if (inRoundTask || !roundPending()) return;
        inRoundTask = true;
        try {
            endRound();
        } finally {
            inRoundTask = false;
        }
    }

    private void roundTask() {
        roundQueued = false;
        inRoundTask = true;
        try {
            endRound();
        } finally {
            inRoundTask = false;
        }
    }

    @Override
    protected void wakeAt(long deadline) {
        if (wake != null) wake.cancel(false);
        wake = null;
        if (deadline == Time.NEVER) return;
        // Refused: the loop stopped, and its termination fails the host's sessions.
        wake = Loops.schedule(loop, timerTask, Math.max(0, deadline - timerNow()), TimeUnit.NANOSECONDS, () -> {});
    }

    private void timerTask() {
        wake = null;
        nativeTimerFired(timerNow());
    }

    /**
     * Fails the host's sessions and stops taking work; waits for that unless on the loop. The
     * hooks run on the loop, or, when the loop terminated first, on the thread that reports its
     * termination standing in for it. The wait is safe because the hooks only fail sessions there and never wait for
     * another thread (completions reach the application through its serial callbacks, off the
     * loop); a hook that blocked on the caller would deadlock here.
     */
    void close() {
        if (closing) return;
        closing = true;
        Runnable shutdown = () -> {
            shutDown();
            hostThread(Thread.currentThread(), false);
        };
        if (loop.inEventLoop()) {
            shutdown.run();
            return;
        }
        try {
            loop.submit(shutdown).awaitUninterruptibly();
        } catch (RejectedExecutionException e) {
            // The loop already stopped: its termination runs the hooks.
        }
    }

    /** Runs tasks on loop after its current iteration (once its pending I/O ran), or as plain tasks. */
    static java.util.concurrent.Executor afterIteration(EventLoop loop) {
        return loop instanceof io.netty.channel.SingleThreadEventLoop single ? single::executeAfterEventLoopIteration : loop;
    }
}
