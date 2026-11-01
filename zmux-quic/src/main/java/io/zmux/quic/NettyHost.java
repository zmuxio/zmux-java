package io.zmux.quic;

import io.netty.channel.EventLoop;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.util.concurrent.ScheduledFuture;
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
    private ScheduledFuture<?> wake;
    private final Runnable roundTask = this::roundTask;
    private final Runnable timerTask = this::timerTask;

    NettyHost(EventLoop loop) {
        super(io.zmux.EventLoops.DEFAULT_IDLE_BUFFER_BYTES);
        this.loop = loop;
        loop.execute(() -> hostThread(Thread.currentThread(), true));
    }

    @Override public boolean inLoop() { return loop.inEventLoop(); }

    @Override
    public boolean tryExecute(Runnable r) {
        if (closing) return false;
        try {
            loop.execute(() -> safe(r));
            return true;
        } catch (RejectedExecutionException e) {
            return false;
        }
    }

    @Override
    protected void requestRoundEnd() {
        if (roundQueued) return;
        roundQueued = true;
        try {
            // From inside a round, the next one waits for the loop's next iteration.
            if (!inRoundTask && loop instanceof SingleThreadEventLoop s) s.executeAfterEventLoopIteration(roundTask);
            else loop.execute(roundTask);
        } catch (RejectedExecutionException e) {
            roundQueued = false;
        }
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
        try {
            wake = loop.schedule(timerTask, Math.max(0, deadline - io.zmux.internal.loop.Clock.now()), TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException e) {
            wake = null;
        }
    }

    private void timerTask() {
        wake = null;
        runTimers(io.zmux.internal.loop.Clock.now());
    }

    /** Fails the host's sessions and stops taking work; waits for that unless on the loop. */
    void close() {
        if (closing) return;
        closing = true;
        Runnable shutdown = () -> {
            runShutdownHooks();
            hostThread(Thread.currentThread(), false);
        };
        if (loop.inEventLoop()) {
            shutdown.run();
            return;
        }
        try {
            loop.submit(shutdown).awaitUninterruptibly();
        } catch (RejectedExecutionException e) {
            // The loop already stopped.
        }
    }
}
