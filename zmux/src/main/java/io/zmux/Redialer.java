package io.zmux;

import io.zmux.carrier.PipeIo;
import io.zmux.internal.loop.Bootstrap;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * The run of a session's {@link ZmuxConfig.Redial redial hook}: it dials as soon as the session
 * suspends or lacks a direction, delayed only by its own wait since its previous attempt, an
 * exponential backoff from the minimum doubling up to the maximum, with jitter, measured from
 * the start of the previous attempt. A join the peer's refusal backoff holds waits as well, so a
 * dial waits for the longer of the two. It ends with the session.
 *
 * <p>Kept apart from {@link PipeMaintenance} on purpose: this run restores
 * the directions the session lacks, under the session's configuration, for as long as the session
 * lives; a pipe pool is an application's run over its own members, counted, rotated and steered by
 * a policy, with its own result and end. One worker for both would mix those two lifecycles and
 * counts. What they must share is that one further pipe is dialled at a time, which
 * {@link ZmuxSession#dialPermit} gives; each side holds it at most for one attempt bounded by a
 * deadline, so neither can starve the other.
 */
final class Redialer {
    private final ZmuxSession session;
    private final ZmuxConfig.Redial hook;
    private final long min, max;
    private final double jitter;
    private final AtomicBoolean changed = new AtomicBoolean(true);
    private final Cancellation cancellation = new Cancellation();
    private volatile Thread worker;
    private final io.zmux.internal.core.Waiter pipeChange = new io.zmux.internal.core.Waiter() {
        @Override public void wake() {
            changed.set(true);
            Thread t = worker;
            if (t != null) LockSupport.unpark(t);
        }
    };

    private Redialer(ZmuxSession session, ZmuxConfig.Redial hook, long min, long max, double jitter) {
        this.session = session; this.hook = hook; this.min = min; this.max = max; this.jitter = jitter;
    }

    static void start(ZmuxSession session, ZmuxConfig.Redial hook, long min, long max, double jitter) {
        var r = new Redialer(session, hook, min, max, jitter);
        Thread.ofVirtual().name("zmux-redial").start(r::run);
    }

    private long jittered(long d) {
        return Math.max(1, (long) (d * (1 - ThreadLocalRandom.current().nextDouble() * jitter)));
    }

    /** The direction the session lacks, or null while it has both: the core's count of live directions, the one authority. */
    private PipeOptions.Direction need() throws IOException {
        boolean[] live = (boolean[]) session.run(op -> session.core.liveDirections());
        if (live[0] && live[1]) return null;
        return live[0] ? PipeOptions.Direction.RECEIVE_ONLY : live[1] ? PipeOptions.Direction.SEND_ONLY : PipeOptions.Direction.BIDIRECTIONAL;
    }

    /**
     * One run of the hook on a thread of its own, bounded by the session's join deadline: an attempt that
     * outlives it is cancelled, interrupted and abandoned, never awaited, so it holds the session's dial
     * permit no longer than that; a pipe input it returns later is closed.
     */
    private PipeIo attempt(PipeOptions.Direction need) throws IOException, InterruptedException {
        var outcome = new CompletableFuture<PipeIo>();
        var attempt = new Cancellation();
        Cancellation.Registration closing = cancellation.onCancel(attempt::cancel);
        Thread dial;
        try {
            dial = Thread.ofVirtual().name("zmux-redial-attempt").start(() -> {
                try {
                    PipeIo io = hook.dial(need, attempt);
                    if (io == null) throw new IOException("the redial hook returned no pipe input");
                    if (!outcome.complete(io)) Bootstrap.discard(io, new java.net.SocketTimeoutException("redial attempt abandoned"));
                } catch (InterruptedException abandoned) {
                    // The attempt was abandoned or the session closed: the thread ends here.
                    outcome.completeExceptionally(abandoned);
                } catch (Throwable failure) {
                    outcome.completeExceptionally(failure);
                }
            });
        } catch (RuntimeException | Error failure) {
            closing.close();
            throw failure;
        }
        try {
            return settle(outcome, f -> f.get(session.core.joinDeadline(), TimeUnit.NANOSECONDS), () -> {
                attempt.cancel();
                dial.interrupt();
            });
        } finally {
            closing.close();
        }
    }

    /** How {@link #settle} waits for an attempt's outcome: bounded, it throws TimeoutException at its deadline. */
    @FunctionalInterface
    interface Wait {
        PipeIo await(CompletableFuture<PipeIo> outcome) throws InterruptedException, ExecutionException, TimeoutException;
    }

    /**
     * The one hand-off of an attempt's pipe input: returned to the caller, who passes it to
     * {@link ZmuxSession#addPipe(PipeIo, PipeOptions)} (which owns it from then on, whatever the
     * outcome), or closed here, exactly once. The caller's side claims the outcome by completing it
     * exceptionally when it stops waiting (deadline, interruption, failure); if the attempt's thread
     * completed it first, the input it delivered is closed here, since nobody else will see it,
     * and an input delivered later meets the claim and is closed by that thread.
     */
    static PipeIo settle(CompletableFuture<PipeIo> outcome, Wait wait, Runnable abandon)
            throws IOException, InterruptedException {
        boolean handedOff = false;
        try {
            PipeIo io = wait.await(outcome);
            handedOff = true;
            return io;
        } catch (TimeoutException late) {
            throw new java.net.SocketTimeoutException("redial attempt outlived the join deadline");
        } catch (ExecutionException failed) {
            Throwable cause = failed.getCause();
            if (cause instanceof IOException io) throw io;
            throw new CallbackFailedException("redial hook failed", cause);
        } finally {
            if (!handedOff) {
                var abandoned = new java.net.SocketTimeoutException("redial attempt abandoned");
                // Lost the claim to a result: nobody returns it, so it is closed here.
                if (!outcome.completeExceptionally(abandoned) && !outcome.isCompletedExceptionally()) {
                    Bootstrap.discard(outcome.join(), abandoned);
                }
                abandon.run();
            }
        }
    }

    private void run() {
        worker = Thread.currentThread();
        Runnable removeClose = session.driver.watchClosed(error -> {
            cancellation.cancel();
            Thread t = worker;
            if (t != null) t.interrupt();
        });
        try {
            session.awaitEstablished(Long.MAX_VALUE);
            if (!session.info().multipath()) return;
            long backoff = min, delay = 0, last = 0;
            boolean attempted = false;
            while (!session.isClosed()) {
                changed.set(false);
                session.driver.executeCarrier(() -> session.driver.waitPipe(pipeChange));
                PipeOptions.Direction need = need();
                if (need == null) {
                    if (!changed.get()) LockSupport.park(this);
                    if (Thread.interrupted() && session.isClosed()) return;
                    continue;
                }
                long wait = attempted ? delay - (System.nanoTime() - last) : 0;
                if (wait > 0) {
                    LockSupport.parkNanos(this, wait);
                    if (Thread.interrupted() && session.isClosed()) return;
                    continue;
                }
                try {
                    session.dialPermit.acquire();
                    try {
                        // Another dial may have restored it meanwhile.
                        need = need();
                        if (need == null) continue;
                        // The backoff counts from the attempt's start, not from the wait for the permit.
                        attempted = true;
                        last = System.nanoTime();
                        session.addPipe(attempt(need), PipeOptions.builder().direction(need).build());
                    } finally {
                        session.dialPermit.release();
                    }
                    backoff = min;
                    delay = jittered(min);
                } catch (InterruptedException | InterruptedIOException stopped) {
                    // A SocketTimeoutException is an InterruptedIOException too: an attempt past its
                    // deadline, a hook's own connect timeout, a peer that did not attach in time. Only
                    // the session's close ends this run; anything else is a failed attempt. The flag is
                    // cleared before the check, so a close that interrupts after it is still seen.
                    //noinspection ResultOfMethodCallIgnored: only clearing the flag; the session's state decides
                    Thread.interrupted();
                    if (session.isClosed()) return;
                    delay = jittered(backoff);
                    backoff = backoff > max / 2 ? max : Math.min(max, backoff * 2);
                } catch (IOException | RuntimeException failed) {
                    if (session.isClosed() || Errors.clientClosed(failed) != null) return; // the hook's client closed: final
                    delay = jittered(backoff);
                    backoff = backoff > max / 2 ? max : Math.min(max, backoff * 2);
                }
            }
        } catch (IOException | RuntimeException ended) {
            // The session ended or never established: nothing to redial.
        } finally {
            removeClose.run();
            cancellation.cancel();
            session.driver.executeCarrier(() -> session.driver.cancelPipeWait(pipeChange));
            worker = null;
        }
    }
}
