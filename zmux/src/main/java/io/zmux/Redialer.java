package io.zmux;

import io.zmux.carrier.PipeIo;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * The run of a session's {@link ZmuxConfig.Redial redial hook}: it dials as soon as the session
 * suspends or lacks a direction, delayed only by its own wait since its previous attempt, an
 * exponential backoff from the minimum doubling up to the maximum, with jitter, measured from
 * the start of the previous attempt. A join the peer's refusal backoff holds waits as well, so a
 * dial waits for the longer of the two. It ends with the session.
 */
final class Redialer {
    private static final double JITTER = 0.2;

    private final ZmuxSession session;
    private final ZmuxConfig.Redial hook;
    private final long min, max;
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

    private Redialer(ZmuxSession session, ZmuxConfig.Redial hook, long min, long max) {
        this.session = session; this.hook = hook; this.min = min; this.max = max;
    }

    static void start(ZmuxSession session, ZmuxConfig.Redial hook, long min, long max) {
        var r = new Redialer(session, hook, min, max);
        Thread.ofVirtual().name("zmux-redial").start(r::run);
    }

    private static long jittered(long d) {
        return Math.max(1, (long) (d * (1 - ThreadLocalRandom.current().nextDouble() * JITTER)));
    }

    /** The direction the session lacks, or null while it has both. */
    private PipeOptions.Direction need() {
        boolean out = false, in = false;
        for (PipeStats st : session.pipeStats()) {
            if (st.outbound() != null) switch (st.outbound()) { case ACTIVE, SUSPECT, JOINING -> out = true; default -> { } }
            if (st.inbound() != null) switch (st.inbound()) { case OPEN, JOINING -> in = true; default -> { } }
        }
        if (out && in) return null;
        return out ? PipeOptions.Direction.RECEIVE_ONLY : in ? PipeOptions.Direction.SEND_ONLY : PipeOptions.Direction.BIDIRECTIONAL;
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
                attempted = true;
                last = System.nanoTime();
                try {
                    PipeIo io = hook.dial(need, cancellation);
                    if (io == null) throw new IOException("the redial hook returned no pipe input");
                    session.addPipe(io, PipeOptions.builder().direction(need).build());
                    backoff = min;
                    delay = jittered(min);
                } catch (InterruptedException | InterruptedIOException stop) {
                    if (session.isClosed()) return;
                    // Only close interrupts this run's own thread. With the session open, the interrupt came from the hook:
                    // it is cleared on purpose and not restored, so the next park and dial do not end on it at once.
                    //noinspection ResultOfMethodCallIgnored
                    Thread.interrupted();
                    delay = jittered(backoff);
                    backoff = backoff > max / 2 ? max : Math.min(max, backoff * 2);
                } catch (IOException | RuntimeException failed) {
                    if (session.isClosed()) return;
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
