package io.zmux.internal.loop;

import io.zmux.internal.core.Time;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * One NIO event loop: a platform thread with a {@link Selector} that owns the state of the
 * sessions bound to it. Other threads interact only through {@link #execute}. One round:
 * select, handle ready channels, run queued tasks, fire due timers, then let every session
 * touched in the round drive its output and wake-ups once (W7).
 */
public final class EventLoop extends SessionHost {
    private static final Logger LOG = Logger.getLogger(EventLoop.class.getName());

    /** A handler of a registered channel. */
    interface IoHandler {
        void ready(SelectionKey key);

        /** An unexpected exception in ready(): the handler fails its owner. */
        void failed(Throwable t);
    }

    private final Selector selector;
    private final Thread thread;
    private final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean wakeupPending = new AtomicBoolean();
    /** Tasks queued and not yet counted into a round. */
    private final java.util.concurrent.atomic.AtomicInteger queued = new java.util.concurrent.atomic.AtomicInteger();
    private volatile boolean closing;
    private volatile boolean terminated;

    EventLoop(String name, boolean daemon, long idleBufferBytes) {
        super(idleBufferBytes);
        try {
            selector = Selector.open();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        thread = Thread.ofPlatform().name(name).daemon(daemon).unstarted(this::run);
        thread.start();
    }

    @Override
    public boolean inLoop() {
        return Thread.currentThread() == thread;
    }

    Thread thread() { return thread; }

    /** Every round ends with {@link #endRound}. */
    @Override
    protected void requestRoundEnd() {}

    /** Each select waits at most until the earliest timer. */
    @Override
    protected void wakeAt(long deadline) {}

    @Override
    public boolean tryExecute(Runnable r) {
        if (closing) return false;
        tasks.add(r);
        // A close may pass the first check and finish draining before this enqueue.
        // Remove our task only if it has not been claimed; the caller then owns cleanup.
        if (closing && tasks.remove(r)) return false;
        queued.incrementAndGet();
        if (!inLoop() && wakeupPending.compareAndSet(false, true)) {
            selector.wakeup();
            LockSupport.unpark(thread);
        }
        return true;
    }

    /** Registers ch for reads (read) or with no interest yet; write interest is added on the key later. */
    SelectionKey register(SelectableChannel ch, boolean read, IoHandler h) throws ClosedChannelException {
        assert inLoop();
        return ch.register(selector, read ? SelectionKey.OP_READ : 0, h);
    }


    /** Stops the loop: its sessions are failed, its selector closed; waits for the thread to end. */
    void close() {
        closing = true;
        selector.wakeup();
        if (!inLoop()) {
            try {
                thread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    boolean terminated() { return terminated; }

    int registeredKeys() {
        return selector.isOpen() ? selector.keys().size() : 0;
    }

    private void run() {
        try {
            while (!closing) round();
            // Already accepted setup tasks may register resource hooks. Run them before
            // taking the final hook snapshot, so no newly admitted carrier escapes cleanup.
            Runnable r;
            while ((r = tasks.poll()) != null) safe(r);
            runShutdownHooks();
        } catch (Throwable t) {
            LOG.log(Level.SEVERE, "zmux event loop failed", t);
        } finally {
            terminated = true;
            try {
                for (SelectionKey k : selector.keys()) k.channel().close();
                selector.close();
            } catch (IOException ignored) {
                // Closing anyway.
            }
        }
    }

    /**
     * The longest park of a sub-millisecond wait between checks for I/O. A deadline less than
     * 1 ms away (a shaping delay or gate, a pacing release) is met by parking in slices of at
     * most this length with a non-blocking select between them, rather than by a whole
     * millisecond of select: up to ten wake-ups per such deadline, the CPU cost of meeting
     * sub-millisecond deadlines precisely.
     */
    private static final long PARK_SLICE = 100_000;

    private void round() throws IOException {
        wakeupPending.set(false);
        long deadline = earliestTimer();
        if (!tasks.isEmpty() || roundPending() || closing) {
            selector.selectNow();
        } else if (deadline == Time.NEVER) {
            selector.select();
        } else {
            long wait = deadline - Clock.now();
            if (wait <= 0) {
                selector.selectNow();
            } else if (wait < 1_000_000) {
                // Below the selector's millisecond: park in short slices until the deadline, so
                // timers fire on time and I/O still ends the wait (tryExecute unparks the loop).
                while (selector.selectNow() == 0 && tasks.isEmpty() && !closing) {
                    long left = deadline - Clock.now();
                    if (left <= 0) break;
                    LockSupport.parkNanos(this, Math.min(left, PARK_SLICE));
                }
            } else {
                // Whole milliseconds only, never past the deadline: the rest is parked next round.
                selector.select(wait / 1_000_000);
            }
        }
        wakeupPending.set(true);
        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
            SelectionKey k = it.next();
            it.remove();
            IoHandler h = (IoHandler) k.attachment();
            try {
                if (k.isValid()) h.ready(k);
            } catch (Throwable t) {
                rethrowFatal(t);
                try {
                    h.failed(t);
                } catch (Throwable t2) {
                    rethrowFatal(t2);
                    LOG.log(Level.WARNING, "zmux handler failure", t2);
                }
            }
        }
        // Only the tasks queued before this point: work they queue runs after the next I/O.
        for (int n = queued.getAndSet(0); n > 0; n--) {
            Runnable r = tasks.poll();
            if (r == null) break;
            safe(r);
        }
        runTimers(Clock.now());
        endRound();
    }
}
