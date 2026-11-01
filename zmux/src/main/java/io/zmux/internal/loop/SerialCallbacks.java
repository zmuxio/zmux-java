package io.zmux.internal.loop;

import java.util.ArrayDeque;

/** A demand-started serial context. The owner bounds queued work before submitting it. */
public final class SerialCallbacks {
    private static final ThreadLocal<Object> CURRENT = new ThreadLocal<>();
    private final Object owner;
    public SerialCallbacks() { this.owner = this; }
    public SerialCallbacks(Object owner) { this.owner = owner; }
    public static boolean onContext(Object owner) { return CURRENT.get() == owner; }
    private final ArrayDeque<Runnable> queue = new ArrayDeque<>();
    private boolean running;
    private boolean closed;

    public synchronized void execute(Runnable task) {
        if (closed) return;
        queue.add(task);
        if (!running) { running = true; Callbacks.EXECUTOR.execute(this::drain); }
    }

    /** Accepts no more work; drains already submitted tasks. */
    public synchronized void close() { closed = true; }

    private void drain() {
        while (true) {
            Runnable task;
            synchronized (this) {
                task = queue.poll();
                if (task == null) { running = false; return; }
            }
            CURRENT.set(owner);
            try { task.run(); }
            catch (RuntimeException | AssertionError ignored) { /* Application hooks cannot stop delivery. */ }
            finally { CURRENT.remove(); }
        }
    }
}
