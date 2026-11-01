package io.zmux.internal.loop;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A fixed set of event loops; sessions are bound to them round-robin. The shared default
 * group starts its daemon loops on demand; other groups are closed by their owner.
 */
public final class EventLoopGroup implements AutoCloseable {
    static final String THREAD_PREFIX = "zmux-loop-";
    private static final AtomicInteger GROUPS = new AtomicInteger();

    private final EventLoop[] loops;
    private final AtomicInteger next = new AtomicInteger();
    private final int id = GROUPS.incrementAndGet();
    private final boolean daemon;
    private final Object lock = new Object();
    private int started;
    private volatile boolean closed;

    private final long idleBufferBytes;

    public EventLoopGroup(int size, long idleBufferBytes) {
        this(size, false, idleBufferBytes);
    }

    private EventLoopGroup(int size, boolean daemon, long idleBufferBytes) {
        if (size < 1) throw new IllegalArgumentException("size " + size);
        if (idleBufferBytes < 0) throw new IllegalArgumentException("idle buffer bytes " + idleBufferBytes);
        this.loops = new EventLoop[size];
        this.daemon = daemon;
        this.idleBufferBytes = idleBufferBytes;
    }

    /** Holds the shared group, created on first use (class initialization is lazy and thread-safe). */
    private static final class Shared {
        static final EventLoopGroup GROUP = new EventLoopGroup(Math.min(Runtime.getRuntime().availableProcessors(), 8), true, BufferPool.DEFAULT_IDLE_BYTES);
    }

    /** The shared group: min(processors, 8) daemon loops, started as sessions need them. */
    public static EventLoopGroup shared() {
        return Shared.GROUP;
    }

    /** A loop of the shared group, borrowed: the group is never closed. */
    public static EventLoop nextShared() {
        return Shared.GROUP.next();
    }

    /** The next loop for a new session. */
    public EventLoop next() {
        int i = Math.floorMod(next.getAndIncrement(), loops.length);
        synchronized (lock) {
            if (closed) throw new IllegalStateException("event loop group closed");
            if (loops[i] == null) {
                loops[i] = new EventLoop(THREAD_PREFIX + id + "-" + (started++), daemon, idleBufferBytes);
                SessionHost.hostThread(loops[i].thread(), true);
            }
            return loops[i];
        }
    }

    /** Closes every loop: their sessions fail, their threads end. */
    @Override
    public void close() {
        List<EventLoop> ls = new ArrayList<>();
        synchronized (lock) {
            if (closed) return;
            closed = true;
            for (EventLoop l : loops) if (l != null) ls.add(l);
        }
        for (EventLoop l : ls) l.close();
        for (EventLoop l : ls) SessionHost.hostThread(l.thread(), false);
    }

    /** Read buffers taken and not yet returned, over all loops (tests). */
    public int outstandingBuffers() {
        int n = 0;
        synchronized (lock) {
            for (EventLoop l : loops) if (l != null) n += l.pool.outstanding();
        }
        return n;
    }

    /** A host counter summed over the group's loops (diagnostics). */
    public long sum(java.util.function.ToLongFunction<SessionHost> counter) {
        long n = 0;
        synchronized (lock) {
            for (EventLoop l : loops) if (l != null) n += counter.applyAsLong(l);
        }
        return n;
    }

    /** Live threads of this group (tests). */
    public int liveThreads() {
        int n = 0;
        synchronized (lock) {
            for (EventLoop l : loops) if (l != null && l.thread().isAlive()) n++;
        }
        return n;
    }
}
