package io.zmux;

import io.zmux.internal.loop.EventLoopGroup;

/**
 * The event loops that run sessions. Each session is bound to one loop for its life; a loop
 * thread owns the state of its sessions and serves many of them. The shared loops start on
 * demand as daemon threads; a group created here is closed by its owner, which fails its
 * sessions and ends its threads.
 */
public final class EventLoops implements AutoCloseable {
    private static final EventLoops SHARED = new EventLoops(null);
    private final EventLoopGroup group;

    private EventLoops(EventLoopGroup group) {
        this.group = group;
    }

    /** The shared loops: min(processors, 8) daemon threads, started as sessions need them. */
    public static EventLoops shared() {
        return SHARED;
    }

    /** A group of n loops of its own; each keeps {@link #DEFAULT_IDLE_BUFFER_BYTES} of idle read buffers. */
    public static EventLoops create(int n) {
        return create(n, DEFAULT_IDLE_BUFFER_BYTES);
    }

    /** Idle read-buffer memory a loop keeps for reuse by default (2 MiB). */
    public static final long DEFAULT_IDLE_BUFFER_BYTES = io.zmux.internal.loop.BufferPool.DEFAULT_IDLE_BYTES;

    /**
     * A group of n loops of its own; each keeps up to idleBufferBytes of returned read buffers
     * for reuse (0: none) and leaves the rest to the garbage collector.
     */
    public static EventLoops create(int n, long idleBufferBytes) {
        return new EventLoops(new EventLoopGroup(n, idleBufferBytes));
    }

    /** A loop for a new session, borrowed from this group (or the shared one), which owns it. */
    io.zmux.internal.loop.EventLoop next() {
        return group != null ? group.next() : EventLoopGroup.nextShared();
    }

    EventLoopGroup group() {
        return group != null ? group : EventLoopGroup.shared();
    }

    static {
        io.zmux.internal.diag.Diagnostics.install(EventLoops::group);
    }

    /** Read buffers taken from the loops' pools and not yet returned (diagnostics for tests). */
    int outstandingBuffers() {
        return io.zmux.internal.diag.Diagnostics.outstandingBuffers(this);
    }

    /** Live loop threads of this group (diagnostics for tests). */
    int liveThreads() {
        return io.zmux.internal.diag.Diagnostics.liveThreads(this);
    }

    /** Closes a group created with {@link #create}; the shared loops are never closed. */
    @Override
    public void close() {
        if (group != null) group.close();
    }
}
