package io.zmux.internal.core;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A read buffer of a pipe. Pieces of received stream data may reference it; the
 * count is atomic because a reader on another thread can drop the last reference.
 * When the count reaches zero the buffer goes back to its pool.
 *
 * <p>With the system property {@code zmux.bufferTracking} set to true (tests only), every
 * buffer records the stack of its creation and of each reference change: a double release
 * carries them as suppressed exceptions, and a leak check can print them ({@link #trace}).
 */
public final class ReadBuf {
    /** Whether buffers record their reference history; read once, tests only. */
    public static final boolean TRACKING = Boolean.getBoolean("zmux.bufferTracking");

    private final ByteBuffer data;
    private final AtomicInteger refs = new AtomicInteger(1);
    private final Consumer<ReadBuf> free;
    /** The reference history when tracking, else null. */
    private final ConcurrentLinkedQueue<Throwable> history;

    /** A buffer holding one reference (the creator's). */
    public ReadBuf(ByteBuffer data, Consumer<ReadBuf> free) {
        this.data = data;
        this.free = free;
        history = TRACKING ? new ConcurrentLinkedQueue<>() : null;
        if (history != null) history.add(new Throwable("created, refs 1"));
    }

    public ByteBuffer data() { return data; }

    public void ref() {
        int n = refs.incrementAndGet();
        if (history != null) history.add(new Throwable("ref, refs " + n));
    }

    public void unref() {
        int n = refs.decrementAndGet();
        if (history != null) history.add(new Throwable("unref, refs " + n));
        if (n == 0) {
            if (free != null) free.accept(this);
        } else if (n < 0) {
            IllegalStateException e = new IllegalStateException("read buffer released twice");
            if (history != null) history.forEach(e::addSuppressed);
            throw e;
        }
    }

    public int refs() { return refs.get(); }

    /** The recorded reference history, one stack per change; empty unless tracking. */
    public String trace() {
        if (history == null) return "";
        var out = new StringBuilder();
        for (Throwable t : history) {
            out.append(t.getMessage()).append('\n');
            for (StackTraceElement e : t.getStackTrace()) out.append("\tat ").append(e).append('\n');
        }
        return out.toString();
    }
}
