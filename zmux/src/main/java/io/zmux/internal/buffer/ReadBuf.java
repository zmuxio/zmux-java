package io.zmux.internal.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
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

    private static final VarHandle REFS, ACCOUNT;
    static {
        try {
            var lookup = MethodHandles.lookup();
            REFS = lookup.findVarHandle(ReadBuf.class, "refs", int.class);
            ACCOUNT = lookup.findVarHandle(ReadBuf.class, "account", Account.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final ByteBuffer data;
    private final int capacity;
    // REFS updates the count atomically without allocating a separate counter.
    @SuppressWarnings({"unused", "FieldMayBeFinal"}) // accessed through REFS
    private volatile int refs = 1;
    private final Consumer<ReadBuf> free;
    private final ReadBuf origin;
    private final ReadBuf parent;
    private Consumer<ReadBuf> movedLoan;
    private volatile Account account;
    /** Set before publishing the root and kept through its final loan notification. */
    private boolean receiveCredit;
    /** The reference history when tracking, else null. */
    private final ConcurrentLinkedQueue<Throwable> history;

    /** A buffer holding one reference (the creator's). */
    public ReadBuf(ByteBuffer data, Consumer<ReadBuf> free) {
        this(data, data.capacity(), free);
    }

    /** A view of a root that retains the given capacity. */
    public ReadBuf(ByteBuffer data, int capacity, Consumer<ReadBuf> free) {
        this(data, capacity, free, null, null);
    }

    private ReadBuf(ByteBuffer data, int capacity, Consumer<ReadBuf> free, ReadBuf origin, ReadBuf parent) {
        this.data = data;
        this.capacity = capacity;
        this.free = free;
        this.origin = origin;
        this.parent = parent;
        history = TRACKING ? new ConcurrentLinkedQueue<>() : null;
        if (history != null) history.add(new Throwable("created, refs 1"));
    }

    /** An independently released lease which preserves this allocation's root identity. */
    ReadBuf alias(Consumer<ReadBuf> released, boolean move) {
        if (!move) ref();
        try {
            return new ReadBuf(data, capacity, released, root(), this);
        } catch (RuntimeException | Error failure) {
            if (!move) unref();
            throw failure;
        }
    }

    /** Installs the lease on the caller's sole root reference. Shared roots need an alias. */
    boolean moveIntoLoan(Consumer<ReadBuf> released) {
        if (origin != null || refs != 1 || movedLoan != null) return false;
        movedLoan = released;
        return true;
    }

    /** Physical root admission and release in one budget domain. */
    public interface Account {
        Object domain();
        boolean reserve(long bytes);
        void release(long bytes);
    }

    /** Called while the holder owns a reference, before retaining the backing in another queue. */
    public boolean cover(Account candidate) {
        ReadBuf root = root();
        Account current = root.account;
        if (current != null) return current.domain() == candidate.domain();
        if (!candidate.reserve(root.capacity)) {
            current = root.account;
            return current != null && current.domain() == candidate.domain();
        }
        if (ACCOUNT.compareAndSet(root, null, candidate)) return true;
        candidate.release(root.capacity);
        return root.account.domain() == candidate.domain();
    }

    /** Records a new allocation whose capacity was reserved before construction. */
    public void prepaid(Account payer, boolean claimsReceiveCredit) {
        if (origin != null || !ACCOUNT.compareAndSet(this, null, payer)) throw new IllegalStateException("backing already covered");
        receiveCredit = claimsReceiveCredit;
    }

    boolean claimsReceiveCredit() { return root().receiveCredit; }

    /** Stable identity shared by all leases of this allocation. */
    public ReadBuf root() { return origin == null ? this : origin; }

    public ByteBuffer data() { return data; }

    public void ref() {
        if (!tryRef()) throw new IllegalStateException("read buffer already released");
    }

    public boolean tryRef() {
        int previous;
        do {
            previous = refs;
            if (previous == 0) return false;
        } while (!REFS.compareAndSet(this, previous, previous + 1));
        if (history != null) history.add(new Throwable("ref, refs " + (previous + 1)));
        return true;
    }

    public void unref() {
        int previous;
        do {
            previous = refs;
            if (previous == 0) throw alreadyReleased();
        } while (!REFS.compareAndSet(this, previous, previous - 1));
        if (history != null) history.add(new Throwable("unref, refs " + (previous - 1)));
        if (previous == 1) {
            Account charge = account;
            account = null;
            Consumer<ReadBuf> loan = movedLoan;
            movedLoan = null;
            try {
                if (parent != null) parent.unref();
            } finally {
                try {
                    if (free != null) free.accept(this);
                } finally {
                    try {
                        if (charge != null) charge.release(capacity);
                    } finally {
                        if (loan != null) loan.accept(this);
                    }
                }
            }
        }
    }

    private IllegalStateException alreadyReleased() {
        var e = new IllegalStateException("read buffer released twice");
        if (history != null) history.forEach(e::addSuppressed);
        return e;
    }

    public int refs() { return refs; }

    public int capacity() { return capacity; }

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
