package io.zmux.internal.loop;

import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.ReadBuf;
import io.zmux.internal.core.Session;
import io.zmux.internal.core.Stream;
import io.zmux.internal.core.StreamError;
import io.zmux.internal.core.Waiter;
import java.io.InterruptedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The sending direction of one stream between its writers and the session's loop
 * (IMPLEMENTATION.md Section 6.4 send buffer; the bridge's write lease). The core grants a
 * lease of the stream's send-buffer share; the holder of the direction's {@link Turn}
 * copies application bytes into lane-owned blocks within the lease and publishes them on
 * an SPSC ring without involving the loop. The loop moves ring entries into the core's
 * send queue without copying, grants a new lease, and wakes a writer that ran out of it.
 *
 * <p>A write returns once its bytes are in the lane. A STOP_SENDING, reset or session end
 * that races it can drop them; the error is reported by the next write or by
 * {@link #finish}, which waits for the loop (as TCP reports an RST).
 *
 * <p>Ownership: {@code lease} is shared (holder decrements, loop grants and reclaims);
 * the ring's tail, the current block and {@code finished} belong to the turn holder; the
 * ring's head and everything the core sees belong to the loop.
 */
public final class SendLane {
    private static final int BLOCK = 16384;
    private static final long FIRST_LEASE = 64 << 10;
    private static final VarHandle WAITER;
    static {
        try { WAITER = MethodHandles.lookup().findVarHandle(SendLane.class, "waiter", Resumable.class); }
        catch (ReflectiveOperationException e) { throw new ExceptionInInitializerError(e); }
    }

    /** Something that waits for the lease: resumed by whoever takes it out of {@link #waiter}. */
    interface Resumable {
        void resume();
    }

    /** A ring entry: a block range the holder may still extend, a chunk, or FIN. */
    private static final class Entry {
        final ReadBuf buf;
        final ByteBuffer view;
        final int from;
        volatile int to;
        final Operation<?> fin;
        volatile Entry next;

        Entry(ReadBuf buf, ByteBuffer view, int from, int to, Operation<?> fin) {
            this.buf = buf;
            this.view = view;
            this.from = from;
            this.to = to;
            this.fin = fin;
        }

        boolean block() { return view == null && fin == null && buf != null; }
    }

    private final SessionRuntime driver;
    private final Session core;
    private final Stream st;
    final Turn turn = new Turn();
    private final AtomicLong lease = new AtomicLong();
    /** The core outcome every later write reports; set on the loop. */
    private volatile CoreException terminal;
    @SuppressWarnings("unused") private volatile Resumable waiter;
    private final AtomicBoolean dirty = new AtomicBoolean();
    private final Mailbox.Command drain = this::drain;
    // Turn holder only.
    private Entry tail;
    private ReadBuf block;
    private byte[] blockBytes;
    private int blockPos;
    private boolean finished;
    // Loop only.
    private Entry head;
    private int headPos;
    private boolean leaseListed;
    /** The loop round in which the lane last had something to drain or a writer waiting. */
    private long activeRound;
    private boolean hostOwned;
    /** Whether the writer used a lease yet. */
    private boolean used;
    private final Waiter hostWaiter = new Waiter() {
        @Override public void wake() { drain(); }
    };
    /** Operations waiting until the core transmitted everything written; loop only. */
    private final java.util.ArrayList<Operation<?>> sentWaiters = new java.util.ArrayList<>(0);
    /** Told the lane's outcome once it ends; loop only. */
    private java.util.function.Consumer<CoreException> endWatcher;
    SendLane nextLeased;

    SendLane(SessionRuntime driver, Stream st) {
        this.driver = driver;
        this.core = driver.core;
        this.st = st;
        tail = head = new Entry(null, null, 0, 0, null);
    }

    // ---- writer side (application threads, completion context) ----

    /** The outcome a write reports now, or null. */
    private CoreException ended() {
        CoreException t = terminal;
        if (t != null) return t;
        if (finished) return CoreException.stream(StreamError.of(StreamError.Kind.CLOSED_LOCALLY, -1));
        return null;
    }

    /** Copies as much of src as the lease admits into the ring; the holder only. */
    private int fill(ByteBuffer src) {
        int want = src.remaining();
        if (want == 0) return 0;
        long l;
        int n;
        do {
            l = lease.get();
            if (l <= 0) return 0;
            n = (int) Math.min(want, l);
        } while (!lease.compareAndSet(l, l - n));
        for (int left = n; left > 0; ) {
            if (block == null || blockPos == BLOCK) newBlock();
            int c = Math.min(left, BLOCK - blockPos);
            src.get(blockBytes, blockPos, c);
            int from = blockPos;
            blockPos += c;
            left -= c;
            Entry t = tail;
            if (t.block() && t.buf == block && t.to == from) t.to = blockPos;
            else { block.ref(); link(new Entry(block, null, from, blockPos, null)); }
        }
        markDirty();
        return n;
    }

    private void newBlock() {
        if (block != null) block.unref();
        blockBytes = new byte[BLOCK];
        block = new ReadBuf(ByteBuffer.wrap(blockBytes), null);
        blockPos = 0;
    }

    /** Queues up to the lease of a chunk's bytes without copying; the chunk keeps its reference. */
    private int fillChunk(ByteBuffer data, ReadBuf buf) {
        int want = data.remaining();
        if (want == 0) return 0;
        long l;
        int n;
        do {
            l = lease.get();
            if (l <= 0) return 0;
            n = (int) Math.min(want, l);
        } while (!lease.compareAndSet(l, l - n));
        buf.ref();
        link(new Entry(buf, data.slice(data.position(), n), 0, n, null));
        data.position(data.position() + n);
        markDirty();
        return n;
    }

    private void link(Entry e) {
        tail.next = e;
        tail = e;
    }

    private void markDirty() {
        if (!dirty.get() && dirty.compareAndSet(false, true) && !driver.mailbox().offer(drain)) dirty.set(false);
    }

    /** Asks the loop for more lease and parks r until it is resumed; false when r was resumed already. */
    private boolean awaitLease(Resumable r) {
        WAITER.setVolatile(this, r);
        if (r instanceof Operation<?> op) op.pause(); // After publishing: a cancel then finds it.
        if (!dirty.get() && dirty.compareAndSet(false, true) && !driver.mailbox().offer(drain)) dirty.set(false);
        // The loop may have granted (or ended the lane) before seeing the waiter.
        return !((lease.get() > 0 || terminal != null) && WAITER.compareAndSet(this, r, null));
    }

    /** Writes without waiting: what the lease admits now; 0 while another write holds the direction. */
    public int tryWrite(ByteBuffer src) throws CoreException {
        CoreException e = ended();
        if (e != null) throw e;
        if (!turn.tryEnter()) return 0;
        try {
            e = ended();
            if (e != null) throw e;
            return fill(src);
        } finally {
            turn.leave();
        }
    }

    /** Writes all of src (data is null) or all of chunk's data, waiting until deadline for the turn and lease. */
    public void write(ByteBuffer src, ByteBuffer data, ReadBuf buf, long deadline) throws CoreException, InterruptedIOException {
        ByteBuffer from = data != null ? data : src;
        int start = from.position();
        CoreException e = ended();
        if (e != null) throw e;
        if (!turn.tryEnter()) waitTurn(deadline, 0);
        try {
            Blocked self = null;
            while (true) {
                e = ended();
                if (e != null) throw e;
                if (data != null) fillChunk(data, buf); else fill(src);
                if (!from.hasRemaining()) return;
                if (self == null) self = new Blocked();
                if (!awaitLease(self)) continue;
                while (waiter == self) {
                    if (Thread.currentThread().isInterrupted() || deadline != Long.MAX_VALUE && deadline - System.nanoTime() <= 0) {
                        if (WAITER.compareAndSet(this, self, null)) throw interrupted(from.position() - start);
                        break; // Resumed meanwhile: take the lease.
                    }
                    self.park(deadline);
                }
            }
        } finally {
            turn.leave();
        }
    }

    static InterruptedIOException interrupted(int bytes) {
        InterruptedIOException x = Thread.currentThread().isInterrupted()
                ? new InterruptedIOException("interrupted") : new SocketTimeoutException("timed out");
        x.bytesTransferred = bytes;
        return x;
    }

    /** A blocked writer: one parked thread. */
    static final class Blocked implements Resumable, Turn.Waiting {
        private final Thread thread = Thread.currentThread();
        private final java.util.concurrent.atomic.AtomicInteger state = new java.util.concurrent.atomic.AtomicInteger();

        @Override public void resume() { java.util.concurrent.locks.LockSupport.unpark(thread); }

        @Override public boolean take() {
            if (!state.compareAndSet(0, 1)) return false;
            java.util.concurrent.locks.LockSupport.unpark(thread);
            return true;
        }

        boolean cancel() { return state.compareAndSet(0, 2); }

        boolean granted() { return state.get() == 1; }

        void park(long deadline) {
            if (deadline == Long.MAX_VALUE) java.util.concurrent.locks.LockSupport.park(this);
            else java.util.concurrent.locks.LockSupport.parkNanos(this, deadline - System.nanoTime());
        }
    }

    /** Waits for the turn behind earlier operations; on timeout nothing was written. */
    static void waitTurn(Turn turn, long deadline, int bytes) throws InterruptedIOException {
        Blocked w = new Blocked();
        turn.enqueue(w);
        while (!w.granted()) {
            if (Thread.currentThread().isInterrupted() || deadline != Long.MAX_VALUE && deadline - System.nanoTime() <= 0) {
                if (w.cancel()) { turn.remove(w); throw interrupted(bytes); }
                break;
            }
            w.park(deadline);
        }
    }

    private void waitTurn(long deadline, int bytes) throws InterruptedIOException {
        waitTurn(turn, deadline, bytes);
    }

    /**
     * Queues FIN behind every earlier write; the returned operation completes when the core
     * took it, with the part's error if the stream can no longer finish.
     */
    public Operation<Void> finish(long deadline) throws CoreException, InterruptedIOException {
        CoreException e = terminal;
        if (e != null) throw e;
        if (!turn.tryEnter()) waitTurn(deadline, 0);
        Operation<Void> op = new Operation<>(driver, self -> {}, Long.MAX_VALUE);
        try {
            queueFin(op);
        } finally {
            turn.leave();
        }
        return op;
    }

    /** Like {@link #finish} without waiting for the turn. */
    public Operation<Void> finishAsync(long deadline) {
        final class Fin extends Operation<Void> implements Turn.Waiting {
            Fin() { super(SendLane.this.driver, self -> turn.remove((Turn.Waiting) self), deadline); }
            @Override public boolean take() {
                if (!grant()) return false;
                driver.completions().execute(() -> { try { queueFin(this); } finally { turn.leave(); } });
                return true;
            }
        }
        Fin op = new Fin();
        if (turn.tryEnter()) {
            try {
                if (op.grant()) queueFin(op);
            } finally {
                turn.leave();
            }
        } else {
            op.armDeadline();
            turn.enqueue(op);
        }
        return op;
    }

    /** Links FIN behind the written data; the turn holder only. The loop settles op. */
    private void queueFin(Operation<Void> op) {
        CoreException e = terminal;
        if (e != null) { op.settle(e); return; }
        if (finished) { op.settle(Boolean.TRUE); return; }
        finished = true;
        link(new Entry(null, null, 0, 0, op));
        markDirty();
    }

    /** Waits until operations that hold or wait for the direction have ended. */
    public void quiesce() throws InterruptedIOException {
        if (!turn.tryEnter()) waitTurn(Long.MAX_VALUE, 0);
        turn.leave();
    }

    // ---- asynchronous writes ----

    /** A write run by its turn holder: first the caller, later the completion context. */
    private final class AsyncWrite extends Operation<Void> implements Resumable, Turn.Waiting {
        private final ByteBuffer from, data;
        private final ByteBuffer src;
        private final ReadBuf buf;
        private final int start;

        AsyncWrite(ByteBuffer src, ByteBuffer data, ReadBuf buf, long deadline) {
            super(SendLane.this.driver, self -> ((AsyncWrite) self).withdraw(), deadline);
            this.src = src;
            this.data = data;
            this.buf = buf;
            this.from = data != null ? data : src;
            this.start = from.position();
        }

        private void withdraw() {
            if (WAITER.compareAndSet(SendLane.this, this, null)) turn.leave(); // Held the turn waiting for lease.
            else turn.remove(this);
            dropChunk();
        }

        /** The operation owned one reference of a chunk's buffer until it ended. */
        void dropChunk() {
            if (data != null) buf.unref();
        }

        @Override public boolean take() {
            if (!grant()) return false;
            resume();
            return true;
        }

        @Override public void resume() {
            driver.completions().execute(this::step);
        }

        /** One attempt by the turn holder. */
        void step() {
            if (!begin()) { turn.leave(); return; }
            while (true) {
                CoreException e = ended();
                if (e != null) { dropChunk(); finish(e); turn.leave(); return; }
                if (data != null) fillChunk(data, buf); else fill(src);
                if (!from.hasRemaining()) { dropChunk(); finish(Boolean.TRUE); turn.leave(); return; }
                if (awaitLease(this)) return;
                if (!begin()) { turn.leave(); return; }
            }
        }

        int written() { return from.position() - start; }
    }

    /**
     * Starts an asynchronous write of src, or of a chunk's data whose buffer reference the
     * operation takes over. The outcome is TRUE or a core outcome.
     */
    public Operation<Void> writeAsync(ByteBuffer src, ByteBuffer data, ReadBuf buf, long deadline) {
        AsyncWrite op = new AsyncWrite(src, data, buf, deadline);
        CoreException e = ended();
        if (e != null) { op.settle(e); return op; }
        op.armDeadline();
        if (turn.tryEnter()) {
            if (op.grant()) op.step();
            else turn.leave();
        } else {
            turn.enqueue(op);
        }
        return op;
    }

    /** Bytes an asynchronous write moved before it ended. */
    public static int written(Operation<?> op) {
        return op instanceof SendLane.AsyncWrite w ? w.written() : 0;
    }

    // ---- loop side ----

    /** Moves the ring into the core, grants a new lease and resumes a writer that waits for it. */
    void drain() {
        dirty.set(false);
        Entry e = head;
        if (e.next != null || waiter != null) used = true;
        activeRound = driver.lanes.round();
        while (true) {
            if (e.block()) consume(e);
            Entry n = e.next;
            if (n == null) break;
            if (e.block()) { consume(e); e.buf.unref(); }
            e = n;
            headPos = e.from;
            if (e.fin != null) {
                long unused = lease.getAndSet(0);
                core.sending().unlease(st, unused);
                CoreException t = terminal;
                if (t == null) {
                    try { core.sending().finish(st); } catch (CoreException x) { t = x; terminate(x); }
                }
                e.fin.settle(t == null ? Boolean.TRUE : t);
            } else if (!e.block()) {
                put(e.buf, e.view);
                e.buf.unref();
            }
        }
        head = e;
        if (hostOwned) return;
        boolean open = terminal == null && !finished();
        if (open) {
            CoreException err = core.sending().writeError(st);
            if (err != null) {
                terminate(err);
                open = false;
            } else {
                // A fresh lane starts with a small lease, so streams that never write keep the
                // small class of the send-buffer split; once it writes it gets its full share.
                long g = core.sending().lease(st, used ? Long.MAX_VALUE : FIRST_LEASE);
                if (g > 0) {
                    lease.addAndGet(g);
                    listLease();
                }
            }
        }
        Resumable w = waiter;
        if (w != null && (lease.get() > 0 || terminal != null) && WAITER.compareAndSet(this, w, null)) w.resume();
        else if (open && w != null) driver.lanes.leaseShort(this);
        if (!sentWaiters.isEmpty() && sentOutcome() != null) wakeSent();
        if (open) {
            if (waiter != null) {
                core.sending().waitWrite(st, hostWaiter);
            } else {
                core.sending().cancelWriteWait(st, hostWaiter);
                core.sending().waitWriteState(st, hostWaiter);
            }
        }
    }

    /** Whether FIN was taken by the core (loop view). */
    private boolean finished() {
        return head.fin != null;
    }

    private void consume(Entry e) {
        int to = e.to;
        if (headPos < to) {
            put(e.buf, ByteBuffer.wrap(e.buf.data().array(), headPos, to - headPos));
            headPos = to;
        }
    }

    private void put(ReadBuf buf, ByteBuffer view) {
        if (terminal != null) {
            core.sending().unlease(st, view.remaining());
            return;
        }
        try {
            core.sending().writeLeased(st, view, buf);
        } catch (CoreException x) {
            terminate(x);
        }
    }

    /** Ends the lane with outcome: returns the lease, fails a waiting writer; later writes report it. */
    void terminate(CoreException outcome) {
        if (terminal != null) return;
        terminal = outcome;
        core.sending().unlease(st, lease.getAndSet(0));
        core.sending().cancelWriteWait(st, hostWaiter);
        Resumable w = (Resumable) WAITER.getAndSet(this, null);
        if (w != null) w.resume();
        wakeSent();
        if (endWatcher != null) endWatcher.accept(outcome);
    }

    /**
     * Completes with TRUE once the core transmitted everything written before, or with the
     * part's error. Written bytes reach the core ahead of it: their drain is queued first.
     */
    public Operation<Void> awaitSent() {
        return new Operation<Void>(driver, self -> {
            Object r = sentOutcome();
            if (r != null) return r;
            sentWaiters.add(self);
            return Operation.PENDING;
        }, sentWaiters::remove).submit();
    }

    private Object sentOutcome() {
        CoreException t = terminal != null ? terminal : core.sending().writeError(st);
        if (t != null) return t;
        return core.sending().holdsUnsent(st) ? null : Boolean.TRUE;
    }

    private void wakeSent() {
        if (sentWaiters.isEmpty()) return;
        Operation<?>[] w = sentWaiters.toArray(new Operation<?>[0]);
        sentWaiters.clear();
        for (Operation<?> op : w) op.wake();
    }

    /** Calls watcher (on the loop) with the lane's outcome when it ends; null removes it. */
    void watchEnd(java.util.function.Consumer<CoreException> watcher) {
        endWatcher = watcher;
        if (watcher != null && terminal != null) watcher.accept(terminal);
    }

    /** Hands the direction to a loop-run carrier: the ring is moved into the core and no lease is granted again. */
    void hostTakeover() {
        drain();
        hostOwned = true;
        core.sending().unlease(st, lease.getAndSet(0));
        core.sending().cancelWriteWait(st, hostWaiter);
    }

    /** Whether a writer waits with no lease while the lane is open; on the loop. */
    boolean starved() {
        return waiter != null && lease.get() <= 0 && terminal == null && !hostOwned;
    }

    private void listLease() {
        if (leaseListed) return;
        leaseListed = true;
        driver.lanes.listLease(this);
    }

    /**
     * Called while another lane waits for lease: a lease untouched since before the previous
     * round goes back to the stream's share. Lanes nobody competes with keep their lease, so
     * a request-response writer never waits for the loop. Returns false when the lane no
     * longer holds one; reclaimed reports whether bytes went back.
     */
    boolean reclaimIdle(long round, boolean[] reclaimed) {
        long l = lease.get();
        if (l == 0 || terminal != null) { leaseListed = false; return false; }
        if (round <= activeRound + 1 || waiter != null || dirty.get()) return true;
        long u = lease.getAndSet(0);
        core.sending().unlease(st, u);
        leaseListed = false;
        if (u > 0) reclaimed[0] = true;
        if (u != l) markDirty(); // The holder took some meanwhile: its bytes are in the ring.
        return false;
    }

}
