package io.zmux.internal.loop;

import io.zmux.internal.buffer.CarrierCharge;
import io.zmux.internal.core.CoreException;
import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.core.Session;
import io.zmux.internal.core.SendBacking;
import io.zmux.internal.core.Stream;
import io.zmux.internal.core.StreamError;
import io.zmux.internal.core.StreamRecv;
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
 * (the send buffer; the bridge's write lease). The core grants a
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
    private static final int BLOCK = SendBacking.BLOCK_BYTES;
    private static final long FIRST_LEASE = 64 << 10;
    private static final VarHandle WAITER, BACKING_NEEDED;
    static {
        try {
            WAITER = MethodHandles.lookup().findVarHandle(SendLane.class, "waiter", Resumable.class);
            BACKING_NEEDED = MethodHandles.lookup().findVarHandle(SendLane.class, "backingNeeded", BackingNeed.class);
        } catch (ReflectiveOperationException e) { throw new ExceptionInInitializerError(e); }
    }

    /** Something that waits for the lease: resumed by whoever takes it out of {@link #waiter}. */
    interface Resumable {
        void resume();
    }

    /** A ring entry: a block range the holder may still extend, a chunk, or FIN. */
    private static final class Entry {
        private static final VarHandle CHARGE;
        static {
            try { CHARGE = MethodHandles.lookup().findVarHandle(Entry.class, "charge", CarrierCharge.Slice.class); }
            catch (ReflectiveOperationException e) { throw new ExceptionInInitializerError(e); }
        }

        private volatile ReadBuf buf;
        private ByteBuffer view;
        private final boolean paidView;
        private final boolean block;
        private volatile boolean sealed;
        final int from;
        volatile int to;
        final Operation<?> fin;
        // CHARGE atomically detaches the charge so only one caller can claim it.
        @SuppressWarnings("FieldMayBeFinal")
        private volatile CarrierCharge.Slice charge;
        volatile Entry next;

        Entry(ReadBuf buf, ByteBuffer view, int from, int to, Operation<?> fin) {
            this(buf, view, from, to, fin, null, false);
        }

        Entry(ReadBuf buf, ByteBuffer view, int from, int to, Operation<?> fin, CarrierCharge.Slice charge, boolean paidView) {
            this.buf = buf;
            this.view = view;
            this.from = from;
            this.to = to;
            this.fin = fin;
            this.charge = charge;
            this.paidView = paidView;
            this.block = view == null && fin == null && buf != null;
        }

        boolean block() { return block; }

        void release() {
            ReadBuf owner = buf;
            buf = null;
            view = null;
            if (owner != null) owner.unref();
        }

        CarrierCharge.Slice takeCharge() {
            return charge == null ? null : (CarrierCharge.Slice) CHARGE.getAndSet(this, null);
        }
    }

    private final SessionRuntime driver;
    private final Session core;
    private final Stream st;
    private final SendBacking backing;
    private volatile BackingNeed backingNeeded;
    private volatile boolean producerClosed;
    private volatile boolean consumerClosed;
    private final AtomicBoolean discarded = new AtomicBoolean();
    final Turn turn = new Turn("write (writeAsync, writeChunkAsync or finishAsync)");
    private final AtomicLong lease = new AtomicLong();
    /**
     * The core outcome every later write reports: a published copy, never a state of the lane's
     * own. Only {@link #terminate} writes it, on the loop, with an outcome the core produced (its
     * push through LaneTable, or an exception a core call threw); application threads read it
     * without a loop round trip on the write fast path, which is why the copy exists.
     */
    private volatile CoreException terminal;
    /** The application is done with this part: it finished or reset it; on the loop. */
    boolean appDone;
    @SuppressWarnings("unused") private volatile Resumable waiter;
    private final AtomicBoolean dirty = new AtomicBoolean();
    private final Mailbox.Command drain = new Mailbox.Command() {
        @Override public void run() { drain(); }

        @Override public void abandon(Throwable outcome) {
            sessionEnded(CoreException.session(driver.terminal()));
        }
    };
    // Turn holder only.
    private Entry tail;
    private ReadBuf block;
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
    /** Told the lane's outcome once it ends; loop only. */
    private java.util.function.Consumer<CoreException> endWatcher;
    SendLane nextLeased;

    SendLane(SessionRuntime driver, Stream st) {
        this.driver = driver;
        this.core = driver.core;
        this.st = st;
        backing = core.sending().backing(st);
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

    /** Copies as much of src as the lease and its physical backing admit. */
    private int fill(ByteBuffer src) {
        int start = src.position();
        while (src.hasRemaining() && lease.get() > 0) {
            if (block == null || blockPos == BLOCK) {
                sealBlock();
                block = takeBlock();
                blockPos = 0;
                if (block == null) break;
            }
            int n = takeLease(Math.min(src.remaining(), BLOCK - blockPos));
            if (n == 0) break;
            src.get(block.data().array(), blockPos, n);
            int from = blockPos;
            blockPos += n;
            if (tail.buf == block && tail.block()) tail.to = blockPos;
            else {
                block.ref();
                link(new Entry(block, null, from, blockPos, null));
            }
        }
        if (src.position() != start || backingNeeded != null) markDirty();
        return src.position() - start;
    }

    private ReadBuf takeBlock() {
        ReadBuf owner = backing.block();
        backingNeeded = owner == null ? new BackingNeed(SendBacking.PAYLOAD_BLOCK) : null;
        return owner;
    }

    private void sealBlock() {
        if (block == null) return;
        if (tail.buf == block && tail.block()) tail.sealed = true;
        block.unref();
        block = null;
    }

    private int takeLease(int want) {
        long available;
        int n;
        do {
            available = lease.get();
            if (available <= 0) return 0;
            n = (int) Math.min(want, available);
        } while (!lease.compareAndSet(available, available - n));
        return n;
    }

    private int fillChunk(ByteBuffer data, ReadBuf buf) {
        return fillChunk(data, buf, null, 0);
    }

    /** Foreign views use optional backing. When unavailable, the prepaid direction can copy. */
    private int fillChunk(ByteBuffer data, ReadBuf buf, CarrierCharge charge, long offset) {
        int want = data.remaining();
        if (want == 0 || lease.get() == 0) return 0;
        if (charge != null && !backing.prepareReceipt(charge)) {
            backingNeeded = new BackingNeed(charge.backingBytes());
            return 0;
        }
        boolean paid = backing.reserveSlice(buf);
        if (!paid && charge == null) return fill(data);
        sealBlock();
        ReadBuf owner;
        if (paid) {
            buf.ref();
            owner = buf;
        } else {
            owner = takeBlock();
            if (owner == null) {
                markDirty();
                return 0;
            }
            want = Math.min(want, BLOCK);
        }
        int n = takeLease(want);
        if (n == 0) {
            if (paid) backing.releaseSlice();
            owner.unref();
            markDirty();
            return 0;
        }
        ByteBuffer view;
        if (paid) view = data.slice(data.position(), n);
        else {
            owner.data().put(0, data, data.position(), n);
            view = owner.data().slice(0, n);
        }
        if (charge != null) {
            charge.retain();
            charge.enqueued(n);
        }
        link(new Entry(owner, view, 0, n, null,
                charge == null ? null : new CarrierCharge.Slice(charge, offset), paid));
        data.position(data.position() + n);
        markDirty();
        return n;
    }

    private void link(Entry e) {
        tail.next = e;
        tail = e;
    }

    /** A queued release belongs to the write that requested it. */
    private final class BackingNeed implements Runnable {
        private final long bytes;

        BackingNeed(long bytes) { this.bytes = bytes; }

        @Override public void run() {
            if (BACKING_NEEDED.compareAndSet(SendLane.this, this, null)) markDirty();
        }
    }

    private void cancelBackingWait() {
        backingNeeded = null;
        backing.cancelWait();
    }

    private boolean writableLease() { return lease.get() > 0 && backingNeeded == null; }

    private void closeProducer() {
        if (producerClosed) return;
        sealBlock();
        producerClosed = true;
        if (!markDirty()) consumerClosed = true;
        discardAbandoned();
    }

    private void discardAbandoned() {
        if (!consumerClosed || !producerClosed || !discarded.compareAndSet(false, true)) return;
        for (Entry e = head; e != null; e = e.next) {
            if (e.paidView && e.buf != null) backing.releaseSlice();
            releaseCharge(e.takeCharge());
            e.release();
            if (e.fin != null) e.fin.settle(terminal);
        }
    }

    private void closeIdleProducer() {
        if (!producerClosed && turn.tryEnter()) {
            try { closeProducer(); }
            finally { turn.leave(); }
        }
    }

    private void leaveTurn() {
        if (terminal != null) closeProducer();
        turn.leave();
        if (terminal != null) closeIdleProducer();
    }

    private boolean markDirty() {
        if (dirty.get() || !dirty.compareAndSet(false, true) || driver.mailbox().offer(drain)) return true;
        dirty.set(false);
        return false;
    }

    /** Asks the loop for more lease and parks r until it is resumed; false when r was resumed already. */
    private boolean awaitLease(Resumable r) {
        WAITER.setVolatile(this, r);
        if (r instanceof Operation<?> op) op.pause(); // After publishing: a cancel then finds it.
        if (!dirty.get() && dirty.compareAndSet(false, true) && !driver.mailbox().offer(drain)) dirty.set(false);
        // The loop may have granted (or ended the lane) before seeing the waiter.
        return !((writableLease() || terminal != null) && WAITER.compareAndSet(this, r, null));
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
            leaveTurn();
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
                parkForLease(self, deadline, from.position() - start);
            }
        } finally {
            leaveTurn();
        }
    }

    /**
     * Waits until a write takes bytes now, or the part ended, and returns the lease a
     * {@link #tryWrite} takes from: tryWrite's own condition. It holds the turn while it
     * waits, as a write that waits for lease does (a write in progress finishes first), and
     * consumes nothing: every other write and finish of the stream queues behind the wait. The lease is not
     * reserved after the turn is released: {@link #reclaimIdle} may hand an untouched one back to the stream's
     * share, and then the {@code tryWrite} takes 0 and the caller waits again.
     */
    public long awaitWritable(long deadline) throws CoreException, InterruptedIOException {
        CoreException e = ended();
        if (e != null) throw e;
        if (!turn.tryEnter()) waitTurn(deadline, 0);
        try {
            Blocked self = null;
            while (true) {
                e = ended();
                if (e != null) throw e;
                long l = lease.get();
                if (l > 0 && backingNeeded == null) return l;
                if (self == null) self = new Blocked();
                parkForLease(self, deadline, 0);
            }
        } finally {
            leaveTurn();
        }
    }

    /** Parks the turn holder until the loop resumes it with lease or an end; bytes is what it wrote so far. */
    private void parkForLease(Blocked self, long deadline, int bytes) throws InterruptedIOException {
        if (!awaitLease(self)) return;
        while (waiter == self) {
            if (Thread.currentThread().isInterrupted() || (deadline != Long.MAX_VALUE && deadline - System.nanoTime() <= 0)) {
                if (WAITER.compareAndSet(this, self, null)) {
                    cancelBackingWait();
                    throw interrupted(bytes);
                }
                return; // Resumed meanwhile: take the lease.
            }
            self.park(deadline);
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
    static void waitTurn(SessionRuntime driver, Turn turn, long deadline, int bytes) throws InterruptedIOException {
        Blocked w = new Blocked();
        turn.enqueue(w);
        // Checked once queued: nothing gets ahead of w afterwards.
        if (!w.granted() && driver.onCompletionContext() && turn.asyncAhead(w) && w.cancel()) {
            turn.remove(w);
            throw new io.zmux.CompletionThreadBlockedException("a blocking stream call on the session's completion thread would wait"
                    + " behind this stream's pending asynchronous " + turn.asyncOperations + ", which needs that thread to complete");
        }
        while (!w.granted()) {
            if (Thread.currentThread().isInterrupted() || (deadline != Long.MAX_VALUE && deadline - System.nanoTime() <= 0)) {
                if (w.cancel()) { turn.remove(w); throw interrupted(bytes); }
                break;
            }
            w.park(deadline);
        }
    }

    private void waitTurn(long deadline, int bytes) throws InterruptedIOException {
        waitTurn(driver, turn, deadline, bytes);
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
            leaveTurn();
        }
        return op;
    }

    /** Like {@link #finish} without waiting for the turn. */
    public Operation<Void> finishAsync(long deadline) {
        final class Fin extends Operation<Void> implements Turn.Waiting {
            Fin() { super(SendLane.this.driver, self -> turn.remove((Turn.Waiting) self), deadline); }
            @Override public boolean take() {
                if (!grant()) return false;
                driver.completions().execute(() -> { try { queueFin(this); } finally { leaveTurn(); } });
                return true;
            }
        }
        Fin op = new Fin();
        if (turn.tryEnterAsync()) {
            try {
                if (op.grant()) queueFin(op);
            } finally {
                leaveTurn();
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
        if (finished) { op.settle(true); return; }
        finished = true;
        sealBlock();
        link(new Entry(null, null, 0, 0, op));
        markDirty();
    }

    /** Waits until operations that hold or wait for the direction have ended. */
    public void quiesce() throws InterruptedIOException {
        if (!turn.tryEnter()) waitTurn(Long.MAX_VALUE, 0);
        leaveTurn();
    }

    // ---- asynchronous writes ----

    /** A write run by its turn holder: first the caller, later the completion context. */
    private final class AsyncWrite extends Operation<Void> implements Resumable, Turn.Waiting {
        private final ByteBuffer from, data;
        private final ByteBuffer src;
        private final ReadBuf buf;
        private final int start;
        private final CarrierCharge charge;
        private final long chargeOffset;
        private final java.util.function.BooleanSupplier startAllowed;
        private volatile boolean pauseRequested;
        private volatile StreamRecv.Chunk paused;

        AsyncWrite(ByteBuffer src, ByteBuffer data, ReadBuf buf, long deadline, CarrierCharge charge, long chargeOffset,
                   java.util.function.BooleanSupplier startAllowed) {
            super(SendLane.this.driver, self -> ((AsyncWrite) self).withdraw(), deadline);
            this.src = src;
            this.data = data;
            this.buf = buf;
            this.from = data != null ? data : src;
            this.start = from.position();
            this.charge = charge;
            this.chargeOffset = chargeOffset;
            this.startAllowed = startAllowed;
        }

        private void withdraw() {
            if (WAITER.compareAndSet(SendLane.this, this, null)) {
                cancelBackingWait();
                leaveTurn();
            } else turn.remove(this);
            if (isCancelled() && pauseRequested && data != null && data.hasRemaining()) {
                paused = new StreamRecv.Chunk(data, buf, false);
            } else dropChunk();
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
            if (!begin()) { leaveTurn(); return; }
            while (true) {
                CoreException e = ended();
                if (e != null) { dropChunk(); finish(e); leaveTurn(); return; }
                if (startAllowed != null && written() == 0 && !startAllowed.getAsBoolean()) {
                    dropChunk();
                    finish(false);
                    leaveTurn();
                    return;
                }
                if (data != null) fillChunk(data, buf, charge, chargeOffset + written()); else fill(src);
                if (!from.hasRemaining()) { dropChunk(); finish(true); leaveTurn(); return; }
                if (awaitLease(this)) return;
                if (!begin()) { leaveTurn(); return; }
            }
        }

        int written() { return from.position() - start; }
    }

    /**
     * Starts an asynchronous write of src, or of a chunk's data whose buffer reference the
     * operation takes over. The outcome is TRUE or a core outcome.
     */
    public Operation<Void> writeAsync(ByteBuffer src, ByteBuffer data, ReadBuf buf, long deadline) {
        return writeAsync(src, data, buf, deadline, null, 0);
    }

    public Operation<Void> writeAsync(ByteBuffer src, ByteBuffer data, ReadBuf buf, long deadline,
                                      CarrierCharge charge, long chargeOffset) {
        return writeAsync(src, data, buf, deadline, charge, chargeOffset, null);
    }

    /** Checks the optional start condition on the turn holder before committing the first range. */
    public Operation<Void> writeAsync(ByteBuffer src, ByteBuffer data, ReadBuf buf, long deadline,
                                      CarrierCharge charge, long chargeOffset, java.util.function.BooleanSupplier startAllowed) {
        AsyncWrite op = new AsyncWrite(src, data, buf, deadline, charge, chargeOffset, startAllowed);
        CoreException e = ended();
        if (e != null) { op.settle(e); return op; }
        op.armDeadline();
        if (turn.tryEnterAsync()) {
            if (op.grant()) op.step();
            else leaveTurn();
        } else {
            turn.enqueue(op);
        }
        return op;
    }

    /** Interrupts a splice byte write and transfers its unwritten range back to the caller. */
    public static StreamRecv.Chunk pauseChunkWrite(Operation<?> operation) {
        if (!(operation instanceof AsyncWrite write)) throw new IllegalArgumentException("not a send-lane write");
        write.pauseRequested = true;
        if (!write.cancel()) return null;
        StreamRecv.Chunk remainder = write.paused;
        write.paused = null;
        return remainder;
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
            if (e.block() && e.buf != null) {
                boolean sealed = e.sealed;
                consume(e);
                if (sealed) e.release();
            }
            Entry n = e.next;
            if (n == null) break;
            if (e.block() && e.buf != null) { consume(e); e.release(); }
            e = n;
            headPos = e.from;
            if (e.fin != null) {
                long unused = lease.getAndSet(0);
                core.sending().unlease(st, unused);
                CoreException t = terminal;
                if (t == null) {
                    try { core.sending().finish(st); } catch (CoreException x) { t = x; terminate(x); }
                }
                if (t == null) {
                    appDone = true;
                    driver.post(() -> driver.lanes.partEnded(st)); // after this step, not inside it
                }
                e.fin.settle(t == null ? true : t);
            } else if (!e.block()) {
                var charge = e.takeCharge();
                try { put(e.buf, e.view, e.paidView, charge); }
                finally {
                    releaseCharge(charge);
                    e.release();
                }
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
        BackingNeed needed = backingNeeded;
        if (open && w != null && needed != null) {
            backing.waitForBacking(needed.bytes, needed);
            if (waiter != w && backingNeeded != needed) backing.cancelWait();
        }
        if (w != null && (writableLease() || terminal != null) && WAITER.compareAndSet(this, w, null)) w.resume();
        else if (open && w != null && backingNeeded == null) driver.lanes.leaseShort(this);
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
            put(e.buf, ByteBuffer.wrap(e.buf.data().array(), headPos, to - headPos), false, null);
            headPos = to;
        }
    }

    private void put(ReadBuf buf, ByteBuffer view, boolean paidView, CarrierCharge.Slice charge) {
        if (terminal != null) {
            if (paidView) backing.releaseSlice();
            core.sending().unlease(st, view.remaining());
            return;
        }
        try {
            core.sending().writeLeased(st, view, buf, paidView, charge);
        } catch (CoreException x) {
            terminate(x);
        }
    }

    private static void releaseCharge(CarrierCharge.Slice charge) {
        if (charge != null) charge.charge().release();
    }

    /** Ends the lane with outcome: returns the lease, fails a waiting writer; later writes report it. */
    void terminate(CoreException outcome) { terminate(outcome, true); }

    void sessionEnded(CoreException outcome) {
        terminate(outcome);
        consumerClosed = true;
        discardAbandoned();
    }

    /** As {@link #terminate(CoreException)}; the end watcher hears of it only when notify, never for a finish the part's owner already saw complete. */
    void terminate(CoreException outcome, boolean notify) {
        if (terminal != null) return;
        terminal = outcome;
        cancelBackingWait();
        closeIdleProducer();
        core.sending().unlease(st, lease.getAndSet(0));
        core.sending().cancelWriteWait(st, hostWaiter);
        Resumable w = (Resumable) WAITER.getAndSet(this, null);
        if (w != null) w.resume();
        if (notify && endWatcher != null) endWatcher.accept(outcome);
    }

    /**
     * Completes with TRUE once the core transmitted everything written before, or with the
     * stream's error: a flush of the part (the core's one rule of what was sent, and its one
     * list of waiters), and the one wait for it, of {@code flush()} and of a splice. Written
     * bytes reach the core first: the lane drains at the attempt. With open, a stream that has
     * not opened yet opens now and the wait includes its opening frame; without, a stream with
     * nothing written and no identifier stays unopened and completes at once.
     */
    public Operation<Void> awaitSent(boolean open, long deadline) {
        return awaitSent(open, deadline, 0);
    }

    /** Waits for the written prefix, excluding a trailing region. */
    public Operation<Void> awaitSent(boolean open, long deadline, long trailing) {
        long[] target = {-1};
        return new Operation<Void>(driver, self -> {
            drain();
            // No early return on the lane's terminal: the core is the authority on the outcome. Every
            // terminal but one mirrors a core error flushDone throws as well (the part's errState, set
            // by a local reset or abort, a STOP_SENDING, a rejection or a failed write; the session's
            // error). The one that does not, the CLOSED_LOCALLY endSending gives a part that finished
            // cleanly, must not fail a flush whose bytes all left, which an early return would.
            if (target[0] < 0) {
                if (!open && core.streams().untouched(st)) return true;
                target[0] = Math.max(0, core.streams().flush(st) - trailing);
            }
            return core.sending().flushDone(st, target[0], self) ? (Object) true : Operation.PENDING;
        }, op -> core.sending().cancelFlushWait(st, op), null, deadline).submit();
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
        return waiter != null && lease.get() <= 0 && backingNeeded == null && terminal == null && !hostOwned;
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
