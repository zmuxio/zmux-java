package io.zmux.internal.loop;

import io.zmux.internal.core.CoreException;
import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.core.Session;
import io.zmux.internal.core.Stream;
import io.zmux.internal.core.StreamError;
import io.zmux.internal.core.StreamRecv;
import io.zmux.internal.core.Waiter;
import java.io.InterruptedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The receiving direction of one stream between the session's loop and its readers. The
 * loop pushes in-order data as soon as the core has it, without copying (views of read
 * buffers, each holding a reference), followed by the part's end; the holder of the
 * direction's {@link Turn} copies into the caller's buffer (or hands a piece over as a
 * chunk) without involving the loop. The loop never touches a caller's buffer.
 *
 * <p>Pushed bytes stay in the stream window and the session's buffer count until the reader
 * reports them consumed: at least every window-update threshold, and before it waits.
 * A reset's Reliable Size is published as {@link #cut}; pushed bytes beyond it are dropped
 * by the reader. A local stop closes the lane and releases what was pushed.
 */
public final class RecvLane {
    private static final int PIECE = 1 << 20;
    private static final VarHandle WAITER;
    static {
        try { WAITER = MethodHandles.lookup().findVarHandle(RecvLane.class, "waiter", SendLane.Resumable.class); }
        catch (ReflectiveOperationException e) { throw new ExceptionInInitializerError(e); }
    }

    /** Pushed data [off, off + data.remaining()) or the part's end. */
    private static final class Piece {
        ByteBuffer data;
        ReadBuf buf;
        long off;
        final Object end;
        final boolean paid;
        volatile Piece next;

        Piece(ByteBuffer data, ReadBuf buf, long off, Object end, boolean paid) {
            this.data = data;
            this.buf = buf;
            this.off = off;
            this.end = end;
            this.paid = paid;
        }
    }

    private final SessionRuntime driver;
    private final Session core;
    private final Stream st;
    final Turn turn = new Turn("read (readAsync or readChunkAsync)");
    private volatile long cut = Long.MAX_VALUE;
    private volatile boolean finKnown;
    private final AtomicLong read = new AtomicLong(), dropped = new AtomicLong();
    private volatile long reportBytes;
    private volatile boolean endSeen;
    // The readiness state, readable from any thread without the turn: how far data was pushed (loop), how far the
    // reader consumed (turn holder), and whether the end was pushed (loop).
    private volatile long pushedTo;
    private volatile long applicationOrigin;
    private final AtomicLong consumedTo = new AtomicLong();
    private volatile boolean endPushed;
    /** The waits for something to read, any number at once; each is woken once and checks again. */
    private final java.util.concurrent.ConcurrentLinkedQueue<Ready> readyWaiters = new java.util.concurrent.ConcurrentLinkedQueue<>();

    /** Withdraws a cancelled or expired readiness wait from the waiters. */
    private void withdrawReady(Operation<?> op) {
        if (op instanceof Ready r) readyWaiters.remove(r);
    }
    private interface Ready { void ready(); }
    /** Set on the loop when the part was stopped or released: nothing more is reported or kept. */
    private volatile boolean closed;
    /** What reads report once closed; null: the part was stopped or released locally. */
    private volatile CoreException closedWith;
    /** Set by the application's stop: reads fail at once, before the loop closes the lane. */
    private volatile boolean stopped;
    @SuppressWarnings("unused") private volatile SendLane.Resumable waiter;
    private final AtomicBoolean dirty = new AtomicBoolean();
    private final Mailbox.Command report = this::reported;
    // Turn holder only.
    private Piece head;
    private long unreported;
    // Loop only.
    private Piece tail;
    private boolean ended;
    /** Whether {@link #consumedTo} started at the first pushed offset; loop only. */
    private boolean based;
    private boolean endReported;
    /** The application is done with this part: it saw the end, or stopped it; on the loop. */
    boolean appDone;
    private Runnable hostReader;
    /** Told a failed end (reset, local stop, session end) once it is pushed; loop only. */
    private java.util.function.Consumer<CoreException> endWatcher;
    private final Waiter hostWaiter = new Waiter() {
        @Override public void wake() { pump(); }
    };

    RecvLane(SessionRuntime driver, Stream st) {
        this.driver = driver;
        this.core = driver.core;
        this.st = st;
        head = tail = new Piece(null, null, 0, null, false);
    }

    // ---- reader side (the turn holder) ----

    /** The next piece or end, or null when nothing is pushed yet; drops what lies beyond the cut. */
    private Piece peek() {
        while (true) {
            Piece p = head.next;
            if (p == null || p.end != null) return p;
            long c = cut;
            int rem = p.data.remaining();
            if (rem > 0 && p.off < c) return p;
            if (rem > 0) {
                dropped.addAndGet(rem);
                unreported += rem;
                p.data.position(p.data.limit());
            }
            release(p);
        }
    }

    private void release(Piece p) {
        finishPiece(p, false);
    }

    private void finishPiece(Piece p, boolean moved) {
        head = p;
        ReadBuf buf = p.buf;
        p.buf = null;
        p.data = null;
        try {
            if (buf != null && !moved) buf.unref();
        } finally {
            if (p.paid) core.receiving().releaseLaneView();
        }
    }

    /** The end outcome once the reader reached it: StreamRecv.FIN or a CoreException. */
    private Object reachEnd(Piece p) {
        if (!endSeen) {
            endSeen = true;
            report();
        }
        return p.end;
    }

    private void consumed(int n) {
        read.addAndGet(n);
        consumedTo.setRelease(consumedTo.getPlain() + n); // the turn holder is the only writer
        unreported += n;
        if (unreported >= reportBytes) report();
    }

    private void report() {
        unreported = 0;
        if (!dirty.get() && dirty.compareAndSet(false, true) && !driver.mailbox().offer(report)) dirty.set(false);
    }

    /**
     * Copies available data into dst: the count, StreamRecv.FIN at the end, a CoreException
     * at a failed end, or null when nothing is available (the caller waits).
     */
    private Object take(ByteBuffer dst) {
        if (closed || stopped) return closedOutcome();
        Piece p = peek();
        if (p == null) return null;
        if (p.end != null) return reachEnd(p);
        int n = 0;
        while (p != null && p.end == null && dst.hasRemaining()) {
            long room = cut - p.off; // One read of cut: a reset may lower it after peek took the piece.
            if (room <= 0) { p = peek(); continue; }
            long lim = Math.min(p.data.remaining(), room);
            int c = (int) Math.min(lim, dst.remaining());
            dst.put(dst.position(), p.data, p.data.position(), c);
            dst.position(dst.position() + c);
            p.data.position(p.data.position() + c);
            p.off += c;
            n += c;
            if (p.data.hasRemaining() && p.off < cut) break;
            p = peek();
        }
        if (n == 0 && p != null && p.end != null) return reachEnd(p); // The cut dropped the rest.
        consumed(n);
        return n;
    }

    /** Takes up to max bytes without copying: a chunk (data, owned reference), FIN, an outcome, or null. */
    private Object takeChunk(int max) { return takeChunk(max, null); }

    private Object takeChunk(int max, java.util.function.Predicate<ReadBuf> fits) {
        if (closed || stopped) return closedOutcome();
        Piece p = peek();
        if (p == null) return null;
        if (p.end != null) return reachEnd(p);
        long room = cut - p.off; // One read of cut: a reset may lower it after peek took the piece.
        if (room <= 0) return takeChunk(max, fits); // The next peek drops the piece.
        int c = (int) Math.min(Math.min(p.data.remaining(), room), max);
        boolean whole = c == p.data.remaining();
        ByteBuffer d = whole ? p.data : p.data.slice(p.data.position(), c);
        var loans = core.receiving().loans();
        ReadBuf buf = fits == null ? loans.acquire(p.buf, whole) : loans.transfer(p.buf, whole, fits);
        if (buf == null) return CoreException.of(CoreException.Reason.CHUNK_BUDGET);
        if (whole) {
            finishPiece(p, true);
        } else {
            p.data.position(p.data.position() + c);
            p.off += c;
            if (p.off >= cut) release(p);
        }
        consumed(c);
        return new StreamRecv.Chunk(d, buf, false);
    }

    private CoreException closedOutcome() {
        CoreException c = closedWith;
        return c != null ? c : CoreException.stream(StreamError.of(StreamError.Kind.CLOSED_LOCALLY, -1));
    }

    private boolean available() {
        return closed || stopped || peek() != null;
    }

    /** Releases the turn; a lane closed meanwhile is emptied by whoever holds the turn last. */
    private void leave() {
        turn.leave();
        if (closed && turn.tryEnter()) {
            discard();
            turn.leave();
        }
    }

    private void discard() {
        for (Piece p; (p = head.next) != null; ) release(p);
    }

    /** Asks for more data; false when r was resumed already. */
    private boolean awaitData(SendLane.Resumable r) {
        WAITER.setVolatile(this, r);
        if (r instanceof Operation<?> op) op.pause();
        if (unreported > 0 || read.get() + dropped.get() > 0) report();
        return !(available() && WAITER.compareAndSet(this, r, null));
    }

    /** Reads at least one byte into dst, waiting until deadline: the count, -1 at FIN; throws the part's failure. */
    public int read(ByteBuffer dst, long deadline) throws CoreException, InterruptedIOException {
        if (!turn.tryEnter()) SendLane.waitTurn(driver, turn, deadline, 0);
        try {
            SendLane.Blocked self = null;
            while (true) {
                Object r = take(dst);
                if (r != null) return result(r);
                if (self == null) self = new SendLane.Blocked();
                if (!awaitData(self)) continue;
                while (waiter == self) {
                    if (Thread.currentThread().isInterrupted() || (deadline != Long.MAX_VALUE && deadline - System.nanoTime() <= 0)) {
                        if (WAITER.compareAndSet(this, self, null)) throw SendLane.interrupted(0);
                        break;
                    }
                    self.park(deadline);
                }
            }
        } finally {
            leave();
        }
    }

    /**
     * Reads what is available into dst without waiting for data: the count, 0 when nothing is readable now, -1 at FIN;
     * throws the part's failure. A read that waits for data holds the direction while nothing is readable, so
     * "nothing readable now" is the answer without waiting for it; while data is readable, whoever holds the direction
     * takes it or leaves in a bounded step, and this call follows.
     *
     * @throws io.zmux.CompletionThreadBlockedException on the completion thread, when what holds the direction is an
     *     asynchronous read, which needs that thread to take the data
     */
    public int tryRead(ByteBuffer dst) throws CoreException {
        while (!turn.tryEnter()) {
            if (readableNow() == 0) return 0;
            if (driver.onCompletionContext() && turn.asyncPending()) {
                throw new io.zmux.CompletionThreadBlockedException("tryRead on the session's completion thread with data readable"
                        + " behind this stream's pending asynchronous " + turn.asyncOperations + ", which needs that thread to take it");
            }
            java.util.concurrent.locks.LockSupport.parkNanos(20_000); // the holder takes the data or leaves in a bounded step
        }
        try {
            Object r = take(dst);
            return r == null ? 0 : result(r);
        } finally {
            leave();
        }
    }

    /**
     * The readiness now: the bytes readable, -1 when the next read reports the part's end (FIN, a reset, a local stop,
     * the session's end), 0 when nothing is readable. Without the turn: the bytes are counted from the offsets.
     */
    public long readableNow() {
        if (closed || stopped) return -1;
        long n = Math.min(pushedTo, cut) - consumedTo.getAcquire();
        if (n > 0) return n;
        return endPushed ? -1 : 0;
    }

    /** Application read position in the core's stream coordinate system. */
    public long readOffset() { return consumedTo.getAcquire(); }
    /** Removes the opening preamble from a position in the stream's wire coordinates. */
    public long applicationOffset(long wireOffset) { return wireOffset - applicationOrigin; }
    /** Reliable Size of the latest reset, or MAX_VALUE while there is no reset. */
    public long reliableCut() { return cut; }
    /** Whether the source announced a final offset with FIN. */
    public boolean finKnown() { return finKnown; }

    private void wakeReady() {
        for (Ready r; (r = readyWaiters.poll()) != null; ) r.ready();
    }

    /** A thread waiting for readiness. */
    private static final class ReadyThread implements Ready {
        private final Thread thread = Thread.currentThread();
        volatile boolean fired;
        @Override public void ready() { fired = true; java.util.concurrent.locks.LockSupport.unpark(thread); }
    }

    /**
     * Waits until deadline for something to read, without taking it and without the turn, so it never queues behind a
     * read and any number of waits proceed at once: the bytes readable now, or -1 when the next read reports the
     * part's end, which that read then reports in its own form, once. Fails only with its own bound.
     */
    public long awaitReadable(long deadline) throws InterruptedIOException {
        while (true) {
            long r = readableNow();
            if (r != 0) return r;
            ReadyThread w = new ReadyThread();
            readyWaiters.add(w);
            r = readableNow(); // after registering: a push between the two checks wakes w
            if (r != 0) { readyWaiters.remove(w); return r; }
            while (!w.fired) {
                if (Thread.currentThread().isInterrupted() || (deadline != Long.MAX_VALUE && deadline - System.nanoTime() <= 0)) {
                    readyWaiters.remove(w);
                    throw SendLane.interrupted(0);
                }
                if (deadline == Long.MAX_VALUE) java.util.concurrent.locks.LockSupport.park(w);
                else java.util.concurrent.locks.LockSupport.parkNanos(w, deadline - System.nanoTime());
            }
        }
    }

    /** Takes up to max bytes without copying, waiting until deadline; null at FIN. */
    public StreamRecv.Chunk readChunk(int max, long deadline) throws CoreException, InterruptedIOException {
        if (!turn.tryEnter()) SendLane.waitTurn(driver, turn, deadline, 0);
        try {
            SendLane.Blocked self = null;
            while (true) {
                Object r = takeChunk(max);
                if (r != null) return chunkResult(r);
                if (self == null) self = new SendLane.Blocked();
                if (!awaitData(self)) continue;
                while (waiter == self) {
                    if (Thread.currentThread().isInterrupted() || (deadline != Long.MAX_VALUE && deadline - System.nanoTime() <= 0)) {
                        if (WAITER.compareAndSet(this, self, null)) throw SendLane.interrupted(0);
                        break;
                    }
                    self.park(deadline);
                }
            }
        } finally {
            leave();
        }
    }

    private static int result(Object r) throws CoreException {
        if (r instanceof Integer n) return n;
        if (r == StreamRecv.FIN) return -1;
        throw (CoreException) r;
    }

    private static StreamRecv.Chunk chunkResult(Object r) throws CoreException {
        if (r instanceof StreamRecv.Chunk c) return c;
        if (r == StreamRecv.FIN) return null;
        throw (CoreException) r;
    }

    /** The application stopped the part: reads fail from now on. */
    public void stopLocally() {
        stopped = true;
        wakeReady();
    }

    /** Waits until operations that hold or wait for the direction have ended. */
    public void quiesce() throws InterruptedIOException {
        if (!turn.tryEnter()) SendLane.waitTurn(driver, turn, Long.MAX_VALUE, 0);
        leave();
    }

    // ---- asynchronous reads ----

    /** A read run by its turn holder: first the caller, later the completion context. */
    private final class AsyncRead extends Operation<Object> implements SendLane.Resumable, Turn.Waiting {
        private final ByteBuffer dst;
        /** The chunk limit of a chunk read (no dst). */
        private final int max;

        private final java.util.function.Predicate<ReadBuf> fits;

        AsyncRead(ByteBuffer dst, int max, long deadline, java.util.function.Predicate<ReadBuf> fits) {
            super(RecvLane.this.driver, self -> ((AsyncRead) self).withdraw(), deadline);
            this.fits = fits;
            this.dst = dst;
            this.max = max;
        }

        private void withdraw() {
            if (WAITER.compareAndSet(RecvLane.this, this, null)) leave();
            else turn.remove(this);
        }

        @Override public boolean take() {
            if (!grant()) return false;
            resume();
            return true;
        }

        @Override public void resume() {
            driver.completions().execute(this::step);
        }

        void step() {
            if (!begin()) { leave(); return; }
            while (true) {
                Object r = dst != null ? RecvLane.this.take(dst) : takeChunk(max, fits);
                if (r != null) {
                    finish(dst != null ? asRead(r) : r); // a chunk read reports FIN itself
                    leave();
                    return;
                }
                if (awaitData(this)) return;
                if (!begin()) { leave(); return; }
            }
        }

        private static Object asRead(Object r) { return r == StreamRecv.FIN ? Integer.valueOf(-1) : r; }
    }

    /**
     * Starts an asynchronous read into dst (or, when dst is null, a chunk read of up to max).
     * The outcome is the count (-1 at FIN), a StreamRecv.Chunk, StreamRecv.FIN, or a core outcome.
     */
    public Operation<Object> readAsync(ByteBuffer dst, int max, long deadline) {
        return readAsync(dst, max, deadline, null);
    }

    public Operation<Object> readAsync(ByteBuffer dst, int max, long deadline, java.util.function.Predicate<ReadBuf> fits) {
        AsyncRead op = new AsyncRead(dst, max, deadline, fits);
        op.armDeadline();
        if (turn.tryEnterAsync()) {
            if (op.grant()) op.step();
            else leave();
        } else {
            turn.enqueue(op);
        }
        return op;
    }

    /** An asynchronous wait for readiness: it holds no turn, and the loop's pushes wake it. */
    private final class ReadyWait extends Operation<Object> implements Ready {
        /** The wait's report: the bytes readable, -1 at the end. */
        volatile long readable;

        ReadyWait(long deadline) {
            super(RecvLane.this.driver, RecvLane.this::withdrawReady, deadline);
        }

        @Override public void ready() { driver.completions().execute(this::step); }

        void step() {
            if (!begin()) return;
            long r = readableNow();
            if (r != 0) { readable = r; finish(true); return; }
            readyWaiters.add(this);
            pause();
            r = readableNow(); // after registering: a push between the two checks wakes this
            if (r != 0 && readyWaiters.remove(this)) ready();
        }
    }

    /**
     * Starts an asynchronous {@link #awaitReadable}: completes TRUE once something can be read
     * ({@link #readable(Operation)} then tells how much, -1 for the end of any kind). Takes nothing, holds no turn.
     */
    public Operation<Object> awaitReadableAsync(long deadline) {
        ReadyWait op = new ReadyWait(deadline);
        op.armDeadline();
        if (op.grant()) op.step();
        return op;
    }

    /** What a completed {@link #awaitReadableAsync} reported: the bytes readable, -1 at the end. */
    public static long readable(Operation<?> op) {
        return op instanceof ReadyWait r ? r.readable : 0;
    }

    // ---- loop side ----

    /** Pushes what the core can deliver and the end, wakes the waiting reader, re-registers. */
    void pump() {
        if (closed || ended) return;
        long c = core.receiving().pushCut(st);
        boolean f = core.receiving().finKnown(st);
        boolean changed = c != cut || f != finKnown;
        cut = c;
        finKnown = f;
        if (changed && endWatcher != null) endWatcher.accept(null);
        boolean pushed = false;
        reportBytes = core.receiving().laneReportBytes(st);
        while (core.receiving().hasQueued(st)) {
            boolean paid = !core.receiving().packed(st) && core.receiving().reserveLaneView();
            if (!paid) {
                reportBytes = 1;
                applyReport();
                if (core.receiving().pushOutstanding(st)) break;
            }
            long off = core.receiving().pushed(st);
            StreamRecv.Chunk ch = core.receiving().push(st, PIECE);
            if (ch == null) {
                if (paid) core.receiving().releaseLaneView();
                break;
            }
            long to = off + ch.data().remaining(); // before the reader may take from the piece
            if (!based) {
                based = true;
                applicationOrigin = off;
                consumedTo.set(off);
            } // offsets need not start at 0; nothing was pushed to consume yet
            append(new Piece(ch.data(), ch.buf(), off, null, paid));
            pushedTo = to;
            pushed = true;
        }
        Object end = core.receiving().pushEnd(st);
        if (end != null) {
            ended = true;
            append(new Piece(null, null, 0, end, false));
            endPushed = true;
            pushed = true;
            if (end instanceof CoreException failure && endWatcher != null) endWatcher.accept(failure);
            core.receiving().cancelReadWait(st, hostWaiter);
        } else {
            core.receiving().waitRead(st, hostWaiter);
        }
        if (pushed) {
            SendLane.Resumable w = waiter;
            if (w != null && WAITER.compareAndSet(this, w, null)) w.resume();
            if (hostReader != null) hostReader.run();
            wakeReady();
        }
    }

    private void append(Piece p) {
        tail.next = p;
        tail = p;
    }

    /** Applies the reader's consumption report, then pushes what the window now admits. */
    private void reported() {
        dirty.set(false);
        if (closed) return;
        applyReport();
        pump();
    }

    private void applyReport() {
        long r = read.getAndSet(0), d = dropped.getAndSet(0);
        core.receiving().laneConsumed(st, r, d);
        if (endSeen && !endReported) {
            endReported = true;
            core.receiving().laneEnded(st);
            appDone = true;
            driver.post(() -> driver.lanes.partEnded(st)); // after this step, not inside it
        }
    }

    /** Stops the lane before a local stop or release: takes the last report, then releases what was pushed. */
    void close() {
        if (closed) return;
        if (closedWith == null) applyReport();
        closed = true;
        if (hostReader != null) core.receiving().cancelLoanWait(hostReader);
        core.receiving().cancelReadWait(st, hostWaiter);
        SendLane.Resumable w = (SendLane.Resumable) WAITER.getAndSet(this, null);
        if (w != null) w.resume();
        wakeReady();
        if (turn.tryEnter()) {
            discard();
            turn.leave();
        }
        // An end the reader already took is no failure for the watcher, however the stream is released after it.
        if (endWatcher != null && !appDone) endWatcher.accept(closedOutcome());
    }

    /** Calls the watcher with a failed end once pushed, or with null when the source final offset changes. */
    void watchEnd(java.util.function.Consumer<CoreException> watcher) {
        endWatcher = watcher;
        if (watcher == null) return;
        if (closed) { if (!appDone) watcher.accept(closedOutcome()); }
        else if (ended && tail.end instanceof CoreException failure) watcher.accept(failure);
        else if (finKnown || cut != Long.MAX_VALUE) watcher.accept(null);
    }

    /** The session ended: like the core, which drops every buffer, the lane releases what it holds; reads report outcome. */
    void sessionEnded(CoreException outcome) {
        if (closed) return;
        closedWith = outcome;
        close();
        if (hostReader != null) hostReader.run();
    }

    // ---- a loop-run reader (a nested-session carrier) ----

    /** Makes the loop this direction's only reader; reader runs after each push. */
    public void hostReader(Runnable reader) {
        if (hostReader != null) core.receiving().cancelLoanWait(hostReader);
        hostReader = reader;
    }

    /** A loop-run reader's take: like a chunk read, never waiting; its consumption applies at once. */
    public Object hostTake(int max) {
        Object r = takeChunk(max);
        if (r instanceof CoreException error && error.reason() == CoreException.Reason.CHUNK_BUDGET) {
            core.receiving().waitLoan(hostReader);
            r = takeChunk(max);
            if (r instanceof CoreException retry && retry.reason() == CoreException.Reason.CHUNK_BUDGET) return null;
        }
        core.receiving().cancelLoanWait(hostReader);
        unreported = 0;
        if (!closed) applyReport();
        return r;
    }
}
