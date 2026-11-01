package io.zmux.internal.loop;

import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.ReadBuf;
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
        final ByteBuffer data;
        final ReadBuf buf;
        long off;
        final Object end;
        volatile Piece next;

        Piece(ByteBuffer data, ReadBuf buf, long off, Object end) {
            this.data = data;
            this.buf = buf;
            this.off = off;
            this.end = end;
        }
    }

    private final SessionRuntime driver;
    private final Session core;
    private final Stream st;
    final Turn turn = new Turn();
    private volatile long cut = Long.MAX_VALUE;
    private final AtomicLong read = new AtomicLong(), dropped = new AtomicLong();
    private volatile long reportBytes;
    private volatile boolean endSeen;
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
    private boolean endReported;
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
        head = tail = new Piece(null, null, 0, null);
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
        head = p;
        if (p.buf != null) p.buf.unref();
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
            long lim = Math.min(p.data.remaining(), cut - p.off);
            int c = (int) Math.min(lim, dst.remaining());
            dst.put(dst.position(), p.data, p.data.position(), c);
            dst.position(dst.position() + c);
            p.data.position(p.data.position() + c);
            p.off += c;
            n += c;
            if (p.data.hasRemaining() && p.off < cut) break;
            p = peek();
        }
        consumed(n);
        return n;
    }

    /** Takes up to max bytes without copying: a chunk (data, owned reference), FIN, an outcome, or null. */
    private Object takeChunk(int max) {
        if (closed || stopped) return closedOutcome();
        Piece p = peek();
        if (p == null) return null;
        if (p.end != null) return reachEnd(p);
        int c = (int) Math.min(Math.min(p.data.remaining(), cut - p.off), max);
        ByteBuffer d = p.data.slice(p.data.position(), c);
        ReadBuf buf = p.buf;
        p.data.position(p.data.position() + c);
        p.off += c;
        if (p.data.hasRemaining() && p.off < cut) {
            if (buf != null) buf.ref();
        } else {
            head = p; // Its reference moves to the chunk.
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
        if (!turn.tryEnter()) SendLane.waitTurn(turn, deadline, 0);
        try {
            SendLane.Blocked self = null;
            while (true) {
                Object r = take(dst);
                if (r != null) return result(r);
                if (self == null) self = new SendLane.Blocked();
                if (!awaitData(self)) continue;
                while (waiter == self) {
                    if (Thread.currentThread().isInterrupted() || deadline != Long.MAX_VALUE && deadline - System.nanoTime() <= 0) {
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
     * Waits until deadline for something to read, without taking it: the bytes readable now,
     * or -1 when the next read returns the end. Throws when the lane was closed.
     */
    public long awaitReadable(long deadline) throws CoreException, InterruptedIOException {
        if (!turn.tryEnter()) SendLane.waitTurn(turn, deadline, 0);
        try {
            SendLane.Blocked self = null;
            while (true) {
                if (closed || stopped) throw closedOutcome();
                Piece p = peek();
                if (p != null) {
                    if (p.end != null) return -1;
                    long n = 0;
                    for (Piece q = p; q != null && q.end == null; q = q.next) n += Math.clamp(cut - q.off, 0, q.data.remaining());
                    return n;
                }
                if (self == null) self = new SendLane.Blocked();
                if (!awaitData(self)) continue;
                while (waiter == self) {
                    if (Thread.currentThread().isInterrupted() || deadline != Long.MAX_VALUE && deadline - System.nanoTime() <= 0) {
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

    /** Takes up to max bytes without copying, waiting until deadline; null at FIN. */
    public StreamRecv.Chunk readChunk(int max, long deadline) throws CoreException, InterruptedIOException {
        if (!turn.tryEnter()) SendLane.waitTurn(turn, deadline, 0);
        try {
            SendLane.Blocked self = null;
            while (true) {
                Object r = takeChunk(max);
                if (r != null) return chunkResult(r);
                if (self == null) self = new SendLane.Blocked();
                if (!awaitData(self)) continue;
                while (waiter == self) {
                    if (Thread.currentThread().isInterrupted() || deadline != Long.MAX_VALUE && deadline - System.nanoTime() <= 0) {
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
    }

    /** Waits until operations that hold or wait for the direction have ended. */
    public void quiesce() throws InterruptedIOException {
        if (!turn.tryEnter()) SendLane.waitTurn(turn, Long.MAX_VALUE, 0);
        leave();
    }

    // ---- asynchronous reads ----

    /** A read run by its turn holder: first the caller, later the completion context. */
    private final class AsyncRead extends Operation<Object> implements SendLane.Resumable, Turn.Waiting {
        private final ByteBuffer dst;
        private final int max;

        AsyncRead(ByteBuffer dst, int max, long deadline) {
            super(RecvLane.this.driver, self -> ((AsyncRead) self).withdraw(), deadline);
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
                Object r = dst != null ? RecvLane.this.take(dst) : takeChunk(max);
                if (r != null) {
                    finish(dst != null ? asRead(r) : r); // a chunk read reports FIN itself
                    leave();
                    return;
                }
                if (awaitData(this)) return;
                if (!begin()) { leave(); return; }
            }
        }

        private Object asRead(Object r) { return r == StreamRecv.FIN ? (Object) (-1) : r; }
    }

    /**
     * Starts an asynchronous read into dst (or, when dst is null, a chunk read of up to max).
     * The outcome is the count (-1 at FIN), a StreamRecv.Chunk, StreamRecv.FIN, or a core outcome.
     */
    public Operation<Object> readAsync(ByteBuffer dst, int max, long deadline) {
        AsyncRead op = new AsyncRead(dst, max, deadline);
        op.armDeadline();
        if (turn.tryEnter()) {
            if (op.grant()) op.step();
            else leave();
        } else {
            turn.enqueue(op);
        }
        return op;
    }

    // ---- loop side ----

    /** Pushes what the core can deliver and the end, wakes the waiting reader, re-registers. */
    void pump() {
        if (closed || ended) return;
        long c = core.receiving().pushCut(st);
        if (c != cut) cut = c;
        boolean pushed = false;
        while (true) {
            long off = core.receiving().pushed(st);
            StreamRecv.Chunk ch = core.receiving().push(st, PIECE);
            if (ch == null) break;
            append(new Piece(ch.data(), ch.buf(), off, null));
            pushed = true;
        }
        Object end = core.receiving().pushEnd(st);
        if (end != null) {
            ended = true;
            append(new Piece(null, null, 0, end));
            pushed = true;
            if (end instanceof CoreException failure && endWatcher != null) endWatcher.accept(failure);
            core.receiving().cancelReadWait(st, hostWaiter);
        } else {
            core.receiving().waitRead(st, hostWaiter);
        }
        reportBytes = core.receiving().laneReportBytes(st);
        if (pushed) {
            SendLane.Resumable w = waiter;
            if (w != null && WAITER.compareAndSet(this, w, null)) w.resume();
            if (hostReader != null) hostReader.run();
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
        }
    }

    /** Stops the lane before a local stop or release: takes the last report, then releases what was pushed. */
    void close() {
        if (closed) return;
        if (closedWith == null) applyReport();
        closed = true;
        core.receiving().cancelReadWait(st, hostWaiter);
        SendLane.Resumable w = (SendLane.Resumable) WAITER.getAndSet(this, null);
        if (w != null) w.resume();
        if (turn.tryEnter()) {
            discard();
            turn.leave();
        }
        if (endWatcher != null) endWatcher.accept(closedOutcome());
    }

    /** Calls watcher (on the loop) with a failed end once it is pushed; null removes it. */
    void watchEnd(java.util.function.Consumer<CoreException> watcher) {
        endWatcher = watcher;
        if (watcher == null) return;
        if (closed) watcher.accept(closedOutcome());
        else if (ended && tail.end instanceof CoreException failure) watcher.accept(failure);
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
        hostReader = reader;
    }

    /** A loop-run reader's take: like a chunk read, never waiting; its consumption applies at once. */
    public Object hostTake(int max) {
        Object r = takeChunk(max);
        unreported = 0;
        if (!closed) applyReport();
        return r;
    }
}
