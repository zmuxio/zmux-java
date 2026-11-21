package io.zmux;

import io.zmux.carrier.PipeIo;
import io.zmux.internal.core.Datagrams;
import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.Session;
import io.zmux.internal.core.StreamRecv;
import io.zmux.internal.core.Stream;
import io.zmux.internal.loop.RecvLane;
import io.zmux.internal.loop.SendLane;
import io.zmux.internal.loop.Operation;
import io.zmux.internal.loop.LaneTable;
import io.zmux.internal.loop.SessionRuntime;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.Cleaner;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A stream handle. Concurrent reads (and concurrent writes) are serialised; reads and writes
 * proceed independently. A handle that becomes unreachable without {@link #close()} releases
 * the stream as close does.
 *
 * <p><b>{@link #close()} is not a clean end.</b> As for every zmux binding it resets a sending
 * part not finished with ZMUX_CANCELLED (0x5a4d0001), so the peer sees an abandoned stream rather
 * than a clean end, and stops a receiving part not read to its end with code 0; unlike
 * {@link java.net.Socket#close()}, it never flushes and sends FIN. After the last write call
 * {@link #finish()}, and only then close:
 * <pre>{@code
 * try (ZmuxStream s = session.openStream()) {
 *     s.write(request);
 *     s.finish();          // FIN: the peer reads everything, then end of stream
 *     ... read the reply ...
 * }                                // releases the handle
 * }</pre>
 *
 * <p><b>Blocking calls on the completion thread.</b> A blocking call may run on the session's completion
 * thread (in a dependent stage of a {@link ZmuxFuture}), except one that would wait behind an asynchronous
 * operation of the same direction: see {@link ZmuxFuture}, which states the rule.
 */
public final class ZmuxStream implements ZmuxSendStream, ZmuxReceiveStream {
    private static final Cleaner CLEANER = Cleaner.create(Thread.ofVirtual().name("zmux-cleaner").factory());

    final ZmuxSession session;
    private final Session core;
    final Stream st;
    final SendLane out;
    final RecvLane in;
    private final Preamble preamble;
    private final Cleaner.Cleanable cleanable;
    private final AtomicBoolean carrierOwned = new AtomicBoolean();
    // The runtime settings as last set (null: not set), read back by their getters.
    private volatile Duration readTimeout;
    private volatile Duration writeTimeout;
    private volatile Priority peerPriority;
    private volatile Scheduler scheduler;
    private volatile Boolean redundant;

    ZmuxStream(ZmuxSession session, LaneTable.StreamLanes lanes, Preamble preamble) {
        this.session = session;
        this.core = session.core;
        this.st = lanes.stream();
        this.out = lanes.send();
        this.in = lanes.recv();
        this.preamble = preamble;
        this.cleanable = CLEANER.register(this, new Release(session.driver, st));
    }

    /** Releases the stream on its loop; holds no reference to the handle. */
    private record Release(SessionRuntime driver, Stream st) implements Runnable {
        @Override
        public void run() {
            driver.post(() -> driver.lanes.releaseStream(st));
        }
    }

    static Preamble preambleOf(Stream st) {
        io.zmux.internal.wire.Preamble p = st.peerPreamble();
        if (p == null) return Preamble.EMPTY;
        List<Preamble.Tlv> tlvs = new ArrayList<>();
        for (var t : p.applicationTlvs()) tlvs.add(new Preamble.Tlv(t.type(), t.value()));
        return new Preamble(p.openInfo(), tlvs, p.pipeCarrier());
    }

    /** Transfers this bidirectional stream to a nonblocking nested-session carrier. Do not use it afterwards. */
    public PipeIo asPipe() {
        claimIo();
        return new StreamPipe(this).io();
    }

    void claimIo() {
        session.driver.checkBlocking();
        if (!st.bidirectional()) throw new IllegalStateException("a transferred stream must be bidirectional");
        if (!carrierOwned.compareAndSet(false, true)) throw new IllegalStateException("stream I/O already transferred");
        try {
            // Operations already in progress end first; later ones fail at entry.
            in.quiesce(); out.quiesce();
        } catch (InterruptedIOException interrupted) {
            carrierOwned.set(false);
            throw new IllegalStateException("interrupted while transferring stream I/O", interrupted);
        } catch (RuntimeException refused) {
            // For one, a wait on the completion thread behind an asynchronous operation: nothing was transferred.
            carrierOwned.set(false);
            throw refused;
        }
    }

    /** Rolls back a transfer that has not started any work. */
    void unclaimIo() { carrierOwned.set(false); }

    private void checkApplicationIo() {
        if (carrierOwned.get()) throw new IllegalStateException("stream owned by a carrier");
    }

    // ---- sending ----

    @Override
    public void write(ByteBuffer src) throws IOException {
        checkApplicationIo();
        Objects.requireNonNull(src);
        session.driver.checkBlocking();
        try {
            out.write(src, null, null, ZmuxSession.until(writeTimeout));
        } catch (CoreException e) {
            throw Errors.io(e);
        }
    }

    @Override
    public void writeChunk(Chunk chunk) throws IOException {
        StreamRecv.Chunk owned = Objects.requireNonNull(chunk).take();
        try {
            checkApplicationIo();
            session.driver.checkBlocking();
            out.write(null, owned.data(), owned.buf(), ZmuxSession.until(writeTimeout));
        } catch (CoreException e) {
            throw Errors.io(e);
        } finally {
            owned.buf().unref();
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        write(ByteBuffer.wrap(b, off, len));
    }

    @Override
    public int tryWrite(ByteBuffer src) throws IOException {
        checkApplicationIo();
        try {
            return out.tryWrite(src);
        } catch (CoreException e) {
            throw Errors.io(e);
        }
    }

    @Override
    public long awaitWritable() throws IOException {
        return awaitWritable(ZmuxSession.until(writeTimeout));
    }

    @Override
    public long awaitWritable(Duration timeout) throws IOException {
        return awaitWritable(ZmuxSession.deadline(timeout));
    }

    private long awaitWritable(long deadline) throws IOException {
        checkApplicationIo();
        session.driver.checkBlocking();
        try {
            return out.awaitWritable(deadline);
        } catch (CoreException e) {
            throw Errors.io(e);
        }
    }

    @Override
    public void finish() throws IOException {
        checkApplicationIo();
        session.driver.checkBlocking();
        long deadline = ZmuxSession.until(writeTimeout);
        try {
            Errors.check(out.finish(deadline).await(deadline));
        } catch (CoreException e) {
            throw Errors.io(e);
        }
    }

    @Override
    public void reset(long code) {
        session.driver.post(() -> session.driver.lanes.resetSending(st, code));
    }

    @Override
    public void markReliableBoundary() {
        session.driver.post(() -> { session.driver.lanes.flushSending(st); core.sending().setReliableBoundary(st); });
    }

    /**
     * The stream's statistics: taken on the session's loop while the
     * session holds the stream, its final values once the session released it.
     */
    public StreamStats stats() throws IOException {
        StreamStats last = st.released();
        if (last != null) return last;
        try {
            return (StreamStats) session.run(op -> { session.driver.lanes.flushSending(st); return st.released() != null ? st.released() : st.stats(); });
        } catch (IOException e) {
            last = st.released();
            if (last != null) return last;
            throw e;
        }
    }

    @Override
    public long sendCapacity() throws IOException {
        return (Long) session.run(op -> { session.driver.lanes.flushSending(st); return core.sending().sendCapacity(st); });
    }

    @Override
    public long awaitSendCapacity(long min) throws IOException {
        return awaitSendCapacity(min, Long.MAX_VALUE);
    }

    @Override
    public long awaitSendCapacity(long min, Duration timeout) throws IOException {
        return awaitSendCapacity(min, ZmuxSession.deadline(timeout));
    }

    private long awaitSendCapacity(long min, long deadline) throws IOException {
        if (min < 1) throw new IllegalArgumentException("min must be at least 1");
        session.driver.checkBlocking();
        return (Long) session.call(op -> {
            session.driver.lanes.flushSending(st);
            if (core.sending().waitSendCapacity(st, min, op)) return core.sending().sendCapacity(st);
            return Operation.PENDING;
        }, op -> core.sending().cancelCapacityWait(st, op), deadline);
    }

    @Override
    public void setPriority(Priority priority) {
        Objects.requireNonNull(priority);
        core.streams().holdPriority(st, priority.urgency(), priority.incremental());
        session.driver.post(() -> core.streams().applyHeldPriority(st));
    }

    @Override
    public Priority priority() {
        int held = core.streams().heldPriority(st);
        return new Priority(held >> 1, (held & 1) != 0);
    }

    @Override
    public void setScheduler(Scheduler s) {
        Objects.requireNonNull(s, "s");
        scheduler = s;
        session.driver.post(() -> session.driver.pickers.install(st, Scheduling.policy(s), Scheduling.picker(s)));
    }

    @Override
    public void clearScheduler() {
        scheduler = null;
        session.driver.post(() -> session.driver.pickers.install(st, null, null));
    }

    @Override
    public java.util.Optional<Scheduler> scheduler() { return java.util.Optional.ofNullable(scheduler); }

    @Override
    public void setRedundant(boolean on) {
        redundant = on;
        session.driver.post(() -> core.setRedundant(st, on));
    }

    @Override
    public void clearRedundant() {
        redundant = null;
        session.driver.post(() -> core.setRedundant(st, null));
    }

    @Override
    public java.util.Optional<Boolean> redundant() { return java.util.Optional.ofNullable(redundant); }

    @Override
    public void setSendBuffer(long bytes) {
        if (bytes < 0) throw new IllegalArgumentException("negative send buffer");
        if (out == null) throw new IllegalStateException("the stream has no sending part");
        core.streams().holdSendBuffer(st, bytes);
        // The loop applies what was held last, so concurrent setters leave the setting and the getter in agreement.
        session.driver.post(() -> core.applyHeldSendBuffer(st));
    }

    @Override
    public int sendBuffer() {
        return (int) Math.min(core.streams().heldSendBuffer(st), Integer.MAX_VALUE);
    }

    @Override
    public void setWriteTimeout(Duration timeout) {
        writeTimeout = positive(timeout);
    }

    @Override
    public void clearWriteTimeout() { writeTimeout = null; }

    @Override
    public java.util.Optional<Duration> writeTimeout() { return java.util.Optional.ofNullable(writeTimeout); }

    private static Duration positive(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("a timeout is a positive duration");
        return timeout;
    }

    @Override
    public OutputStream outputStream() {
        return new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                ZmuxStream.this.write(new byte[] {(byte) b}, 0, 1);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                ZmuxStream.this.write(b, off, len);
            }

            @Override
            public void close() throws IOException {
                ZmuxStream.this.finish();
            }
        };
    }

    // ---- receiving ----

    @Override
    public int read(ByteBuffer dst) throws IOException {
        checkApplicationIo();
        checkReadDestination(dst);
        if (!dst.hasRemaining()) return 0;
        session.driver.checkBlocking();
        try {
            return in.read(dst, ZmuxSession.until(readTimeout));
        } catch (CoreException e) {
            throw Errors.io(e);
        }
    }

    @Override
    public void awaitReadable() throws IOException {
        awaitReadable(ZmuxSession.until(readTimeout));
    }

    @Override
    public void awaitReadable(Duration timeout) throws IOException {
        awaitReadable(ZmuxSession.deadline(timeout));
    }

    private void awaitReadable(long deadline) throws IOException {
        checkApplicationIo();
        session.driver.checkBlocking();
        in.awaitReadable(deadline);
    }

    @Override
    public int tryRead(ByteBuffer dst) throws IOException {
        checkApplicationIo();
        checkReadDestination(dst);
        if (!dst.hasRemaining()) return 0;
        try {
            return in.tryRead(dst);
        } catch (CoreException e) {
            throw Errors.io(e);
        }
    }

    @Override
    public Chunk readChunk(int max) throws IOException {
        checkApplicationIo();
        if (max < 1) throw new IllegalArgumentException("max must be positive");
        session.driver.checkBlocking();
        try {
            StreamRecv.Chunk c = in.readChunk(max, ZmuxSession.until(readTimeout));
            return c == null ? null : new Chunk(c);
        } catch (CoreException e) {
            throw Errors.io(e);
        }
    }

    @Override
    public byte[] readAll(long maxBytes) throws IOException {
        if (maxBytes < 0) throw new IllegalArgumentException("negative maxBytes");
        var out = new java.io.ByteArrayOutputStream();
        for (Chunk next; (next = readChunk(64 << 10)) != null; ) {
            try (Chunk c = next) {
                ByteBuffer b = c.buffer();
                if (out.size() + (long) b.remaining() > maxBytes) {
                    stop(0);
                    throw new LimitExceededException(st.id(), maxBytes);
                }
                byte[] a = new byte[b.remaining()];
                b.get(a);
                out.writeBytes(a);
            }
        }
        return out.toByteArray();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return read(ByteBuffer.wrap(b, off, len));
    }

    @Override
    public void stop(long code) {
        in.stopLocally();
        session.driver.post(() -> session.driver.lanes.stopReceiving(st, code));
    }

    @Override
    public void reject() {
        if (in != null) in.stopLocally();
        session.driver.post(() -> session.driver.lanes.rejectStream(st));
    }

    @Override
    public void setPeerPriority(Priority priority) throws IOException {
        int urgency = priority.urgency();
        boolean incremental = priority.incremental();
        session.run(op -> {
            core.streams().setPeerPriority(st, urgency, incremental);
            return true;
        });
        peerPriority = priority;
    }

    @Override
    public java.util.Optional<Priority> peerPriority() { return java.util.Optional.ofNullable(peerPriority); }

    @Override
    public void setReceiveWindow(long initial, long max) {
        io.zmux.internal.core.StreamRecv.checkReceiveWindow(initial, max);
        if (in == null) throw new IllegalStateException("the stream has no receiving part");
        core.streams().holdReceiveWindow(st, initial, max);
        // The loop applies what was held last, the initial and the cap together.
        session.driver.post(() -> core.applyHeldReceiveWindow(st));
    }

    @Override
    public ReceiveWindow receiveWindow() {
        var w = core.streams().heldWindow(st); // one read: the initial and the cap of one setting
        return new ReceiveWindow(w.initial(), w.max());
    }

    @Override
    public Preamble preamble() {
        return preamble;
    }

    @Override
    public java.util.Optional<Priority> preamblePriority() {
        io.zmux.internal.wire.Preamble p = st.peerPreamble();
        return p == null || !p.hasPriority() ? java.util.Optional.empty() : java.util.Optional.of(new Priority(p.urgency(), p.incremental()));
    }

    @Override
    public boolean isReplayable() {
        return false; // No Java carrier takes carrier early data.
    }

    @Override
    public void setReadTimeout(Duration timeout) {
        readTimeout = positive(timeout);
    }

    @Override
    public void clearReadTimeout() { readTimeout = null; }

    @Override
    public java.util.Optional<Duration> readTimeout() { return java.util.Optional.ofNullable(readTimeout); }

    @Override
    public InputStream inputStream() {
        return new InputStream() {
            @Override
            public int read() throws IOException {
                byte[] one = new byte[1];
                int n = ZmuxStream.this.read(one, 0, 1);
                return n < 0 ? -1 : one[0] & 0xff;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return len == 0 ? 0 : ZmuxStream.this.read(b, off, len);
            }

            @Override
            public void close() {
                stop(0);
            }
        };
    }

    // ---- both ----

    /**
     * Waits until the stream's opening frame and everything written before the call were sent. Once they were, the
     * flush succeeds even if the peer stops or resets the stream afterwards; the next operation reports that. A
     * stream that failed before its data was sent fails the flush.
     */
    @Override
    public void flush() throws IOException {
        checkApplicationIo();
        session.driver.checkBlocking();
        long deadline = ZmuxSession.until(writeTimeout);
        Errors.check(out.awaitSent(true, deadline).await(deadline));
    }

    @Override
    public long id() throws IOException {
        return (Long) session.run(op -> core.streams().pinId(st));
    }

    /**
     * The largest payload of a datagram of this stream's flow with the given context that the
     * peer accepts now, 0 when none (no datagram flows): packet-carrying applications use it as
     * their MTU. Before the stream has its identifier the flow prefix is counted at its longest.
     * As of the loop's last round; no I/O.
     */
    public int maxDatagramPayload(long context) {
        var i = session.driver.info();
        return flowPayload(i, i.datagramLimit(), context);
    }

    /**
     * The largest payload of a datagram of this stream's flow with the given context that leaves as one datagram of
     * the carrier instead of in a record ({@link ConnectionInfo#pathDatagramPayload()}), 0 when the peer accepts none
     * on this stream. As of the loop's last round; no I/O.
     */
    public int pathDatagramPayload(long context) {
        var i = session.driver.info();
        return flowPayload(i, i.datagramPathLimit(), context);
    }

    /** What a limit before the flow prefix leaves for a datagram of this stream's flow with context, 0 without flows. */
    private int flowPayload(io.zmux.internal.loop.SessionRuntime.SessionInfo i, int limit, long context) {
        if (!i.datagramFlows()) return 0;
        // The flow prefix: the stream's flow (its ID + 1) and the context, as variable-length integers.
        int flow = st.idSet() ? io.zmux.internal.wire.VarInt.length(st.id() + 1) : 8;
        return Math.max(0, limit - flow - io.zmux.internal.wire.VarInt.length(context));
    }

    /**
     * Datagram of this stream's flow (datagram flows in use); never blocks. Returns false when
     * the session's datagram limiter dropped it.
     */
    public boolean sendDatagram(long context, ByteBuffer payload) throws IOException {
        return session.sendDatagram(st, context, payload);
    }

    /** Waits for the next datagram of this stream's flow. */
    public StreamDatagram receiveDatagram() throws IOException {
        return receiveDatagram(Long.MAX_VALUE);
    }

    /** Waits at most timeout for the next datagram of this stream's flow; {@link SocketTimeoutException} after it. */
    public StreamDatagram receiveDatagram(Duration timeout) throws IOException {
        return receiveDatagram(ZmuxSession.deadline(timeout));
    }

    private StreamDatagram receiveDatagram(long deadline) throws IOException {
        Object r = session.call(op -> {
            Datagrams.StreamDatagram d = core.datagrams().receive(st);
            if (d != null) return new StreamDatagram(d.context(), d.payload());
            if (core.error() != null) throw CoreException.session(core.error());
            core.datagrams().waitReceive(st, op);
            return Operation.PENDING;
        }, op -> core.datagrams().cancelReceiveWait(st, op), deadline);
        return (StreamDatagram) r;
    }

    /**
     * Releases the stream. A sending part that was {@link #finish() finished} and a receiving
     * part read to its end are left as they are, so the block {@code write}, {@code finish},
     * read the reply, {@code close} (the end of a try-with-resources) ends the stream cleanly.
     * Only a part left open is ended abruptly: an unfinished sending part is reset with
     * ZMUX_CANCELLED (RESET_STREAM, <b>its unsent data discarded</b>) and an unread receiving part
     * stopped with code 0 (STOP_SENDING). Close never finishes the stream: call {@link #finish()} after the last write.
     */
    @Override
    public void close() {
        cleanable.clean();
    }

    private static void checkReadDestination(ByteBuffer dst) {
        Objects.requireNonNull(dst);
        if (dst.hasRemaining() && dst.isReadOnly()) throw new ReadOnlyBufferException();
    }

    /** Borrows dst until completion; successful cancellation also ends all access to it. */
    @Override public ZmuxFuture<Integer> readAsync(ByteBuffer dst) {
        checkApplicationIo(); checkReadDestination(dst);
        if (!dst.hasRemaining()) return ZmuxFuture.completed(0);
        return ZmuxSession.future(in.readAsync(dst, 0, ZmuxSession.until(readTimeout)), r -> {
            if (r instanceof Throwable) throw Errors.outcome(r, 0);
            return (Integer) r;
        });
    }

    @Override public ZmuxFuture<Chunk> readChunkAsync(int max) {
        checkApplicationIo(); if (max < 1) throw new IllegalArgumentException("max must be positive");
        return ZmuxSession.future(in.readAsync(null, max, ZmuxSession.until(readTimeout)), r -> {
            if (r instanceof Throwable) throw Errors.outcome(r, 0);
            return r == StreamRecv.FIN ? null : new Chunk((StreamRecv.Chunk) r);
        });
    }

    /** Borrows src until completion; a failed or cancelled write can have sent a prefix. */
    @Override public ZmuxFuture<Void> writeAsync(ByteBuffer src) {
        checkApplicationIo(); Objects.requireNonNull(src);
        var op = out.writeAsync(src, null, null, ZmuxSession.until(writeTimeout));
        return ZmuxSession.future(op, r -> {
            if (r instanceof Throwable) throw Errors.outcome(r, SendLane.written(op));
            return null;
        });
    }

    @Override public ZmuxFuture<Void> writeChunkAsync(Chunk chunk) {
        StreamRecv.Chunk owned = Objects.requireNonNull(chunk).take();
        try { checkApplicationIo(); }
        catch (RuntimeException error) { owned.buf().unref(); throw error; }
        var op = out.writeAsync(null, owned.data(), owned.buf(), ZmuxSession.until(writeTimeout));
        return ZmuxSession.future(op, r -> {
            if (r instanceof Throwable) throw Errors.outcome(r, SendLane.written(op));
            return null;
        });
    }

    @Override public ZmuxFuture<Void> finishAsync() {
        checkApplicationIo();
        return ZmuxSession.future(out.finishAsync(ZmuxSession.until(writeTimeout)), r -> {
            if (r instanceof Throwable) throw Errors.outcome(r, 0);
            return null;
        });
    }

    @Override public ZmuxFuture<Long> idAsync() {
        return session.async(op -> core.streams().pinId(st), null, null, Long.MAX_VALUE, raw -> (Long) raw);
    }
}
