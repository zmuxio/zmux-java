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
 * the stream (reset and stop with code 0).
 *
 * <p><b>{@link #close()} is not a clean end.</b> As for every zmux binding it resets the
 * sending part and stops the receiving part (code 0), unlike {@link java.net.Socket#close()},
 * which flushes and sends FIN. After the last write call {@link #finish()}, and only
 * then close:
 * <pre>{@code
 * try (ZmuxStream s = session.openStream()) {
 *     s.write(request);
 *     s.finish();          // FIN: the peer reads everything, then end of stream
 *     ... read the reply ...
 * }                                // releases the handle
 * }</pre>
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
    private volatile Duration readTimeout;
    private volatile Duration writeTimeout;

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
            out.write(src, null, null, ZmuxSession.deadline(writeTimeout));
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
            out.write(null, owned.data(), owned.buf(), ZmuxSession.deadline(writeTimeout));
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
    public void finish() throws IOException {
        checkApplicationIo();
        session.driver.checkBlocking();
        long deadline = ZmuxSession.deadline(writeTimeout);
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
     * The stream's statistics (API.md Section 7.2): taken on the session's loop while the
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
        return awaitSendCapacity(min, null);
    }

    @Override
    public long awaitSendCapacity(long min, Duration timeout) throws IOException {
        if (min < 1) throw new IllegalArgumentException("min must be at least 1");
        session.driver.checkBlocking();
        return (Long) session.call(op -> {
            session.driver.lanes.flushSending(st);
            if (core.sending().waitSendCapacity(st, min, op)) return core.sending().sendCapacity(st);
            return Operation.PENDING;
        }, op -> core.sending().cancelCapacityWait(st, op), ZmuxSession.deadline(timeout));
    }

    @Override
    public void setPriority(Priority priority) {
        Objects.requireNonNull(priority);
        session.driver.post(() -> core.streams().setPriority(st, priority.urgency(), priority.incremental()));
    }

    /** This direction's own scheduling policy; null returns it to the session's. */
    public void setScheduler(Scheduler s) {
        session.driver.post(() -> session.driver.pickers.install(st, s == null ? null : Scheduling.policy(s), Scheduling.picker(s)));
    }

    /** Sends this direction's data on two pipes when possible; null returns it to the session's default. */
    public void setRedundant(Boolean on) {
        session.driver.post(() -> core.setRedundant(st, on));
    }

    @Override
    public void setSendBuffer(long bytes) {
        if (bytes < 0) throw new IllegalArgumentException("negative send buffer");
        session.driver.post(() -> core.sending().setSendBuffer(st, bytes));
    }

    @Override
    public void setWriteTimeout(Duration timeout) {
        writeTimeout = timeout;
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
            return in.read(dst, ZmuxSession.deadline(readTimeout));
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
            StreamRecv.Chunk c = in.readChunk(max, ZmuxSession.deadline(readTimeout));
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
                    throw new IOException("stream longer than " + maxBytes + " bytes");
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
            return Boolean.TRUE;
        });
    }

    @Override
    public void setReceiveWindow(long initial, long max) {
        if (initial < 0 || max < 0) throw new IllegalArgumentException("negative receive window");
        session.driver.post(() -> core.receiving().setReceiveWindow(st, initial, max));
    }

    @Override
    public Preamble preamble() {
        return preamble;
    }

    @Override
    public boolean isReplayable() {
        return st.replayable(); // A loop-published volatile flag: true only until the handshake completes.
    }

    @Override
    public void setReadTimeout(Duration timeout) {
        readTimeout = timeout;
    }

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

    @Override
    public long id() throws IOException {
        return (Long) session.run(op -> core.streams().pinId(st));
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
        return receiveDatagram(null);
    }

    /** Waits at most timeout (null: no limit) for the next datagram of this stream's flow; {@link SocketTimeoutException} after it. */
    public StreamDatagram receiveDatagram(Duration timeout) throws IOException {
        Object r = session.call(op -> {
            Datagrams.StreamDatagram d = core.datagrams().receive(st);
            if (d != null) return new StreamDatagram(d.context(), d.payload());
            if (core.error() != null) throw CoreException.session(core.error());
            core.datagrams().waitReceive(st, op);
            return Operation.PENDING;
        }, op -> core.datagrams().cancelReceiveWait(st, op), ZmuxSession.deadline(timeout));
        return (StreamDatagram) r;
    }

    /**
     * Releases the stream: RESET_STREAM and STOP_SENDING with code 0 for the parts that have
     * not ended (API.md Section 5.1). <b>This discards unsent data</b>; call
     * {@link #finish()} first to end the stream cleanly.
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
        return ZmuxSession.future(in.readAsync(dst, 0, ZmuxSession.deadline(readTimeout)), r -> {
            if (r instanceof Throwable) throw Errors.outcome(r, 0);
            return (Integer) r;
        });
    }

    @Override public ZmuxFuture<Chunk> readChunkAsync(int max) {
        checkApplicationIo(); if (max < 1) throw new IllegalArgumentException("max must be positive");
        return ZmuxSession.future(in.readAsync(null, max, ZmuxSession.deadline(readTimeout)), r -> {
            if (r instanceof Throwable) throw Errors.outcome(r, 0);
            return r == StreamRecv.FIN ? null : new Chunk((StreamRecv.Chunk) r);
        });
    }

    /** Borrows src until completion; a failed or cancelled write can have sent a prefix. */
    @Override public ZmuxFuture<Void> writeAsync(ByteBuffer src) {
        checkApplicationIo(); Objects.requireNonNull(src);
        var op = out.writeAsync(src, null, null, ZmuxSession.deadline(writeTimeout));
        return ZmuxSession.future(op, r -> {
            if (r instanceof Throwable) throw Errors.outcome(r, SendLane.written(op));
            return null;
        });
    }

    @Override public ZmuxFuture<Void> writeChunkAsync(Chunk chunk) {
        StreamRecv.Chunk owned = Objects.requireNonNull(chunk).take();
        try { checkApplicationIo(); }
        catch (RuntimeException error) { owned.buf().unref(); throw error; }
        var op = out.writeAsync(null, owned.data(), owned.buf(), ZmuxSession.deadline(writeTimeout));
        return ZmuxSession.future(op, r -> {
            if (r instanceof Throwable) throw Errors.outcome(r, SendLane.written(op));
            return null;
        });
    }

    @Override public ZmuxFuture<Void> finishAsync() {
        checkApplicationIo();
        return ZmuxSession.future(out.finishAsync(ZmuxSession.deadline(writeTimeout)), r -> {
            if (r instanceof Throwable) throw Errors.outcome(r, 0);
            return null;
        });
    }

    @Override public ZmuxFuture<Long> idAsync() {
        return session.async(op -> core.streams().pinId(st), null, null, Long.MAX_VALUE, raw -> (Long) raw);
    }
}
