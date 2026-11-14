package io.zmux.helper;

import io.zmux.Chunk;
import io.zmux.DatagramTooLargeException;
import io.zmux.NotNegotiatedException;
import io.zmux.PacketErrorException;
import io.zmux.Preamble;
import io.zmux.StreamClosedException;
import io.zmux.StreamDatagram;
import io.zmux.ZmuxFuture;
import io.zmux.ZmuxSession;
import io.zmux.ZmuxStream;
import io.zmux.internal.spi.Spi;
import io.zmux.internal.wire.Capsule;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.VarInt;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A packet stream (EXTENSIONS.md Section 12): a bidirectional stream that carries packets, each
 * a context ID and a payload, sent either as a datagram of the stream's flow or as a DATAGRAM
 * capsule in the stream's bytes. Both endpoints agree by their own convention (for example the
 * stream's preamble) that the stream is one; nothing on the wire announces it.
 *
 * <p>Packets are not ordered relative to each other: capsules arrive in order and reliably,
 * datagrams may be lost or overtake them. A packet is never fragmented. Received capsules of
 * other types are skipped. A capsule above {@link Options#maxPacket()} (plus its context ID), or
 * the stream ending inside a capsule, fails the stream: it is stopped and reset with
 * {@link #PACKET_ERROR} and {@link #receive()} throws {@link PacketErrorException}.
 *
 * <p>The wrapped stream belongs to the packet stream: do not read, write or close it directly.
 * Received packets are read by background virtual threads, started at the first receive, into a
 * small queue; a slow receiver back-pressures the sender's capsules through flow control, and
 * datagrams then wait in, or are dropped from, the session's queues. {@link #close()} or
 * {@link #abort(long)} a packet stream to end those threads.
 *
 * <p>Thread-safe: sends and receives may run concurrently; capsule sends are written one at a
 * time, in the order they acquired the stream.
 */
public final class PacketStream implements AutoCloseable {
    /** ZMUX_PACKET_ERROR (EXTENSIONS.md Section 7.1): the bytes of a packet stream were malformed. */
    public static final long PACKET_ERROR = TransportError.PACKET_ERROR;
    /** The default {@link Options#maxPacket()}: 65535 payload bytes. */
    public static final int DEFAULT_MAX_PACKET = 65535;

    /** How a packet is sent. */
    public enum SendPolicy {
        /**
         * As a datagram when the stream's datagrams are negotiated and the packet fits
         * {@link ZmuxStream#maxDatagramPayload(long)}, evaluated at each send; otherwise as a capsule.
         * A datagram the limiter refuses is dropped, never sent as a capsule instead; a full
         * datagram queue drops its oldest datagram.
         */
        PREFER_DATAGRAM,
        /** Always as a capsule: ordered and reliable. */
        STREAM_ONLY,
        /** Always as a datagram: a packet that does not fit fails with {@link DatagramTooLargeException}. */
        DATAGRAM_ONLY
    }

    /**
     * Packet stream policy.
     *
     * @param policy how packets are sent
     * @param maxPacket the largest payload (context ID excluded) sent or accepted in a capsule;
     *     a larger received capsule is a packet error
     */
    public record Options(SendPolicy policy, int maxPacket) {
        public Options {
            Objects.requireNonNull(policy);
            if (maxPacket < 0 || maxPacket > Integer.MAX_VALUE - Capsule.MAX_DATAGRAM_HEADER) throw new IllegalArgumentException("maxPacket out of range");
        }

        /** {@link SendPolicy#PREFER_DATAGRAM} and {@link #DEFAULT_MAX_PACKET}. */
        public static Options defaults() { return new Options(SendPolicy.PREFER_DATAGRAM, DEFAULT_MAX_PACKET); }
    }

    /**
     * Counters of one packet stream.
     *
     * @param sentAsDatagrams packets queued as datagrams
     * @param sentAsCapsules packets written as capsules
     * @param droppedTooLarge {@link SendPolicy#DATAGRAM_ONLY} packets that did not fit
     * @param droppedLimited datagrams the session's datagram limiter refused
     * @param receivedAsDatagrams packets received as datagrams
     * @param receivedAsCapsules packets received as capsules
     * @param capsulesSkipped received capsules of other types
     */
    public record Stats(long sentAsDatagrams, long sentAsCapsules, long droppedTooLarge, long droppedLimited,
                        long receivedAsDatagrams, long receivedAsCapsules, long capsulesSkipped) {
        /** All dropped packets. */
        public long dropped() { return droppedTooLarge + droppedLimited; }
    }

    private static final int QUEUE = 64;
    private static final int READ = 64 << 10;
    private static final Object END = new Object();

    private final ZmuxStream stream;
    private final SendPolicy policy;
    private final int maxPacket;
    private final AtomicLong sentDatagrams = new AtomicLong(), sentCapsules = new AtomicLong(), tooLarge = new AtomicLong(),
            limited = new AtomicLong(), receivedDatagrams = new AtomicLong(), receivedCapsules = new AtomicLong(), skipped = new AtomicLong();

    // Receiving: guarded by lock.
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition readable = lock.newCondition(), writable = lock.newCondition();
    private final ArrayDeque<StreamDatagram> queue = new ArrayDeque<>();
    private final ArrayDeque<CompletableFuture<StreamDatagram>> receivers = new ArrayDeque<>();
    /** {@link #END}, or the IOException the capsules ended with; null while they run. */
    private Object terminal;
    /** The datagram reader has taken every datagram that arrived before the capsules ended (or stopped for good); the end is reported only after it. */
    private boolean datagramsDone;
    private boolean closed, started;
    private Thread capsuleReader, datagramReader;

    // Sending: one capsule writer at a time. Guarded by writers.
    private final ArrayDeque<Writer> writers = new ArrayDeque<>();
    private boolean writing;
    /** The blocking writers' header; used only by the holder of the writing turn. */
    private final ByteBuffer header = ByteBuffer.allocate(Capsule.MAX_DATAGRAM_HEADER);

    private PacketStream(ZmuxStream stream, Options options) {
        this.stream = Objects.requireNonNull(stream);
        this.policy = options.policy();
        this.maxPacket = options.maxPacket();
    }

    /** Wraps an open bidirectional stream, opened or accepted, with {@link Options#defaults()}. */
    public static PacketStream of(ZmuxStream stream) { return of(stream, Options.defaults()); }

    /** Wraps an open bidirectional stream, opened or accepted. */
    public static PacketStream of(ZmuxStream stream, Options options) { return new PacketStream(stream, Objects.requireNonNull(options)); }

    /** Opens a stream of session as a packet stream with {@link Options#defaults()}. */
    public static PacketStream open(ZmuxSession session) throws IOException { return open(session, Preamble.EMPTY, Options.defaults()); }

    /**
     * Opens a stream of session with preamble as a packet stream, and opens it at the peer at
     * once ({@link ZmuxStream#flush()}): its first packet may be a datagram, which needs the
     * stream to exist there.
     */
    public static PacketStream open(ZmuxSession session, Preamble preamble, Options options) throws IOException {
        Objects.requireNonNull(options);
        ZmuxStream stream = session.openStream(preamble);
        try {
            stream.flush();
        } catch (IOException | RuntimeException error) {
            stream.close();
            throw error;
        }
        return new PacketStream(stream, options);
    }

    // ---- sending ----

    /** {@link #send(long, ByteBuffer)} with context ID 0. */
    public boolean send(ByteBuffer payload) throws IOException { return send(0, payload); }

    /**
     * Sends the payload's remaining bytes as one packet with context, by the send policy. A
     * capsule waits for the stream's flow control (bounded by the stream's write timeout);
     * a datagram never waits. True when the packet was queued or written; false when the
     * session's datagram limiter dropped it. A timeout or interruption before the first byte of the
     * capsule was written fails the send and the packet is never sent later, so the caller may
     * retry it without a duplicate; one after a partial write resets the stream with
     * ZMUX_CANCELLED, as the capsules after it could not be framed.
     *
     * @throws IllegalArgumentException a payload above {@link Options#maxPacket()} or a context above 2<sup>62</sup>-1
     * @throws DatagramTooLargeException {@link SendPolicy#DATAGRAM_ONLY} and the packet does not fit
     * @throws NotNegotiatedException {@link SendPolicy#DATAGRAM_ONLY} without datagrams
     */
    public boolean send(long context, ByteBuffer payload) throws IOException {
        int n = checkSend(context, payload);
        int sent = sendDatagram(context, payload, n);
        if (sent != CAPSULE) return sent == SENT;
        CompletableFuture<Void> turn = acquire();
        try {
            writeCapsule(context, payload, n);
        } finally {
            release(turn);
        }
        sentCapsules.incrementAndGet();
        return true;
    }

    /**
     * {@link #send(long, ByteBuffer)} without blocking: completes once the packet is queued or
     * written, with the same result. Borrows payload until completion. Capsules are written in
     * the order of the calls that send them; cancelling the future cancels a capsule none of whose
     * bytes was written (it is never sent later), and one already partly written resets the
     * stream with ZMUX_CANCELLED.
     */
    public ZmuxFuture<Boolean> sendAsync(long context, ByteBuffer payload) {
        var outcome = new CompletableFuture<Boolean>();
        try {
            int n = checkSend(context, payload);
            int sent = sendDatagram(context, payload, n);
            if (sent != CAPSULE) outcome.complete(sent == SENT);
        } catch (IOException error) {
            outcome.completeExceptionally(error);
        }
        if (outcome.isDone()) return Spi.future(outcome, null);
        var writer = new AsyncWriter(context, payload, outcome);
        enqueue(writer);
        return Spi.future(outcome, writer::cancel);
    }

    private int checkSend(long context, ByteBuffer payload) throws IOException {
        Objects.requireNonNull(payload);
        if (context < 0 || context > VarInt.MAX) throw new IllegalArgumentException("context out of range");
        int n = payload.remaining();
        if (n > maxPacket) throw new IllegalArgumentException("packet above maxPacket (" + maxPacket + ")");
        if (isClosed()) throw new StreamClosedException(-1, "packet stream closed");
        return n;
    }

    private static final int SENT = 1, DROPPED = 0, CAPSULE = -1;

    /** The datagram path: {@link #SENT}, {@link #DROPPED} by the limiter, or {@link #CAPSULE} when the packet goes as a capsule. */
    private int sendDatagram(long context, ByteBuffer payload, int n) throws IOException {
        if (policy == SendPolicy.STREAM_ONLY) return CAPSULE;
        if (policy == SendPolicy.PREFER_DATAGRAM) {
            int max = stream.maxDatagramPayload(context);
            if (max <= 0 || n > max) return CAPSULE;
        }
        int position = payload.position();
        try {
            if (stream.sendDatagram(context, payload)) { sentDatagrams.incrementAndGet(); return SENT; }
            limited.incrementAndGet();
            return DROPPED;
        } catch (DatagramTooLargeException | NotNegotiatedException error) {
            payload.position(position);
            if (policy == SendPolicy.PREFER_DATAGRAM) return CAPSULE; // The limit changed since it was read.
            if (error instanceof DatagramTooLargeException) tooLarge.incrementAndGet();
            throw error;
        }
    }

    private void writeCapsule(long context, ByteBuffer payload, int n) throws IOException {
        header.clear();
        Capsule.putDatagramHeader(header, context, n);
        header.flip();
        boolean started = false;
        try {
            stream.write(header);
            started = true;
            stream.write(payload);
        } catch (InterruptedIOException error) {
            if (started || error.bytesTransferred > 0) stream.reset(TransportError.CANCELLED);
            throw error;
        }
    }

    /** A queued capsule writer. */
    private interface Writer {
        /** Starts with the writing turn; the stage completes, always normally, when the turn is released. */
        CompletionStage<?> start();
    }

    /**
     * Takes the writing turn, waiting for the writers before; returns the completion to signal
     * when the turn was handed over by a writer's release, or null when it was free.
     */
    private CompletableFuture<Void> acquire() throws IOException {
        var turn = new CompletableFuture<Void>();
        var done = new CompletableFuture<Void>();
        Writer waiter = () -> { turn.complete(null); return done; };
        synchronized (writers) {
            if (!writing) { writing = true; return null; }
            writers.add(waiter);
        }
        try {
            turn.get();
            return done;
        } catch (InterruptedException interrupted) {
            synchronized (writers) {
                if (!writers.remove(waiter)) done.complete(null); // Handed over meanwhile: pass it on.
            }
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted while waiting to send");
        } catch (ExecutionException error) {
            throw new IllegalStateException(error);
        }
    }

    /** Releases the turn {@link #acquire()} took: the queue runner waiting for done continues, or this thread runs it. */
    private void release(CompletableFuture<Void> done) {
        if (done != null) done.complete(null); else drain();
    }

    /** Queues a writer, or runs it when the turn is free. */
    private void enqueue(Writer writer) {
        synchronized (writers) {
            if (writing) { writers.add(writer); return; }
            writing = true;
        }
        CompletableFuture<?> stage = writer.start().toCompletableFuture();
        if (stage.isDone()) drain(); else stage.whenComplete((v, e) -> drain());
    }

    /** Runs the queued writers in turn, without recursion; called by the holder of the turn as it releases it. */
    private void drain() {
        while (true) {
            Writer next;
            synchronized (writers) {
                next = writers.poll();
                if (next == null) { writing = false; return; }
            }
            CompletableFuture<?> stage = next.start().toCompletableFuture();
            if (!stage.isDone()) { stage.whenComplete((v, e) -> drain()); return; }
        }
    }

    private final class AsyncWriter implements Writer {
        private final long context;
        private final ByteBuffer payload;
        private final CompletableFuture<Boolean> outcome;
        private volatile CompletableFuture<?> current;
        private volatile boolean cancelled;

        AsyncWriter(long context, ByteBuffer payload, CompletableFuture<Boolean> outcome) {
            this.context = context; this.payload = payload; this.outcome = outcome;
        }

        void cancel() {
            cancelled = true;
            CompletableFuture<?> c = current;
            if (c != null) c.cancel(true);
        }

        @Override public CompletionStage<?> start() {
            if (cancelled || outcome.isDone()) return CompletableFuture.completedFuture(null);
            int n = payload.remaining();
            ByteBuffer head = ByteBuffer.allocate(Capsule.datagramHeaderLength(context, n));
            Capsule.putDatagramHeader(head, context, n);
            head.flip();
            var done = new CompletableFuture<Void>();
            ZmuxFuture<Void> first;
            try { first = stream.writeAsync(head); }
            catch (RuntimeException error) { outcome.completeExceptionally(error); return CompletableFuture.completedFuture(null); }
            current = first;
            if (cancelled) first.cancel(true);
            first.whenComplete((v, e) -> {
                if (e != null) { fail(e, head.position() > 0); done.complete(null); return; }
                ZmuxFuture<Void> second;
                try { second = stream.writeAsync(payload); }
                catch (RuntimeException error) { fail(error, true); done.complete(null); return; }
                current = second;
                if (cancelled) second.cancel(true);
                second.whenComplete((v2, e2) -> {
                    if (e2 != null) fail(e2, true);
                    else { sentCapsules.incrementAndGet(); outcome.complete(true); }
                    done.complete(null);
                });
            });
            return done;
        }

        private void fail(Throwable error, boolean started) {
            if (started) stream.reset(TransportError.CANCELLED);
            if (error instanceof java.util.concurrent.CompletionException c && c.getCause() != null) error = c.getCause();
            outcome.completeExceptionally(error);
        }
    }

    // ---- receiving ----

    /** Waits for the next packet; null once the peer finished the stream at a capsule boundary. */
    public StreamDatagram receive() throws IOException { return receive(Long.MAX_VALUE); }

    /** Waits at most timeout for the next packet; {@link SocketTimeoutException} after it, the stream staying usable. */
    public StreamDatagram receive(Duration timeout) throws IOException {
        return receive(Math.clamp(timeout.toNanos(), 0, Long.MAX_VALUE - 1));
    }

    /** Waits at most nanos; Long.MAX_VALUE: no bound. */
    private StreamDatagram receive(long nanos) throws IOException {
        lock.lock();
        try {
            start();
            while (true) {
                if (closed) throw new StreamClosedException(-1, "packet stream closed");
                StreamDatagram next = queue.poll();
                if (next != null) { writable.signal(); return next; }
                Object end = terminal;
                if (end == END && datagramsDone) return null;
                if (end != null && end != END) throw (IOException) end;
                if (nanos == Long.MAX_VALUE) readable.await();
                else if (nanos <= 0) throw new SocketTimeoutException("packet receive timed out");
                else nanos = readable.awaitNanos(nanos);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted while receiving a packet");
        } finally {
            lock.unlock();
        }
    }

    /** {@link #receive()} without blocking; cancelling the future leaves the stream usable. */
    public ZmuxFuture<StreamDatagram> receiveAsync() {
        var waiter = new CompletableFuture<StreamDatagram>();
        lock.lock();
        try {
            start();
            StreamDatagram next = closed ? null : queue.poll();
            if (closed) waiter.completeExceptionally(new StreamClosedException(-1, "packet stream closed"));
            else if (next != null) { writable.signal(); waiter.complete(next); }
            else if (terminal == END) { if (datagramsDone) waiter.complete(null); else receivers.add(waiter); }
            else if (terminal != null) waiter.completeExceptionally((IOException) terminal);
            else receivers.add(waiter);
        } finally {
            lock.unlock();
        }
        return Spi.future(waiter, () -> {
            lock.lock();
            try { receivers.removeIf(w -> w == waiter); } finally { lock.unlock(); }
        });
    }

    /** Starts the readers; holds lock. */
    private void start() {
        if (started || closed) return;
        started = true;
        capsuleReader = Thread.ofVirtual().name("zmux-packets").start(this::readCapsules);
        datagramReader = Thread.ofVirtual().name("zmux-packet-datagrams").start(this::readDatagrams);
    }

    /** Queues a packet, waiting for room; false once the packet stream is closed. */
    private boolean deliver(StreamDatagram packet) throws InterruptedException {
        CompletableFuture<StreamDatagram> waiter;
        lock.lock();
        try {
            while (true) {
                if (closed) return false;
                waiter = receivers.poll();
                if (waiter != null) break;
                if (queue.size() < QUEUE) { queue.add(packet); readable.signal(); return true; }
                writable.await();
            }
        } finally {
            lock.unlock();
        }
        waiter.complete(packet);
        return true;
    }

    /** Ends the capsules with outcome ({@link #END} or an IOException) and stops the datagram reader. */
    private void end(Object outcome) {
        var waiting = new ArrayDeque<CompletableFuture<StreamDatagram>>();
        Thread datagrams;
        lock.lock();
        try {
            if (terminal != null || closed) return;
            terminal = outcome;
            readable.signalAll();
            // At the end, the datagram reader still hands over the datagrams that arrived before it and then reports it.
            if (outcome != END && queue.isEmpty()) { waiting.addAll(receivers); receivers.clear(); }
            datagrams = datagramReader;
        } finally {
            lock.unlock();
        }
        if (datagrams != null) datagrams.interrupt();
        for (var w : waiting) {
            w.completeExceptionally((IOException) outcome); // waiting is only filled for a failure
        }
    }

    /** Clears the interrupt that stopped the datagram reader; the capsules already decide how the stream ends. */
    private static void clearInterrupt() {
        // The call is the clearing; there is no state to read.
        //noinspection ResultOfMethodCallIgnored
        Thread.interrupted();
    }

    private void readDatagrams() {
        StreamDatagram held = null;
        try {
            while (true) {
                held = stream.receiveDatagram();
                receivedDatagrams.incrementAndGet();
                StreamDatagram d = held;
                if (!deliver(d)) return;
                held = null;
            }
        } catch (IOException | InterruptedException error) {
            // The capsules decide how the packet stream ends.
        }
        try {
            clearInterrupt();
            if (held != null) offer(held);
            boolean ended;
            lock.lock();
            try { ended = terminal == END && !closed; } finally { lock.unlock(); }
            // The stream ended at a capsule boundary: datagrams that arrived before the end are still packets.
            if (ended) {
                while (true) {
                    StreamDatagram d;
                    try { d = stream.receiveDatagram(Duration.ZERO); } catch (IOException none) { break; }
                    receivedDatagrams.incrementAndGet();
                    offer(d);
                }
            }
        } finally {
            datagramsDone();
        }
    }

    /** Queues a packet past the queue bound (the session's own queue bounds how many there are) or gives it to a waiting receiver. */
    private void offer(StreamDatagram packet) {
        CompletableFuture<StreamDatagram> waiter;
        lock.lock();
        try {
            if (closed) return;
            waiter = receivers.poll();
            if (waiter == null) { queue.add(packet); readable.signal(); return; }
        } finally {
            lock.unlock();
        }
        waiter.complete(packet);
    }

    /** The datagram reader is finished: a pending end is reported now. */
    private void datagramsDone() {
        var waiting = new ArrayDeque<CompletableFuture<StreamDatagram>>();
        lock.lock();
        try {
            datagramsDone = true;
            readable.signalAll();
            if (terminal == END && queue.isEmpty()) { waiting.addAll(receivers); receivers.clear(); }
        } finally {
            lock.unlock();
        }
        for (var w : waiting) w.complete(null);
    }

    private static final int HEADER = 0, CONTEXT = 1, PAYLOAD = 2, SKIP = 3;

    private void readCapsules() {
        var head = new Capsule.Header();
        var contextField = new Capsule.Field();
        int state = HEADER;
        long remaining = 0, context = 0;
        byte[] payload = null;
        int filled = 0;
        try {
            while (true) {
                Chunk chunk = stream.readChunk(READ);
                if (chunk == null) {
                    if (state != HEADER || !head.idle()) { malformed("stream ended inside a capsule"); return; }
                    end(END);
                    return;
                }
                try (chunk) {
                    ByteBuffer b = chunk.buffer();
                    while (b.hasRemaining()) {
                        switch (state) {
                            case HEADER -> {
                                if (!head.feed(b)) break;
                                remaining = head.length();
                                if (remaining > (long) maxPacket + 8) { malformed("capsule above the bound"); return; }
                                if (head.type() == Capsule.DATAGRAM) {
                                    if (remaining == 0) { malformed("DATAGRAM capsule without a context ID"); return; }
                                    state = CONTEXT;
                                } else {
                                    skipped.incrementAndGet();
                                    state = remaining == 0 ? HEADER : SKIP;
                                }
                            }
                            case CONTEXT -> {
                                int before = b.position();
                                boolean done = contextField.feed(b);
                                remaining -= b.position() - before;
                                if (remaining < 0 || (!done && remaining == 0)) { malformed("context ID past the capsule"); return; }
                                if (!done) break;
                                context = contextField.value();
                                if (remaining > maxPacket) { malformed("capsule above the bound"); return; }
                                payload = new byte[(int) remaining];
                                filled = 0;
                                if (remaining == 0) {
                                    state = HEADER;
                                    if (!packet(context, payload)) return;
                                } else state = PAYLOAD;
                            }
                            case PAYLOAD -> {
                                int n = (int) Math.min(remaining, b.remaining());
                                b.get(payload, filled, n);
                                filled += n; remaining -= n;
                                if (remaining == 0) {
                                    state = HEADER;
                                    if (!packet(context, payload)) return;
                                }
                            }
                            default -> {
                                int n = (int) Math.min(remaining, b.remaining());
                                b.position(b.position() + n);
                                remaining -= n;
                                if (remaining == 0) state = HEADER;
                            }
                        }
                    }
                }
            }
        } catch (InterruptedException interrupted) {
            // Closed.
        } catch (IOException error) {
            end(error);
        }
    }

    private boolean packet(long context, byte[] payload) throws InterruptedException {
        receivedCapsules.incrementAndGet();
        return deliver(new StreamDatagram(context, payload));
    }

    private void malformed(String why) {
        stream.stop(PACKET_ERROR);
        stream.reset(PACKET_ERROR);
        long id;
        try { id = stream.id(); } catch (IOException e) { id = -1; }
        end(new PacketErrorException(id, why));
    }

    // ---- both ----

    /** This packet stream's counters. */
    public Stats stats() {
        return new Stats(sentDatagrams.get(), sentCapsules.get(), tooLarge.get(), limited.get(),
                receivedDatagrams.get(), receivedCapsules.get(), skipped.get());
    }

    private boolean isClosed() {
        lock.lock();
        try { return closed; } finally { lock.unlock(); }
    }

    /**
     * Half-close: finishes the sending part (FIN) after the capsules already sent or queued, and
     * completes once the FIN is written. Packets can still be received until the peer finishes;
     * {@link #close()} then releases the stream.
     */
    public ZmuxFuture<Void> finishAsync() {
        var outcome = new CompletableFuture<Void>();
        enqueue(() -> {
            CompletableFuture<Void> f;
            try { f = stream.finishAsync(); }
            catch (RuntimeException error) { outcome.completeExceptionally(error); return CompletableFuture.completedFuture(null); }
            return f.handle((v, e) -> {
                if (e == null) outcome.complete(null);
                else outcome.completeExceptionally(e instanceof java.util.concurrent.CompletionException c && c.getCause() != null ? c.getCause() : e);
                return null;
            });
        });
        return Spi.future(outcome, null);
    }

    /**
     * Graceful: finishes the stream after the capsules already sent or queued and releases it;
     * returns without waiting. Received packets not yet taken are discarded and a receiving part
     * not read to its end is stopped with code 0. Idempotent.
     */
    @Override public void close() {
        if (!shut()) return;
        var finisher = new Writer() {
            @Override public CompletionStage<?> start() {
                CompletableFuture<Void> f;
                try { f = stream.finishAsync(); } catch (RuntimeException error) { f = CompletableFuture.completedFuture(null); }
                return f.handle((v, e) -> { stream.close(); return null; });
            }
        };
        enqueue(finisher);
    }

    /** At once: resets the sending part and stops the receiving part with code, discarding what is unsent. Idempotent. */
    public void abort(long code) {
        shut();
        stream.reset(code);
        stream.stop(code);
        stream.close();
    }

    /** Marks the packet stream closed and ends its readers; false when it already was. */
    private boolean shut() {
        Thread capsules, datagrams;
        ArrayDeque<CompletableFuture<StreamDatagram>> waiting;
        lock.lock();
        try {
            if (closed) return false;
            closed = true;
            queue.clear();
            waiting = new ArrayDeque<>(receivers); receivers.clear();
            readable.signalAll(); writable.signalAll();
            capsules = capsuleReader; datagrams = datagramReader;
        } finally {
            lock.unlock();
        }
        if (capsules != null) capsules.interrupt();
        if (datagrams != null) datagrams.interrupt();
        for (var w : waiting) w.completeExceptionally(new StreamClosedException(-1, "packet stream closed"));
        return true;
    }
}
