package io.zmux.internal.loop;

import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import io.zmux.carrier.OutboundBatch;
import io.zmux.internal.core.ReadBuf;
import io.zmux.carrier.VarInt;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A transport over a pipe input whose reader and writer run on other threads. Their
 * callbacks reach the loop through one ordered inbox, which also carries callback ownership
 * along when a listener routes the transport to another loop.
 *
 * <p>Input: a session parses lent chunks in place ({@link PipeReader.Sink#onChunk}); only a
 * record cut by the chunk's end is carried, and the next chunk first completes that record
 * and is then parsed in place again. A lent byte-stream batch ({@link PipeReader.Sink#onData})
 * goes through the loop's read buffer; a lent message is parsed in place and the session
 * copies what it keeps. Output: the writer gets the session's batch itself.
 */
final class ExternalTransport extends AbstractTransport {
    private final PipeIo pio;
    private final boolean messages;
    private final TransportInfo info;
    private final Inbox inbox;
    private boolean inherited;
    private volatile boolean ready;
    private boolean inEnded;
    private boolean writerDone;
    private boolean readerClosed;
    private boolean shutAfterWrite;
    private volatile boolean closed;

    ExternalTransport(SessionHost loop, PipeIo pipeIo) { this(loop, pipeIo, new Inbox()); }

    private ExternalTransport(SessionHost loop, PipeIo pipeIo, Inbox inbox) {
        super(loop);
        pio = pipeIo; this.inbox = inbox;
        messages = pio.framing() == io.zmux.carrier.Framing.MESSAGES;
        info = TransportInfo.of(pio);
        loop.externalTransport();
        inbox.owner = this;
        if (inbox.limit == 0) inbox.limit = admittedLimit();
    }

    /**
     * What an admitted reader may queue ahead of the loop: the receive memory it declared,
     * which is charged to the memory budget, plus one reader batch. Beyond it the reader
     * ignored the release backpressure of {@link PipeReader.Sink#onData}.
     */
    private long admittedLimit() {
        long declared = pio.capabilities().receiveMemory();
        return declared > Long.MAX_VALUE - BufferPool.SIZE ? Long.MAX_VALUE : declared + BufferPool.SIZE;
    }

    @Override public java.util.concurrent.CompletionStage<Void> whenClosed() { return inbox.closed.minimalCompletionStage(); }
    @Override public java.util.concurrent.CompletionStage<Boolean> whenTaken() { return inbox.taken.stage(); }
    @Override public void taken() { inbox.taken.decide(true); }

    @Override public TransportInfo info() { return info; }
    @Override public boolean closed() { return closed; }
    @Override public java.util.concurrent.CompletionStage<PipeIo> openLane() {
        var opener = pio.capabilities().laneOpener();
        return opener == null ? null : opener.open();
    }
    @Override public byte[] export(String label, byte[] context, int length) {
        var exporter = pio.capabilities().exporter();
        return exporter == null ? null : exporter.export(label, context, length);
    }
    @Override public int directions() {
        return ((pio.reader() == null ? 0 : io.zmux.internal.core.Action.DIR_IN)
                | (pio.writer() == null ? 0 : io.zmux.internal.core.Action.DIR_OUT)) & mask();
    }
    @Override public boolean ownsHalf(Object half) { return half != null && (half == pio.reader() || half == pio.writer()); }

    /** One pending first record plus the existing bounded carrier read batch, including queue metadata. */
    void boundPendingInput(long recordBytes) { inbox.strict = true; inbox.limit = recordBytes + BufferPool.SIZE; }
    /** Admission done: the reader's declared receive memory bounds its queue from now on. */
    void unboundPendingInput() { inbox.limit = admittedLimit(); inbox.strict = false; }

    @Override public boolean writable() { return super.writable() && (directions() & io.zmux.internal.core.Action.DIR_OUT) != 0; }

    private record Input(ByteBuffer data, Runnable release, OwnedInput owned, boolean end, Throwable error, long cost) {
        Input(ByteBuffer data, Runnable release, OwnedInput owned, boolean end, Throwable error) {
            this(data, release, owned, end, error, 0);
        }
        void discard() { if (owned != null) owned.release(); if (release != null) release.run(); }
    }

    /** One drain owns the queue even across loop migration, so new callbacks cannot overtake old ones. */
    private static final class Inbox {
        final java.util.concurrent.CompletableFuture<Void> closed = new java.util.concurrent.CompletableFuture<>();
        /** Whether a session took the carrier before it closed. */
        final TakenSignal taken = new TakenSignal();
        final ConcurrentLinkedDeque<Input> queue = new ConcurrentLinkedDeque<>();
        final AtomicBoolean scheduled = new AtomicBoolean();
        volatile ExternalTransport owner;
        /** Queue bound in bytes (set by the first owner); strict while a first record is pending. */
        volatile long limit;
        volatile boolean strict;
        final java.util.concurrent.atomic.AtomicLong held = new java.util.concurrent.atomic.AtomicLong();
        final AtomicBoolean overflow = new AtomicBoolean();

        void offer(Input input) {
            long bytes = (input.data == null ? 0 : input.data.remaining()) + 128L; // Payload plus queue metadata.
            long total = held.addAndGet(bytes);
            // After admission a batch is always taken while nothing else is queued: a reader
            // that waits for its releases never fails, whatever its batch size.
            if (total < 0 || total > limit && (strict || total != bytes)) {
                held.addAndGet(-bytes); input.discard();
                if (overflow.compareAndSet(false, true)) failOwner();
                return;
            }
            queue.add(new Input(input.data, input.release, input.owned, input.end, input.error, bytes)); schedule();
        }
        void failOwner() {
            ExternalTransport target = owner;
            target.loop.tryExecute(() -> {
                ExternalTransport current = owner;
                if (!current.loop.inLoop()) { failOwner(); return; }
                if (current.closed) return;
                current.inputEnded(false, true);
                current.close(new java.io.IOException("pending carrier input budget exceeded"));
            });
        }
        void schedule() {
            ExternalTransport c = owner;
            if (c.closed) { discard(); return; }
            if (!c.ready || !scheduled.compareAndSet(false, true)) return;
            if (!c.loop.tryExecute(this::drain)) { scheduled.set(false); discard(); }
        }
        void discard() {
            for (Input in; (in = queue.poll()) != null;) { if (in.cost() != 0) held.addAndGet(-in.cost()); in.discard(); }
        }
        void drain() {
            while (true) {
                ExternalTransport c = owner;
                if (!c.loop.inLoop()) {
                    if (!c.loop.tryExecute(this::drain)) { scheduled.set(false); discard(); }
                    return;
                }
                if (!c.ready) { scheduled.set(false); return; }
                Input input = queue.poll();
                if (input == null) {
                    scheduled.set(false);
                    if (!queue.isEmpty()) schedule();
                    return;
                }
                boolean transferred = false;
                long cost = input.cost();
                try {
                    if (!c.closed) {
                        if (input.end) c.ended(input.error);
                        else if (input.data != null) c.received(input.data, input.owned == null ? null : input.owned.buf);
                    }
                    if (input.data != null && (!c.ready || input.data.hasRemaining() && owner != c)) {
                        Input rest = input.data.hasRemaining() ? input : new Input(null, input.release, input.owned, false, null, cost);
                        queue.addFirst(rest); transferred = true;
                    }
                } finally {
                    if (!transferred) { if (cost != 0) held.addAndGet(-cost); input.discard(); }
                }
            }
        }
    }

    @Override protected void begin() {
        openReader();
        ready = true;
        if (!replay(reader)) return;
        if (!inherited && !readerClosed && pio.reader() != null) pio.reader().start(new PipeReader.Sink() {
            @Override public void onData(ByteBuffer data, Runnable release) {
                inbox.offer(new Input(data, release, null, false, null));
            }
            @Override public void onChunk(io.zmux.Chunk chunk, Runnable consumed) {
                ByteBuffer bytes = chunk.buffer();
                inbox.offer(new Input(bytes, consumed, new OwnedInput(loop, bytes, chunk::close), false, null));
            }
            @Override public void onEnd() { inbox.offer(new Input(null, null, null, true, null)); }
            @Override public void onError(Throwable cause) {
                inbox.offer(new Input(null, null, null, true, cause == null ? new java.io.IOException("carrier failed") : cause));
            }
        });
        inbox.schedule();
    }

    /** One input batch: owned (a chunk the session may keep, retained) or lent (retained null). */
    private void received(ByteBuffer data, ReadBuf retained) {
        if (closed || inEnded) return;
        Inbound in = inbound();
        if (!messages) {
            input(data, retained);
            return;
        }
        if (in.session()) {
            // Lent or owned, parsed where it is: the session copies what it keeps of a lent one.
            in.message(this, retained, data, data.position(), data.limit());
            loop.inPlaceInput(data.remaining());
            data.position(data.limit());
            return;
        }
        // A message before admission is classified as a length-prefixed record; the
        // prefix goes into the read buffer ahead of the payload, without a second copy.
        header.clear(); VarInt.put(header, data.remaining()); header.flip();
        if (!feed(header)) return;
        feed(data);
    }

    private final ByteBuffer header = ByteBuffer.allocate(8);

    @Override protected boolean accepting() { return ready && !inEnded && !closed; }

    @Override protected void inputFailed(java.io.IOException cause) {
        ended(cause);
        close(cause);
    }

    /** The input ended: gracefully without an error. */
    private void ended(Throwable error) {
        if (closed || inEnded) return;
        inEnded = true;
        boolean partial = reader.hasPartial(); reader.release();
        if (error == null) inputEnded(true, partial);
        else inputEnded(partial, error);
    }

    /** Holds the first record and its current delivery until split-bootstrap pairing completes. */
    @Override public void pauseInput() { ready = false; }

    @Override public void resumeInput() {
        if (ready || closed) return;
        ready = true;
        if (!inEnded && !redeliver(reader)) return;
        inbox.schedule();
    }

    @Override public void redeliver() { if (!inEnded && !closed) redeliver(reader); }

    /** Transfers an unstarted sending half to this paused input's loop and inbox. */
    ExternalTransport pairWith(ExternalTransport outgoing) {
        if (closed || writing() || outgoing.closed || outgoing.writing() || attached() || outgoing.attached()
                || pio.reader() == null || pio.writer() != null || outgoing.pio.reader() != null || outgoing.pio.writer() == null
                || pio.framing() != outgoing.pio.framing()
                || !pio.capabilities().applicationProtocol().equals(outgoing.pio.capabilities().applicationProtocol())) return null;
        PipeIo combined;
        try { combined = PipeIo.split(pio, outgoing.pio); }
        catch (IllegalArgumentException incompatible) { return null; }
        ExternalTransport next = new ExternalTransport(loop, combined, inbox);
        next.inherited = reader != null; next.prefix(unread(reader));
        if (reader != null) reader.release();
        closed = true; outgoing.closed = true;
        next.whenClosed().whenComplete((value, error) -> outgoing.inbox.closed.complete(null));
        next.whenTaken().whenComplete((value, error) -> outgoing.inbox.taken.decide(Boolean.TRUE.equals(value)));
        return next;
    }

    /** Whether a session owns this carrier. */
    private boolean attached() { return inbound() != null && inbound().session(); }

    @Override public Transport moveTo(SessionHost to) {
        if (closed || writing() || attached()) { close(null); return null; }
        byte[] prefix = unread(reader);
        ExternalTransport next = new ExternalTransport(to, pio, inbox);
        next.inherited = reader != null;
        next.prefix(prefix);
        if (reader != null) reader.release(); closed = true;
        return next;
    }

    @Override public long unsent() {
        io.zmux.carrier.PipeWriter w = pio.writer();
        if (w == null || closed) return -1;
        try {
            java.util.OptionalLong n = w.unsent();
            return n.isPresent() ? Math.max(0, n.getAsLong()) : -1;
        } catch (RuntimeException e) {
            return -1; // A failing query reports no count; the write path reports the carrier's failure.
        }
    }

    @Override public long retransmitted() {
        io.zmux.carrier.PipeWriter w = pio.writer();
        if (w == null || closed) return -1;
        try {
            java.util.OptionalLong n = w.retransmitted();
            return n.isPresent() ? Math.max(0, n.getAsLong()) : -1;
        } catch (RuntimeException e) {
            return -1;
        }
    }

    @Override protected void send(OutboundBatch batch) {
        AtomicBoolean reported = new AtomicBoolean();
        PipeWriter.Completion completion = new PipeWriter.Completion() {
            private void report(Throwable cause) {
                if (!reported.compareAndSet(false, true)) return;
                // On the loop (also from inside write) the outcome is taken at once, as a
                // socket's is; the writer may still reference the batch until it reports, so it
                // is released only then, also after the owning loop has stopped.
                if (loop.inLoop()) done(cause);
                else if (!loop.tryExecute(() -> done(cause))) dropWrite(); // The loop stopped: nothing else will.
            }
            @Override public void completed() { report(null); }
            @Override public void failed(Throwable cause) {
                report(cause == null ? new java.io.IOException("write failed") : cause);
            }
        };
        try { pio.writer().write(batch, completion); }
        catch (RuntimeException failure) { completion.failed(failure); }
    }

    private void done(Throwable cause) {
        if (!writing()) return;
        if (closed) { dropWrite(); return; }
        writeDone(cause);
        if (shutAfterWrite) shutdownOutput();
    }
    @Override public void shutdownOutput() {
        if (outputShut || closed) return;
        if (writing()) { shutAfterWrite = true; return; }
        outputShut = true; closeWriter(null);
    }
    private void closeWriter(Throwable cause) {
        if (writerDone || pio.writer() == null) return;
        writerDone = true;
        try { if (cause != null) pio.writer().abort(cause); else pio.writer().close(); }
        catch (RuntimeException ignored) { /* Already closing. */ }
    }
    @Override public void shutdownInput() {
        if (readerClosed) return;
        inEnded = true;
        if (pio.reader() != null) {
            readerClosed = true;
            try { pio.reader().close(); } catch (RuntimeException ignored) { /* Already closing. */ }
        }
        inbox.discard();
        if (reader != null) reader.release();
    }
    @Override public void close(Throwable cause) {
        if (closed) return;
        closed = true;
        inbox.taken.decide(false); // Not taken before: the listener refused it.
        shutdownInput(); closeWriter(cause);
        Callbacks.EXECUTOR.execute(() -> inbox.closed.complete(null));
        // An in-flight batch belongs to its completion, even after the carrier closes.
    }
}
