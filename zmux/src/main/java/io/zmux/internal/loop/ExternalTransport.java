package io.zmux.internal.loop;

import io.zmux.carrier.CarrierChannel;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import io.zmux.carrier.OutboundBatch;
import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.wire.VarInt;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A transport over a pipe input whose reader and writer run on other threads. Their
 * callbacks reach the loop through one ordered inbox, which also carries callback ownership
 * along when a listener routes the transport to another loop.
 *
 * <p>Input: a session parses owned chunks in place ({@link PipeReader.Sink#onChunk}); only a
 * record cut by the chunk's end is carried, and the next chunk first completes that record
 * and is then parsed in place again. Output: the writer gets the session's batch itself.
 */
final class ExternalTransport extends AbstractTransport {
    private final PipeIo pio;
    private final boolean messages;
    private final TransportInfo info;
    private final CarrierChannel channel;
    private final Inbox inbox;
    private boolean inherited;
    private volatile boolean ready;
    private boolean inEnded;
    private final WriterClose writerClose = new WriterClose();
    private boolean readerClosed;
    private boolean shutAfterWrite;
    private volatile boolean closed;

    ExternalTransport(SessionHost loop, PipeIo pipeIo) { this(loop, pipeIo, new Inbox()); }

    private ExternalTransport(SessionHost loop, PipeIo pipeIo, Inbox inbox) {
        super(loop);
        pio = pipeIo; this.inbox = inbox;
        messages = pio.framing() == io.zmux.carrier.Framing.MESSAGES;
        var raw = pio.capabilities().datagramChannel();
        channel = raw == null ? null : new ChannelBridge(raw);
        info = TransportInfo.of(pio).withChannel(channel);
        loop.externalTransport();
        inbox.owner = this;
        if (inbox.limit == 0) inbox.limit = admittedLimit();
    }

    /**
     * What an admitted reader may queue ahead of the loop: the receive memory it declared,
     * which is charged to the memory budget, plus one reader batch. Beyond it the reader
     * ignored the consumed backpressure of {@link PipeReader.Sink#onChunk} and {@link PipeReader.Sink#onLent}.
     */
    @Override public long inputQueueCapacity() { return admittedLimit(); }

    @Override java.util.List<TlsTransport.RecordWorkspace> tlsWorkspaces() {
        return Forwarded.tlsWorkspaces(pio);
    }

    private long admittedLimit() {
        long declared = pio.capabilities().receiveMemory();
        return declared > Long.MAX_VALUE - InputCharge.BATCH ? Long.MAX_VALUE : declared + InputCharge.BATCH;
    }

    @Override public java.util.concurrent.CompletionStage<Void> whenClosed() { return inbox.closed.minimalCompletionStage(); }
    @Override public java.util.concurrent.CompletionStage<Boolean> whenTaken() { return inbox.taken.stage(); }
    @Override public void taken() { inbox.taken.decide(true); }

    @Override public TransportInfo info() { return info; }
    @Override public io.zmux.carrier.CarrierHalfSource receiveSource() { return pio.capabilities().receiveSource(); }
    @Override public io.zmux.carrier.CarrierHalfSource sendSource() { return pio.capabilities().sendSource(); }

    @Override public CarrierChannel channel() { return channel; }

    private final class ChannelBridge implements CarrierChannel {
        private final CarrierChannel lower;
        private final java.util.concurrent.atomic.AtomicLong queued = new java.util.concurrent.atomic.AtomicLong();

        ChannelBridge(CarrierChannel lower) { this.lower = lower; }
        @Override public long channelStreamId() { return lower.channelStreamId(); }
        @Override public int maxChannelDatagram() {
            return !closed && (directions() & io.zmux.internal.core.Action.DIR_OUT) != 0 ? lower.maxChannelDatagram() : 0;
        }

        @Override public int channelDatagramLimit(int room) {
            return !closed && (directions() & io.zmux.internal.core.Action.DIR_OUT) != 0 ? lower.channelDatagramLimit(room) : 0;
        }

        @Override public void send(byte[] bytes, long expiry, java.util.function.Consumer<Outcome> completion) {
            lower.send(bytes, expiry, completion);
        }

        @Override public Runnable receive(Sink sink) {
            return lower.receive((bytes, from, to) -> {
                if (loop.inLoop()) {
                    sink.datagram(bytes, from, to);
                    return;
                }
                int length = to - from;
                int charge = Math.max(length, 1);
                long held = queued.addAndGet(charge);
                if (held > admittedLimit()) {
                    queued.addAndGet(-charge);
                    return;
                }
                byte[] copy = new byte[length];
                bytes.get(from, copy);
                if (!loop.tryExecute(() -> {
                    queued.addAndGet(-charge);
                    sink.datagram(ByteBuffer.wrap(copy), 0, copy.length);
                })) queued.addAndGet(-charge);
            });
        }

        @Override public Runnable watch(Runnable changed) {
            return lower.watch(() -> onOwner(changed));
        }

        private void onOwner(Runnable event) {
            if (loop.inLoop()) event.run();
            else loop.tryExecute(event);
        }
    }

    @Override public java.util.List<Object> physicalConnections() {
        var connections = PipeIoAccess.get().physicalConnections(pio);
        return connections.isEmpty() ? java.util.List.of(this) : connections;
    }

    @Override public Object carrierIdentity() {
        return pio.writer() instanceof io.zmux.internal.spi.CarrierProvider provider
                ? provider.carrierIdentity() : PipeIoAccess.get().sendingConnection(pio);
    }

    @Override public Runnable watchCarrier(Runnable changed) {
        return pio.writer() instanceof io.zmux.internal.spi.CarrierProvider provider ? provider.watchCarrier(changed) : null;
    }

    @Override public Query carrierQuery(Object identity) {
        return pio.writer() instanceof io.zmux.internal.spi.CarrierProvider provider ? provider.carrierQuery(identity) : null;
    }
    @Override public boolean isClosed() { return closed; }
    @Override public java.util.concurrent.CompletionStage<PipeIo> openLane() {
        var opener = pio.capabilities().laneOpener();
        if (opener == null) return null;
        var opening = opener.open();
        return opening == null ? null : opening.thenApply(w -> w == null ? null
                : new PipeIo(null, w, pio.framing(), pio.label(), pio.carrierConn()).withCarrier(pio.carrier()).withVariant(pio.variant()));
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
    void boundPendingInput(long recordBytes) { inbox.strict = true; inbox.limit = recordBytes + InputCharge.BATCH; }
    /** Admission done: the reader's declared receive memory bounds its queue from now on. */
    void unboundPendingInput() { inbox.limit = admittedLimit(); inbox.strict = false; }

    @Override public boolean writable() { return super.writable() && (directions() & io.zmux.internal.core.Action.DIR_OUT) != 0; }

    private record Input(ByteBuffer data, Runnable release, ReadBuf owned, boolean end, Throwable error) {
        void consumed() { if (release != null) release.run(); }
    }

    /** One drain owns the queue even across loop migration, so new callbacks cannot overtake old ones. */
    private static final class Inbox {
        final java.util.concurrent.CompletableFuture<Void> closed = new java.util.concurrent.CompletableFuture<>();
        /** Whether a session took the carrier before it closed. */
        final TakenSignal taken = new TakenSignal();
        final java.util.concurrent.CompletableFuture<Void> writesReleased = new java.util.concurrent.CompletableFuture<>();
        final ConcurrentLinkedDeque<Input> queue = new ConcurrentLinkedDeque<>();
        final AtomicBoolean scheduled = new AtomicBoolean();
        volatile ExternalTransport owner;
        /** Queue bound in bytes (set by the first owner); strict while a first record is pending. */
        volatile long limit;
        volatile boolean strict;
        private final InputCharge charge = new InputCharge();
        private boolean terminal;
        private boolean stopped;
        final java.util.concurrent.CompletableFuture<Void> inputReleased = new java.util.concurrent.CompletableFuture<>();
        final AtomicBoolean overflow = new AtomicBoolean();

        void offer(Input input) {
            if (input.end) {
                synchronized (this) {
                    if (terminal || stopped) return;
                    terminal = true;
                }
                enqueue(input);
                return;
            }
            if (!reserve(input.owned, 0)) {
                overflow(() -> {
                    try { input.owned.unref(); } finally { input.consumed(); }
                });
                return;
            }
            enqueue(input);
        }

        void lend(ByteBuffer data, Runnable consumed) {
            int bytes = data.remaining();
            if (!reserve(null, bytes)) {
                overflow(consumed);
                return;
            }
            ReadBuf copy;
            try {
                copy = new ReadBuf(ByteBuffer.allocate(bytes).put(data).flip(), null);
            } catch (RuntimeException | Error failure) {
                unreserve(null, bytes);
                if (consumed != null) consumed.run();
                throw failure;
            }
            synchronized (this) { charge.adoptCopy(copy); }
            enqueue(new Input(copy.data(), consumed, copy, false, null));
        }

        private synchronized boolean reserve(ReadBuf owner, int copyBytes) {
            return !stopped && !terminal && !overflow.get() && charge.reserve(owner, copyBytes, limit);
        }

        private void enqueue(Input input) {
            // The terminal signal stays in order behind any input still owned by the inbox.
            queue.add(input);
            schedule();
        }

        private void overflow(Runnable release) {
            boolean first = overflow.compareAndSet(false, true);
            try {
                if (release != null) release.run();
            } finally {
                if (first) failOwner();
            }
        }

        private void release(Input input) {
            try {
                if (input.owned != null) {
                    try { input.owned.unref(); } finally { unreserve(input.owned, 0); }
                }
            } finally {
                input.consumed();
            }
        }

        private void unreserve(ReadBuf owner, int copyBytes) {
            boolean drained;
            synchronized (this) {
                charge.release(owner, copyBytes);
                drained = stopped && charge.bytes() == 0;
            }
            if (drained) inputReleased.complete(null);
        }

        void stop() {
            boolean drained;
            synchronized (this) {
                stopped = true;
                drained = charge.bytes() == 0;
            }
            discard();
            if (drained) inputReleased.complete(null);
        }
        void failOwner() {
            ExternalTransport target = owner;
            target.loop.tryExecute(() -> {
                ExternalTransport current = owner;
                if (!current.loop.inLoop()) { failOwner(); return; }
                if (current.closed) return;
                current.inputEnded(false, true);
                CarrierSetup.connectionLost(current, new java.io.IOException("pending carrier input budget exceeded"));
            });
        }
        void schedule() {
            ExternalTransport c = owner;
            if (c.closed) { discard(); return; }
            if (!c.ready || !scheduled.compareAndSet(false, true)) return;
            if (!c.loop.tryExecute(this::drain)) { scheduled.set(false); discard(); }
        }
        void discard() {
            for (Input in; (in = queue.poll()) != null;) release(in);
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
                try {
                    if (!c.closed) {
                        if (input.end) c.ended(input.error);
                        else if (input.data != null) c.received(input.data, input.owned);
                    }
                    // After admission what was taken goes back to the reader even in a pause: a carrier whose write reports
                    // when its peer read it would otherwise wait on this side's pause, while this side waits on that write.
                    boolean keep = input.data != null && (!c.closed || owner != c) && (input.data.hasRemaining() ? !c.ready || owner != c : !c.ready && strict);
                    if (keep) {
                        Input rest = input.data.hasRemaining() ? input : new Input(null, input.release, input.owned, false, null);
                        queue.addFirst(rest); transferred = true;
                    }
                } finally {
                    if (!transferred) release(input);
                }
            }
        }
    }

    @Override protected void begin() {
        openReader();
        ready = true;
        if (!redeliverBuffered()) return;
        if (!inherited && !readerClosed && pio.reader() != null) startReader();
        inbox.schedule();
    }

    private void startReader() {
        var sink = new PipeReader.Sink() {
            @Override public void onChunk(io.zmux.Chunk chunk, Runnable consumed) {
                var owned = ChunkAccess.take(chunk);
                inbox.offer(new Input(owned.data(), consumed, owned.buf(), false, null));
            }
            @Override public void onLent(ByteBuffer data, Runnable consumed) { inbox.lend(data, consumed); }
            @Override public void onEnd() { inbox.offer(new Input(null, null, null, true, null)); }
            @Override public void onError(Throwable cause) {
                inbox.offer(new Input(null, null, null, true, cause == null ? new java.io.IOException("carrier failed") : cause));
            }
        };
        try { setup().host(() -> { pio.reader().start(sink); return null; }); }
        catch (RuntimeException failure) { sink.onError(failure); }
    }

    /** A batch owned by the inbox until dispatch finishes. */
    private void received(ByteBuffer data, ReadBuf retained) {
        if (closed || inEnded) return;
        Inbound in = inbound();
        if (!messages) {
            input(data, retained);
            return;
        }
        if (in.session()) {
            // The inbox holds the owner throughout dispatch.
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

    @Override public boolean exporterPresent() { return pio.capabilities().exporter() != null; }

    @Override public boolean isSecure() { return pio.isCarrierSecure(); }

    @Override public io.zmux.RecordMode firstPipeMode() { return PipeIoAccess.get().recordMode(pio); }

    @Override public Object carrierPeer() { return PipeIoAccess.get().carrierPeer(pio); }
    @Override public io.zmux.carrier.HandshakeCost handshakeCost() { return pio.capabilities().handshakeCost(); }

    @Override public void limitFirstMessage(int limit) {
        if (messages && pio.reader() != null) pio.reader().limitFirstMessage(limit);
    }

    @Override protected boolean accepting() { return ready && !inEnded && !closed; }

    @Override protected void inputFailed(java.io.IOException cause) {
        ended(cause);
        CarrierSetup.connectionLost(this, cause);
    }

    /** The input ended: gracefully without an error. */
    private void ended(Throwable error) {
        if (closed || inEnded) return;
        inEnded = true;
        boolean partial = dropPartialInput();
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
        next.inherited = moveInputTo(next);
        closed = true; outgoing.closed = true;
        next.whenClosed().whenComplete((value, error) -> outgoing.inbox.closed.complete(null));
        next.writesReleased().whenComplete((value, error) -> outgoing.inbox.writesReleased.complete(null));
        next.whenTaken().whenComplete((value, error) -> outgoing.inbox.taken.decide(Boolean.TRUE.equals(value)));
        return next;
    }

    /** Whether a session owns this carrier. */
    private boolean attached() { return inbound() != null && inbound().session(); }

    @Override Transport movePhysical(CarrierSetup.Bound.Permit permit, SessionHost to) {
        java.util.Objects.requireNonNull(permit);
        if (closed || writing() || attached()) { CarrierSetup.connectionLost(this, null); return null; }
        ExternalTransport next = new ExternalTransport(to, pio, inbox);
        next.inherited = moveInputTo(next);
        closed = true;
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

    @Override protected void send(OutboundBatch batch) {
        AtomicBoolean reported = new AtomicBoolean();
        PipeWriter.Completion completion = new PipeWriter.Completion() {
            private final java.util.concurrent.atomic.AtomicInteger prefix = new java.util.concurrent.atomic.AtomicInteger();
            private final AtomicBoolean progressQueued = new AtomicBoolean();
            private boolean finishedOnLoop;

            @Override public void progress(int bytes) {
                if (reported.get() || bytes <= 0 || bytes > batch.bytes()) return;
                int before = prefix.getAndAccumulate(bytes, Math::max);
                if (bytes <= before || !progressQueued.compareAndSet(false, true)) return;
                Runnable update = () -> {
                    progressQueued.set(false);
                    if (!finishedOnLoop) writeProgress(prefix.get());
                };
                if (loop.inLoop()) update.run();
                else loop.tryExecute(update);
            }

            private void report(int confirmed, Throwable cause, boolean unknown) {
                if (!reported.compareAndSet(false, true)) return;
                // On the loop (also from inside write) the outcome is taken at once, as a
                // socket's is; the writer may still reference the batch until it reports, so it
                // is released only then, also after the owning loop has stopped.
                Runnable finish = () -> {
                    finishedOnLoop = true;
                    writeProgress(prefix.get());
                    if (unknown && batch instanceof io.zmux.internal.core.Batch b) b.markProgressUnknown();
                    done(confirmed, cause);
                };
                if (loop.inLoop()) finish.run();
                else if (!loop.tryExecute(finish)) {
                    loop.releaseWriteWhenStopped(() -> {
                        int proved = prefix.get();
                        boolean valid = confirmed >= 0 && confirmed <= batch.bytes();
                        if ((unknown || !valid) && batch instanceof io.zmux.internal.core.Batch b) b.markProgressUnknown();
                        releaseFinalWrite(valid ? Math.max(proved, confirmed) : proved);
                    });
                }
            }
            @Override public void completed() { report(batch.bytes(), null, false); }
            @Override public void failed(Throwable cause) {
                failed(cause, 0);
            }
            @Override public void failedUnknown(Throwable cause, int proved) {
                report(proved, cause == null ? new java.io.IOException("unknown write progress") : cause, true);
            }
            @Override public void failed(Throwable cause, int confirmed) {
                report(confirmed, cause == null ? new java.io.IOException("write failed") : cause, false);
            }
        };
        try { pio.writer().write(batch, completion); }
        catch (RuntimeException failure) { completion.failedUnknown(failure, 0); }
    }

    private void releaseFinalWrite(int confirmed) {
        try {
            dropWrite(confirmed);
        } finally {
            inbox.writesReleased.complete(null);
        }
    }

    private void done(int confirmed, Throwable cause) {
        if (!writing()) return;
        writeDone(confirmed, cause);
        if (closed) { inbox.writesReleased.complete(null); return; }
        if (shutAfterWrite) shutdownOutput();
    }
    @Override public void shutdownOutput() {
        if (outputShut || closed) return;
        if (writing()) { shutAfterWrite = true; return; }
        outputShut = true; closeWriter(null);
    }
    /** A half's close is the host's code: it never runs on the loop, where it could hold up every session there. */
    private void closeWriter(Throwable cause) {
        if (pio.writer() == null) return;
        writerClose.start(cause);
    }

    /** The writer can keep flushing after its graceful close call returns. */
    private final class WriterClose {
        private Thread graceful;
        private java.util.concurrent.CompletableFuture<Void> flushBound = DONE;

        private boolean started, aborted;
        private final java.util.concurrent.CompletableFuture<Void> finished = new java.util.concurrent.CompletableFuture<>();
        private java.util.concurrent.CompletableFuture<Void> escalation = DONE;

        /** A public writer cannot acknowledge that a flush outliving close() ended. */
        synchronized java.util.function.Consumer<Throwable> arm() {
            if (pio.writer() == null) return null;
            if (!(pio.writer() instanceof io.zmux.internal.spi.AdapterPorts.ReleasingHalf))
                flushBound = new java.util.concurrent.CompletableFuture<>();
            return this::escalate;
        }

        synchronized void start(Throwable cause) {
            if (started) {
                if (cause != null) escalate(cause);
                return;
            }
            started = true;
            aborted = cause != null;
            var writer = pio.writer();
            Thread thread = Thread.ofVirtual().name("zmux-close-writer").unstarted(() -> {
                try {
                    if (cause == null) writer.close(); else writer.abort(cause);
                } catch (RuntimeException ignored) {
                    // The release notification still owns any asynchronous flush.
                } finally {
                    synchronized (this) { graceful = null; }
                    if (cause == null) loop.tryExecute(() -> { if (!closed) outputShutDone(); });
                    try { io.zmux.internal.spi.AdapterPorts.releasedHalf(writer).whenComplete((v, e) -> returned()); }
                    catch (RuntimeException failed) { returned(); }
                }
            });
            if (cause == null) graceful = thread;
            thread.start();
        }

        synchronized void escalate(Throwable cause) {
            if (!started || aborted || (pio.writer() instanceof io.zmux.internal.spi.AdapterPorts.ReleasingHalf && finished.isDone())) return;
            aborted = true;
            if (graceful != null) graceful.interrupt();
            escalation = java.util.concurrent.CompletableFuture.runAsync(() -> pio.writer().abort(cause), Callbacks.EXECUTOR);
            escalation.whenComplete((v, e) -> flushBound.complete(null));
        }

        private synchronized void returned() {
            escalation.whenComplete((v, e) -> finished.complete(null));
        }
    }

    private static final java.util.concurrent.CompletableFuture<Void> DONE = java.util.concurrent.CompletableFuture.completedFuture(null);

    private java.util.concurrent.CompletableFuture<Void> readerClosing = DONE;

    @Override public void shutdownInput() {
        if (readerClosed) return;
        inEnded = true;
        if (pio.reader() != null) {
            readerClosed = true;
            PipeReader r = pio.reader();
            readerClosing = java.util.concurrent.CompletableFuture.runAsync(() -> {
                try { r.close(); } catch (RuntimeException ignored) { /* Already closing. */ }
            }, Callbacks.EXECUTOR);
        }
        inbox.stop();
        if (reader != null) reader.release();
    }
    @Override protected java.util.function.Consumer<Throwable> closeEscalation() { return writerClose.arm(); }

    @Override public java.util.concurrent.CompletionStage<Void> writesReleased() {
        return inbox.writesReleased.minimalCompletionStage();
    }

    @Override void closePhysical(CarrierSetup.Bound.Permit permit, Throwable cause) {
        java.util.Objects.requireNonNull(permit);
        // An unsubmitted transport can be returned on the submitting thread.
        // Only the winner releases halves and input. Registered transports close on their owner.
        synchronized (this) {
            if (closed) return;
            closed = true;
        }
        setup().transportClosed(this);
        if (!writing()) inbox.writesReleased.complete(null);
        inbox.taken.decide(false); // Not taken before: the listener refused it.
        shutdownInput();
        closeWriter(cause);
        java.util.concurrent.CompletableFuture.allOf(readerClosing, inbox.inputReleased, writerClose.flushBound,
                pio.writer() == null ? DONE : writerClose.finished, io.zmux.internal.spi.AdapterPorts.releasedHalf(pio.reader()))
                .whenCompleteAsync((v, e) -> inbox.closed.complete(null), Callbacks.EXECUTOR);
        // An in-flight batch belongs to its completion, even after the carrier closes.
    }
}
