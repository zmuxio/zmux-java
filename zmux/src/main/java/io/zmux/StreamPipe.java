package io.zmux;

import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.Session;
import io.zmux.internal.core.StreamRecv;
import io.zmux.internal.core.Waiter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Nonblocking stream carrier; all stream operations run on the outer session's loop. */
final class StreamPipe {
    private final ZmuxStream stream;
    private final Runnable released;
    private final boolean sendOnlyLane;
    private boolean leaseReleased;
    private PipeReader.Sink sink;
    private boolean reading;
    private boolean inputClosed;
    private boolean outputClosed;
    private List<StreamRecv.Chunk> pending;
    private int index;
    private PipeWriter.Completion completion;
    private io.zmux.internal.loop.RecvLane input;
    private final Waiter writable = new Waiter() { @Override public void wake() { write(); } };
    private final AtomicBoolean started = new AtomicBoolean();
    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    StreamPipe(ZmuxStream stream) { this.stream = stream; this.released = () -> {}; this.sendOnlyLane = false; }
    private StreamPipe(ZmuxStream stream, Runnable released) { this.stream = stream; this.released = released; this.sendOnlyLane = true; }

    private java.util.concurrent.CompletionStage<PipeIo> openLane() {
        var result = new java.util.concurrent.CompletableFuture<PipeIo>();
        if (!post(() -> {
            if (result.isCancelled()) return;
            var outer = stream.session;
            if (!outer.driver.acquireNestedLane()) {
                result.completeExceptionally(new IOException("no spare independent outer stream credit")); return;
            }
            io.zmux.internal.core.Stream opened = null;
            try {
                opened = outer.core.streams().openStream(true);
                if (outer.core.preambleInUse()) outer.core.streams().setPipeCarrier(opened);
                var lane = new StreamPipe(new ZmuxStream(outer, outer.driver.lanes.of(opened), Preamble.EMPTY), outer.driver::releaseNestedLane);
                PipeIo io = lane.io();
                // The reverse direction must end gracefully, never STOP_SENDING/RESET.
                io.reader().start(new PipeReader.Sink() {
                    @Override public void onData(ByteBuffer ignored, Runnable release) { release.run(); }
                    @Override public void onChunk(Chunk chunk, Runnable consumed) { chunk.close(); consumed.run(); }
                    @Override public void onEnd() {}
                    @Override public void onError(Throwable ignored) {}
                });
                if (!result.complete(new PipeIo(null, io.writer(), io.framing(), io.label(), io.carrierConn()))) {
                    io.writer().abort(new IOException("lane opening cancelled"));
                    io.reader().close();
                }
            } catch (CoreException | RuntimeException error) {
                if (opened != null) outer.core.streams().release(opened);
                outer.driver.releaseNestedLane(); result.completeExceptionally(error);
            }
        })) result.completeExceptionally(new IOException("outer session closed"));
        return result;
    }

    PipeIo io() {
        // Before any carrier work: data written so far moves into the core, reads come from the lane.
        post(() -> {
            stream.session.driver.lanes.hostOwnSend(stream.st);
            input = stream.session.driver.lanes.hostOwnRecv(stream.st, this::read);
        });
        PipeReader reader = new PipeReader() {
            @Override public void start(Sink target) {
                if (!started.compareAndSet(false, true)) throw new IllegalStateException("carrier already started");
                if (!post(() -> { sink = target; read(); })) target.onError(new IOException("outer session closed"));
            }
            @Override public void close() { post(() -> {
                if (inputClosed) return;
                inputClosed = true; stream.session.driver.lanes.stopReceiving(stream.st, PipeIo.CARRIER_FAILED);
                releaseIfClosed();
            }); }
        };
        PipeWriter writer = new PipeWriter() {
            @Override public void write(OutboundBatch batch, Completion done) {
                // The outer stream queues the payload without copying: the batch is retained
                // until the last piece is sent, and the write completes once it is queued.
                int n = batch.segmentCount();
                if (n == 0) { done.completed(); return; }
                ByteBuffer[] views = batch.segments(null);
                io.zmux.internal.core.ReadBuf holder = releasing(batch.retain());
                List<StreamRecv.Chunk> owned = new ArrayList<>(n);
                for (int i = 0; i < n; i++) { holder.ref(); owned.add(new StreamRecv.Chunk(views[i], holder, false)); }
                holder.unref();
                if (!post(() -> {
                    if (outputClosed || pending != null) { owned.forEach(chunk -> chunk.buf().unref()); done.failed(new IOException("outer stream unavailable")); return; }
                    pending = owned; index = 0; completion = done; StreamPipe.this.write();
                })) { owned.forEach(chunk -> chunk.buf().unref()); done.failed(new IOException("outer session closed")); }
            }
            @Override public void close() { post(() -> {
                if (outputClosed) return;
                if (pending != null) { failWrite(new IOException("carrier closed with a pending write")); stream.session.core.sending().reset(stream.st, PipeIo.CARRIER_FAILED); }
                else try { stream.session.core.sending().finish(stream.st); } catch (CoreException ignored) { /* Already ended. */ }
                outputClosed = true; releaseIfClosed();
            }); }
            @Override public void abort(Throwable cause) { post(() -> {
                if (outputClosed) return;
                outputClosed = true; failWrite(cause == null ? new IOException("carrier aborted") : cause);
                stream.session.core.sending().reset(stream.st, PipeIo.CARRIER_FAILED);
                if (sendOnlyLane) {
                    inputClosed = true; stream.session.driver.lanes.stopReceiving(stream.st, PipeIo.CARRIER_FAILED);
                }
                releaseIfClosed();
            }); }
        };
        return PipeIo.of(reader, writer).withCarrierConn(stream.session.driver).withLaneOpener(this::openLane);
    }

    private boolean post(Runnable task) { return stream.session.driver.executeCarrier(task); }

    private void read() {
        if (inputClosed || reading || sink == null || input == null) return;
        Object r = input.hostTake(Session.DEFAULT_BATCH);
        if (r == null) return; // The lane calls back after its next push.
        if (r == StreamRecv.FIN) { inputClosed = true; sink.onEnd(); releaseIfClosed(); return; }
        if (r instanceof CoreException error) { inputClosed = true; sink.onError(Errors.io(error)); releaseIfClosed(); return; }
        reading = true;
        sink.onChunk(new Chunk((StreamRecv.Chunk) r), () -> post(() -> { reading = false; read(); }));
    }

    private void write() {
        if (pending == null) return;
        stream.session.core.sending().cancelWriteWait(stream.st, writable);
        try {
            while (index < pending.size()) {
                StreamRecv.Chunk chunk = pending.get(index);
                stream.session.core.sending().writeChunk(stream.st, chunk.data(), chunk.buf(), false);
                if (chunk.data().hasRemaining()) { stream.session.core.sending().waitWrite(stream.st, writable); return; }
                chunk.buf().unref(); index++;
            }
            PipeWriter.Completion done = completion; pending = null; completion = null;
            done.completed();
        } catch (CoreException error) { failWrite(Errors.io(error)); }
    }

    private void failWrite(Throwable error) {
        stream.session.core.sending().cancelWriteWait(stream.st, writable);
        if (pending == null) return;
        for (; index < pending.size(); index++) pending.get(index).buf().unref();
        PipeWriter.Completion done = completion; pending = null; completion = null; done.failed(error);
    }

    private void releaseIfClosed() {
        if (inputClosed && outputClosed && !leaseReleased) {
            leaseReleased = true; stream.close(); released.run();
        }
    }

    /** A read buffer that owns kept: releasing its last reference closes the retained batch. */
    private static io.zmux.internal.core.ReadBuf releasing(OutboundBatch.Retained kept) {
        return new io.zmux.internal.core.ReadBuf(EMPTY, ignored -> kept.close());
    }
}
