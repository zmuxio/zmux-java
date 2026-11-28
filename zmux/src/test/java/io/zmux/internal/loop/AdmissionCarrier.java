package io.zmux.internal.loop;

import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import io.zmux.internal.core.CarrierInfo;
import io.zmux.internal.core.Framing;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** An owner-driven byte carrier whose prefix can resume after the admission deadline. */
public final class AdmissionCarrier extends AbstractTransport {
    private static final TransportInfo INFO = new TransportInfo(
            new CarrierInfo(Framing.BYTES, false, false, false, false), "", null, 0, 0);
    private final ByteArrayOutputStream written = new ByteArrayOutputStream();
    private final Semaphore resumed = new Semaphore(0);
    private boolean paused, closed;

    public AdmissionCarrier(EventLoop owner) { super(owner); }

    public PipeIo pipe() {
        class Reader implements PipeReader, NativePipe {
            @Override public Transport claim(PipeIo io) { return AdmissionCarrier.this; }
            @Override public void start(Sink sink) { throw new AssertionError("native reader"); }
            @Override public void close() {}
        }
        return PipeIo.of(new Reader(), new PipeWriter() {
            @Override public void write(OutboundBatch batch, Completion completion) { throw new AssertionError("native writer"); }
            @Override public void close() {}
        });
    }

    public void feed(byte[] bytes) throws Exception {
        ExternalTransportTest.run((EventLoop) loop(), () -> feed(ByteBuffer.wrap(bytes)));
    }

    public void eof() throws Exception {
        ExternalTransportTest.run((EventLoop) loop(), () -> inputEnded(true));
    }

    public void awaitResume() throws InterruptedException {
        org.junit.jupiter.api.Assertions.assertTrue(resumed.tryAcquire(20, TimeUnit.SECONDS), "input resumed");
    }
    public byte[] written() { return written.toByteArray(); }

    @Override public TransportInfo info() { return INFO; }
    @Override public boolean resumesAfterPeekTimeout() { return true; }
    @Override public CompletableFuture<Boolean> whenTaken() { return CompletableFuture.completedFuture(false); }
    @Override public boolean isClosed() { return closed; }
    @Override protected void begin() { openReader(); }
    @Override protected boolean accepting() { return !paused && !closed; }
    @Override public void pauseInput() { paused = true; }
    @Override public void resumeInput() { paused = false; redeliverBuffered(); resumed.release(); }
    @Override public void redeliver() { redeliverBuffered(); }
    @Override protected void inputFailed(IOException cause) { reject(cause); }
    @Override public void shutdownInput() { dropPartialInput(); }
    @Override public void shutdownOutput() { outputShut = true; outputShutDone(); }

    @Override protected void send(OutboundBatch batch) {
        for (int i = 0; i < batch.segmentCount(); i++) {
            ByteBuffer bytes = batch.segment(i);
            while (bytes.hasRemaining()) written.write(bytes.get());
        }
        writeDone(null);
    }

    @Override void closePhysical(CarrierSetup.Bound.Permit permit, Throwable cause) {
        closed = true;
        shutdownInput();
    }

    @Override Transport movePhysical(CarrierSetup.Bound.Permit permit, SessionHost to) { throw new AssertionError("one owner"); }
}
