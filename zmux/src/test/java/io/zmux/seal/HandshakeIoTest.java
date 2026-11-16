package io.zmux.seal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.zmux.Chunk;
import io.zmux.carrier.Framing;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** What a handshake holds of its carrier goes when it fails, and so does what arrives after. */
class HandshakeIoTest {
    @Test void failClosesWhatIsHeldAndWhatArrives() {
        AtomicInteger released = new AtomicInteger(), consumed = new AtomicInteger();
        PipeReader.Sink[] sink = new PipeReader.Sink[1];
        PipeReader r = new PipeReader() {
            @Override public void start(Sink s) { sink[0] = s; }
            @Override public void close() {}
        };
        PipeWriter w = new PipeWriter() {
            @Override public void write(OutboundBatch batch, Completion done) { done.completed(); }
            @Override public void close() {}
            @Override public void abort(Throwable cause) {}
        };
        HandshakeIo io = new HandshakeIo(new PipeIo(r, w, Framing.BYTE_STREAM, "test", null));
        for (int i = 0; i < 2; i++) sink[0].onChunk(Chunk.of(ByteBuffer.allocate(8), released::incrementAndGet), consumed::incrementAndGet);
        io.fail();
        assertEquals(2, released.get());
        assertEquals(2, consumed.get());
        sink[0].onChunk(Chunk.of(ByteBuffer.allocate(8), released::incrementAndGet), consumed::incrementAndGet);
        assertEquals(3, released.get(), "a failed handshake closes what still arrives");
        assertEquals(3, consumed.get());
    }

    @Test void closingTheRestBeforeItIsStartedClosesWhatIsHeld() {
        AtomicInteger released = new AtomicInteger(), consumed = new AtomicInteger(), carrierClosed = new AtomicInteger();
        PipeReader.Sink[] sink = new PipeReader.Sink[1];
        PipeReader r = new PipeReader() {
            @Override public void start(Sink s) { sink[0] = s; }
            @Override public void close() { carrierClosed.incrementAndGet(); }
        };
        PipeWriter w = new PipeWriter() {
            @Override public void write(OutboundBatch batch, Completion done) { done.completed(); }
            @Override public void close() {}
            @Override public void abort(Throwable cause) {}
        };
        HandshakeIo io = new HandshakeIo(new PipeIo(r, w, Framing.BYTE_STREAM, "test", null));
        PipeIo rest = io.rest(null);
        // Records the peer sent right behind its reply, held for a session that never takes them.
        for (int i = 0; i < 2; i++) sink[0].onChunk(Chunk.of(ByteBuffer.allocate(8), released::incrementAndGet), consumed::incrementAndGet);
        rest.reader().close();
        assertEquals(2, released.get());
        assertEquals(2, consumed.get());
        assertEquals(1, carrierClosed.get());
    }
}
