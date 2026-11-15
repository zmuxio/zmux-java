package io.zmux.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.zmux.Chunk;
import io.zmux.carrier.Framing;
import io.zmux.carrier.PipeReader;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BodyCarrierLeakTest {
    private static BodyCarrier carrier(EmbeddedChannel ch) {
        return new BodyCarrier(ch, Framing.BYTE_STREAM, true, true, new BodyCarrier.Out() {
            @Override public Object content(ByteBuf data) { return data; }
            @Override public Object end() { return null; }
        }, () -> {}, e -> null);
    }

    /** A carrier kept for a replay is lost before zmux started its reader: nothing stays referenced. */
    @Test void lostBeforeReaderStartReleasesQueue() {
        EmbeddedChannel ch = new EmbeddedChannel();
        BodyCarrier c = carrier(ch);
        PipeReader r = c.reader();
        c.keepForReplay();
        ByteBuf a = Unpooled.buffer().writeBytes(new byte[] {1, 2, 3});
        ByteBuf b = Unpooled.buffer().writeBytes(new byte[] {4});
        c.in(a);
        c.in(b);
        c.lost(new ClosedChannelException());
        assertEquals(0, a.refCnt());
        assertEquals(0, b.refCnt());
        // A late start sees the error only.
        List<Throwable> errors = new ArrayList<>();
        r.start(new PipeReader.Sink() {
            @Override public void onData(java.nio.ByteBuffer data, Runnable release) { throw new AssertionError("data after loss"); }
            @Override public void onChunk(Chunk chunk, Runnable consumed) { throw new AssertionError("data after loss"); }
            @Override public void onEnd() { throw new AssertionError("end after loss"); }
            @Override public void onError(Throwable t) { errors.add(t); }
        });
        assertEquals(1, errors.size());
        assertTrue(errors.get(0) instanceof ClosedChannelException);
        r.close();
        ch.finishAndReleaseAll();
    }
}
