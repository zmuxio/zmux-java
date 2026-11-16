package io.zmux.internal.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.zmux.Chunk;
import io.zmux.carrier.PipeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class LentInputTest {
    /** A chunk whose release is counted. */
    private static Chunk chunk(AtomicInteger released, int n) {
        return Chunk.of(ByteBuffer.allocate(n), released::incrementAndGet);
    }

    private static final class Sink implements PipeReader.Sink {
        final List<Chunk> chunks = new ArrayList<>();
        final List<Runnable> consumed = new ArrayList<>();
        final List<String> ends = new ArrayList<>();
        final List<Throwable> errors = new ArrayList<>();
        boolean closeAtOnce;

        @Override public void onChunk(Chunk c, Runnable done) {
            if (closeAtOnce) { c.close(); done.run(); return; }
            chunks.add(c);
            consumed.add(done);
        }
        @Override public void onEnd() { ends.add("end"); }
        @Override public void onError(Throwable cause) { errors.add(cause); }
    }

    private static LentInput input(long budget, List<Boolean> demands) {
        return new LentInput(budget, Runnable::run, () -> true, demands::add);
    }

    @Test void failDropsWhatIsQueuedAndReachesALateReader() {
        AtomicInteger released = new AtomicInteger();
        LentInput in = input(10, new ArrayList<>());
        in.offer(chunk(released, 3), 3);
        in.offer(chunk(released, 3), 3);
        in.end();
        IOException cause = new IOException("lost");
        in.lost(cause); // ended, but no reader takes it: it fails
        assertEquals(2, released.get());
        Sink s = new Sink();
        in.start(s);
        assertEquals(List.of(cause), s.errors);
        assertTrue(s.ends.isEmpty());
    }

    @Test void lostKeepsACompleteInputForAReaderTakingIt() {
        AtomicInteger released = new AtomicInteger();
        LentInput in = input(4, new ArrayList<>());
        Sink s = new Sink();
        in.start(s);
        in.offer(chunk(released, 4), 4);
        in.offer(chunk(released, 4), 4); // beyond the budget: waits
        in.end();
        in.lost(new IOException("lost"));
        assertEquals(1, s.chunks.size());
        s.chunks.getFirst().close();
        s.consumed.getFirst().run();
        assertEquals(2, s.chunks.size());
        s.chunks.get(1).close();
        assertEquals(List.of("end"), s.ends);
        assertEquals(2, released.get());
    }

    @Test void consumedInlineDoesNotReenter() {
        AtomicInteger released = new AtomicInteger();
        LentInput in = input(1, new ArrayList<>());
        for (int i = 0; i < 5; i++) in.offer(chunk(released, 1), 1);
        Sink s = new Sink();
        s.closeAtOnce = true;
        in.start(s);
        assertEquals(5, released.get());
        assertEquals(0, in.bytes());
    }

    @Test void closeDropsAndAsksForMore() {
        AtomicInteger released = new AtomicInteger();
        List<Boolean> demands = new ArrayList<>();
        LentInput in = input(10, demands);
        in.offer(chunk(released, 2), 2);
        assertFalse(demands.getLast(), "no reader: the source pauses");
        in.close();
        assertEquals(1, released.get());
        assertTrue(demands.getLast());
        in.offer(chunk(released, 2), 2);
        assertEquals(2, released.get(), "a closed input closes what still arrives");
        assertTrue(in.isClosed());
    }
}
