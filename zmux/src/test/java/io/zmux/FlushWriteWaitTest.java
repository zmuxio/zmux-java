package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A flush waits beside the stream's writer, never in its place: one that times out leaves the writer to wake on credit. */
@Timeout(30)
class FlushWriteWaitTest {
    @Test void writerResumesAfterAFlushTimedOut() throws Exception {
        try (var loops = EventLoops.create(1); var pair = Pair.memory(NonBlockingCallsTest.small(loops))) {
            var s = pair.client().openStream();
            s.setSendBuffer(0); // the least buffer: the writer waits for credit soon
            byte[] big = new byte[4 << 20];
            var written = new CompletableFuture<Void>();
            Thread writer = Thread.ofVirtual().start(() -> {
                try { s.write(big); written.complete(null); } catch (IOException e) { written.completeExceptionally(e); }
            });
            // The writer waits once the peer's window is used up.
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (s.sendCapacity() > 0 || (writer.getState() != Thread.State.WAITING && writer.getState() != Thread.State.TIMED_WAITING)) {
                assertTrue(System.nanoTime() - until < 0, "the writer never blocked: capacity " + s.sendCapacity() + ", " + writer.getState());
                Thread.onSpinWait();
            }
            s.setWriteTimeout(Duration.ofMillis(20));
            assertThrows(SocketTimeoutException.class, s::flush);
            s.setWriteTimeout(null);
            var r = pair.server().acceptStream(Duration.ofSeconds(5));
            Thread reader = Thread.ofVirtual().start(() -> {
                byte[] b = new byte[64 << 10];
                try { while (r.read(b, 0, b.length) >= 0) { /* drain */ } } catch (IOException ignored) { /* the close below */ }
            });
            written.get(10, TimeUnit.SECONDS); // credit came back: the writer woke
            s.flush();
            r.close();
            reader.join(5000);
        }
    }
}
