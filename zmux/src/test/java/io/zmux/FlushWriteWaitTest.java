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
            // The send buffer holds more than the peer's window: the part beyond it can leave only on credit, which the
            // peer's reader gives, so a flush of the whole buffer cannot finish before that. (With no buffer the flush would
            // wait only for the congestion window's tail, which the peer's delayed acknowledgement, 25 ms, opens.)
            s.setSendBuffer(2L * NonBlockingCallsTest.WINDOW);
            byte[] big = new byte[4 << 20];
            var written = new CompletableFuture<Void>();
            Thread writer = Thread.ofVirtual().start(() -> {
                try { s.write(big); written.complete(null); } catch (IOException e) { written.completeExceptionally(e); }
            });
            // The window is used up and the buffer is full beyond it, with the writer parked for space.
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!blockedOnCredit(s) || (writer.getState() != Thread.State.WAITING && writer.getState() != Thread.State.TIMED_WAITING)) {
                assertTrue(System.nanoTime() - until < 0, "the writer never blocked: " + s.stats() + ", " + writer.getState());
                Thread.onSpinWait();
            }
            s.setWriteTimeout(Duration.ofMillis(20));
            assertThrows(SocketTimeoutException.class, s::flush);
            s.clearWriteTimeout();
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

    private static boolean blockedOnCredit(ZmuxStream s) throws IOException {
        var st = s.stats();
        return st.bytesSent() >= NonBlockingCallsTest.WINDOW && st.sendBuffered() > 0;
    }
}
