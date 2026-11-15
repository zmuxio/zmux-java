package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** awaitWritable is ready exactly when tryWrite takes bytes: the send buffer's room, not credit. */
@Timeout(30)
class AwaitWritableTest {
    @Test void readyMeansTryWriteTakesBytes() throws Exception {
        try (var loops = EventLoops.create(1); var pair = Pair.memory(NonBlockingCallsTest.small(loops))) {
            var s = pair.client().openStream();
            ByteBuffer src = ByteBuffer.allocate(1 << 20);
            // Without credit coming back the buffer fills: every ready wait is followed by a
            // write that takes at least what it reported, until no write takes anything.
            while (true) {
                long room;
                try {
                    room = s.awaitWritable(Duration.ofMillis(100));
                } catch (SocketTimeoutException e) {
                    break;
                }
                assertTrue(room > 0);
                src.clear();
                int n = s.tryWrite(src);
                assertTrue(n >= Math.min(room, src.capacity()), "ready with " + room + ", took " + n);
            }
            assertEquals(0, s.sendCapacity(), "the peer's window is used up");
            // Cancelled waits consume nothing.
            for (int i = 0; i < 3; i++) {
                assertThrows(SocketTimeoutException.class, () -> s.awaitWritable(Duration.ofMillis(10)));
            }
            src.clear();
            assertEquals(0, s.tryWrite(src), "still full");
            var r = pair.server().acceptStream(Duration.ofSeconds(5));
            Thread reader = Thread.ofVirtual().start(() -> {
                byte[] b = new byte[64 << 10];
                try {
                    while (r.read(b, 0, b.length) >= 0) { /* drain */ }
                } catch (IOException ignored) {
                    // the stream's reset below ends the read
                }
            });
            long room = s.awaitWritable(Duration.ofSeconds(5));
            src.clear();
            assertTrue(s.tryWrite(src) >= Math.min(room, src.capacity()));
            // A writer loop never spins: each ready wait is followed by a write that takes bytes.
            int waits = 0;
            int writes = 0;
            for (long total = 0; total < (8 << 20); writes++) {
                s.awaitWritable(Duration.ofSeconds(5));
                waits++;
                src.clear();
                int n = s.tryWrite(src);
                assertTrue(n > 0, "a ready wait whose write took nothing");
                total += n;
            }
            assertEquals(waits, writes);
            s.reset(1);
            assertThrows(IOException.class, () -> {
                for (int i = 0; i < 100; i++) { s.awaitWritable(Duration.ofSeconds(5)); Thread.sleep(5); }
            });
            reader.interrupt();
            reader.join(5000);
        }
    }
}
