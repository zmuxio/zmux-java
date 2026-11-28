package io.zmux.internal.loop;

import io.zmux.internal.spi.ProviderBackings;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ProviderBackingsTest {
    @Test void slicesShareThePhysicalCapacityAndLastRelease() {
        var retains = new AtomicInteger();
        var releases = new AtomicInteger();
        var held = new AtomicLong();
        var backings = new ProviderBackings(held::addAndGet);
        var root = ByteBuffer.allocate(8192).putInt(4094, 0x01020304);
        var first = ChunkAccess.take(backings.chunk(root, root.capacity(), root.slice(4094, 2),
                retains::incrementAndGet, releases::incrementAndGet));
        var second = ChunkAccess.take(backings.chunk(root, root.capacity(), root.slice(4096, 2),
                retains::incrementAndGet, releases::incrementAndGet));
        try {
            assertSame(first.buf(), second.buf());
            assertEquals(8192, first.buf().capacity());
            assertEquals(8192, held.get());
            assertEquals(1, retains.get());
            assertEquals(0x0102, first.data().getShort());
            assertEquals(0x0304, second.data().getShort());
        } finally {
            first.buf().unref();
            assertEquals(0, releases.get());
            second.buf().unref();
        }
        assertEquals(1, releases.get());
        assertEquals(0, held.get());
    }
}
