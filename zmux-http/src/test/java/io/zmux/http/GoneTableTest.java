package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class GoneTableTest {
    private static ByteBuffer id(int i) { return ByteBuffer.wrap(new byte[] {(byte) (i >> 8), (byte) i}); }

    @Test void expiredEntriesAreDroppedInExpiryOrderWithoutAFullScan() {
        AtomicLong now = new AtomicLong();
        GoneTable<String> t = new GoneTable<>(100, now::get);
        for (int i = 0; i < 50; i++) { t.remember(id(i), "a" + i); now.addAndGet(1); }
        assertEquals("a0", t.get(id(0)));
        now.set(149); // entries 0..48 expired (until 100..148), 49 not (until 149)
        t.remember(id(1000), "late");
        assertEquals(2, t.size());
        assertEquals(2, t.queued());
        assertNull(t.get(id(0)));
        assertEquals("a49", t.get(id(49)));
    }

    @Test void aSmallTableIsPrunedToo() {
        AtomicLong now = new AtomicLong();
        GoneTable<String> t = new GoneTable<>(10, now::get);
        t.remember(id(1), "x");
        now.set(11);
        t.remember(id(2), "y");
        assertEquals(1, t.size(), "pruned below any size threshold");
    }

    @Test void aRepeatedIdKeepsItsFirstEntry() {
        AtomicLong now = new AtomicLong();
        GoneTable<String> t = new GoneTable<>(10, now::get);
        t.remember(id(1), "x");
        t.remember(id(1), "y");
        assertEquals("x", t.get(id(1)));
        assertEquals(1, t.queued());
    }
}
