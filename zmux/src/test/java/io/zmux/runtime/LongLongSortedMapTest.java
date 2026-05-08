package io.zmux.runtime;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class LongLongSortedMapTest {
    @Test
    void storesPrimitiveEntriesInAscendingKeyOrder() {
        LongLongSortedMap map = new LongLongSortedMap();

        map.put(9L, 90L);
        map.put(1L, 10L);
        map.put(5L, 50L);
        map.put(5L, 55L);

        assertEquals(3, map.size());
        assertEquals(1L, map.keyAt(0));
        assertEquals(10L, map.valueAt(0));
        assertEquals(5L, map.keyAt(1));
        assertEquals(55L, map.valueAt(1));
        assertEquals(9L, map.keyAt(2));
        assertEquals(90L, map.valueAt(2));
        assertEquals(1, map.indexOf(5L));

        map.removeAt(1);
        assertEquals(2, map.size());
        assertEquals(-1, map.indexOf(5L));
        assertEquals(9L, map.keyAt(1));

        map.clear();
        assertTrue(map.isEmpty());
        assertEquals(0, map.size());
        assertEquals(-1, map.indexOf(1L));
        assertFalse(map.indexOf(9L) >= 0);
    }
}
