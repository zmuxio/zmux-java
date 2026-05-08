package io.zmux;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class LongIntCounterMapTest {
    @Test
    void incrementsSaturateAndIgnoreInvalidKeys() {
        LongIntCounterMap map = new LongIntCounterMap();

        map.incrementSaturating(7L);
        assertEquals(1, map.getOrDefault(7L, 0));

        map.put(7L, Integer.MAX_VALUE);
        map.incrementSaturating(7L);
        assertEquals(Integer.MAX_VALUE, map.getOrDefault(7L, 0));

        map.put(0L, 10);
        map.incrementSaturating(-1L);
        assertEquals(99, map.getOrDefault(0L, 99));
        assertEquals(99, map.getOrDefault(-1L, 99));
    }
}
