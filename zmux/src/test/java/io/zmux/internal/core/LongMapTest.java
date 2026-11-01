package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class LongMapTest {
    @Test void matchesHashMapUnderRandomInsertsAndBackwardShiftRemovals() {
        var random = new SplittableRandom(7);
        var map = new LongMap<Long>(); var reference = new HashMap<Long, Long>();
        for (int i = 0; i < 200_000; i++) {
            long key = random.nextInt(4096) * 4L; // Stream-ID-like keys that differ only above bit 1.
            switch (random.nextInt(3)) {
                case 0 -> assertEquals(reference.put(key, (long) i), map.put(key, (long) i));
                case 1 -> assertEquals(reference.remove(key), map.remove(key));
                default -> assertEquals(reference.get(key), map.get(key));
            }
            assertEquals(reference.size(), map.size());
        }
        var seen = new HashSet<Long>(); for (Long v : map.values()) seen.add(v);
        assertEquals(new HashSet<>(reference.values()), seen);
    }
}
