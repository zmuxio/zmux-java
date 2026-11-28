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

    @Test void dropsExpandedArraysAsStreamIdsRetire() {
        var streams = new LongMap<Long>();
        int initialSlots = ((long[]) Poke.get(streams, "keys")).length;
        for (long id = 0; id < 512; id++) streams.put(id, id);
        for (long id = 0; id < 511; id++) assertEquals(id, streams.remove(id));
        assertEquals(511L, streams.get(511));
        assertEquals(initialSlots, ((long[]) Poke.get(streams, "keys")).length);
    }

    @Test void identityIndexReleasesItsPeakTableWhileKeepingTheLiveRoot() {
        var index = new io.zmux.internal.buffer.IdentityIndex<Object, Integer>();
        var roots = new Object[512];
        for (int at = 0; at < roots.length; at++) {
            roots[at] = new Object();
            index.put(roots[at], at);
        }
        Object peakTable = Poke.get(index, "values");
        for (int at = 0; at + 1 < roots.length; at++) assertEquals(at, index.remove(roots[at]));
        assertEquals(511, index.get(roots[511]));
        assertNotSame(peakTable, Poke.get(index, "values"));
    }
}
