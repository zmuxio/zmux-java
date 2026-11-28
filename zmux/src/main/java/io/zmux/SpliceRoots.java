package io.zmux;

import java.util.IdentityHashMap;
import java.util.Map;

/** Distinct live allocations held by a bounded group or a queue. Its owner serializes changes. */
final class SpliceRoots {
    record Size(long bytes) {}

    private static final class Charge {
        final long capacity;
        int holders;
        Charge(long capacity) { this.capacity = capacity; }
    }

    private final IdentityHashMap<Object, Charge> charges = new IdentityHashMap<>();
    private long bytes;

    long bytes() { return bytes; }

    long extra(Object root, long capacity) { return charges.containsKey(root) ? 0 : capacity; }

    void retain(Object root, long capacity) {
        Charge charge = charges.get(root);
        if (charge == null) {
            charge = new Charge(capacity);
            charges.put(root, charge);
            bytes += capacity;
        }
        charge.holders++;
    }

    void release(Object root) {
        Charge charge = charges.get(root);
        if (--charge.holders == 0) {
            charges.remove(root);
            bytes -= charge.capacity;
        }
    }

    boolean retain(Map<Object, Size> roots, long limit) {
        long room = limit - bytes;
        for (var entry : roots.entrySet()) {
            long n = extra(entry.getKey(), entry.getValue().bytes());
            if (n > room) return false;
            room -= n;
        }
        roots.forEach((root, size) -> retain(root, size.bytes()));
        return true;
    }

    void snapshot(IdentityHashMap<Object, Size> roots) {
        charges.forEach((key, charge) -> roots.put(key, new Size(charge.capacity)));
    }
}
