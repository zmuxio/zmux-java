package io.zmux.internal.core;

import io.zmux.internal.wire.VarInt;

/** A record's length on the carrier against its frames bytes. */
final class WireLength {
    private WireLength() {}

    /** The length on the carrier of a record of f frames bytes. */
    static int wireOf(int f, boolean sizeField, int over) {
        return (sizeField ? VarInt.length(f) : 0) + f + over;
    }

    /** The smallest frames bytes whose record reaches target on the carrier. */
    static int framesOf(long target, boolean sizeField, int over) {
        long x = target - over;
        if (x <= 0) return 0;
        if (!sizeField) return (int) x;
        int f = (int) Math.max(x - 8, 0);
        while (f + VarInt.length(f) < x) f++;
        return f;
    }
}
