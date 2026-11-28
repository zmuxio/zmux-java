package io.zmux.internal.core;

import io.zmux.carrier.HandshakeShaper;
import io.zmux.carrier.HandshakeShaper.UnitPlan;
import io.zmux.internal.wire.VarInt;
import io.zmux.shaping.ShapingLimits;
import java.time.Duration;

/** The arithmetic of a {@link UnitPlan}: lengths, limits and the padding item, for Establish and Join alike. */
public final class HandshakePlans {
    /** No padding item; an item's value length is 0 or more. */
    public static final int NO_ITEM = -1;

    private HandshakePlans() {}

    /** The plan of a message; null without one, and when the call fails, which onFault is told. */
    public static UnitPlan ask(HandshakeShaper shaper, HandshakeShaper.View view, Runnable onFault) {
        try {
            return shaper.plan(view);
        } catch (RuntimeException failed) {
            onFault.run();
            return null;
        }
    }

    /** Remaining room under the handshake message floor, including existing unit framing. */
    public static int structuralRoom(int encodedLength) {
        return Math.max(0, io.zmux.internal.join.Sealer.MESSAGE_FLOOR - encodedLength);
    }

    /** The length of a unit of n bytes under p. */
    public static int padFor(UnitPlan p, int n) {
        return io.zmux.shaping.UnitPlan.target(n, p.padTo(), p.padMultiple(), p.pad());
    }

    /** p under limits: no padding clears the lengths, no pause the delay, and the bound on one wait bounds the delay. */
    public static UnitPlan limited(UnitPlan p, ShapingLimits limits) {
        return limited(p, limits.under(io.zmux.shaping.ShapingBudget.NONE));
    }

    public static UnitPlan limited(UnitPlan p, ShapingLimits.Effective limits) {
        int to = p.padTo(), multiple = p.padMultiple(), add = p.pad();
        Duration d = p.delay();
        if (!limits.padding()) to = multiple = add = 0;
        if (!limits.pause()) {
            d = null;
        } else if (d != null) {
            if (d.compareTo(limits.maxAddedDelay()) > 0) d = limits.maxAddedDelay();
        }
        return new UnitPlan(to, multiple, add, d);
    }

    /** p's delay in nanoseconds; 0: none. */
    public static long delayNanos(UnitPlan p) {
        Duration d = p.delay();
        if (d == null || !d.isPositive()) return 0;
        return Time.nanos(d);
    }

    /** Structural padding occupies one contiguous span in the encoded handshake. */
    public record Padding(int offset, int length) {
        public static final Padding NONE = new Padding(0, 0);
        public Padding {
            if (offset < 0 || length < 0) throw new IllegalArgumentException("negative handshake span");
        }
    }

    /** A representable TLV at or below both the sampled target and the protocol room. */
    public static int item(UnitPlan plan, int baseline, int room) {
        return fit(fill(plan, baseline, room));
    }

    /** The zero bytes that take a unit of wire bytes to its planned length under p, at most room. */
    public static int fill(UnitPlan p, int wire, int room) {
        // Not Math.clamp: a negative room is a valid argument here and yields 0, where clamp would throw.
        //noinspection MathClampMigration
        return Math.max(Math.min(padFor(p, wire) - wire, room), 0);
    }

    /** The smallest value length whose item reaches gap, cut to fit room; {@link #NO_ITEM} when none is due or none fits. */
    public static int itemFor(int gap, int room) {
        if (gap <= 0) return NO_ITEM;
        if (gap > room) return fit(room);
        int v = Math.max(gap - 2, 0);
        while (v > 0 && itemSize(v - 1) >= gap) v--;
        while (itemSize(v) < gap) v++;
        return itemSize(v) > room ? fit(room) : v;
    }

    /** The largest value length whose item fits in room, or {@link #NO_ITEM}. */
    public static int fit(int room) {
        if (room < 2) return NO_ITEM;
        int v = room - 2;
        while (v > 0 && itemSize(v) > room) v--;
        return v;
    }

    /** What an item with a value of v bytes takes. */
    public static int itemSize(int v) { return 1 + VarInt.length(v) + v; }
}
