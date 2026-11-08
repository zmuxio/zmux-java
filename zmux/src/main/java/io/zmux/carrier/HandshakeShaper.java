package io.zmux.carrier;

import io.zmux.shaping.ShapingLimits;
import java.time.Duration;

/**
 * Asked for the size and timing of each handshake message a side writes on an Establish or a Join. Called outside the
 * session, from concurrent handshakes; it does not block and sees no sizes. A runtime exception, or no plan, leaves the
 * message as it is.
 */
@FunctionalInterface
public interface HandshakeShaper {
    /**
     * One message.
     *
     * @param kind the carrier's kind; null when it declares none
     * @param handshake {@code establish} or {@code join}
     * @param message {@code hello}, {@code hello_early}, {@code reply}, {@code finish} or {@code confirm}
     */
    record View(io.zmux.CarrierKind kind, String handshake, String message) {}

    /**
     * The size and timing of one unit written to a carrier, in bytes on the carrier.
     *
     * @param padTo pad a shorter unit up to this length; 0: none
     * @param padMultiple round up to a multiple of this; 0: none
     * @param pad bytes added after both; 0: none
     * @param delay the wait before the write; null: none
     */
    record UnitPlan(int padTo, int padMultiple, int pad, Duration delay) {
        public static final UnitPlan NONE = new UnitPlan(0, 0, 0, null);

        /** The length of a unit of n bytes under this plan. */
        public int padFor(int n) {
            long t = Math.max(n, padTo);
            if (padMultiple > 0 && t % padMultiple != 0) t += padMultiple - t % padMultiple;
            return (int) Math.min(t + Math.max(pad, 0), Integer.MAX_VALUE);
        }

        /** This plan under limits: no padding clears the lengths, no pause the delay, and the bound on one wait bounds the delay. */
        public UnitPlan limited(ShapingLimits limits) {
            int to = padTo, multiple = padMultiple, add = pad;
            Duration d = delay;
            if (limits.noPadding()) to = multiple = add = 0;
            if (limits.noPause()) {
                d = null;
            } else if (d != null) {
                Duration m = limits.maxDelay().isZero() ? ShapingLimits.DEFAULT_MAX_DELAY : limits.maxDelay();
                if (m.isPositive() && d.compareTo(m) > 0) d = m;
            }
            return new UnitPlan(to, multiple, add, d);
        }

        /** The delay in nanoseconds; 0: none. */
        public long delayNanos() {
            if (delay == null || !delay.isPositive()) return 0;
            try { return delay.toNanos(); } catch (ArithmeticException e) { return Long.MAX_VALUE; }
        }

        /** The value length of the padding item that takes a unit of wire bytes to its planned length, or {@link #NO_ITEM}. */
        public int item(int wire, int room) {
            return itemFor(padFor(wire) - wire, room);
        }

        /** The zero bytes that take a unit of wire bytes to its planned length, at most room. */
        public int fill(int wire, int room) {
            // Not Math.clamp: a negative room is a valid argument here and yields 0, where clamp would throw.
            //noinspection MathClampMigration
            return Math.max(Math.min(padFor(wire) - wire, room), 0);
        }

        /** The smallest value length whose item reaches gap, cut to fit room; {@link #NO_ITEM} when none is due or none fits. */
        public static int itemFor(int gap, int room) {
            if (gap <= 0) return NO_ITEM;
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

    /** No padding item; an item's value length is 0 or more. */
    int NO_ITEM = -1;

    /** The plan of a message; null when it has none. */
    UnitPlan plan(View view);

    /** The plan of a message; null without one, and when the call fails, which onFault is told. */
    static UnitPlan ask(HandshakeShaper shaper, View view, Runnable onFault) {
        try {
            return shaper.plan(view);
        } catch (RuntimeException failed) {
            onFault.run();
            return null;
        }
    }
}
