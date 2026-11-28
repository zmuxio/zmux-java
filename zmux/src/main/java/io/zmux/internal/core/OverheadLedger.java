package io.zmux.internal.core;

import io.zmux.shaping.OverheadMeters;
import io.zmux.shaping.OverheadMeters.Precision;
import io.zmux.shaping.OverheadMeters.Window;
import io.zmux.shaping.ProfileLimits;
import io.zmux.shaping.ShapingLimits;
import java.util.TreeMap;
import java.util.function.LongSupplier;

/** One session clock, one owner. Live reservations survive expiry of their dispatch bin. */
final class OverheadLedger {
    private static final class Bin {
        private long baseline, consumed, unresolved;
        private boolean unknown;
    }

    private final LongSupplier clock;
    private final long origin;
    private final TreeMap<Long, Bin> bins = new TreeMap<>();
    private ShapingLimits.Effective limits;
    private long reserved, measured, handshake, lastDiscarded = -1;
    private long handshakeReserved, handshakeUnresolved;
    private int unknownBins;
    private long changes;

    long revision() { return changes + second(); }

    OverheadLedger(long origin, LongSupplier clock, ShapingLimits.Effective limits) {
        this.origin = origin;
        this.clock = clock;
        this.limits = limits;
    }

    void configure(ShapingLimits.Effective next) {
        prune();
        changes++;
        limits = next;
        prune();
    }

    private long second() { return Math.max(0, clock.getAsLong() - origin) / Time.SECOND; }

    private long prune() {
        long window = limits.policy().windowSeconds();
        if (limits.local() != null) window = Math.max(window, limits.local().windowSeconds());
        long first = second() - window;
        while (!bins.isEmpty() && bins.firstKey() < first) {
            var oldest = bins.pollFirstEntry();
            Bin bin = oldest.getValue();
            if (bin.unknown) unknownBins--;
            if (bin.unknown || bin.consumed != 0) lastDiscarded = Math.max(lastDiscarded, oldest.getKey());
        }
        return first;
    }

    private Window window(ProfileLimits profile) {
        long first = second() - profile.windowSeconds();
        long baseline = 0, used = 0, held = reserved;
        for (var entry : bins.tailMap(first).entrySet()) {
            Bin b = entry.getValue();
            if (entry.getKey() > first) baseline = WireMeter.addSat(baseline, b.baseline);
            used = WireMeter.addSat(used, b.consumed);
            held = WireMeter.addSat(held, b.unresolved);
        }
        return new Window(profile, baseline, profile.allowance(baseline), used, held,
                lastDiscarded < 0 || lastDiscarded < first);
    }

    boolean exact() { prune(); return unknownBins == 0; }

    OverheadMeters snapshot() {
        prune();
        return new OverheadMeters(unknownBins == 0 ? Precision.EXACT : Precision.UNAVAILABLE,
                window(limits.policy()), limits.local() == null ? null : window(limits.local()), measured, handshakeReserved, handshake, handshakeUnresolved);
    }

    private static long remaining(Window w) {
        if (!w.historyComplete()) return 0;
        long spent = WireMeter.addSat(w.consumedBytes(), w.reservedBytes());
        if (Long.compareUnsigned(spent, w.allowanceBytes()) >= 0) return 0;
        long left = w.allowanceBytes() - spent;
        return left < 0 ? Long.MAX_VALUE : left;
    }

    long room() {
        prune();
        if (unknownBins != 0) return 0;
        long room = remaining(window(limits.policy()));
        return limits.local() == null ? room : Math.min(room, remaining(window(limits.local())));
    }

    void handshake(long bytes) { changes++; handshake = WireMeter.addSat(handshake, bytes); }

    /** A handed-off Establish write can predate the session's first second. */
    void transferHandshake(long baseline, int extra, long age) {
        long first = prune();
        long second = Math.floorDiv(Time.sub(Time.sub(clock.getAsLong(), origin), age), Time.SECOND);
        if (baseline != 0 && second >= first) {
            var bin = bins.computeIfAbsent(second, ignored -> new Bin());
            bin.baseline = WireMeter.addSat(bin.baseline, baseline);
        }
        handshake(extra);
    }

    final class HandshakeReservation {
        private final int size, fixed;
        private final HandshakePlans.Padding padding;
        private final boolean atomic;
        private final Reservation baseline;
        private int prefix, covered;
        private boolean closed;

        HandshakeReservation(int size, int fixed, HandshakePlans.Padding padding, boolean atomic) {
            if (padding.offset() > size - padding.length() || fixed < 0) throw new IllegalArgumentException("invalid handshake layout");
            this.size = size;
            this.fixed = fixed;
            this.padding = padding;
            this.atomic = atomic;
            handshakeReserved = Math.addExact(handshakeReserved, padding.length());
            baseline = reserve(0);
            baseline.commit();
            changes++;
        }

        void report(int confirmed, boolean terminal, boolean unknown) {

            if (closed) return;
            if (confirmed < 0 || confirmed > size || (atomic && confirmed != 0 && confirmed < size)) {
                unknown = true;
                confirmed = prefix;
            }
            int proved = Math.max(prefix, confirmed);
            int extra = Math.clamp(proved - padding.offset(), 0, padding.length());
            int delta = extra - covered;
            long base = proved - prefix - delta + (proved == size && prefix < size ? (long) fixed : 0);
            handshakeReserved -= delta;
            handshake(delta);
            prefix = proved;
            covered = extra;
            if (terminal) {
                int left = padding.length() - covered;
                handshakeReserved -= left;
                if (unknown) handshakeUnresolved = WireMeter.addSat(handshakeUnresolved, left);
                baseline.settle(base, 0, false);
                closed = true;
            } else {
                baseline.progress(base, 0);
            }
        }
    }

    Reservation reserve(long extra) {
        if (extra < 0 || (extra != 0 && extra > room())) throw new IllegalStateException("layout exceeds shaping allowance");
        if (extra != 0) changes++;
        reserved = Math.addExact(reserved, extra);
        return new Reservation(extra);
    }

    final class Reservation {
        private long amount;
        private long dispatch = -1;
        private boolean closed;

        private Reservation(long amount) { this.amount = amount; }

        void release() {
            if (closed || dispatch >= 0) throw new IllegalStateException("reservation already submitted or released");
            if (amount != 0) changes++;
            reserved -= amount;
            closed = true;
        }

        void commit() {
            if (closed || dispatch >= 0) throw new IllegalStateException("reservation already submitted or released");
            // Layout and submission share an owner turn. Held plans release their estimate before final sizing.
            dispatch = second();
        }

        void settle(long baseline, long extra, boolean unknown) {
            report(baseline, extra, true, unknown);
        }

        void progress(long baseline, long extra) {
            report(baseline, extra, false, false);
        }

        private void report(long baseline, long extra, boolean terminal, boolean unknown) {
            if (closed) return;
            if (dispatch < 0 || extra < 0 || extra > amount) throw new IllegalStateException("invalid shaping receipt");
            changes++;
            closed = terminal;
            reserved -= terminal ? amount : extra;
            amount -= extra;
            measured = WireMeter.addSat(measured, extra);
            if (dispatch < prune()) {
                if (extra != 0 || unknown) lastDiscarded = Math.max(lastDiscarded, dispatch);
                return;
            }
            if ((baseline | extra) == 0 && !unknown) return;
            Bin b = bins.computeIfAbsent(dispatch, ignored -> new Bin());
            b.baseline = WireMeter.addSat(b.baseline, baseline);
            b.consumed = WireMeter.addSat(b.consumed, extra);
            if (unknown) {
                if (!b.unknown) unknownBins++;
                b.unknown = true;
                b.unresolved = WireMeter.addSat(b.unresolved, amount);
            }
        }
    }
}
