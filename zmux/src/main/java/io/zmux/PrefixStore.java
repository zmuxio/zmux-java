package io.zmux;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * The linearizable ticket authority of an admission domain. Share it across all entrances
 * and generations. Restore the schedule, tickets, floor and live claims together after
 * restart, or retire the affected tickets before accepting presentations again.
 *
 * <p>Host operations may complete asynchronously and must return without blocking the
 * listener. Slot fields are unsigned 64-bit integers represented by Java longs.
 */
public interface PrefixStore extends AutoCloseable {
    /** Slot length and tolerance are immutable for the lifetime of this replay history. */
    record Window(Duration slotLength, int skew) {
        public Window {
            Objects.requireNonNull(slotLength);
            if (slotLength.getSeconds() < 1 || slotLength.getNano() != 0 || skew < 0
                    || skew > (Integer.MAX_VALUE - 1) / 2) {
                throw new IllegalArgumentException("invalid prefix slot length or skew");
            }
        }

        long slot(long seconds) { return seconds / slotLength.toSeconds(); }

        long first(long current) {
            return Long.compareUnsigned(current, skew) < 0 ? 0 : current - skew;
        }

        long last(long current) {
            return Long.compareUnsigned(current, -1L - skew) > 0 ? -1L : current + skew;
        }
    }

    /** An opaque 16-byte ticket ID or nonce, in wire order. */
    record Id(long high, long low) {
        public static Id of(byte[] bytes) {
            if (bytes.length != 16) throw new IllegalArgumentException("expected 16 bytes");
            return read(ByteBuffer.wrap(bytes));
        }

        static Id read(ByteBuffer bytes) { return new Id(bytes.getLong(), bytes.getLong()); }

        public byte[] bytes() { return ByteBuffer.allocate(16).putLong(high).putLong(low).array(); }
    }

    /** An outer key and its inclusive issue-slot interval. Use -1L for the last uint64 slot. */
    final class Generation {
        private final PrefixKey key;
        private final long firstSlot, lastSlot;

        public Generation(byte[] key, long firstSlot, long lastSlot) {
            requireInterval(firstSlot, lastSlot);
            this.key = new PrefixKey(key);
            this.firstSlot = firstSlot;
            this.lastSlot = lastSlot;
        }

        private Generation(PrefixKey key, long firstSlot, long lastSlot) {
            this.key = key;
            this.firstSlot = firstSlot;
            this.lastSlot = lastSlot;
        }

        public byte[] key() { return key.copy(); }
        public long firstSlot() { return firstSlot; }
        public long lastSlot() { return lastSlot; }

        Generation ownedCopy() { return new Generation(key.duplicate(), firstSlot, lastSlot); }
        void clearKey() { key.clear(); }
        javax.crypto.Mac mac() { return key.mac(); }

        boolean contains(long slot) { return containsSlot(firstSlot, lastSlot, slot); }

        boolean eligible(Window window, long current) {
            return Long.compareUnsigned(firstSlot, window.last(current)) <= 0
                    && Long.compareUnsigned(lastSlot, window.first(current)) >= 0;
        }
    }

    /**
     * A ticket version and the inclusive interval in which it can be redeemed.
     * The authority may retire key material after the ticket ceases to be live.
     */
    final class Ticket {
        private final Id id;
        private final PrefixKey key;
        private final long version, firstSlot, lastSlot;
        private final boolean singleUse;

        public Ticket(Id id, byte[] key, long version, long firstSlot, long lastSlot, boolean singleUse) {
            requireInterval(firstSlot, lastSlot);
            this.id = Objects.requireNonNull(id);
            this.key = new PrefixKey(key);
            this.version = version;
            this.firstSlot = firstSlot;
            this.lastSlot = lastSlot;
            this.singleUse = singleUse;
        }

        private Ticket(Ticket from) {
            id = from.id;
            key = from.key.duplicate();
            version = from.version;
            firstSlot = from.firstSlot;
            lastSlot = from.lastSlot;
            singleUse = from.singleUse;
        }

        public Id id() { return id; }
        public byte[] key() { return key.copy(); }
        public long version() { return version; }
        public long firstSlot() { return firstSlot; }
        public long lastSlot() { return lastSlot; }
        public boolean singleUse() { return singleUse; }

        Ticket ownedCopy() { return new Ticket(this); }
        void clearKey() { key.clear(); }
        javax.crypto.Mac mac() { return key.mac(); }

        boolean liveAt(long current) { return containsSlot(firstSlot, lastSlot, current); }
    }

    static List<Generation> copyGenerations(List<Generation> generations) {
        var sorted = new Schedule(0, generations, 0).generations();
        var copies = new ArrayList<Generation>(sorted.size());
        try {
            for (Generation generation : sorted) copies.add(generation.ownedCopy());
            return List.copyOf(copies);
        } catch (RuntimeException unavailable) {
            copies.forEach(Generation::clearKey);
            throw unavailable;
        }
    }

    /**
     * A cached schedule and an optional lower bound on the floor (zero if unknown).
     * The schedule version changes on every rotation or revocation. An authority may
     * retire the keys in a replaced schedule.
     */
    record Schedule(long version, List<Generation> generations, long floorHint) {
        public Schedule {
            var sorted = new ArrayList<>(generations);
            sorted.sort((a, b) -> Long.compareUnsigned(a.firstSlot(), b.firstSlot()));
            for (int i = 1; i < sorted.size(); i++) {
                if (Long.compareUnsigned(sorted.get(i - 1).lastSlot(), sorted.get(i).firstSlot()) >= 0) {
                    throw new IllegalArgumentException("overlapping prefix generation intervals");
                }
            }
            generations = List.copyOf(sorted);
        }

        boolean sameEligible(Window window, long checked, long current) {
            return checked == current || generations.stream()
                    .allMatch(g -> g.eligible(window, checked) == g.eligible(window, current));
        }
    }

    /** The authenticated data to revalidate and the stable identifier of this commit attempt. */
    record Claim(Id id, Id nonce, UUID commitId, long ticketVersion, long scheduleVersion,
                 long generation, long issueSlot, long checkedSlot) {}

    /**
     * The listener's monotonic deadline, cancellation signal and the verifier's current UTC
     * slot. At the commit point, check {@link #active()} and sample {@link #localSlot()}.
     */
    record Attempt(Admission.Deadline deadline, BooleanSupplier cancelled, Supplier<OptionalLong> utcSlot) {
        public boolean active() { return !cancelled.getAsBoolean() && !deadline.cancelled() && !deadline.expired(); }

        /** Empty before the Unix epoch. The value otherwise represents a uint64. */
        public OptionalLong localSlot() { return utcSlot.get(); }
    }

    Window window();

    /** Local snapshot only. Publish schedule changes independently of carrier processing. */
    Schedule localSchedule();

    /** Reads the authoritative floor after a unique outer MAC match. Zero is the initial floor. */
    CompletionStage<Long> floor(Attempt attempt);

    /** Side-effect-free lookup after outer authentication. Completes with null for an unavailable ticket. */
    CompletionStage<Ticket> lookup(Id id, Attempt attempt);

    /**
     * Atomically checks the attempt, current C (local slot or floor, whichever is greater),
     * the schedule version, generation eligibility, the issue window, the live ticket
     * version and the availability of both claims. On success record the nonce, consume a
     * single-use ID and advance the floor to C together. Keep the nonce through
     * issueSlot + skew (without wrap) and the single-use consumption through ticket expiry.
     * Retiring replay state must advance the floor in the same operation.
     */
    CompletionStage<Boolean> claim(Claim claim, Attempt attempt);

    /**
     * Queries the terminal outcome of the same commit ID after an exceptional claim
     * completion. This operation neither creates nor reverses a claim. A result arriving
     * after the original admission deadline only confirms accounting.
     */
    CompletionStage<Boolean> resolveClaim(Claim claim, Admission.Deadline deadline);

    /** Closes this authority, stops admissions and clears its owned key material. */
    @Override void close();

    private static void requireInterval(long first, long last) {
        if (Long.compareUnsigned(first, last) > 0) throw new IllegalArgumentException("invalid prefix slot interval");
    }

    static boolean containsSlot(long first, long last, long slot) {
        return Long.compareUnsigned(first, slot) <= 0 && Long.compareUnsigned(slot, last) <= 0;
    }

    static long later(long a, long b) { return Long.compareUnsigned(a, b) >= 0 ? a : b; }
}
