package io.zmux.shaping.machine;

import java.util.OptionalInt;

/**
 * The bounds of what a reader takes. Everything else about a definition is bounded by its own structure and the size of its text.
 *
 * @param maxBytes JSON input characters, tables included; unset is unbounded, zero allows none
 * @param maxSlots the counters and timers of all a candidate's machines together, which each pipe allocates and a short text
 *     could otherwise ask for without end: 0 is {@link #DEFAULT_MAX_SLOTS}, a negative value is no bound
 */
public record SizeLimits(OptionalInt maxBytes, int maxSlots) {
    /** The bound on a candidate's counters and timers when {@code maxSlots} is 0. */
    public static final int DEFAULT_MAX_SLOTS = 4096;

    public static final SizeLimits NONE = new SizeLimits(OptionalInt.empty(), 0);

    public SizeLimits {
        if (maxBytes.isPresent() && maxBytes.getAsInt() < 0) throw new IllegalArgumentException("negative size limit");
    }

    public SizeLimits withMaxBytes(int n) { return new SizeLimits(OptionalInt.of(n), maxSlots); }

    public SizeLimits withMaxSlots(int n) { return new SizeLimits(maxBytes, n); }

    void check(int n) throws DefinitionException {
        if (maxBytes.isPresent() && n > maxBytes.getAsInt()) {
            throw new DefinitionException(DefinitionException.Category.LIMIT, n + " bytes above the limit of " + maxBytes.getAsInt());
        }
    }

    boolean slotsOver(long n) {
        return maxSlots >= 0 && n > (maxSlots == 0 ? DEFAULT_MAX_SLOTS : maxSlots);
    }
}
