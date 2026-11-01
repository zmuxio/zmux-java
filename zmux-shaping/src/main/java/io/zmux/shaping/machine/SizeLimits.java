package io.zmux.shaping.machine;

import java.util.Objects;
import java.util.OptionalInt;

/**
 * Bounds of the size of a definition; an unset bound is unbounded, one set to zero allows none.
 * Every bound set is zero or more.
 *
 * @param maxBytes JSON input characters
 * @param maxMachines machines
 * @param maxStates per machine
 * @param maxActions per state
 * @param maxTransitions per state
 * @param maxPoints points or components per distribution
 */
public record SizeLimits(OptionalInt maxBytes, OptionalInt maxMachines, OptionalInt maxStates, OptionalInt maxActions,
                         OptionalInt maxTransitions, OptionalInt maxPoints) {
    public static final SizeLimits NONE = new SizeLimits(OptionalInt.empty(), OptionalInt.empty(), OptionalInt.empty(),
            OptionalInt.empty(), OptionalInt.empty(), OptionalInt.empty());

    public SizeLimits {
        for (OptionalInt b : new OptionalInt[] {maxBytes, maxMachines, maxStates, maxActions, maxTransitions, maxPoints}) {
            if (Objects.requireNonNull(b).isPresent() && b.getAsInt() < 0) throw new IllegalArgumentException("negative size limit");
        }
    }

    public SizeLimits withMaxBytes(int n) { return new SizeLimits(OptionalInt.of(n), maxMachines, maxStates, maxActions, maxTransitions, maxPoints); }
    public SizeLimits withMaxMachines(int n) { return new SizeLimits(maxBytes, OptionalInt.of(n), maxStates, maxActions, maxTransitions, maxPoints); }
    public SizeLimits withMaxStates(int n) { return new SizeLimits(maxBytes, maxMachines, OptionalInt.of(n), maxActions, maxTransitions, maxPoints); }
    public SizeLimits withMaxActions(int n) { return new SizeLimits(maxBytes, maxMachines, maxStates, OptionalInt.of(n), maxTransitions, maxPoints); }
    public SizeLimits withMaxTransitions(int n) { return new SizeLimits(maxBytes, maxMachines, maxStates, maxActions, OptionalInt.of(n), maxPoints); }
    public SizeLimits withMaxPoints(int n) { return new SizeLimits(maxBytes, maxMachines, maxStates, maxActions, maxTransitions, OptionalInt.of(n)); }

    /** Fails when JSON input of n characters exceeds {@link #maxBytes()}; what names the input. */
    void checkBytes(int n, String what) throws DefinitionException {
        int limit = maxBytes.orElse(Integer.MAX_VALUE);
        if (n > limit) throw new DefinitionException(what + ": " + n + " bytes, limit " + limit);
    }
}
