package io.zmux;

import java.util.List;

/**
 * How {@link Zmux#dial(CarrierMode, ZmuxConfig, ModePolicy)} chooses the carrier (API.md Section
 * 4.8), or a pool's members ({@link PipePool#mode}): one kind only ({@link #explicit}) or among
 * candidates ({@link #auto}), each of its own kind.
 */
public final class CarrierMode {
    private final boolean auto;
    private final List<Candidate> candidates;

    private CarrierMode(boolean auto, List<Candidate> candidates) {
        if (candidates.isEmpty()) throw new IllegalArgumentException("a carrier mode without a candidate");
        var kinds = java.util.EnumSet.noneOf(CarrierKind.class);
        for (Candidate c : candidates) {
            if (!kinds.add(java.util.Objects.requireNonNull(c, "candidate").kind())) throw new IllegalArgumentException("a carrier mode with two candidates of kind " + c.kind());
        }
        this.auto = auto;
        this.candidates = List.copyOf(candidates);
    }

    /** That candidate only: a failure is reported, never replaced by another kind. */
    public static CarrierMode explicit(Candidate candidate) { return new CarrierMode(false, List.of(candidate)); }

    /** Chooses among the candidates, in their order of preference (most preferred first). */
    public static CarrierMode auto(Candidate... candidates) { return new CarrierMode(true, List.of(candidates)); }

    /** Chooses among the candidates, in their order of preference (most preferred first). */
    public static CarrierMode auto(List<Candidate> candidates) { return new CarrierMode(true, candidates); }

    public boolean isAuto() { return auto; }

    public List<Candidate> candidates() { return candidates; }
}
