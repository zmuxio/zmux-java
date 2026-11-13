package io.zmux;

import java.util.List;

/**
 * How {@link Zmux#dial(CarrierChoice, ZmuxConfig, CarrierChoicePolicy)} chooses the carrier, or a pool's members ({@link PipePool#choice}): one kind only ({@link #explicit}) or among
 * candidates ({@link #auto}), each of its own kind.
 */
public final class CarrierChoice {
    private final boolean auto;
    private final List<Candidate> candidates;

    private CarrierChoice(boolean auto, List<Candidate> candidates) {
        if (candidates.isEmpty()) throw new IllegalArgumentException("a carrier choice without a candidate");
        var kinds = java.util.EnumSet.noneOf(CarrierKind.class);
        for (Candidate c : candidates) {
            if (!kinds.add(java.util.Objects.requireNonNull(c, "candidate").kind())) throw new IllegalArgumentException("a carrier choice with two candidates of kind " + c.kind());
        }
        this.auto = auto;
        this.candidates = List.copyOf(candidates);
    }

    /** That candidate only: a failure is reported, never replaced by another kind. */
    public static CarrierChoice explicit(Candidate candidate) { return new CarrierChoice(false, List.of(candidate)); }

    /** Chooses among the candidates, in their order of preference (most preferred first). */
    public static CarrierChoice auto(Candidate... candidates) { return new CarrierChoice(true, List.of(candidates)); }

    /** Chooses among the candidates, in their order of preference (most preferred first). */
    public static CarrierChoice auto(List<Candidate> candidates) { return new CarrierChoice(true, candidates); }

    public boolean isAuto() { return auto; }

    public List<Candidate> candidates() { return candidates; }
}
