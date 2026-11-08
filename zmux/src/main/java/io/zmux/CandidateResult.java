package io.zmux;

import java.util.Objects;

/**
 * The outcome of one try of a candidate.
 *
 * @param joined whether it was tried as a further pipe of the established session
 * @param error the failure; null when usable
 * @param variant the candidate's variant; "" when none
 */
public record CandidateResult(CarrierKind kind, CandidateOutcome outcome, boolean joined, Throwable error, String variant) {
    public CandidateResult {
        Objects.requireNonNull(kind);
        Objects.requireNonNull(outcome);
        if (variant == null) variant = "";
    }

    /** A candidate of no variant. */
    public CandidateResult(CarrierKind kind, CandidateOutcome outcome, boolean joined, Throwable error) {
        this(kind, outcome, joined, error, "");
    }
}
