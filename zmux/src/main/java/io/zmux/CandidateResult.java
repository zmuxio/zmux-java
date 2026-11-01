package io.zmux;

import java.util.Objects;

/**
 * The outcome of one try of a candidate.
 *
 * @param joined whether it was tried as a further pipe of the established session
 * @param error the failure; null when usable
 */
public record CandidateResult(CarrierKind kind, CandidateOutcome outcome, boolean joined, Throwable error) {
    public CandidateResult {
        Objects.requireNonNull(kind);
        Objects.requireNonNull(outcome);
    }
}
