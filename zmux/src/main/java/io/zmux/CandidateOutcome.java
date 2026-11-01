package io.zmux;

import java.util.Locale;

/** The outcome of one try of a {@link Candidate}. */
public enum CandidateOutcome {
    /** The peer's transport parameters (for a join, the join's confirmation) arrived within the timer. */
    USABLE,
    /** The carrier was opened but the timer expired; it was closed without CONNECTION_CLOSE. */
    TIMED_OUT,
    /** The server or an intermediary refused the carrier, such as with an HTTP status other than 2xx or a refused connection. */
    REFUSED,
    /** Any other failure. */
    FAILED;

    /** "usable", "timed_out", "refused" or "failed". */
    @Override public String toString() { return name().toLowerCase(Locale.ROOT); }
}
