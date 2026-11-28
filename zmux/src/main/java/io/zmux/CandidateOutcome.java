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
    /** Classifies the failure of a try. */
    static CandidateOutcome outcomeOf(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof CarrierRetryException || t instanceof HttpStatusException
                    || t instanceof HttpRedirectException || t instanceof java.net.ConnectException) {
                return CandidateOutcome.REFUSED;
            }
            if (t instanceof java.net.SocketTimeoutException || t instanceof java.util.concurrent.TimeoutException) return CandidateOutcome.TIMED_OUT;
            if (t instanceof SessionClosedException sc && sc.error() instanceof SessionError.HandshakeTimeout) return CandidateOutcome.TIMED_OUT;
            // A carrier that opened at once and failed later (an HTTP response refusing it): its pipe's error.
            if (t instanceof SessionClosedException sc && sc.error() instanceof SessionError.PipesLost(Throwable last) && last != null) {
                return outcomeOf(last);
            }
        }
        return CandidateOutcome.FAILED;
    }
}
