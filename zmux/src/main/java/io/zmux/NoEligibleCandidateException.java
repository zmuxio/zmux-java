package io.zmux;

/** The requested bootstrap candidate is absent from the controller's registrations. */
public final class NoEligibleCandidateException extends ZmuxException {
    public NoEligibleCandidateException(String candidate) {
        super("bootstrap candidate " + candidate + " is not registered");
    }
}
