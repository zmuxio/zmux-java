package io.zmux;

/**
 * A blocking stream call on the session's completion thread that would wait behind an asynchronous
 * operation of the same stream direction: that operation needs this thread to complete, so the call
 * fails at once instead of deadlocking. The rule is stated on {@link ZmuxFuture}.
 */
public final class CompletionThreadBlockedException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    /** For zmux's own use. */
    public CompletionThreadBlockedException(String message) {
        super(message);
    }
}
