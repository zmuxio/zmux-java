package io.zmux;

/** A failure of one pipe of a session; the session goes on over its other pipes. */
public abstract sealed class PipeException extends ZmuxException permits PipeGoneException, PipeRefusedException, JoinRefusedException, JoinEvictedException {
    PipeException(String message) {
        super(message);
    }
}
