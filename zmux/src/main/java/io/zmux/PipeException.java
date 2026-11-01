package io.zmux;

/** A failure of one pipe of a session; the session goes on over its other pipes. */
public abstract sealed class PipeException extends ZmuxException permits PipeGoneException, PipeRefusedException {
    private final long pipeId;

    PipeException(String message, long pipeId) {
        super(message);
        this.pipeId = pipeId;
    }

    /** The pipe's identifier, or -1 when it has none yet. */
    public long pipeId() { return pipeId; }
}
