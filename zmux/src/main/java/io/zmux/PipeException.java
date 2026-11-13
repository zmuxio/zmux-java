package io.zmux;

/** A failure of one pipe of a session; the session goes on over its other pipes. */
public abstract sealed class PipeException extends ZmuxException permits PipeGoneException, PipeRefusedException, JoinRefusedException {
    private final PipeId pipeId;

    PipeException(String message, PipeId pipeId) {
        super(message);
        this.pipeId = pipeId;
    }

    /** The pipe's identifier; empty when it has none yet. */
    public java.util.Optional<PipeId> pipeId() { return java.util.Optional.ofNullable(pipeId); }
}
