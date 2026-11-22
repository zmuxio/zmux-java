package io.zmux;

/** The result of one authorized inbound or carrier-backed outbound join. Calls may overlap. */
@FunctionalInterface
public interface JoinOutcome {
    /**
     * Runs outside the session loop. A successful pipe's events are held until this returns.
     * Waiting here for an event of this pipe would deadlock its delivery.
     * The pipe ID is {@link PipeId#UNASSIGNED} if local setup failed before assigning one. The error is null on success.
     */
    void ended(ZmuxSession session, PipeId pipe, Throwable error);
}
