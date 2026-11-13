package io.zmux;

/**
 * The peer's verified Reply refused the join, for lack of room: the carrier was closed and the
 * join refusal backoff started, so the next join waits. It is no failure of the session.
 */
public final class JoinRefusedException extends PipeException {
    JoinRefusedException(PipeId pipeId) {
        super("join refused by the peer", pipeId);
    }
}
