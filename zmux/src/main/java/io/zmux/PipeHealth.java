package io.zmux;

import java.time.Duration;
import java.util.OptionalLong;

/**
 * One session-loop snapshot of both directions of an attachment.
 *
 * @param joinedAt when Join succeeded, or when the bootstrap was established
 * @param attachedAt when the peer first confirmed attachment
 * @param sinceJoin age of the Join at observation, zero until Join succeeds
 */
public record PipeHealth(long seq, DirectionHealth send, DirectionHealth receive, boolean peerAttached,
                         OptionalLong joinedAt, OptionalLong attachedAt, Duration sinceJoin, Throwable failure) {
    public boolean alive() { return send.alive() && receive.alive(); }

    public boolean usable() { return peerAttached && carries(send) && carries(receive); }

    public boolean settled() { return peerAttached && settled(send) && settled(receive); }

    private static boolean carries(DirectionHealth direction) {
        return direction.state() == DirectionHealth.State.ABSENT || direction.canCarry();
    }

    private static boolean settled(DirectionHealth direction) {
        return direction.state() == DirectionHealth.State.ABSENT || direction.settled();
    }
}
