package io.zmux.shaping;

import java.time.Duration;

/**
 * Counters over a session.
 *
 * @param wireSpent wire bytes added: padding, splits, handshakes, injections; unsigned, saturating
 * @param stallSpent the waits applied
 * @param injected records of PADDING only sent
 * @param injectDropped injections not sent
 * @param clamped decisions reduced by a structural limit, a bound on one wait or a switch
 * @param faults calls that threw, of shapers and of the handshake shaper on a Join
 * @param handshakeUnplanned Join messages the handshake shaper had no plan for
 */
public record ShapingStats(long wireSpent, Duration stallSpent, long injected, long injectDropped, long clamped,
                           long faults, long handshakeUnplanned) {
    public static final ShapingStats ZERO = new ShapingStats(0, Duration.ZERO, 0, 0, 0, 0, 0);
}
