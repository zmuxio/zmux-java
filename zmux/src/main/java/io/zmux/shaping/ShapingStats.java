package io.zmux.shaping;

import java.time.Duration;

/**
 * Counters over a session.
 *
 * @param paddingBytes PADDING bytes added
 * @param injected records of PADDING only sent
 * @param injectDropped injections not sent
 * @param clamped decisions reduced to limits
 * @param pauseTotal pauses and delays applied
 * @param faults calls that threw
 */
public record ShapingStats(long paddingBytes, long injected, long injectDropped, long clamped, Duration pauseTotal,
                           long faults) {
    public static final ShapingStats ZERO = new ShapingStats(0, 0, 0, 0, Duration.ZERO, 0);
}
