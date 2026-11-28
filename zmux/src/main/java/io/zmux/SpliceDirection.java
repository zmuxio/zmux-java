package io.zmux;

import java.util.Map;
import java.time.Duration;
import java.util.Optional;

/**
 * One direction of a splice: what it forwarded and how it ended.
 *
 * @param bytes reliable stream bytes submitted, including headers of source and injected capsules
 * @param datagramBytes payload bytes of datagrams submitted on the next hop or encoded as complete reliable capsules
 * @param datagramsAsCapsules datagrams fully submitted as reliable DATAGRAM capsules
 * @param end how the direction ended
 * @param cause the original failure, when available
 * @param inputUnits units admitted for policy or injection
 * @param inputBytes payload bytes admitted for policy or injection
 * @param enqueuedItems complete policy or injection outputs submitted
 * @param hookCalls callback executions
 * @param replacements replacement decisions
 * @param mirrorMisses samples refused by the mirror
 * @param waitTime time spent waiting for policy conditions
 */
public record SpliceDirection(long bytes, long datagramBytes, long datagramsAsCapsules, SpliceEnd end, Optional<Throwable> cause,
                              long inputUnits, long inputBytes, long enqueuedItems, long hookCalls, long replacements,
                              long preAdmissionRefundedBytes, long mirrorMisses, Duration waitTime, SpliceSettlement settlement,
                              Map<Drop, Long> dropCounts, long payloadCopyBytes, long encodeBytes,
                              long mirrorCopyBytes, long nativeCount, long recordCount) {
    public long capsuleCount() { return datagramsAsCapsules; }
    /** Why a unit was discarded by the splice. */
    public enum Drop {
        DROPPED_HOOK, DROPPED_EXPIRED, DROPPED_QUEUE_FULL, DROPPED_LIMITER, DROPPED_BLOCKED,
        DROPPED_TOO_LARGE, DROPPED_NO_PATH, DROPPED_SEQUENCE_EXHAUSTED, DROPPED_FLOW_CLOSED, DROPPED_CODEL
    }
    public long datagramsDropped() { return dropCounts.values().stream().mapToLong(Long::longValue).sum(); }
    public long datagramsTooLarge() { return dropCounts.getOrDefault(Drop.DROPPED_TOO_LARGE, 0L); }
    public long datagramsLimited() { return dropCounts.getOrDefault(Drop.DROPPED_LIMITER, 0L); }

    public SpliceDirection { dropCounts = Map.copyOf(dropCounts); }
}
