package io.zmux;

import io.zmux.internal.core.Time;
import io.zmux.internal.wire.Capsule;
import java.time.Duration;
import java.util.Objects;

/** Capacity limits for a direction. A zero residence selects 30 seconds, a negative one disables the safety cap. */
public record SpliceLimits(int maxHookSteps, int maxOutputs, int maxSegments, int maxGroupBytes,
                          long maxBackingBytes, int maxQueuedUnits, long maxQueuedBytes, Duration maxResidence,
                          int reliableCleanupSlots, int datagramCleanupSlots) {
    public static final SpliceLimits DEFAULT = defaults(SpliceOptions.DEFAULT_CHUNK, Capsule.DEFAULT_MAX_DATAGRAM_PAYLOAD,
            Capsule.MAX_OPAQUE_VALUE);

    static SpliceLimits defaults(int chunk, int packet, int opaque) {
        int unit = largestUnit(chunk, packet, opaque);
        int group = Math.max(256 << 10, unit);
        return new SpliceLimits(16, 16, 256, group, Math.max(1 << 20, (long) unit + group),
                64, Math.max(4L << 20, unit), Duration.ZERO, 1, 4);
    }

    private static int largestUnit(int chunk, int packet, int opaque) {
        long unit = Math.max(chunk, Math.max((long) packet + Capsule.MAX_HEADER, (long) opaque + 16));
        if (unit > Integer.MAX_VALUE) throw new IllegalArgumentException("splice unit size overflow");
        return (int) unit;
    }

    public SpliceLimits {
        Objects.requireNonNull(maxResidence);
        if (maxHookSteps <= 0 || maxOutputs <= 0 || maxSegments <= 0 || maxGroupBytes <= 0
                || maxBackingBytes <= 0 || maxQueuedUnits <= 0 || maxQueuedBytes <= 0
                || reliableCleanupSlots <= 0 || datagramCleanupSlots <= 0)
            throw new IllegalArgumentException("invalid splice limits");
    }

    long deadline(long at) {
        if (maxResidence.isNegative()) return Time.NEVER;
        return Time.add(at, maxResidence.isZero() ? 30 * Time.SECOND : Time.nanos(maxResidence));
    }

    void validate(int chunk, int packet, int opaque, boolean packets) {
        int unit = packets ? largestUnit(chunk, packet, opaque) : Math.max(chunk, packet);
        if (maxGroupBytes < unit || maxQueuedBytes < unit || maxBackingBytes - unit < maxGroupBytes)
            throw new IllegalArgumentException("splice budget must hold a maximum unit and its outputs");
    }
}
