package io.zmux;

import java.time.Duration;
import java.util.Optional;

/** Outbound protocol evidence from one attachment, in the session clock domain. */
public record PathQuality(long generation, Optional<Timing> currentRtt, Optional<Timing> baselineRtt,
                          Optional<Delivery> delivery) {
    /** A local protocol RTT with its original observation and validity. */
    public record Timing(long sampledAt, long validUntil, Duration rtt) {}

    /** Whether the sender observed a limit or lacks proof about the whole interval. */
    public enum DeliveryLoad { BACKLOGGED, LIMITED, UNKNOWN }

    /** Bytes acknowledged over the actual interval, with its source load classification. */
    public record Delivery(long start, long end, long bytes, double bytesPerSecond, long validUntil,
                           DeliveryLoad load) {}
}
