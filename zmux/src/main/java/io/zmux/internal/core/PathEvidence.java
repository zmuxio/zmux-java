package io.zmux.internal.core;

import io.zmux.Direction;
import io.zmux.DirectionHealth;
import io.zmux.PipeHealth;
import java.time.Duration;
import java.util.Optional;

/** The protocol's outbound observations, in the session clock domain. */
public record PathEvidence(long generation, Optional<Timing> currentRtt, Optional<Timing> baselineRtt,
                           Optional<Delivery> delivery) {
    public static boolean matches(PipeHealth health, Direction direction, long generation) {
        var directional = direction == Direction.OUTBOUND ? health.send() : health.receive();
        return directional.state() != DirectionHealth.State.ABSENT && directional.evidenceGeneration() == generation;
    }

    /** An RTT observation and its validity interval. */
    public record Timing(long sampledAt, long validUntil, Duration rtt) {}

    /** The sender’s evidence about load during the delivery interval. */
    public enum Load { BACKLOGGED, LIMITED, UNKNOWN }

    /** Measured delivery over an acknowledged interval. */
    public record Delivery(long start, long end, long bytes, double bytesPerSecond, long validUntil, Load load) {}

    /** The directional health or measurement selected by a decision. */
    public enum Field { HEALTH, CURRENT_RTT, BASELINE_RTT, DELIVERY, LOSS }

    /** The identity and original validity of a specific observation. */
    public record Sample(Field field, long identity, long sampledAt, long validUntil,
                         Object connection, long counterGeneration) {}

    /** Metadata the owner needs to validate a carrier-provided interval. */
    public interface Loss {
        long sampledAt();
        long validUntil();
        Object connection();
        long counterGeneration();
    }
}
