package io.zmux;

import java.util.Objects;
import java.util.Optional;

/**
 * An immutable observation of the local sender on a connection, shared by its pipes and sessions.
 *
 * @param carrierId the connection's process-local identity
 * @param generation the unsigned generation, starting at zero and advancing on counter reset
 * @param direction the side whose counters were sampled
 * @param source the provider and its counter semantics
 * @param sampledAt the sampling time in the {@link System#nanoTime()} domain
 * @param counts the measured pair, empty if the provider could not report it
 */
public record CarrierSample(CarrierId carrierId, long generation, Direction direction, CarrierSource source,
                            long sampledAt, Optional<CarrierCounts> counts) {
    public CarrierSample {
        Objects.requireNonNull(carrierId);
        Objects.requireNonNull(direction);
        Objects.requireNonNull(source);
        Objects.requireNonNull(counts);
    }
}
