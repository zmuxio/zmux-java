package io.zmux;

import java.time.Duration;
import java.util.Optional;

/** Forward-path observations from explicitly matched DATAGRAM_ACK ranges. */
public record DatagramPathFeedback(PipeId pipe, Channel channel, long acknowledged, int pending, long expired,
                                   long unknown, int rttSamples, Optional<Duration> roundTrip,
                                   Optional<Duration> sampleAge) {
    /** The carriage of the acknowledged datagrams. */
    public enum Channel { RECORD, NATIVE }
}
