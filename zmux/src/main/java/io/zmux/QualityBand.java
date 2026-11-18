package io.zmux;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;

/**
 * When an outbound pipe reports a quality change ({@link SessionEvent.PipeQualityChanged}): its smoothed RTT
 * leaving +-{@link #rtt()} around the last reported value, or its delivery rate leaving
 * [last / {@link #rateRatio()}, last x {@link #rateRatio()}], at most once per {@link #minInterval()}. Built with
 * {@link #builder()}; a half whose method is not called is off, and a band needs at least one half. Every value is
 * checked where it is set, and the band when it is built: an invalid band is never made.
 */
public final class QualityBand {
    /** The default least time between two reports of a pipe, 1 s. */
    public static final Duration DEFAULT_MIN_INTERVAL = Duration.ofSeconds(1);

    private final Duration rtt;
    private final double rateRatio;
    private final Duration minInterval;

    private QualityBand(Builder b) { rtt = b.rtt; rateRatio = b.rateRatio; minInterval = b.minInterval; }

    public static Builder builder() { return new Builder(); }

    /** The RTT half: the departure that reports; empty when that half is off. */
    public Optional<Duration> rtt() { return Optional.ofNullable(rtt); }

    /** The rate half: the ratio that reports (1: every change); empty when that half is off. */
    public OptionalDouble rateRatio() { return rateRatio == 0 ? OptionalDouble.empty() : OptionalDouble.of(rateRatio); }

    /** The least time between two reports of a pipe; empty for no minimum. */
    public Optional<Duration> minInterval() { return Optional.ofNullable(minInterval); }

    /** The core's form: nanoseconds, 0 for off (RTT) or no minimum. */
    long rttNanos() { return rtt == null ? 0 : ZmuxConfig.nanos(rtt); }
    long minIntervalNanos() { return minInterval == null ? 0 : ZmuxConfig.nanos(minInterval); }
    /** The core's form: 0 for off. */
    double rawRateRatio() { return rateRatio; }

    @Override public boolean equals(Object o) {
        return o instanceof QualityBand q && Objects.equals(rtt, q.rtt) && Double.compare(rateRatio, q.rateRatio) == 0
                && Objects.equals(minInterval, q.minInterval);
    }

    @Override public int hashCode() { return Objects.hash(rtt, rateRatio, minInterval); }

    @Override public String toString() {
        return "QualityBand[rtt=" + (rtt == null ? "off" : rtt) + ", rateRatio=" + (rateRatio == 0 ? "off" : rateRatio)
                + ", minInterval=" + (minInterval == null ? "none" : minInterval) + "]";
    }

    /** Builds a {@link QualityBand}. */
    public static final class Builder {
        private Duration rtt;
        private double rateRatio;
        private Duration minInterval = DEFAULT_MIN_INTERVAL;

        private Builder() {}

        /** Turns the RTT half on: a report when the smoothed RTT departs by more than d, a positive duration. */
        public Builder rtt(Duration d) {
            Objects.requireNonNull(d, "rtt");
            if (d.isZero() || d.isNegative()) throw new IllegalArgumentException("rtt must be positive");
            rtt = d; return this;
        }

        /** Turns the rate half on: a report when the rate departs by more than the ratio r, a finite number of at least 1 (1: every change). */
        public Builder rateRatio(double r) {
            if (!Double.isFinite(r) || r < 1) throw new IllegalArgumentException("rateRatio must be a finite number of at least 1");
            rateRatio = r; return this;
        }

        /** The least time between two reports of a pipe, a positive duration; default {@link #DEFAULT_MIN_INTERVAL}. */
        public Builder minInterval(Duration d) {
            Objects.requireNonNull(d, "minInterval");
            if (d.isZero() || d.isNegative()) throw new IllegalArgumentException("minInterval must be positive");
            minInterval = d; return this;
        }

        /** No least time between two reports. */
        public Builder noMinInterval() { minInterval = null; return this; }

        /** The band; a band with both halves off is refused (clearing is {@link ZmuxSession#clearQualityBand()}). */
        public QualityBand build() {
            if (rtt == null && rateRatio == 0) throw new IllegalArgumentException("quality band with both halves off");
            return new QualityBand(this);
        }
    }
}
