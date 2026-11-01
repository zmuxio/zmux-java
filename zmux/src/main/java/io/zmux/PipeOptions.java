package io.zmux;

import io.zmux.internal.core.Joins;
import java.time.Duration;

/**
 * The options of a pipe the application adds; {@link #defaults()} is a bidirectional pipe
 * that follows the peer's tier requests and the session's defaults. Immutable.
 */
public final class PipeOptions {
    /** The directions of a pipe from the local endpoint. */
    public enum Direction { BIDIRECTIONAL, SEND_ONLY, RECEIVE_ONLY }

    private static final PipeOptions DEFAULTS = builder().build();

    final Direction direction;
    final Long sendPriority;
    final Boolean sendStandby;
    final long receivePriority;
    final boolean receiveStandby;
    final long weight;
    final Duration probation;
    final long fateGroup;
    final Duration suspectFloor;
    final Duration deadTimeout;
    final Duration probeInterval;
    final boolean idleProbesOff;
    final double probeJitter;
    final boolean carrierBound;

    private PipeOptions(Builder b) {
        direction = b.direction;
        sendPriority = b.sendPriority;
        sendStandby = b.sendStandby;
        receivePriority = b.receivePriority;
        receiveStandby = b.receiveStandby;
        weight = b.weight;
        probation = b.probation;
        fateGroup = b.fateGroup;
        suspectFloor = b.suspectFloor;
        deadTimeout = b.deadTimeout;
        probeInterval = b.probeInterval;
        idleProbesOff = b.idleProbesOff;
        probeJitter = b.probeJitter;
        carrierBound = b.carrierBound;
    }

    public static PipeOptions defaults() {
        return DEFAULTS;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** These options with the probe settings of a pool decision; unset items keep the current ones. */
    PipeOptions withProbe(PoolPolicy.Decision d) {
        java.util.Optional<Duration> interval = d.probeInterval();
        java.util.OptionalDouble jitter = d.probeJitter();
        if (interval.isEmpty() && !d.idleProbesOff() && jitter.isEmpty()) return this;
        Builder b = toBuilder();
        interval.ifPresent(b::probeInterval);
        if (d.idleProbesOff()) b.disableIdleProbes();
        jitter.ifPresent(b::probeJitter);
        return b.build();
    }

    /** A builder holding these options. */
    public Builder toBuilder() {
        Builder b = new Builder();
        b.direction = direction; b.sendPriority = sendPriority; b.sendStandby = sendStandby; b.receivePriority = receivePriority;
        b.receiveStandby = receiveStandby; b.weight = weight; b.probation = probation; b.fateGroup = fateGroup;
        b.suspectFloor = suspectFloor; b.deadTimeout = deadTimeout; b.probeInterval = probeInterval; b.idleProbesOff = idleProbesOff; b.probeJitter = probeJitter;
        b.carrierBound = carrierBound;
        return b;
    }

    Joins.JoinOptions join() {
        return Joins.JoinOptions.builder()
                .dir(switch (direction) {
                    case SEND_ONLY -> 0;
                    case RECEIVE_ONLY -> 1;
                    case BIDIRECTIONAL -> 2;
                })
                .sendPriority(sendPriority)
                .sendStandby(sendStandby)
                .recvPriority(receivePriority)
                .recvStandby(receiveStandby)
                .weight(weight)
                .probation(probation == null ? null : ZmuxConfig.nanos(probation))
                .fateGroup(fateGroup)
                .suspectFloor(suspectFloor == null ? 0 : ZmuxConfig.nanos(suspectFloor))
                .deadTimeout(deadTimeout == null ? 0 : ZmuxConfig.nanos(deadTimeout))
                .probeInterval(idleProbesOff ? 0 : probeInterval == null ? -1 : ZmuxConfig.nanos(probeInterval))
                .probeJitter(probeJitter)
                .carrierBound(carrierBound)
                .build();
    }

    /** Builds {@link PipeOptions}; every setter validates its value. */
    public static final class Builder {
        private Direction direction = Direction.BIDIRECTIONAL;
        private Long sendPriority;
        private Boolean sendStandby;
        private long receivePriority;
        private boolean receiveStandby;
        private long weight = 1;
        private Duration probation;
        private long fateGroup;
        private Duration suspectFloor;
        private Duration deadTimeout;
        private Duration probeInterval;
        private boolean idleProbesOff;
        private double probeJitter = Double.NaN;
        private boolean carrierBound;

        private Builder() {}

        public Builder direction(Direction d) { direction = java.util.Objects.requireNonNull(d); return this; }

        /** The tier of the local outbound direction (lower is preferred); unset: the peer's request. */
        public Builder sendPriority(long tier) {
            if (tier < 0 || tier > io.zmux.carrier.VarInt.MAX) throw new IllegalArgumentException("tier exceeds varint62");
            sendPriority = tier;
            return this;
        }

        /** Makes the local outbound direction carry data only on failover; unset: the peer's request. */
        public Builder sendStandby(boolean on) { sendStandby = on; return this; }

        /** The tier requested for the peer's outbound direction. */
        public Builder receivePriority(long tier) {
            if (tier < 0 || tier > io.zmux.carrier.VarInt.MAX) throw new IllegalArgumentException("tier exceeds varint62");
            receivePriority = tier;
            return this;
        }

        /** Asks the peer to make its outbound direction standby. */
        public Builder receiveStandby(boolean on) { receiveStandby = on; return this; }

        /** The share for the round-robin and rate-weighted schedulers; 0 excludes the pipe unless all are 0. */
        public Builder weight(long w) {
            if (w < 0) throw new IllegalArgumentException("negative weight");
            weight = w;
            return this;
        }

        /** The base of the pipe's probation; zero disables it. Unset: the session's. */
        public Builder probation(Duration d) {
            if (d.isNegative()) throw new IllegalArgumentException("negative probation");
            probation = d;
            return this;
        }

        /** Groups pipes that share a path; 0: an independent path. */
        public Builder fateGroup(long g) {
            if (g < 0 || g > io.zmux.carrier.VarInt.MAX) throw new IllegalArgumentException("fate group exceeds varint62");
            fateGroup = g;
            return this;
        }

        /** The pipe's suspicion floor, at least 50 ms; unset: the session's. */
        public Builder suspectFloor(Duration d) {
            suspectFloor = MultipathConfig.atLeast(d, Duration.ofMillis(50), "suspectFloor");
            return this;
        }

        /** The pipe's dead timeout, at least 1 s; unset: the session's. */
        public Builder deadTimeout(Duration d) {
            deadTimeout = MultipathConfig.atLeast(d, Duration.ofSeconds(1), "deadTimeout");
            return this;
        }

        /**
         * The probe interval k of the pipe while it is idle, at least 1 s; unset: the session's.
         * Without an outbound direction at the peer at most 15 s takes effect.
         */
        public Builder probeInterval(Duration d) {
            MultipathConfig.probeInterval(d, "probeInterval");
            ZmuxConfig.nanos(d);
            probeInterval = d;
            idleProbesOff = false;
            return this;
        }

        /**
         * The idle probes of the pipe off, except without an outbound direction at the peer,
         * where they cannot be turned off.
         */
        public Builder disableIdleProbes() {
            probeInterval = null;
            idleProbesOff = true;
            return this;
        }

        /** The probe jitter j of the pipe, {@code 0 <= j < 1}: each probe waits a uniform draw from [k(1 - j), k]; unset: the session's. */
        public Builder probeJitter(double j) {
            probeJitter = MultipathConfig.jitter(j, "probeJitter");
            return this;
        }

        /** Ties the join to the new carrier's TLS 1.3 handshake (MULTIPATH.md 10.5); the carrier must be TLS. */
        public Builder carrierBound(boolean on) { carrierBound = on; return this; }

        public PipeOptions build() {
            return new PipeOptions(this);
        }
    }
}
