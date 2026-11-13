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
    /** null: the session's; {@link Duration#ZERO}: off. */
    final Duration probation;
    final long fateGroup;
    final PipeLiveness liveness;
    final RecordMode recordMode;
    /** The key group a join exchanges keys in; 0: Group 1 when usable, otherwise Group 2. */
    final long keyGroup;
    final boolean returnOnReply;

    private PipeOptions(Builder b) {
        direction = b.direction;
        sendPriority = b.sendPriority;
        sendStandby = b.sendStandby;
        receivePriority = b.receivePriority;
        receiveStandby = b.receiveStandby;
        weight = b.weight;
        probation = b.probation;
        fateGroup = b.fateGroup;
        liveness = b.liveness;
        recordMode = b.recordMode;
        keyGroup = b.keyGroup;
        returnOnReply = b.returnOnReply;
    }

    public static PipeOptions defaults() {
        return DEFAULTS;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** These options with the liveness settings of a pool decision over their own. */
    PipeOptions withLiveness(PipeLiveness over) {
        PipeLiveness l = liveness.over(over);
        if (l.equals(liveness)) return this;
        Builder b = toBuilder();
        b.liveness = l;
        return b.build();
    }

    /** A builder holding these options. */
    public Builder toBuilder() {
        Builder b = new Builder();
        b.direction = direction; b.sendPriority = sendPriority; b.sendStandby = sendStandby; b.receivePriority = receivePriority;
        b.receiveStandby = receiveStandby; b.weight = weight; b.probation = probation; b.fateGroup = fateGroup;
        b.liveness = liveness;
        b.recordMode = recordMode;
        b.keyGroup = keyGroup;
        b.returnOnReply = returnOnReply;
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
                .suspectFloor(liveness.suspectFloorNanos())
                .deadTimeout(liveness.deadTimeoutNanos())
                .probeInterval(liveness.probeIntervalNanos())
                .probeJitter(liveness.probeJitterRaw())
                .mode(recordMode == null ? null : io.zmux.internal.join.Mode.of(recordMode))
                .group(keyGroup)
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
        private PipeLiveness liveness = PipeLiveness.inherit();
        private RecordMode recordMode;
        private long keyGroup;
        private boolean returnOnReply;

        private Builder() {}

        public Builder direction(Direction d) { direction = java.util.Objects.requireNonNull(d); return this; }

        /** The tier of the local outbound direction (lower is preferred); unset: the peer's request. */
        public Builder sendPriority(long tier) {
            if (tier < 0 || tier > io.zmux.internal.wire.VarInt.MAX) throw new IllegalArgumentException("tier exceeds varint62");
            sendPriority = tier;
            return this;
        }

        /** Makes the local outbound direction carry data only on failover; unset: the peer's request. */
        public Builder sendStandby(boolean on) { sendStandby = on; return this; }

        /** The tier requested for the peer's outbound direction. */
        public Builder receivePriority(long tier) {
            if (tier < 0 || tier > io.zmux.internal.wire.VarInt.MAX) throw new IllegalArgumentException("tier exceeds varint62");
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

        /** The base of the pipe's probation, positive; not calling it: the session's. */
        public Builder probation(Duration d) {
            if (java.util.Objects.requireNonNull(d, "probation").isNegative() || d.isZero()) throw new IllegalArgumentException("probation must be positive");
            probation = d;
            return this;
        }

        /** The pipe's probation off. */
        public Builder noProbation() { probation = Duration.ZERO; return this; }

        /** Groups pipes that share a path; 0: an independent path. */
        public Builder fateGroup(long g) {
            if (g < 0 || g > io.zmux.internal.wire.VarInt.MAX) throw new IllegalArgumentException("fate group exceeds varint62");
            fateGroup = g;
            return this;
        }

        /** The pipe's own liveness settings; not calling it: every one follows the session's. */
        public Builder liveness(PipeLiveness l) { liveness = java.util.Objects.requireNonNull(l, "liveness"); return this; }

        /**
         * The key group the join exchanges keys in, 1 or 2; not calling it: Group 1 when it is
         * usable in the session, otherwise Group 2. A key group the session cannot use falls back to
         * that default.
         */
        public Builder keyGroup(long g) {
            if (g < 1) throw new IllegalArgumentException("keyGroup " + g);
            keyGroup = g;
            return this;
        }

        /** The carrier's record mode; not calling it: the configuration's {@link ZmuxConfig.Builder#recordPolicy}, else Sealed. */
        public Builder recordMode(RecordMode m) {
            if (java.util.Objects.requireNonNull(m, "recordMode") == RecordMode.UNMANAGED) throw new IllegalArgumentException("a join has no Unmanaged record mode");
            recordMode = m;
            return this;
        }

        /**
         * Whether adding the pipe returns once the Reply verified instead of once the peer attached
         * it ({@link ZmuxPipe#awaitPeerAttached}); default false. Fixed when the pipe is added.
         * Before the peer attached it, closing the session's last other pipes loses the session
         * unless it resumes: an application that replaces pipes itself waits for the attachment
         * first. Pools and their redials always join this way and judge the attachment themselves.
         */
        public Builder returnOnReply(boolean on) { returnOnReply = on; return this; }

        public PipeOptions build() {
            return new PipeOptions(this);
        }
    }
}
