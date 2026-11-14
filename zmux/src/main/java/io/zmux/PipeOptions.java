package io.zmux;

import io.zmux.internal.core.Joins;
import java.time.Duration;
import java.util.Objects;

/**
 * The options of a pipe the application adds; {@link #defaults()} is a bidirectional pipe
 * that follows the peer's tier requests and the session's defaults. The runtime settings are
 * the fields of {@link PipeUpdate} with the same builder methods, where keeping a setting and
 * clearing it both mean its default; the others are fixed when the pipe is added. Immutable.
 */
public final class PipeOptions {
    /** The directions of a pipe from the local endpoint. */
    public enum Direction { BIDIRECTIONAL, SEND_ONLY, RECEIVE_ONLY }

    private static final PipeOptions DEFAULTS = builder().build();

    final Direction direction;
    /** The runtime settings the pipe starts with. */
    final PipeUpdate settings;
    final long fateGroup;
    final RecordMode recordMode;
    /** The key group a join exchanges keys in; 0: Group 1 when usable, otherwise Group 2. */
    final long keyGroup;
    final boolean returnOnReply;

    private PipeOptions(Builder b) {
        direction = b.direction;
        settings = b.settings.build();
        fateGroup = b.fateGroup;
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

    /** The pipe's liveness settings: its own, or (unwritten) every one following the session's. */
    PipeLiveness liveness() {
        return settings.liveness == PipeUpdate.Change.SET ? settings.livenessValue : PipeLiveness.inherit();
    }

    /** These options with the liveness settings of a pool decision over their own. */
    PipeOptions withLiveness(PipeLiveness over) {
        PipeLiveness own = liveness(), l = own.over(over);
        if (l.equals(own)) return this;
        Builder b = toBuilder();
        b.liveness(l);
        return b.build();
    }

    /** A builder holding these options. */
    public Builder toBuilder() {
        Builder b = new Builder();
        b.direction = direction;
        b.settings = settings.toBuilder();
        b.fateGroup = fateGroup;
        b.recordMode = recordMode;
        b.keyGroup = keyGroup;
        b.returnOnReply = returnOnReply;
        return b;
    }

    Joins.JoinOptions join() {
        PipeUpdate s = settings;
        PipeLiveness l = liveness();
        return Joins.JoinOptions.builder()
                .dir(switch (direction) {
                    case SEND_ONLY -> 0;
                    case RECEIVE_ONLY -> 1;
                    case BIDIRECTIONAL -> 2;
                })
                .sendPriority(s.sendPriority == PipeUpdate.Change.SET ? Long.valueOf(s.sendPriorityValue) : null)
                .sendStandby(s.sendStandby == PipeUpdate.Change.SET ? Boolean.valueOf(s.sendStandbyValue) : null)
                .recvPriority(s.receivePriority == PipeUpdate.Change.SET ? s.receivePriorityValue : 0)
                .recvStandby(s.receiveStandby == PipeUpdate.Change.SET && s.receiveStandbyValue)
                .weight(s.weight == PipeUpdate.Change.SET ? s.weightValue : 1)
                .probation(s.probation != PipeUpdate.Change.SET ? null
                        : s.probationValue == null ? Long.valueOf(0) : Long.valueOf(ZmuxConfig.nanos(s.probationValue)))
                .fateGroup(fateGroup)
                .suspectFloor(l.suspectFloorNanos())
                .deadTimeout(l.deadTimeoutNanos())
                .probeInterval(l.probeIntervalNanos())
                .probeJitter(l.probeJitterRaw())
                .mode(recordMode == null ? null : io.zmux.internal.join.Mode.of(recordMode))
                .group(keyGroup)
                .build();
    }

    /** Builds {@link PipeOptions}; every setter validates its value. */
    public static final class Builder {
        private Direction direction = Direction.BIDIRECTIONAL;
        private PipeUpdate.Builder settings = PipeUpdate.builder();
        private long fateGroup;
        private RecordMode recordMode;
        private long keyGroup;
        private boolean returnOnReply;

        private Builder() {}

        public Builder direction(Direction d) { direction = Objects.requireNonNull(d); return this; }

        /** {@link PipeUpdate.Builder#sendPriority(long)}; not set: the peer's request, else 0. */
        public Builder sendPriority(long tier) { settings.sendPriority(tier); return this; }

        /** {@link PipeUpdate.Builder#clearSendPriority}: the default. */
        public Builder clearSendPriority() { settings.clearSendPriority(); return this; }

        /** {@link PipeUpdate.Builder#receivePriority}; not set: the default, 0. */
        public Builder receivePriority(long tier) { settings.receivePriority(tier); return this; }

        /** {@link PipeUpdate.Builder#clearReceivePriority}: the default. */
        public Builder clearReceivePriority() { settings.clearReceivePriority(); return this; }

        /** {@link PipeUpdate.Builder#sendStandby}; not set: the peer's request, else available. */
        public Builder sendStandby(boolean on) { settings.sendStandby(on); return this; }

        /** {@link PipeUpdate.Builder#clearSendStandby}: the default. */
        public Builder clearSendStandby() { settings.clearSendStandby(); return this; }

        /** {@link PipeUpdate.Builder#receiveStandby(boolean)}; not set: available. */
        public Builder receiveStandby(boolean on) { settings.receiveStandby(on); return this; }

        /** {@link PipeUpdate.Builder#clearReceiveStandby}: the default. */
        public Builder clearReceiveStandby() { settings.clearReceiveStandby(); return this; }

        /** {@link PipeUpdate.Builder#weight}; not set: 1. */
        public Builder weight(long w) { settings.weight(w); return this; }

        /** {@link PipeUpdate.Builder#clearWeight}: the default. */
        public Builder clearWeight() { settings.clearWeight(); return this; }

        /** {@link PipeUpdate.Builder#probation}; not set: the session's. */
        public Builder probation(Duration d) { settings.probation(d); return this; }

        /** {@link PipeUpdate.Builder#noProbation}. */
        public Builder noProbation() { settings.noProbation(); return this; }

        /** {@link PipeUpdate.Builder#clearProbation}: the session's. */
        public Builder clearProbation() { settings.clearProbation(); return this; }

        /** {@link PipeUpdate.Builder#liveness}; not set: every one follows the session's. */
        public Builder liveness(PipeLiveness l) { settings.liveness(l); return this; }

        /** {@link PipeUpdate.Builder#clearLiveness}: the session's. */
        public Builder clearLiveness() { settings.clearLiveness(); return this; }

        /** {@link PipeUpdate.Builder#qualityBand(QualityBand)}; not set: the session's. */
        public Builder qualityBand(QualityBand b) { settings.qualityBand(b); return this; }

        /** {@link PipeUpdate.Builder#clearQualityBand}: the session's. */
        public Builder clearQualityBand() { settings.clearQualityBand(); return this; }

        /** Groups pipes that share a path; 0: an independent path. */
        public Builder fateGroup(long g) {
            if (g < 0 || g > io.zmux.internal.wire.VarInt.MAX) throw new IllegalArgumentException("fate group exceeds varint62");
            fateGroup = g;
            return this;
        }

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

        /** The carrier's record mode; not calling it: the configuration's {@link ZmuxConfig.Builder#recordPolicy(RecordPolicy)}, else Sealed. */
        public Builder recordMode(RecordMode m) {
            if (Objects.requireNonNull(m, "recordMode") == RecordMode.UNMANAGED) throw new IllegalArgumentException("a join has no Unmanaged record mode");
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

        /**
         * Builds the options.
         *
         * @throws IllegalArgumentException for a receive-only pipe with a quality band (the band governs outbound quality)
         */
        public PipeOptions build() {
            PipeOptions o = new PipeOptions(this);
            if (o.direction == Direction.RECEIVE_ONLY && o.settings.qualityBand == PipeUpdate.Change.SET)
                throw new IllegalArgumentException("a receive-only pipe has no outbound quality to band");
            return o;
        }
    }
}
