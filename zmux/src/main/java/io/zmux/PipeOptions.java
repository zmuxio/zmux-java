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
        settings = b.settings.build().asHeld();
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

    // ---- the options as a pipe holds them (ZmuxPipe#options): each setting set, or unset: the default, or the session's ----

    /** The directions of the pipe from this endpoint. */
    public Direction direction() { return direction; }

    /** The tier of the local outbound direction when set; empty: it follows the peer's request, else 0. */
    public java.util.OptionalLong sendPriority() { return settings.sendPriority == PipeUpdate.Change.SET ? java.util.OptionalLong.of(settings.sendPriorityValue) : java.util.OptionalLong.empty(); }

    /** The tier requested for the peer's outbound direction when set; empty: 0. */
    public java.util.OptionalLong receivePriority() { return settings.receivePriority == PipeUpdate.Change.SET ? java.util.OptionalLong.of(settings.receivePriorityValue) : java.util.OptionalLong.empty(); }

    /** Whether the local outbound direction is standby when set; empty: it follows the peer's request, else available. */
    public java.util.Optional<Boolean> sendStandby() { return settings.sendStandby == PipeUpdate.Change.SET ? java.util.Optional.of(settings.sendStandbyValue) : java.util.Optional.empty(); }

    /** Whether the peer's outbound direction is requested standby when set; empty: available. */
    public java.util.Optional<Boolean> receiveStandby() { return settings.receiveStandby == PipeUpdate.Change.SET ? java.util.Optional.of(settings.receiveStandbyValue) : java.util.Optional.empty(); }

    /** The weight when set; empty: 1. */
    public java.util.OptionalLong weight() { return settings.weight == PipeUpdate.Change.SET ? java.util.OptionalLong.of(settings.weightValue) : java.util.OptionalLong.empty(); }

    /** The probation base when set to a duration; empty when it follows the session's or is off ({@link #probationOff()}). */
    public java.util.Optional<Duration> probation() { return settings.probation == PipeUpdate.Change.SET ? java.util.Optional.ofNullable(settings.probationValue) : java.util.Optional.empty(); }

    /** Whether probation is set off for this pipe. */
    public boolean probationOff() { return settings.probation == PipeUpdate.Change.SET && settings.probationValue == null; }

    /** The pipe's liveness settings: its own, or (unwritten) every one following the session's. */
    public PipeLiveness liveness() {
        return settings.liveness == PipeUpdate.Change.SET ? settings.livenessValue : PipeLiveness.inherit();
    }

    /** The pipe's own quality band when set to one; empty when it follows the session's or is off ({@link #qualityBandOff()}). */
    public java.util.Optional<QualityBand> qualityBand() { return settings.qualityBand == PipeUpdate.Change.SET ? java.util.Optional.ofNullable(settings.qualityBandValue) : java.util.Optional.empty(); }

    /** Whether quality reports are set off for this pipe, whatever the session's band is. */
    public boolean qualityBandOff() { return settings.qualityBand == PipeUpdate.Change.SET && settings.qualityBandValue == null; }

    /** The group of pipes that share a path; 0: an independent path. */
    public long fateGroup() { return fateGroup; }

    /**
     * The record mode: as given to {@code addPipe}, empty when the configuration's record policy decides; a pipe
     * reads the mode actually in use, {@link RecordMode#UNMANAGED} for a first pipe without the secure carrier.
     */
    public java.util.Optional<RecordMode> recordMode() { return java.util.Optional.ofNullable(recordMode); }

    /** The key group: as given, empty for the default (Group 1 when usable, otherwise Group 2); a pipe reads the one its join used. */
    public java.util.OptionalLong keyGroup() { return keyGroup == 0 ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(keyGroup); }

    /** Whether adding the pipe returned at the verified Reply. */
    public boolean returnOnReply() { return returnOnReply; }

    /** These options once update was applied to a pipe holding them: what it sets is set, what it clears is unset. */
    PipeOptions applied(PipeUpdate update) {
        Builder b = toBuilder();
        b.settings = settings.applied(update).toBuilder();
        return b.buildHeld();
    }

    @Override public boolean equals(Object o) {
        return o instanceof PipeOptions p && direction == p.direction && fateGroup == p.fateGroup && recordMode == p.recordMode
                && keyGroup == p.keyGroup && returnOnReply == p.returnOnReply && settings.equals(p.settings);
    }

    @Override public int hashCode() { return Objects.hash(direction, fateGroup, recordMode, keyGroup, returnOnReply, settings); }

    /** These options with the liveness settings of a pool decision over their own. */
    PipeOptions withLiveness(PipeLiveness over) {
        PipeLiveness own = liveness(), l = own.over(over);
        if (l.equals(own)) return this;
        Builder b = toBuilder();
        b.liveness(l);
        return b.build();
    }

    /**
     * These options with the values the pipe was fixed as (direction, record mode, key group, fate group) in place of
     * those given: what a pipe holds is the actual value, also where the options left it to a policy or a default.
     */
    PipeOptions fixed(io.zmux.internal.core.Session.PipeFixed f) {
        Builder b = toBuilder();
        b.direction = switch (f.direction()) {
            case 0 -> Direction.SEND_ONLY;
            case 1 -> Direction.RECEIVE_ONLY;
            default -> Direction.BIDIRECTIONAL;
        };
        b.recordMode = io.zmux.internal.join.Mode.record(f.mode());
        b.keyGroup = f.keyGroup();
        b.fateGroup = f.fateGroup();
        return b.buildHeld();
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
                .sendPriority(s.sendPriority == PipeUpdate.Change.SET ? s.sendPriorityValue : null)
                .sendStandby(s.sendStandby == PipeUpdate.Change.SET ? s.sendStandbyValue : null)
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
                .mode(recordMode == null || recordMode == RecordMode.UNMANAGED ? null : io.zmux.internal.join.Mode.of(recordMode))
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

        /** {@link PipeUpdate.Builder#noQualityBand}: no quality reports for this pipe. */
        public Builder noQualityBand() { settings.noQualityBand(); return this; }

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
         * it ({@link ZmuxPipe#awaitAttached}); default false. Fixed when the pipe is added.
         * Before the peer attached it, closing the session's last other pipes loses the session
         * unless it resumes: an application that replaces pipes itself waits for the attachment
         * first.
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

        /** The options of a pipe the core already accepted, with no check of their combination. */
        private PipeOptions buildHeld() { return new PipeOptions(this); }
    }
}
