package io.zmux;

import java.time.Duration;
import java.util.Objects;

/**
 * A batch change of a live pipe's runtime settings, applied by {@link ZmuxPipe#update}. Each
 * setting has three states: not calling its methods keeps it; {@code x(v)} sets it;
 * {@code clearX()} clears it:
 *
 * <ul>
 *   <li>{@code sendPriority}, {@code sendStandby}: the local outbound direction (no frame). Set,
 *       the value wins over the peer's request; cleared, it follows the peer's latest PIPE_STATUS
 *       request, else the default (0, available).
 *   <li>{@code receivePriority}, {@code receiveStandby}: the request for the peer's outbound
 *       direction. Set, a PIPE_STATUS goes when the value differs from the request in force;
 *       cleared, the default (0, available) is requested again, a PIPE_STATUS going only when a
 *       non-default request was sent before.
 *   <li>{@code weight}: set, that share (0 excludes the pipe); cleared, the default 1.
 *   <li>{@code probation}: set to a duration or off ({@link Builder#noProbation()}); cleared, the
 *       session's.
 *   <li>{@code liveness}: set, the pipe's {@link PipeLiveness} as a whole; cleared, the session's.
 *   <li>{@code qualityBand}: set, the pipe's own quality event band ({@link QualityBand}, outbound
 *       pipes only), or off ({@link Builder#noQualityBand()}: no reports for this pipe whatever the session's band
 *       is); cleared, the session's ({@link ZmuxSession#setQualityBand}).
 * </ul>
 *
 * One update applies all its settings together (one that changes both receive settings sends one
 * PIPE_STATUS); an invalid value is rejected when it is set and changes nothing. The settings
 * fixed when the pipe is added ({@link PipeOptions}: direction, record mode, key group, fate
 * group, returnOnReply) are not here; {@link PipeOptions.Builder} takes these same settings, where
 * keeping and clearing both mean the default. Immutable.
 */
public final class PipeUpdate {
    /** A setting's change. */
    enum Change { KEEP, CLEAR, SET }

    /** The update that changes nothing. */
    static final PipeUpdate KEEP_ALL = builder().build();

    final Change sendPriority, receivePriority, sendStandby, receiveStandby, weight, probation, liveness, qualityBand;
    final long sendPriorityValue, receivePriorityValue, weightValue;
    final boolean sendStandbyValue, receiveStandbyValue;
    /** Set probation; null with {@link Change#SET}: off. */
    final Duration probationValue;
    final PipeLiveness livenessValue;
    final QualityBand qualityBandValue;

    private PipeUpdate(Builder b) {
        sendPriority = b.sendPriority; receivePriority = b.receivePriority; sendStandby = b.sendStandby;
        receiveStandby = b.receiveStandby; weight = b.weight; probation = b.probation; liveness = b.liveness;
        sendPriorityValue = b.sendPriorityValue; receivePriorityValue = b.receivePriorityValue; weightValue = b.weightValue;
        sendStandbyValue = b.sendStandbyValue; receiveStandbyValue = b.receiveStandbyValue;
        probationValue = b.probationValue; livenessValue = b.livenessValue;
        qualityBand = b.qualityBand; qualityBandValue = b.qualityBandValue;
    }

    /** Whether no setting but the quality band changes. */
    boolean onlyBand() {
        return sendPriority == Change.KEEP && sendStandby == Change.KEEP && receivePriority == Change.KEEP
                && receiveStandby == Change.KEEP && weight == Change.KEEP && probation == Change.KEEP && liveness == Change.KEEP;
    }

    /** This update with every clear taken as keep: the form options are held in, where clearing and keeping both mean the default. */
    PipeUpdate asHeld() {
        Builder b = toBuilder();
        b.sendPriority = held(sendPriority); b.receivePriority = held(receivePriority); b.sendStandby = held(sendStandby);
        b.receiveStandby = held(receiveStandby); b.weight = held(weight); b.probation = held(probation);
        b.liveness = held(liveness); b.qualityBand = held(qualityBand);
        return b.build();
    }

    private static Change held(Change c) { return c == Change.CLEAR ? Change.KEEP : c; }

    /**
     * The settings a pipe holding these ones holds once u was applied to it: what u sets replaces them, what u clears
     * is the default again, what it keeps stays. Held form: no clears.
     */
    PipeUpdate applied(PipeUpdate u) {
        Builder b = toBuilder();
        if (u.sendPriority != Change.KEEP) { b.sendPriority = held(u.sendPriority); b.sendPriorityValue = u.sendPriorityValue; }
        if (u.receivePriority != Change.KEEP) { b.receivePriority = held(u.receivePriority); b.receivePriorityValue = u.receivePriorityValue; }
        if (u.sendStandby != Change.KEEP) { b.sendStandby = held(u.sendStandby); b.sendStandbyValue = u.sendStandbyValue; }
        if (u.receiveStandby != Change.KEEP) { b.receiveStandby = held(u.receiveStandby); b.receiveStandbyValue = u.receiveStandbyValue; }
        if (u.weight != Change.KEEP) { b.weight = held(u.weight); b.weightValue = u.weightValue; }
        if (u.probation != Change.KEEP) { b.probation = held(u.probation); b.probationValue = u.probationValue; }
        if (u.liveness != Change.KEEP) { b.liveness = held(u.liveness); b.livenessValue = u.livenessValue; }
        if (u.qualityBand != Change.KEEP) { b.qualityBand = held(u.qualityBand); b.qualityBandValue = u.qualityBandValue; }
        return b.build();
    }

    /** Equal when every setting is in the same state and the set ones have the same value. */
    @Override public boolean equals(Object o) {
        return o instanceof PipeUpdate u
                && sendPriority == u.sendPriority && receivePriority == u.receivePriority && sendStandby == u.sendStandby
                && receiveStandby == u.receiveStandby && weight == u.weight && probation == u.probation
                && liveness == u.liveness && qualityBand == u.qualityBand
                && (sendPriority != Change.SET || sendPriorityValue == u.sendPriorityValue)
                && (receivePriority != Change.SET || receivePriorityValue == u.receivePriorityValue)
                && (sendStandby != Change.SET || sendStandbyValue == u.sendStandbyValue)
                && (receiveStandby != Change.SET || receiveStandbyValue == u.receiveStandbyValue)
                && (weight != Change.SET || weightValue == u.weightValue)
                && (probation != Change.SET || Objects.equals(probationValue, u.probationValue))
                && (liveness != Change.SET || Objects.equals(livenessValue, u.livenessValue))
                && (qualityBand != Change.SET || Objects.equals(qualityBandValue, u.qualityBandValue));
    }

    @Override public int hashCode() {
        return Objects.hash(sendPriority, receivePriority, sendStandby, receiveStandby, weight, probation, liveness, qualityBand,
                sendPriority == Change.SET ? sendPriorityValue : 0, receivePriority == Change.SET ? receivePriorityValue : 0,
                sendStandby == Change.SET && sendStandbyValue, receiveStandby == Change.SET && receiveStandbyValue,
                weight == Change.SET ? weightValue : 0, probation == Change.SET ? probationValue : null,
                liveness == Change.SET ? livenessValue : null, qualityBand == Change.SET ? qualityBandValue : null);
    }

    /** A builder holding this update. */
    Builder toBuilder() {
        Builder b = new Builder();
        b.sendPriority = sendPriority; b.receivePriority = receivePriority; b.sendStandby = sendStandby;
        b.receiveStandby = receiveStandby; b.weight = weight; b.probation = probation; b.liveness = liveness;
        b.sendPriorityValue = sendPriorityValue; b.receivePriorityValue = receivePriorityValue; b.weightValue = weightValue;
        b.sendStandbyValue = sendStandbyValue; b.receiveStandbyValue = receiveStandbyValue;
        b.probationValue = probationValue; b.livenessValue = livenessValue;
        b.qualityBand = qualityBand; b.qualityBandValue = qualityBandValue;
        return b;
    }

    public static Builder builder() { return new Builder(); }

    /** Builds a {@link PipeUpdate}; the last call for a setting wins. */
    public static final class Builder {
        private Change sendPriority = Change.KEEP, receivePriority = Change.KEEP, sendStandby = Change.KEEP,
                receiveStandby = Change.KEEP, weight = Change.KEEP, probation = Change.KEEP, liveness = Change.KEEP,
                qualityBand = Change.KEEP;
        private long sendPriorityValue, receivePriorityValue, weightValue = 1;
        private boolean sendStandbyValue, receiveStandbyValue;
        private Duration probationValue;
        private PipeLiveness livenessValue;
        private QualityBand qualityBandValue;

        private Builder() {}

        /** The tier of the local outbound direction (lower is preferred), from 0 to 2^62 - 1. */
        public Builder sendPriority(long tier) { sendPriorityValue = tier(tier); sendPriority = Change.SET; return this; }

        /** The local outbound tier follows the peer's request again. */
        public Builder clearSendPriority() { sendPriority = Change.CLEAR; return this; }

        /** The tier requested for the peer's outbound direction, from 0 to 2^62 - 1. */
        public Builder receivePriority(long tier) { receivePriorityValue = tier(tier); receivePriority = Change.SET; return this; }

        /** Requests the default tier, 0, for the peer's outbound direction again. */
        public Builder clearReceivePriority() { receivePriority = Change.CLEAR; return this; }

        /** Whether the local outbound direction carries data only on failover. */
        public Builder sendStandby(boolean on) { sendStandbyValue = on; sendStandby = Change.SET; return this; }

        /** The local outbound standby follows the peer's request again. */
        public Builder clearSendStandby() { sendStandby = Change.CLEAR; return this; }

        /** Asks the peer to make its outbound direction standby, or available. */
        public Builder receiveStandby(boolean on) { receiveStandbyValue = on; receiveStandby = Change.SET; return this; }

        /** Requests the default, available, for the peer's outbound direction again. */
        public Builder clearReceiveStandby() { receiveStandby = Change.CLEAR; return this; }

        /** The share for the round-robin and rate-weighted schedulers; 0 excludes the pipe unless all are 0. */
        public Builder weight(long w) {
            if (w < 0) throw new IllegalArgumentException("negative weight");
            weightValue = w; weight = Change.SET; return this;
        }

        /** The default weight, 1, again. */
        public Builder clearWeight() { weight = Change.CLEAR; return this; }

        /** The base of the pipe's probation, positive. */
        public Builder probation(Duration d) {
            if (Objects.requireNonNull(d, "probation").isNegative() || d.isZero()) throw new IllegalArgumentException("probation must be positive");
            probationValue = d; probation = Change.SET; return this;
        }

        /** The pipe's probation off. */
        public Builder noProbation() { probationValue = null; probation = Change.SET; return this; }

        /** The pipe's probation follows the session's again. */
        public Builder clearProbation() { probation = Change.CLEAR; return this; }

        /** The pipe's own liveness settings, as a whole; a setting it leaves unwritten follows the session's. */
        public Builder liveness(PipeLiveness l) { livenessValue = Objects.requireNonNull(l, "liveness"); liveness = Change.SET; return this; }

        /** The pipe's liveness settings follow the session's again. */
        public Builder clearLiveness() { liveness = Change.CLEAR; return this; }

        /** The pipe's own quality event band; the pipe must have an outbound direction. */
        public Builder qualityBand(QualityBand b) { qualityBandValue = Objects.requireNonNull(b, "qualityBand"); qualityBand = Change.SET; return this; }

        /** The pipe's quality event band follows the session's again. */
        public Builder clearQualityBand() { qualityBand = Change.CLEAR; return this; }

        /** No quality reports for this pipe, whatever the session's band is; the pipe must have an outbound direction. */
        public Builder noQualityBand() { qualityBandValue = null; qualityBand = Change.SET; return this; }

        public PipeUpdate build() { return new PipeUpdate(this); }

        private static long tier(long t) {
            if (t < 0 || t > io.zmux.internal.wire.VarInt.MAX) throw new IllegalArgumentException("tier outside 0 to 2^62 - 1");
            return t;
        }
    }
}
