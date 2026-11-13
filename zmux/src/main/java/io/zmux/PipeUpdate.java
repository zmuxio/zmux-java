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
 * </ul>
 *
 * One update applies all its settings together (one that changes both receive settings sends one
 * PIPE_STATUS); an invalid value is rejected when it is set and changes nothing. The settings
 * fixed when the pipe is added ({@link PipeOptions}: direction, record mode, key group, label,
 * fate group, returnOnReply) are not here. Immutable.
 */
public final class PipeUpdate {
    /** A setting's change. */
    enum Change { KEEP, CLEAR, SET }

    final Change sendPriority, receivePriority, sendStandby, receiveStandby, weight, probation, liveness;
    final long sendPriorityValue, receivePriorityValue, weightValue;
    final boolean sendStandbyValue, receiveStandbyValue;
    /** Set probation; null with {@link Change#SET}: off. */
    final Duration probationValue;
    final PipeLiveness livenessValue;

    private PipeUpdate(Builder b) {
        sendPriority = b.sendPriority; receivePriority = b.receivePriority; sendStandby = b.sendStandby;
        receiveStandby = b.receiveStandby; weight = b.weight; probation = b.probation; liveness = b.liveness;
        sendPriorityValue = b.sendPriorityValue; receivePriorityValue = b.receivePriorityValue; weightValue = b.weightValue;
        sendStandbyValue = b.sendStandbyValue; receiveStandbyValue = b.receiveStandbyValue;
        probationValue = b.probationValue; livenessValue = b.livenessValue;
    }

    public static Builder builder() { return new Builder(); }

    /** Builds a {@link PipeUpdate}; the last call for a setting wins. */
    public static final class Builder {
        private Change sendPriority = Change.KEEP, receivePriority = Change.KEEP, sendStandby = Change.KEEP,
                receiveStandby = Change.KEEP, weight = Change.KEEP, probation = Change.KEEP, liveness = Change.KEEP;
        private long sendPriorityValue, receivePriorityValue, weightValue = 1;
        private boolean sendStandbyValue, receiveStandbyValue;
        private Duration probationValue;
        private PipeLiveness livenessValue;

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

        public PipeUpdate build() { return new PipeUpdate(this); }

        private static long tier(long t) {
            if (t < 0 || t > (1L << 62) - 1) throw new IllegalArgumentException("tier outside 0 to 2^62 - 1");
            return t;
        }
    }
}
