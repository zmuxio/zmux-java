package io.zmux.internal.core;

/**
 * One core event for the driver; fields not used by the kind are zero or null.
 *
 * @param dir {@link Action#DIR_IN} or {@link Action#DIR_OUT}, for PIPE_STATE; {@link Action#DIR_BIDI} for the retirement
 * @param code GOAWAY code, tier priority (TIER_SATURATION) or effective priority (PIPE_PRIORITY)
 * @param count datagrams dropped so far, or the weight (PIPE_PRIORITY)
 * @param flag saturated (TIER_SATURATION) or standby (PIPE_PRIORITY)
 * @param rtt smoothed RTT (PIPE_QUALITY), nanoseconds
 * @param rate delivery rate (PIPE_QUALITY, CAPACITY_*), bytes per second
 * @param carrier the pipe's carrier kind, for the pipe kinds; null when undeclared
 */
public record Event(Kind kind, long pipe, int dir, Enum<?> state, SessionError error, long maxBidi, long maxUni,
                    long code, long count, byte[] frame, String reason, Features features, boolean initiator,
                    boolean flag, long rtt, double rate, io.zmux.CarrierKind carrier) {

    /** Event kinds. */
    public enum Kind {
        ESTABLISHED, CLOSED, GOAWAY_RECEIVED, PIPE_STATE, JOIN_AUTH_FAILED, SUSPENDED, RESUMED, DATAGRAM_DROPPED,
        OOB_JOIN, JOIN_SECURITY_UNAVAILABLE, TIER_SATURATION, PIPE_PRIORITY, PIPE_QUALITY, CAPACITY_BELOW,
        CAPACITY_RESTORED
    }

    static Event simple(Kind k) {
        return new Event(k, 0, 0, null, null, 0, 0, 0, 0, null, null, null, false, false, 0, 0, null);
    }

    static Event established(Features f, boolean initiator) {
        return new Event(Kind.ESTABLISHED, 0, 0, null, null, 0, 0, 0, 0, null, null, f, initiator, false, 0, 0, null);
    }

    static Event closed(SessionError e) {
        return new Event(Kind.CLOSED, 0, 0, null, e, 0, 0, 0, 0, null, null, null, false, false, 0, 0, null);
    }

    static Event goAway(long maxBidi, long maxUni, long code) {
        return new Event(Kind.GOAWAY_RECEIVED, 0, 0, null, null, maxBidi, maxUni, code, 0, null, null, null, false, false, 0, 0, null);
    }

    static Event datagramDropped(long total) {
        return new Event(Kind.DATAGRAM_DROPPED, 0, 0, null, null, 0, 0, 0, total, null, null, null, false, false, 0, 0, null);
    }

    static Event pipeState(long pipe, io.zmux.CarrierKind carrier, int dir, Enum<?> state) {
        return new Event(Kind.PIPE_STATE, pipe, dir, state, null, 0, 0, 0, 0, null, null, null, false, false, 0, 0, carrier);
    }

    static Event oobJoin(long pipe, byte[] frame) {
        return new Event(Kind.OOB_JOIN, pipe, 0, null, null, 0, 0, 0, 0, frame, null, null, false, false, 0, 0, null);
    }

    static Event joinAuthFailed(String reason) {
        return new Event(Kind.JOIN_AUTH_FAILED, 0, 0, null, null, 0, 0, 0, 0, null, reason, null, false, false, 0, 0, null);
    }

    static Event tierSaturation(long prio, boolean saturated) {
        return new Event(Kind.TIER_SATURATION, 0, 0, null, null, 0, 0, prio, 0, null, null, null, false, saturated, 0, 0, null);
    }

    static Event pipePriority(long pipe, io.zmux.CarrierKind carrier, long prio, boolean standby, long weight) {
        return new Event(Kind.PIPE_PRIORITY, pipe, 0, null, null, 0, 0, prio, weight, null, null, null, false, standby, 0, 0, carrier);
    }

    static Event pipeQuality(long pipe, io.zmux.CarrierKind carrier, long rtt, double rate) {
        return new Event(Kind.PIPE_QUALITY, pipe, 0, null, null, 0, 0, 0, 0, null, null, null, false, false, rtt, rate, carrier);
    }

    static Event capacity(boolean below, double rate) {
        return new Event(below ? Kind.CAPACITY_BELOW : Kind.CAPACITY_RESTORED, 0, 0, null, null, 0, 0, 0, 0, null, null,
                null, false, false, 0, rate, null);
    }
}
