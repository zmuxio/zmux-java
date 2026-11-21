package io.zmux.internal.core;

/**
 * One core event for the driver; fields not used by the kind are zero or null.
 *
 * @param dir {@link Action#DIR_IN} or {@link Action#DIR_OUT}, for PIPE_STATE and the application's pipe kinds;
 *     {@link Action#DIR_BIDI} for the retirement
 * @param code the epoch (EPOCH_*), GOAWAY code, tier priority (TIER_SATURATION) or effective priority (PIPE_PRIORITY)
 * @param count the weight (PIPE_PRIORITY); the failures of the reason (JOIN_AUTH_FAILED), 1 as the core emits it;
 *     the payload (DATAGRAM_PATH)
 * @param frame the Locator (LOCATOR_*)
 * @param flag saturated (TIER_SATURATION), standby (PIPE_PRIORITY), or failed (the retirement: an outbound
 *     direction FAILED)
 * @param initiator for PIPE_PRIORITY: the peer's request changed the settings, not a local setting
 * @param rtt smoothed RTT (PIPE_QUALITY), nanoseconds
 * @param rate delivery rate (PIPE_QUALITY, CAPACITY_*), bytes per second
 * @param carrier the pipe's carrier kind, for the pipe kinds; null when undeclared
 */
public record Event(Kind kind, long pipe, int dir, Enum<?> state, SessionError error, long maxBidi, long maxUni,
                    long code, long count, byte[] frame, String reason, Features features, boolean initiator,
                    boolean flag, long rtt, double rate, io.zmux.CarrierKind carrier) {

    /** Event kinds. */
    public enum Kind {
        ESTABLISHED, CLOSED, GOAWAY_RECEIVED,
        /** Every state of a pipe direction, for the driver and the retirement; not an application event. */
        PIPE_STATE,
        /** The application's pipe events, one direction each. */
        PIPE_ADDED, PIPE_JOINED, PIPE_SUSPECT, PIPE_RECOVERED, PIPE_FAILED, PIPE_CLOSED,
        JOIN_AUTH_FAILED, SUSPENDED, RESUME_UNATTENDED, RESUMED,
        TIER_SATURATION, PIPE_PRIORITY, PIPE_QUALITY, CAPACITY_BELOW,
        CAPACITY_RESTORED,
        /** A session epoch (code) was committed or deleted. */
        EPOCH_COMMITTED, EPOCH_DELETED,
        /** A local Locator (frame) joins may now name, or no longer name; not an application event. */
        LOCATOR_ADDED, LOCATOR_REMOVED,
        /** The path datagram payload of a session datagram (count) changed. */
        DATAGRAM_PATH
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

    static Event pipe(Kind k, long pipe, io.zmux.CarrierKind carrier, int dir) {
        return new Event(k, pipe, dir, null, null, 0, 0, 0, 0, null, null, null, false, false, 0, 0, carrier);
    }

    /** Whether this is an application pipe event of a terminal state (failed or closed). */
    public boolean pipeTerminal() {
        return kind == Kind.PIPE_FAILED || kind == Kind.PIPE_CLOSED;
    }

    static Event pipeState(long pipe, io.zmux.CarrierKind carrier, int dir, Enum<?> state) {
        return new Event(Kind.PIPE_STATE, pipe, dir, state, null, 0, 0, 0, 0, null, null, null, false, false, 0, 0, carrier);
    }

    /** The retirement of a pipe; failed: its outbound direction FAILED (else it ended cleanly). */
    static Event pipeRetired(long pipe, io.zmux.CarrierKind carrier, boolean failed) {
        return new Event(Kind.PIPE_STATE, pipe, Action.DIR_BIDI, PipeInfo.Out.RETIRED, null, 0, 0, 0, 0, null, null, null, false, failed, 0, 0, carrier);
    }

    /** This event with another {@code count}. */
    public Event withCount(long n) {
        return new Event(kind, pipe, dir, state, error, maxBidi, maxUni, code, n, frame, reason, features, initiator, flag, rtt, rate, carrier);
    }

    static Event joinAuthFailed(String reason) {
        return new Event(Kind.JOIN_AUTH_FAILED, 0, 0, null, null, 0, 0, 0, 1, null, reason, null, false, false, 0, 0, null);
    }

    /** A new path datagram payload of a session datagram. */
    static Event datagramPath(int payload) {
        return new Event(Kind.DATAGRAM_PATH, 0, 0, null, null, 0, 0, 0, payload, null, null, null, false, false, 0, 0, null);
    }

    static Event epoch(Kind k, long n) {
        return new Event(k, 0, 0, null, null, 0, 0, n, 0, null, null, null, false, false, 0, 0, null);
    }

    static Event locator(Kind k, byte[] locator) {
        return new Event(k, 0, 0, null, null, 0, 0, 0, 0, locator, null, null, false, false, 0, 0, null);
    }

    static Event tierSaturation(long prio, boolean saturated) {
        return new Event(Kind.TIER_SATURATION, 0, 0, null, null, 0, 0, prio, 0, null, null, null, false, saturated, 0, 0, null);
    }

    static Event pipePriority(long pipe, io.zmux.CarrierKind carrier, long prio, boolean standby, long weight, boolean peer) {
        return new Event(Kind.PIPE_PRIORITY, pipe, 0, null, null, 0, 0, prio, weight, null, null, null, peer, standby, 0, 0, carrier);
    }

    static Event pipeQuality(long pipe, io.zmux.CarrierKind carrier, long rtt, double rate) {
        return new Event(Kind.PIPE_QUALITY, pipe, 0, null, null, 0, 0, 0, 0, null, null, null, false, false, rtt, rate, carrier);
    }

    static Event capacity(boolean below, double rate) {
        return new Event(below ? Kind.CAPACITY_BELOW : Kind.CAPACITY_RESTORED, 0, 0, null, null, 0, 0, 0, 0, null, null,
                null, false, false, 0, rate, null);
    }
}
