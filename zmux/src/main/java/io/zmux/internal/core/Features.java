package io.zmux.internal.core;

/**
 * Features in use (SPEC.md Section 5.5).
 *
 * @param lanesOut lanes available toward the peer (we may open them)
 * @param lanesIn lanes available toward us (the peer may open them)
 * @param joinSecurityUnavailable both offered multipath, no secret source matched
 * @param carrierDatagrams DATAGRAM frames may go on the datagram channels of pipe carriers
 *     (EXTENSIONS.md Section 6.8): both sent zmux_carrier_datagrams and datagrams are enabled
 */
public record Features(boolean role, boolean goAway, boolean preamble, boolean priority, boolean flows,
                       boolean multipath, boolean pipeAck, boolean resume, boolean lanesOut, boolean lanesIn,
                       boolean joinSecurityUnavailable, boolean carrierDatagrams) {

    public static final Features NONE = new Features(false, false, false, false, false, false, false, false, false,
            false, false, false);

    /** A session with acknowledgement (SPEC.md Section 6.4). */
    public boolean acked() {
        return multipath || pipeAck;
    }
}
