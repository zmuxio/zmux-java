package io.zmux.internal.core;

/**
 * Features in use.
 *
 * @param lanesOut lanes available toward the peer (we may open them)
 * @param lanesIn lanes available toward us (the peer may open them)
 * @param joinSecurityUnavailable both offered multipath and the peer has no secret source (a local endpoint without one fails to start)
 * @param carrierDatagrams DATAGRAM frames may go on the datagram channels of pipe carriers
 *    : both sent zmux_carrier_datagrams and datagrams are enabled
 */
public record Features(boolean role, boolean goAway, boolean preamble, boolean priority, boolean flows,
                       boolean multipath, boolean pipeAck, boolean resume, boolean lanesOut, boolean lanesIn,
                       boolean joinSecurityUnavailable, boolean carrierDatagrams, boolean carrierDatagramPadding) {

    public static final Features NONE = new Features(false, false, false, false, false, false, false, false, false,
            false, false, false, false);

    /** A session with acknowledgement. */
    public boolean acked() {
        return multipath || pipeAck;
    }
}
