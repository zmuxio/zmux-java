package io.zmux;

/**
 * The negotiation results of a session (API.md Section 5.4).
 *
 * @param initiator whether this endpoint has the initiator role
 * @param peerIsZmux whether the peer sent zmux parameters (otherwise it is a plain QMux peer)
 * @param applicationProtocol the negotiated ALPN identifier, or "" when none
 * @param maxDatagramPayload the largest session datagram the peer accepts now, 0 when none
 */
public record ConnectionInfo(boolean established, boolean initiator, boolean peerIsZmux, String applicationProtocol,
                              boolean goAway, boolean preamble, boolean priority, boolean datagramFlows,
                              boolean pipeAck, boolean multipath, boolean lanesToPeer, int maxDatagramPayload) {}
