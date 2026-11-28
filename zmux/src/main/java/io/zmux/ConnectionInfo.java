package io.zmux;

import java.util.Optional;

/**
 * The negotiation results of a session as of its loop's last round: the complete list every zmux implementation reports. The
 * weakest record mode of its pipes is {@link ZmuxSession#weakestRecordMode()}, the largest
 * datagram payload of a stream {@link ZmuxStream#maxDatagramPayload(long)} and
 * {@link ZmuxStream#pathDatagramPayload(long)}.
 *
 * @param established whether establishment has completed
 * @param initiator whether this endpoint has the initiator role
 * @param qmuxVersion the QMux version in use, {@link #QMUX_VERSION} (the only one)
 * @param applicationProtocol the application protocol the bootstrap carrier negotiated, or "" when none
 * @param tls the TLS session of the handshake this process ran for the bootstrap carrier; empty where
 *     TLS ended outside the process, or the carrier has none or does not report it (the QUIC adapter)
 * @param peerIsZmux whether the peer sent zmux parameters (otherwise it is a plain QMux peer)
 * @param localDatagrams whether this endpoint offers datagrams
 * @param peerDatagrams whether the peer accepts datagrams
 * @param localResetStreamAt whether this endpoint advertises RESET_STREAM_AT (every zmux endpoint does)
 * @param peerResetStreamAt whether the peer accepts RESET_STREAM_AT
 * @param multipath whether multipath is in use
 * @param pipeAck whether pipe acknowledgement is in use
 * @param resume whether resume is in use
 * @param joinSecurityUnavailable both ends offered multipath and the peer has no secret source, so
 *     neither end can join (a local endpoint without one fails to start instead)
 * @param joinPending the session is join pending: it has no join keys yet ({@link JoinPendingException})
 * @param suspended whether the session is suspended, waiting for a join to resume it
 * @param peerGoAway the watermarks of the GOAWAY frames received and the latest code; empty while none was received
 * @param maxDatagramPayload the largest session datagram payload the peer accepts now, 0 when none
 * @param pathDatagramPayload the largest payload on an eligible path
 * @param nativeDatagramPayload the largest payload on an eligible native channel, zero when unavailable
 * @param nativeDatagramAvailable whether an eligible native channel can carry at least an empty datagram
 * @param preamble whether stream preambles are in use
 * @param priority whether stream priorities are in use
 * @param datagramFlows whether datagram flows are in use
 * @param lanesToPeer whether lanes toward the peer are available
 */
public record ConnectionInfo(boolean established, boolean initiator, String qmuxVersion, String applicationProtocol,
                             Optional<javax.net.ssl.SSLSession> tls, boolean peerIsZmux, boolean localDatagrams,
                             boolean peerDatagrams, boolean localResetStreamAt, boolean peerResetStreamAt,
                             boolean multipath, boolean pipeAck, boolean resume, boolean joinSecurityUnavailable,
                             boolean joinPending, boolean suspended, Optional<GoAway> peerGoAway, int maxDatagramPayload,
                             int pathDatagramPayload, int nativeDatagramPayload, boolean nativeDatagramAvailable,
                             boolean preamble, boolean priority, boolean datagramFlows, boolean lanesToPeer) {
    /** The QMux version of every session. */
    public static final String QMUX_VERSION = "draft-02";

    /**
     * Watermarks of the GOAWAY frames received (the lowest per stream type) and the latest code:
     * streams this endpoint opened at or above them were not and will not be processed by the peer.
     */
    public record GoAway(long maxBidi, long maxUni, long code) {}
}
