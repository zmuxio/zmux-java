package io.zmux;

import java.util.OptionalLong;

/**
 * A snapshot of one stream (API.md Section 7.2), in payload bytes, the preamble excluded. The
 * fields of a direction the stream does not have stay 0. Once the session released the
 * stream, {@link ZmuxStream#stats()} returns its final values.
 *
 * @param bytesWritten payload bytes the application handed to the sending part (API.md Section 7.1)
 * @param bytesSent payload bytes put into STREAM frames, each copy (reinjection, redundancy,
 *     opportunistic, retransmission) again
 * @param bytesAcknowledged distinct payload offsets acknowledged: passed to the carrier without
 *     multipath, covered by PIPE_ACK in a multipath session
 * @param bytesReceived distinct payload bytes received, read or not; after a stop, every byte
 *     below the highest offset received
 * @param bytesRead payload bytes returned to the application (API.md Section 7.1)
 * @param datagramsSent the stream's datagrams handed to a carrier
 * @param datagramsReceived the stream's datagrams received and queued for the application
 * @param datagramsDroppedRecv the stream's datagrams dropped from its full queue
 * @param datagramBytesSent payload bytes of the stream's datagrams handed to a carrier
 * @param datagramBytesReceived payload bytes of the stream's datagrams returned to the application
 * @param sendBuffered gauge: written bytes not yet released (unsent; unacknowledged in multipath)
 * @param recvBuffered gauge: received bytes not yet read
 * @param recvWindow gauge: the current, auto-tuned receive window
 * @param pipeId the pipe the sending direction is bound to under affinity; empty when unbound
 */
public record StreamStats(long bytesWritten, long bytesSent, long bytesAcknowledged, long bytesReceived, long bytesRead,
                          long datagramsSent, long datagramsReceived, long datagramsDroppedRecv, long datagramBytesSent,
                          long datagramBytesReceived, long sendBuffered, long recvBuffered, long recvWindow,
                          OptionalLong pipeId) {
    public StreamStats {
        java.util.Objects.requireNonNull(pipeId);
    }
}
