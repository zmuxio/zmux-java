package io.zmux;

import java.time.Duration;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * A snapshot of one attachment (API.md Section 7.2), taken on the session's loop. A field that
 * can be absent is null (objects and durations) or an empty {@link OptionalLong} (counts,
 * positions, IDs, rates and points in time). Rates are bytes per second; {@link #deliveryRate()} is a sender
 * estimate, not guaranteed bandwidth. Points in time are {@link System#nanoTime()} values. The
 * pipe's addresses are carrier metadata (API.md Section 4.1), not statistics.
 *
 * @param pipe the pipe ID
 * @param local this endpoint joined the attachment (the dialer, for the bootstrap pipe)
 * @param fateGroup its fate group, 0 for none
 * @param label the label the application gave its carrier, or null
 * @param carrier its carrier's kind, or null when the adapter declares none
 * @param outbound the outbound direction's state, or null without one
 * @param inbound the inbound direction's state, or null without one
 * @param attachedFor time since the attachment was made
 * @param suspectCount times the outbound pipe became suspect
 * @param failureCount 0 or 1: whether the attachment failed
 * @param failure why it failed, or null
 * @param sendPriority the outbound direction's effective priority (lower preferred)
 * @param sendPrioritySource where it comes from
 * @param standby the outbound direction's effective standby flag
 * @param standbySource where it comes from
 * @param receivePriority the priority requested of the peer for its outbound direction
 * @param receiveStandby the standby flag requested of the peer
 * @param requestSeq the sequence number of the last PIPE_STATUS request sent
 * @param peerRequest the peer's latest request for the outbound direction, or null before one
 * @param weight the outbound direction's weight
 * @param probationUntil the end of the outbound pipe's probation; empty outside probation or when it never ends
 * @param inProbation whether the outbound pipe is in probation now
 * @param revalidating awaiting revalidation
 * @param eligible eligible under tier gating now
 * @param writable the outbound carrier takes bytes now
 * @param saturated the outbound pipe's tier is saturated
 * @param rttSource where smoothedRtt and rttvar come from; null before the first sample
 * @param smoothedRtt estimate of the outbound pipe; null before the first sample
 * @param minRtt the carrier's minimum where it reports one, else the 300 s minimum of the samples
 * @param rttvar the RTT variation estimate
 * @param oneWayDelay estimate; null without acknowledgements
 * @param deliveryRate R_p; empty without acknowledgements
 * @param queuedBytes gauge: bytes the outbound carrier holds unsent, the adapter's unsent count over the lanes;
 *     empty where the adapter reports none ({@link io.zmux.carrier.PipeWriter#unsent()}). It is the carrier's part
 *     of Q_p only; the bytes of a write call in progress are not reported separately
 * @param queueDelay estimate: queuedBytes / R_p; null when either is absent
 * @param bytesInFlight gauge: bytes sent and not acknowledged; empty without acknowledgements
 * @param bytesSent the outbound position over all lanes
 * @param bytesReceived the inbound position over all lanes
 * @param bytesAcked the acknowledged outbound position; empty without acknowledgements
 * @param recordsSent records written
 * @param recordsReceived records completely received
 * @param bytesReinjected stream bytes sent again elsewhere because this pipe failed or became suspect
 * @param bytesLost the outbound bytes sent and not acknowledged when the pipe failed, over all lanes (0 before a
 *     failure); empty without acknowledgements. Unlike bytesReinjected (stream payload sent again elsewhere) it
 *     counts this pipe's carrier bytes, frame overhead included
 * @param recordsLost bytesLost in records, estimated as bytesLost &times; recordsSent / bytesSent rounded up; empty
 *     without acknowledgements
 * @param bytesRetransmitted the bytes the carrier itself retransmitted, as its adapter reports them
 *     ({@link io.zmux.carrier.PipeWriter#retransmitted()}); pipes that share one carrier connection report that
 *     connection's count; empty where the adapter reports none
 * @param lossRate the carrier's smoothed loss rate: an EWMA (gain 1/8) of the bytes retransmitted per byte sent
 *     between samples at least 1 s apart; empty without a retransmission count and before the first sample
 * @param bytesOpportunistic opportunistic copies sent on the outbound pipe
 * @param bytesRedundant redundant copies sent on the outbound pipe
 * @param bytesDuplicateReceived duplicate stream bytes received on the inbound pipe
 * @param pingsSent PIPE_PING frames sent
 * @param pingsAnswered of these, those answered by an acknowledgement
 * @param sinceLastAck time since the acknowledged position last advanced; null without an
 *     outbound direction or acknowledgements
 * @param sinceLastReceive time since the inbound pipe last completed a record; null without one
 * @param sinceLastTraffic time since a record carrying a traffic frame (STREAM, RESET_STREAM,
 *     RESET_STREAM_AT, STOP_SENDING or DATAGRAM) was sent or completely received on the pipe, or
 *     since it became live; null while neither direction is live
 * @param lanesOut gauge: the lanes of the outbound direction, lane 0 included
 * @param lanesIn gauge: the lanes of the inbound direction, lane 0 included
 * @param lanesRefused own lanes of the outbound direction the peer refused
 * @param lanes per lane, for a pipe with lanes; empty without
 */
public record PipeStats(long pipe, boolean local, long fateGroup, String label, CarrierKind carrier,
                        PipeState.Outbound outbound, PipeState.Inbound inbound, Duration attachedFor,
                        long suspectCount, long failureCount, Failure failure,
                        long sendPriority, SettingSource sendPrioritySource, boolean standby, SettingSource standbySource,
                        long receivePriority, boolean receiveStandby, long requestSeq, PeerRequest peerRequest, long weight,
                        OptionalLong probationUntil, boolean inProbation, boolean revalidating, boolean eligible,
                        boolean writable, boolean saturated, RttSource rttSource, Duration smoothedRtt, Duration minRtt,
                        Duration rttvar, Duration oneWayDelay, OptionalLong deliveryRate, OptionalLong queuedBytes,
                        Duration queueDelay, OptionalLong bytesInFlight, long bytesSent, long bytesReceived,
                        OptionalLong bytesAcked, long recordsSent, long recordsReceived, long bytesReinjected,
                        OptionalLong bytesLost, OptionalLong recordsLost, OptionalLong bytesRetransmitted, OptionalDouble lossRate,
                        long bytesOpportunistic, long bytesRedundant, long bytesDuplicateReceived, long pingsSent,
                        long pingsAnswered, Duration sinceLastAck, Duration sinceLastReceive, Duration sinceLastTraffic,
                        int lanesOut, int lanesIn, long lanesRefused, List<LaneStats> lanes) {
    public PipeStats {
        lanes = List.copyOf(lanes);
    }

    /** Who set an effective pipe setting. */
    public enum SettingSource { DEFAULT, LOCAL, PEER }

    /** Where an RTT estimate comes from. */
    public enum RttSource { ACK, KERNEL, PING }

    /** The peer's request for the outbound direction (PIPE_STATUS). */
    public record PeerRequest(long priority, boolean standby) {}

    /**
     * Why an attachment failed.
     *
     * @param code the transport error code (ZMUX_PIPE_FAILED, ZMUX_PIPE_RESOURCE_LIMIT, ...)
     * @param cause the local cause, or null
     */
    public record Failure(long code, Throwable cause) {}

    /**
     * The positions of one lane; each is empty for a direction the lane does not have.
     *
     * @param lane its number
     * @param positionSent bytes written on the outbound lane, its first record included
     * @param positionAcked the position the peer acknowledged on it
     * @param positionReceived the position of the inbound lane
     */
    public record LaneStats(long lane, OptionalLong positionSent, OptionalLong positionAcked, OptionalLong positionReceived) {}
}
