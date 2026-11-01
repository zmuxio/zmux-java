package io.zmux;

import java.time.Duration;
import java.util.List;

/**
 * A snapshot of a session (API.md Section 7.2), as of its loop's last round. Counts only
 * increase; gauges are current values; RTTs are null before the first sample.
 *
 * @param carrier the carrier kind a carrier mode chose (the latest, after a switch); null for a
 *     session not dialed in a mode
 * @param carrierCandidates the tries of the mode's candidates in order, joins included: the latest 64
 * @param bytesSent bytes of the records passed to the carriers, every frame included
 * @param bytesReceived bytes of the records received from the carriers
 * @param recordsSent records passed to the carriers
 * @param recordsReceived records received from the carriers
 * @param bytesWritten application bytes written to streams (API.md Section 7.1)
 * @param bytesRead application bytes read from streams (API.md Section 7.1)
 * @param datagramBytesSent payload bytes of datagrams handed to a carrier (API.md Section 7.1)
 * @param datagramBytesReceived payload bytes of datagrams returned to the application
 * @param streamsOpenedLocal streams opened by this endpoint: counted at their first frame or when pinned
 * @param streamsOpenedRemote streams opened by the peer: counted when first seen
 * @param streamsActive gauge: streams the session holds
 * @param streamCreditWaits opens that had to wait for stream-count credit
 * @param datagramsSent session and stream datagrams handed to a carrier
 * @param datagramsReceived datagrams received and queued for the application
 * @param datagramsSentChannel of datagramsSent, those on a carrier datagram channel (EXTENSIONS.md Section 6.8)
 * @param datagramsReceivedChannel of datagramsReceived, those that arrived on a carrier datagram channel
 * @param datagramsDroppedSend datagrams dropped from the send queue (full, or not allowed by the peer)
 * @param datagramsDroppedRecv received datagrams dropped: a full queue, an unknown or closed stream, the byte limit
 * @param datagramsDroppedLimit datagrams dropped by a datagram limiter, sent or received
 * @param noEffectBudget gauge: the current tokens of the no-effect budget (SPEC.md Section 9.4)
 * @param smoothedRtt pipe 0's estimate outside multipath, the primary pipe's in a multipath session
 * @param minRtt the minimum RTT (SPEC.md Section 8.3 rule 6)
 * @param latestRtt the latest sample
 * @param meanDeviation the RTT variation; null where the source measures none
 * @param bytesReinjected stream bytes sent again after a pipe failed or became suspect, or opportunistically
 * @param bytesOpportunistic of bytesReinjected, those sent opportunistically
 * @param penalties pipes penalized for slowing down
 * @param reinjections reinjection episodes: a pipe's data reinjected
 * @param bytesRedundant stream bytes sent as redundant copies
 * @param bytesDuplicateReceived stream bytes received that had been received already (multipath)
 * @param sendBuffered gauge: written stream bytes not yet sent
 * @param recvBuffered gauge: received stream bytes not yet read
 * @param retained gauge: sent stream bytes not yet acknowledged (multipath)
 * @param pipesActive gauge: outbound pipes that are ACTIVE
 * @param pipesAdded attachments added, joined either way, the bootstrap pair included
 * @param pipeFailures attachments that failed
 * @param joinAuthFailures joins whose token did not verify ({@link SessionEvent.JoinAuthFailed})
 * @param joinReplays joins closed as replays or too late
 * @param joinRefusals own joins the peer refused with ZMUX_PIPE_RESOURCE_LIMIT
 * @param suspensions times the session entered SUSPENDED
 * @param suspendedTime time spent suspended, the current suspension included
 * @param schedulerAnswers custom scheduler answers applied
 * @param schedulerFallbacks custom scheduler answers replaced by the affinity choice
 */
public record SessionStats(CarrierKind carrier, List<CandidateResult> carrierCandidates,
                           long bytesSent, long bytesReceived, long recordsSent, long recordsReceived,
                           long bytesWritten, long bytesRead, long datagramBytesSent, long datagramBytesReceived,
                           long streamsOpenedLocal, long streamsOpenedRemote, long streamsActive, long streamCreditWaits,
                           long datagramsSent, long datagramsReceived, long datagramsSentChannel, long datagramsReceivedChannel,
                           long datagramsDroppedSend, long datagramsDroppedRecv, long datagramsDroppedLimit,
                           long noEffectBudget, Duration smoothedRtt, Duration minRtt, Duration latestRtt,
                           Duration meanDeviation, long bytesReinjected, long bytesOpportunistic, long penalties,
                           long reinjections, long bytesRedundant, long bytesDuplicateReceived, long sendBuffered,
                           long recvBuffered, long retained, long pipesActive, long pipesAdded, long pipeFailures,
                           long joinAuthFailures, long joinReplays, long joinRefusals, long suspensions,
                           Duration suspendedTime, long schedulerAnswers, long schedulerFallbacks) {
    public SessionStats {
        carrierCandidates = List.copyOf(carrierCandidates);
        java.util.Objects.requireNonNull(suspendedTime);
    }

    static SessionStats of(io.zmux.internal.core.SessionStats.Snapshot s, CarrierKind carrier, List<CandidateResult> tries,
                           long now) {
        var g = s.gauges();
        long suspended = s.suspendedTotal() + (s.suspendedSince() >= 0 ? Math.max(0, now - s.suspendedSince()) : 0);
        return new SessionStats(carrier, tries, s.bytesSent(), s.bytesReceived(), s.recordsSent(), s.recordsReceived(),
                s.appBytesWritten(), s.appBytesRead(), s.datagramBytesSent(), s.datagramBytesReceived(),
                s.streamsOpenedLocal(), s.streamsOpenedRemote(), g.streamsActive(), s.streamCreditWaits(),
                s.datagramsSent(), s.datagramsReceived(), s.datagramsSentChannel(), s.datagramsReceivedChannel(),
                s.datagramsDroppedSend(), s.datagramsDroppedRecv(), s.datagramsDroppedLimit(), s.budgetTokens(),
                rtt(g.smoothedRtt()), rtt(g.minRtt()), rtt(g.latestRtt()), rtt(g.meanDeviation()),
                s.bytesReinjected(), s.bytesOpportunistic(), s.penalties(), s.reinjections(), s.bytesRedundant(),
                s.bytesDuplicateReceived(), g.sendBuffered(), g.recvBuffered(), g.retained(), g.pipesActive(),
                s.pipesAdded(), s.pipeFailures(), s.joinAuthFailures(), s.joinReplays(), s.joinRefusals(), s.suspensions(),
                Duration.ofNanos(suspended), s.schedulerAnswers(), s.schedulerFallbacks());
    }

    private static Duration rtt(long nanos) {
        return nanos < 0 ? null : Duration.ofNanos(nanos);
    }
}
