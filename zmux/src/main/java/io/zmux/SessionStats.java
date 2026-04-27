package io.zmux;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class SessionStats {
    private final SessionState state;
    private final long sentFrames;
    private final long receivedFrames;
    private final long sentDataBytes;
    private final long receivedDataBytes;
    private final long openStreams;
    private final long acceptedStreams;
    private final ActiveStreamStats activeStreams;
    private final AcceptBacklogStats acceptBacklog;
    private final long retainedOpenInfoBytes;
    private final long retainedOpenInfoBudget;
    private final long retainedPeerReasonBytes;
    private final long retainedPeerReasonBudget;
    private final KeepaliveStats keepalive;
    private final ProgressStats progress;
    private final FlushStats flush;
    private final long blockedWriteTotalNanos;
    private final long lastOpenLatencyNanos;
    private final QueueStats queues;
    private final ProvisionalStats provisionals;
    private final HiddenStateStats hiddenState;
    private final ReasonStats reasons;
    private final DiagnosticStats diagnostics;
    private final PressureStats pressure;

    public SessionStats(SessionState state,
                        long sentFrames,
                        long receivedFrames,
                        long sentDataBytes,
                        long receivedDataBytes,
                        long openStreams,
                        long acceptedStreams,
                        AcceptBacklogStats acceptBacklog,
                        long retainedOpenInfoBytes,
                        long retainedOpenInfoBudget,
                        long retainedPeerReasonBytes,
                        long retainedPeerReasonBudget,
                        KeepaliveStats keepalive,
                        ProgressStats progress,
                        FlushStats flush,
                        long blockedWriteTotalNanos,
                        long lastOpenLatencyNanos,
                        QueueStats queues,
                        ProvisionalStats provisionals,
                        HiddenStateStats hiddenState,
                        ReasonStats reasons,
                        DiagnosticStats diagnostics,
                        PressureStats pressure) {
        this(
                state,
                sentFrames,
                receivedFrames,
                sentDataBytes,
                receivedDataBytes,
                openStreams,
                acceptedStreams,
                ActiveStreamStats.empty(),
                acceptBacklog,
                retainedOpenInfoBytes,
                retainedOpenInfoBudget,
                retainedPeerReasonBytes,
                retainedPeerReasonBudget,
                keepalive,
                progress,
                flush,
                blockedWriteTotalNanos,
                lastOpenLatencyNanos,
                queues,
                provisionals,
                hiddenState,
                reasons,
                diagnostics,
                pressure
        );
    }

    public SessionStats(SessionState state,
                        long sentFrames,
                        long receivedFrames,
                        long sentDataBytes,
                        long receivedDataBytes,
                        long openStreams,
                        long acceptedStreams,
                        ActiveStreamStats activeStreams,
                        AcceptBacklogStats acceptBacklog,
                        long retainedOpenInfoBytes,
                        long retainedOpenInfoBudget,
                        long retainedPeerReasonBytes,
                        long retainedPeerReasonBudget,
                        KeepaliveStats keepalive,
                        ProgressStats progress,
                        FlushStats flush,
                        long blockedWriteTotalNanos,
                        long lastOpenLatencyNanos,
                        QueueStats queues,
                        ProvisionalStats provisionals,
                        HiddenStateStats hiddenState,
                        ReasonStats reasons,
                        DiagnosticStats diagnostics,
                        PressureStats pressure) {
        this.state = state;
        this.sentFrames = sentFrames;
        this.receivedFrames = receivedFrames;
        this.sentDataBytes = sentDataBytes;
        this.receivedDataBytes = receivedDataBytes;
        this.openStreams = openStreams;
        this.acceptedStreams = acceptedStreams;
        this.activeStreams = activeStreams == null ? ActiveStreamStats.empty() : activeStreams;
        this.acceptBacklog = acceptBacklog;
        this.retainedOpenInfoBytes = retainedOpenInfoBytes;
        this.retainedOpenInfoBudget = retainedOpenInfoBudget;
        this.retainedPeerReasonBytes = retainedPeerReasonBytes;
        this.retainedPeerReasonBudget = retainedPeerReasonBudget;
        this.keepalive = keepalive;
        this.progress = progress;
        this.flush = flush;
        this.blockedWriteTotalNanos = blockedWriteTotalNanos;
        this.lastOpenLatencyNanos = lastOpenLatencyNanos;
        this.queues = queues;
        this.provisionals = provisionals;
        this.hiddenState = hiddenState;
        this.reasons = reasons;
        this.diagnostics = diagnostics;
        this.pressure = pressure;
    }

    public static SessionStats empty(SessionState state) {
        return new SessionStats(
                state,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                AcceptBacklogStats.empty(),
                0L,
                0L,
                0L,
                0L,
                KeepaliveStats.empty(),
                ProgressStats.empty(),
                FlushStats.empty(),
                0L,
                0L,
                QueueStats.empty(),
                ProvisionalStats.empty(),
                HiddenStateStats.empty(),
                ReasonStats.empty(),
                DiagnosticStats.empty(),
                PressureStats.empty()
        );
    }

    private static long saturatingAdd(long left, long right) {
        if (left <= 0L) {
            return Math.max(0L, right);
        }
        if (right <= 0L) {
            return left;
        }
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    public SessionState state() {
        return state;
    }

    public long sentFrames() {
        return sentFrames;
    }

    public long receivedFrames() {
        return receivedFrames;
    }

    public long sentDataBytes() {
        return sentDataBytes;
    }

    public long receivedDataBytes() {
        return receivedDataBytes;
    }

    public long openStreams() {
        return openStreams;
    }

    public long acceptedStreams() {
        return acceptedStreams;
    }

    public ActiveStreamStats activeStreams() {
        return activeStreams;
    }

    public AcceptBacklogStats acceptBacklog() {
        return acceptBacklog;
    }

    public long retainedOpenInfoBytes() {
        return retainedOpenInfoBytes;
    }

    public long retainedOpenInfoBudget() {
        return retainedOpenInfoBudget;
    }

    public long retainedPeerReasonBytes() {
        return retainedPeerReasonBytes;
    }

    public long retainedPeerReasonBudget() {
        return retainedPeerReasonBudget;
    }

    public KeepaliveStats keepalive() {
        return keepalive;
    }

    public ProgressStats progress() {
        return progress;
    }

    public FlushStats flush() {
        return flush;
    }

    public long blockedWriteTotalNanos() {
        return blockedWriteTotalNanos;
    }

    public long lastOpenLatencyNanos() {
        return lastOpenLatencyNanos;
    }

    public QueueStats queues() {
        return queues;
    }

    public ProvisionalStats provisionals() {
        return provisionals;
    }

    public HiddenStateStats hiddenState() {
        return hiddenState;
    }

    public ReasonStats reasons() {
        return reasons;
    }

    public DiagnosticStats diagnostics() {
        return diagnostics;
    }

    public PressureStats pressure() {
        return pressure;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SessionStats)) {
            return false;
        }
        SessionStats that = (SessionStats) other;
        return sentFrames == that.sentFrames
                && receivedFrames == that.receivedFrames
                && sentDataBytes == that.sentDataBytes
                && receivedDataBytes == that.receivedDataBytes
                && openStreams == that.openStreams
                && acceptedStreams == that.acceptedStreams
                && retainedOpenInfoBytes == that.retainedOpenInfoBytes
                && retainedOpenInfoBudget == that.retainedOpenInfoBudget
                && retainedPeerReasonBytes == that.retainedPeerReasonBytes
                && retainedPeerReasonBudget == that.retainedPeerReasonBudget
                && blockedWriteTotalNanos == that.blockedWriteTotalNanos
                && lastOpenLatencyNanos == that.lastOpenLatencyNanos
                && state == that.state
                && Objects.equals(activeStreams, that.activeStreams)
                && Objects.equals(acceptBacklog, that.acceptBacklog)
                && Objects.equals(keepalive, that.keepalive)
                && Objects.equals(progress, that.progress)
                && Objects.equals(flush, that.flush)
                && Objects.equals(queues, that.queues)
                && Objects.equals(provisionals, that.provisionals)
                && Objects.equals(hiddenState, that.hiddenState)
                && Objects.equals(reasons, that.reasons)
                && Objects.equals(diagnostics, that.diagnostics)
                && Objects.equals(pressure, that.pressure);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                state,
                sentFrames,
                receivedFrames,
                sentDataBytes,
                receivedDataBytes,
                openStreams,
                acceptedStreams,
                activeStreams,
                acceptBacklog,
                retainedOpenInfoBytes,
                retainedOpenInfoBudget,
                retainedPeerReasonBytes,
                retainedPeerReasonBudget,
                keepalive,
                progress,
                flush,
                blockedWriteTotalNanos,
                lastOpenLatencyNanos,
                queues,
                provisionals,
                hiddenState,
                reasons,
                diagnostics,
                pressure
        );
    }

    public static final class ActiveStreamStats {
        private final long localBidi;
        private final long localUni;
        private final long peerBidi;
        private final long peerUni;
        private final long total;

        public ActiveStreamStats(long localBidi, long localUni, long peerBidi, long peerUni) {
            this.localBidi = Math.max(0L, localBidi);
            this.localUni = Math.max(0L, localUni);
            this.peerBidi = Math.max(0L, peerBidi);
            this.peerUni = Math.max(0L, peerUni);
            this.total = saturatingAdd(
                    saturatingAdd(this.localBidi, this.localUni),
                    saturatingAdd(this.peerBidi, this.peerUni)
            );
        }

        public static ActiveStreamStats empty() {
            return new ActiveStreamStats(0L, 0L, 0L, 0L);
        }

        public long localBidi() {
            return localBidi;
        }

        public long localUni() {
            return localUni;
        }

        public long peerBidi() {
            return peerBidi;
        }

        public long peerUni() {
            return peerUni;
        }

        public long total() {
            return total;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ActiveStreamStats)) {
                return false;
            }
            ActiveStreamStats that = (ActiveStreamStats) other;
            return localBidi == that.localBidi
                    && localUni == that.localUni
                    && peerBidi == that.peerBidi
                    && peerUni == that.peerUni
                    && total == that.total;
        }

        @Override
        public int hashCode() {
            return Objects.hash(localBidi, localUni, peerBidi, peerUni, total);
        }
    }

    public static final class AcceptBacklogStats {
        private final long count;
        private final long bytes;
        private final int countLimit;
        private final long bytesLimit;
        private final boolean atCountCap;
        private final boolean atBytesCap;
        private final long refused;

        public AcceptBacklogStats(long count, long bytes, int countLimit, long bytesLimit, boolean atCountCap, boolean atBytesCap, long refused) {
            this.count = count;
            this.bytes = bytes;
            this.countLimit = countLimit;
            this.bytesLimit = bytesLimit;
            this.atCountCap = atCountCap;
            this.atBytesCap = atBytesCap;
            this.refused = refused;
        }

        public static AcceptBacklogStats empty() {
            return new AcceptBacklogStats(0L, 0L, 0, 0L, false, false, 0L);
        }

        public long count() {
            return count;
        }

        public long bytes() {
            return bytes;
        }

        public int countLimit() {
            return countLimit;
        }

        public long bytesLimit() {
            return bytesLimit;
        }

        public boolean atCountCap() {
            return atCountCap;
        }

        public boolean atBytesCap() {
            return atBytesCap;
        }

        public long refused() {
            return refused;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof AcceptBacklogStats)) {
                return false;
            }
            AcceptBacklogStats that = (AcceptBacklogStats) other;
            return count == that.count && bytes == that.bytes && countLimit == that.countLimit
                    && bytesLimit == that.bytesLimit && atCountCap == that.atCountCap
                    && atBytesCap == that.atBytesCap && refused == that.refused;
        }

        @Override
        public int hashCode() {
            return Objects.hash(count, bytes, countLimit, bytesLimit, atCountCap, atBytesCap, refused);
        }
    }

    public static final class KeepaliveStats {
        private final boolean enabled;
        private final long intervalNanos;
        private final long maxPingIntervalNanos;
        private final long timeoutNanos;
        private final boolean pingOutstanding;
        private final boolean pingStalled;
        private final long lastPingRttNanos;
        private final long sendRateEstimateBytesPerSecond;

        public KeepaliveStats(boolean enabled,
                              long intervalNanos,
                              long maxPingIntervalNanos,
                              long timeoutNanos,
                              boolean pingOutstanding,
                              boolean pingStalled,
                              long lastPingRttNanos,
                              long sendRateEstimateBytesPerSecond) {
            this.enabled = enabled;
            this.intervalNanos = intervalNanos;
            this.maxPingIntervalNanos = maxPingIntervalNanos;
            this.timeoutNanos = timeoutNanos;
            this.pingOutstanding = pingOutstanding;
            this.pingStalled = pingStalled;
            this.lastPingRttNanos = lastPingRttNanos;
            this.sendRateEstimateBytesPerSecond = sendRateEstimateBytesPerSecond;
        }

        public static KeepaliveStats empty() {
            return new KeepaliveStats(false, 0L, 0L, 0L, false, false, 0L, 0L);
        }

        public boolean enabled() {
            return enabled;
        }

        public long intervalNanos() {
            return intervalNanos;
        }

        public long maxPingIntervalNanos() {
            return maxPingIntervalNanos;
        }

        public long timeoutNanos() {
            return timeoutNanos;
        }

        public boolean pingOutstanding() {
            return pingOutstanding;
        }

        public boolean pingStalled() {
            return pingStalled;
        }

        public long lastPingRttNanos() {
            return lastPingRttNanos;
        }

        public long sendRateEstimateBytesPerSecond() {
            return sendRateEstimateBytesPerSecond;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof KeepaliveStats)) {
                return false;
            }
            KeepaliveStats that = (KeepaliveStats) other;
            return enabled == that.enabled && intervalNanos == that.intervalNanos
                    && maxPingIntervalNanos == that.maxPingIntervalNanos && timeoutNanos == that.timeoutNanos
                    && pingOutstanding == that.pingOutstanding && pingStalled == that.pingStalled
                    && lastPingRttNanos == that.lastPingRttNanos
                    && sendRateEstimateBytesPerSecond == that.sendRateEstimateBytesPerSecond;
        }

        @Override
        public int hashCode() {
            return Objects.hash(enabled, intervalNanos, maxPingIntervalNanos, timeoutNanos, pingOutstanding, pingStalled, lastPingRttNanos, sendRateEstimateBytesPerSecond);
        }
    }

    public static final class ProgressStats {
        private final Instant inboundFrameAt;
        private final Instant controlProgressAt;
        private final Instant transportWriteAt;
        private final Instant streamProgressAt;
        private final Instant applicationProgressAt;
        private final Instant pingSentAt;
        private final Instant pongAt;

        public ProgressStats(Instant inboundFrameAt,
                             Instant controlProgressAt,
                             Instant transportWriteAt,
                             Instant streamProgressAt,
                             Instant applicationProgressAt,
                             Instant pingSentAt,
                             Instant pongAt) {
            this.inboundFrameAt = inboundFrameAt;
            this.controlProgressAt = controlProgressAt;
            this.transportWriteAt = transportWriteAt;
            this.streamProgressAt = streamProgressAt;
            this.applicationProgressAt = applicationProgressAt;
            this.pingSentAt = pingSentAt;
            this.pongAt = pongAt;
        }

        public static ProgressStats empty() {
            return new ProgressStats(null, null, null, null, null, null, null);
        }

        public Instant inboundFrameAt() {
            return inboundFrameAt;
        }

        public Instant controlProgressAt() {
            return controlProgressAt;
        }

        public Instant transportWriteAt() {
            return transportWriteAt;
        }

        public Instant streamProgressAt() {
            return streamProgressAt;
        }

        public Instant applicationProgressAt() {
            return applicationProgressAt;
        }

        public Instant pingSentAt() {
            return pingSentAt;
        }

        public Instant pongAt() {
            return pongAt;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ProgressStats)) {
                return false;
            }
            ProgressStats that = (ProgressStats) other;
            return Objects.equals(inboundFrameAt, that.inboundFrameAt)
                    && Objects.equals(controlProgressAt, that.controlProgressAt)
                    && Objects.equals(transportWriteAt, that.transportWriteAt)
                    && Objects.equals(streamProgressAt, that.streamProgressAt)
                    && Objects.equals(applicationProgressAt, that.applicationProgressAt)
                    && Objects.equals(pingSentAt, that.pingSentAt)
                    && Objects.equals(pongAt, that.pongAt);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inboundFrameAt, controlProgressAt, transportWriteAt, streamProgressAt, applicationProgressAt, pingSentAt, pongAt);
        }
    }

    public static final class FlushStats {
        private final long count;
        private final Instant lastAt;
        private final int lastFrames;
        private final long lastBytes;

        public FlushStats(long count, Instant lastAt, int lastFrames, long lastBytes) {
            this.count = count;
            this.lastAt = lastAt;
            this.lastFrames = lastFrames;
            this.lastBytes = lastBytes;
        }

        public static FlushStats empty() {
            return new FlushStats(0L, null, 0, 0L);
        }

        public long count() {
            return count;
        }

        public Instant lastAt() {
            return lastAt;
        }

        public int lastFrames() {
            return lastFrames;
        }

        public long lastBytes() {
            return lastBytes;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof FlushStats)) {
                return false;
            }
            FlushStats that = (FlushStats) other;
            return count == that.count && lastFrames == that.lastFrames && lastBytes == that.lastBytes
                    && Objects.equals(lastAt, that.lastAt);
        }

        @Override
        public int hashCode() {
            return Objects.hash(count, lastAt, lastFrames, lastBytes);
        }
    }

    public static final class QueueStats {
        private final int urgentFrames;
        private final int advisoryStreams;
        private final int dataFrames;
        private final long queuedDataBytes;
        private final long reservedSendBytes;
        private final long urgentQueuedControlBytes;
        private final long ordinaryQueuedControlBytes;
        private final long pendingControlBytes;
        private final long pendingPriorityBytes;
        private final long writerHeldRetainedBytes;

        public QueueStats(int urgentFrames,
                          int advisoryStreams,
                          int dataFrames,
                          long queuedDataBytes,
                          long reservedSendBytes,
                          long urgentQueuedControlBytes,
                          long ordinaryQueuedControlBytes,
                          long pendingControlBytes,
                          long pendingPriorityBytes,
                          long writerHeldRetainedBytes) {
            this.urgentFrames = urgentFrames;
            this.advisoryStreams = advisoryStreams;
            this.dataFrames = dataFrames;
            this.queuedDataBytes = queuedDataBytes;
            this.reservedSendBytes = reservedSendBytes;
            this.urgentQueuedControlBytes = urgentQueuedControlBytes;
            this.ordinaryQueuedControlBytes = ordinaryQueuedControlBytes;
            this.pendingControlBytes = pendingControlBytes;
            this.pendingPriorityBytes = pendingPriorityBytes;
            this.writerHeldRetainedBytes = writerHeldRetainedBytes;
        }

        public static QueueStats empty() {
            return new QueueStats(0, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        }

        public int urgentFrames() {
            return urgentFrames;
        }

        public int advisoryStreams() {
            return advisoryStreams;
        }

        public int dataFrames() {
            return dataFrames;
        }

        public long queuedDataBytes() {
            return queuedDataBytes;
        }

        public long reservedSendBytes() {
            return reservedSendBytes;
        }

        public long urgentQueuedControlBytes() {
            return urgentQueuedControlBytes;
        }

        public long ordinaryQueuedControlBytes() {
            return ordinaryQueuedControlBytes;
        }

        public long pendingControlBytes() {
            return pendingControlBytes;
        }

        public long pendingPriorityBytes() {
            return pendingPriorityBytes;
        }

        public long writerHeldRetainedBytes() {
            return writerHeldRetainedBytes;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof QueueStats)) {
                return false;
            }
            QueueStats that = (QueueStats) other;
            return urgentFrames == that.urgentFrames
                    && advisoryStreams == that.advisoryStreams
                    && dataFrames == that.dataFrames
                    && queuedDataBytes == that.queuedDataBytes
                    && reservedSendBytes == that.reservedSendBytes
                    && urgentQueuedControlBytes == that.urgentQueuedControlBytes
                    && ordinaryQueuedControlBytes == that.ordinaryQueuedControlBytes
                    && pendingControlBytes == that.pendingControlBytes
                    && pendingPriorityBytes == that.pendingPriorityBytes
                    && writerHeldRetainedBytes == that.writerHeldRetainedBytes;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    urgentFrames,
                    advisoryStreams,
                    dataFrames,
                    queuedDataBytes,
                    reservedSendBytes,
                    urgentQueuedControlBytes,
                    ordinaryQueuedControlBytes,
                    pendingControlBytes,
                    pendingPriorityBytes,
                    writerHeldRetainedBytes
            );
        }
    }

    public static final class ProvisionalStats {
        private final int bidi;
        private final int uni;
        private final int softCap;
        private final int hardCap;
        private final long maxAgeNanos;
        private final boolean bidiAtSoftCap;
        private final boolean uniAtSoftCap;
        private final boolean bidiAtHardCap;
        private final boolean uniAtHardCap;
        private final long limited;
        private final long expired;

        public ProvisionalStats(int bidi,
                                int uni,
                                int softCap,
                                int hardCap,
                                long maxAgeNanos,
                                boolean bidiAtSoftCap,
                                boolean uniAtSoftCap,
                                boolean bidiAtHardCap,
                                boolean uniAtHardCap,
                                long limited,
                                long expired) {
            this.bidi = bidi;
            this.uni = uni;
            this.softCap = softCap;
            this.hardCap = hardCap;
            this.maxAgeNanos = maxAgeNanos;
            this.bidiAtSoftCap = bidiAtSoftCap;
            this.uniAtSoftCap = uniAtSoftCap;
            this.bidiAtHardCap = bidiAtHardCap;
            this.uniAtHardCap = uniAtHardCap;
            this.limited = limited;
            this.expired = expired;
        }

        public static ProvisionalStats empty() {
            return new ProvisionalStats(0, 0, 0, 0, 0L, false, false, false, false, 0L, 0L);
        }

        public int bidi() {
            return bidi;
        }

        public int uni() {
            return uni;
        }

        public int softCap() {
            return softCap;
        }

        public int hardCap() {
            return hardCap;
        }

        public long maxAgeNanos() {
            return maxAgeNanos;
        }

        public boolean bidiAtSoftCap() {
            return bidiAtSoftCap;
        }

        public boolean uniAtSoftCap() {
            return uniAtSoftCap;
        }

        public boolean bidiAtHardCap() {
            return bidiAtHardCap;
        }

        public boolean uniAtHardCap() {
            return uniAtHardCap;
        }

        public long limited() {
            return limited;
        }

        public long expired() {
            return expired;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ProvisionalStats)) {
                return false;
            }
            ProvisionalStats that = (ProvisionalStats) other;
            return bidi == that.bidi
                    && uni == that.uni
                    && softCap == that.softCap
                    && hardCap == that.hardCap
                    && maxAgeNanos == that.maxAgeNanos
                    && bidiAtSoftCap == that.bidiAtSoftCap
                    && uniAtSoftCap == that.uniAtSoftCap
                    && bidiAtHardCap == that.bidiAtHardCap
                    && uniAtHardCap == that.uniAtHardCap
                    && limited == that.limited
                    && expired == that.expired;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    bidi,
                    uni,
                    softCap,
                    hardCap,
                    maxAgeNanos,
                    bidiAtSoftCap,
                    uniAtSoftCap,
                    bidiAtHardCap,
                    uniAtHardCap,
                    limited,
                    expired
            );
        }
    }

    public static final class HiddenStateStats {
        private final int retained;
        private final int softCap;
        private final int hardCap;
        private final boolean atSoftCap;
        private final boolean atHardCap;
        private final int visibleTombstones;
        private final int markerOnly;
        private final long refused;
        private final long reaped;
        private final long unreadBytesDiscarded;

        public HiddenStateStats(int retained,
                                int softCap,
                                int hardCap,
                                boolean atSoftCap,
                                boolean atHardCap,
                                int visibleTombstones,
                                int markerOnly,
                                long refused,
                                long reaped,
                                long unreadBytesDiscarded) {
            this.retained = retained;
            this.softCap = softCap;
            this.hardCap = hardCap;
            this.atSoftCap = atSoftCap;
            this.atHardCap = atHardCap;
            this.visibleTombstones = visibleTombstones;
            this.markerOnly = markerOnly;
            this.refused = refused;
            this.reaped = reaped;
            this.unreadBytesDiscarded = unreadBytesDiscarded;
        }

        public static HiddenStateStats empty() {
            return new HiddenStateStats(0, 0, 0, false, false, 0, 0, 0L, 0L, 0L);
        }

        public int retained() {
            return retained;
        }

        public int softCap() {
            return softCap;
        }

        public int hardCap() {
            return hardCap;
        }

        public boolean atSoftCap() {
            return atSoftCap;
        }

        public boolean atHardCap() {
            return atHardCap;
        }

        public int visibleTombstones() {
            return visibleTombstones;
        }

        public int markerOnly() {
            return markerOnly;
        }

        public long refused() {
            return refused;
        }

        public long reaped() {
            return reaped;
        }

        public long unreadBytesDiscarded() {
            return unreadBytesDiscarded;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof HiddenStateStats)) {
                return false;
            }
            HiddenStateStats that = (HiddenStateStats) other;
            return retained == that.retained
                    && softCap == that.softCap
                    && hardCap == that.hardCap
                    && atSoftCap == that.atSoftCap
                    && atHardCap == that.atHardCap
                    && visibleTombstones == that.visibleTombstones
                    && markerOnly == that.markerOnly
                    && refused == that.refused
                    && reaped == that.reaped
                    && unreadBytesDiscarded == that.unreadBytesDiscarded;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    retained,
                    softCap,
                    hardCap,
                    atSoftCap,
                    atHardCap,
                    visibleTombstones,
                    markerOnly,
                    refused,
                    reaped,
                    unreadBytesDiscarded
            );
        }
    }

    public static final class ReasonStats {
        public static final int MAX_TRACKED_CODES = 1024;

        private final Map<Long, Long> reset;
        private final long resetOverflow;
        private final Map<Long, Long> abort;
        private final long abortOverflow;

        public ReasonStats(Map<Long, Long> reset, Map<Long, Long> abort) {
            this(reset, 0L, abort, 0L);
        }

        public ReasonStats(Map<Long, Long> reset, long resetOverflow, Map<Long, Long> abort, long abortOverflow) {
            this.reset = immutableMap(reset);
            this.resetOverflow = Math.max(0L, resetOverflow);
            this.abort = immutableMap(abort);
            this.abortOverflow = Math.max(0L, abortOverflow);
        }

        private static Map<Long, Long> immutableMap(Map<Long, Long> source) {
            if (source == null || source.isEmpty()) {
                return Collections.emptyMap();
            }
            return Collections.unmodifiableMap(new LinkedHashMap<>(source));
        }

        public static ReasonStats empty() {
            return new ReasonStats(Collections.emptyMap(), 0L, Collections.emptyMap(), 0L);
        }

        public Map<Long, Long> reset() {
            return reset;
        }

        public long resetOverflow() {
            return resetOverflow;
        }

        public Map<Long, Long> abort() {
            return abort;
        }

        public long abortOverflow() {
            return abortOverflow;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ReasonStats)) {
                return false;
            }
            ReasonStats that = (ReasonStats) other;
            return resetOverflow == that.resetOverflow
                    && abortOverflow == that.abortOverflow
                    && Objects.equals(reset, that.reset)
                    && Objects.equals(abort, that.abort);
        }

        @Override
        public int hashCode() {
            return Objects.hash(reset, resetOverflow, abort, abortOverflow);
        }
    }

    public static final class DiagnosticStats {
        private final long droppedPriorityUpdates;
        private final long droppedLocalPriorityUpdates;
        private final long lateDataAfterCloseRead;
        private final long lateDataAfterReset;
        private final long lateDataAfterAbort;
        private final long visibleTerminalChurnEvents;
        private final long groupRebucketEvents;
        private final long protocolBacklogBlocked;
        private final long skippedCloseOnDeadIO;
        private final long closeFrameFlushErrors;
        private final long closeCompletionTimeouts;
        private final long gracefulCloseTimeouts;
        private final long keepaliveTimeouts;
        private final long coalescedTerminalSignals;
        private final long supersededTerminalSignals;
        private final long closeFrameAdmissionTimeouts;
        private final long closeFrameFlushTimeouts;
        private final int markerOnlyRangeCount;

        public DiagnosticStats(long droppedPriorityUpdates,
                               long droppedLocalPriorityUpdates,
                               long lateDataAfterCloseRead,
                               long lateDataAfterReset,
                               long lateDataAfterAbort,
                               long visibleTerminalChurnEvents,
                               long groupRebucketEvents,
                               long protocolBacklogBlocked,
                               long skippedCloseOnDeadIO,
                               long closeFrameFlushErrors,
                               long closeCompletionTimeouts,
                               long gracefulCloseTimeouts,
                               long keepaliveTimeouts,
                               long coalescedTerminalSignals,
                               long supersededTerminalSignals,
                               long closeFrameAdmissionTimeouts,
                               long closeFrameFlushTimeouts,
                               int markerOnlyRangeCount) {
            this.droppedPriorityUpdates = droppedPriorityUpdates;
            this.droppedLocalPriorityUpdates = droppedLocalPriorityUpdates;
            this.lateDataAfterCloseRead = lateDataAfterCloseRead;
            this.lateDataAfterReset = lateDataAfterReset;
            this.lateDataAfterAbort = lateDataAfterAbort;
            this.visibleTerminalChurnEvents = visibleTerminalChurnEvents;
            this.groupRebucketEvents = groupRebucketEvents;
            this.protocolBacklogBlocked = protocolBacklogBlocked;
            this.skippedCloseOnDeadIO = skippedCloseOnDeadIO;
            this.closeFrameFlushErrors = closeFrameFlushErrors;
            this.closeCompletionTimeouts = closeCompletionTimeouts;
            this.gracefulCloseTimeouts = gracefulCloseTimeouts;
            this.keepaliveTimeouts = keepaliveTimeouts;
            this.coalescedTerminalSignals = coalescedTerminalSignals;
            this.supersededTerminalSignals = supersededTerminalSignals;
            this.closeFrameAdmissionTimeouts = closeFrameAdmissionTimeouts;
            this.closeFrameFlushTimeouts = closeFrameFlushTimeouts;
            this.markerOnlyRangeCount = markerOnlyRangeCount;
        }

        public static DiagnosticStats empty() {
            return new DiagnosticStats(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0);
        }

        public long droppedPriorityUpdates() {
            return droppedPriorityUpdates;
        }

        public long droppedLocalPriorityUpdates() {
            return droppedLocalPriorityUpdates;
        }

        public long lateDataAfterCloseRead() {
            return lateDataAfterCloseRead;
        }

        public long lateDataAfterReset() {
            return lateDataAfterReset;
        }

        public long lateDataAfterAbort() {
            return lateDataAfterAbort;
        }

        public long visibleTerminalChurnEvents() {
            return visibleTerminalChurnEvents;
        }

        public long groupRebucketEvents() {
            return groupRebucketEvents;
        }

        public long protocolBacklogBlocked() {
            return protocolBacklogBlocked;
        }

        public long skippedCloseOnDeadIO() {
            return skippedCloseOnDeadIO;
        }

        public long closeFrameFlushErrors() {
            return closeFrameFlushErrors;
        }

        public long closeCompletionTimeouts() {
            return closeCompletionTimeouts;
        }

        public long gracefulCloseTimeouts() {
            return gracefulCloseTimeouts;
        }

        public long keepaliveTimeouts() {
            return keepaliveTimeouts;
        }

        public long coalescedTerminalSignals() {
            return coalescedTerminalSignals;
        }

        public long supersededTerminalSignals() {
            return supersededTerminalSignals;
        }

        public long closeFrameAdmissionTimeouts() {
            return closeFrameAdmissionTimeouts;
        }

        public long closeFrameFlushTimeouts() {
            return closeFrameFlushTimeouts;
        }

        public int markerOnlyRangeCount() {
            return markerOnlyRangeCount;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof DiagnosticStats)) {
                return false;
            }
            DiagnosticStats that = (DiagnosticStats) other;
            return droppedPriorityUpdates == that.droppedPriorityUpdates
                    && droppedLocalPriorityUpdates == that.droppedLocalPriorityUpdates
                    && lateDataAfterCloseRead == that.lateDataAfterCloseRead
                    && lateDataAfterReset == that.lateDataAfterReset
                    && lateDataAfterAbort == that.lateDataAfterAbort
                    && visibleTerminalChurnEvents == that.visibleTerminalChurnEvents
                    && groupRebucketEvents == that.groupRebucketEvents
                    && protocolBacklogBlocked == that.protocolBacklogBlocked
                    && skippedCloseOnDeadIO == that.skippedCloseOnDeadIO
                    && closeFrameFlushErrors == that.closeFrameFlushErrors
                    && closeCompletionTimeouts == that.closeCompletionTimeouts
                    && gracefulCloseTimeouts == that.gracefulCloseTimeouts
                    && keepaliveTimeouts == that.keepaliveTimeouts
                    && coalescedTerminalSignals == that.coalescedTerminalSignals
                    && supersededTerminalSignals == that.supersededTerminalSignals
                    && closeFrameAdmissionTimeouts == that.closeFrameAdmissionTimeouts
                    && closeFrameFlushTimeouts == that.closeFrameFlushTimeouts
                    && markerOnlyRangeCount == that.markerOnlyRangeCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    droppedPriorityUpdates,
                    droppedLocalPriorityUpdates,
                    lateDataAfterCloseRead,
                    lateDataAfterReset,
                    lateDataAfterAbort,
                    visibleTerminalChurnEvents,
                    groupRebucketEvents,
                    protocolBacklogBlocked,
                    skippedCloseOnDeadIO,
                    closeFrameFlushErrors,
                    closeCompletionTimeouts,
                    gracefulCloseTimeouts,
                    keepaliveTimeouts,
                    coalescedTerminalSignals,
                    supersededTerminalSignals,
                    closeFrameAdmissionTimeouts,
                    closeFrameFlushTimeouts,
                    markerOnlyRangeCount
            );
        }
    }

    public static final class RetainedBucketStats {
        private final long count;
        private final long bytes;

        public RetainedBucketStats(long count, long bytes) {
            this.count = count;
            this.bytes = bytes;
        }

        public static RetainedBucketStats empty() {
            return new RetainedBucketStats(0L, 0L);
        }

        public long count() {
            return count;
        }

        public long bytes() {
            return bytes;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof RetainedBucketStats)) {
                return false;
            }
            RetainedBucketStats that = (RetainedBucketStats) other;
            return count == that.count && bytes == that.bytes;
        }

        @Override
        public int hashCode() {
            return Objects.hash(count, bytes);
        }
    }

    public static final class RetainedStateBreakdownStats {
        private final RetainedBucketStats hiddenControl;
        private final RetainedBucketStats acceptBacklog;
        private final RetainedBucketStats provisionals;
        private final RetainedBucketStats visibleTombstones;
        private final RetainedBucketStats markerOnly;

        public RetainedStateBreakdownStats(RetainedBucketStats hiddenControl,
                                           RetainedBucketStats acceptBacklog,
                                           RetainedBucketStats provisionals,
                                           RetainedBucketStats visibleTombstones,
                                           RetainedBucketStats markerOnly) {
            this.hiddenControl = hiddenControl;
            this.acceptBacklog = acceptBacklog;
            this.provisionals = provisionals;
            this.visibleTombstones = visibleTombstones;
            this.markerOnly = markerOnly;
        }

        public static RetainedStateBreakdownStats empty() {
            return new RetainedStateBreakdownStats(
                    RetainedBucketStats.empty(),
                    RetainedBucketStats.empty(),
                    RetainedBucketStats.empty(),
                    RetainedBucketStats.empty(),
                    RetainedBucketStats.empty()
            );
        }

        public RetainedBucketStats hiddenControl() {
            return hiddenControl;
        }

        public RetainedBucketStats acceptBacklog() {
            return acceptBacklog;
        }

        public RetainedBucketStats provisionals() {
            return provisionals;
        }

        public RetainedBucketStats visibleTombstones() {
            return visibleTombstones;
        }

        public RetainedBucketStats markerOnly() {
            return markerOnly;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof RetainedStateBreakdownStats)) {
                return false;
            }
            RetainedStateBreakdownStats that = (RetainedStateBreakdownStats) other;
            return Objects.equals(hiddenControl, that.hiddenControl)
                    && Objects.equals(acceptBacklog, that.acceptBacklog)
                    && Objects.equals(provisionals, that.provisionals)
                    && Objects.equals(visibleTombstones, that.visibleTombstones)
                    && Objects.equals(markerOnly, that.markerOnly);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hiddenControl, acceptBacklog, provisionals, visibleTombstones, markerOnly);
        }
    }

    public static final class PressureStats {
        private final long trackedSessionMemoryBytes;
        private final long trackedRetainedStateMemoryBytes;
        private final RetainedStateBreakdownStats retainedStateBreakdown;
        private final long sessionMemoryHighThresholdBytes;
        private final long sessionMemoryHardCapBytes;
        private final boolean memoryPressureHigh;
        private final long bufferedReceiveBytes;
        private final long bufferedReceiveStorageBytes;
        private final long recvSessionAdvertisedBytes;
        private final long recvSessionReceivedBytes;
        private final long recvSessionPendingBytes;
        private final long outstandingPingBytes;

        public PressureStats(long trackedSessionMemoryBytes,
                             long trackedRetainedStateMemoryBytes,
                             RetainedStateBreakdownStats retainedStateBreakdown,
                             long sessionMemoryHighThresholdBytes,
                             long sessionMemoryHardCapBytes,
                             boolean memoryPressureHigh,
                             long bufferedReceiveBytes,
                             long bufferedReceiveStorageBytes,
                             long recvSessionAdvertisedBytes,
                             long recvSessionReceivedBytes,
                             long recvSessionPendingBytes,
                             long outstandingPingBytes) {
            this.trackedSessionMemoryBytes = trackedSessionMemoryBytes;
            this.trackedRetainedStateMemoryBytes = trackedRetainedStateMemoryBytes;
            this.retainedStateBreakdown = retainedStateBreakdown;
            this.sessionMemoryHighThresholdBytes = sessionMemoryHighThresholdBytes;
            this.sessionMemoryHardCapBytes = sessionMemoryHardCapBytes;
            this.memoryPressureHigh = memoryPressureHigh;
            this.bufferedReceiveBytes = bufferedReceiveBytes;
            this.bufferedReceiveStorageBytes = bufferedReceiveStorageBytes;
            this.recvSessionAdvertisedBytes = recvSessionAdvertisedBytes;
            this.recvSessionReceivedBytes = recvSessionReceivedBytes;
            this.recvSessionPendingBytes = recvSessionPendingBytes;
            this.outstandingPingBytes = outstandingPingBytes;
        }

        public static PressureStats empty() {
            return new PressureStats(0L, 0L, RetainedStateBreakdownStats.empty(), 0L, 0L, false, 0L, 0L, 0L, 0L, 0L, 0L);
        }

        public long trackedSessionMemoryBytes() {
            return trackedSessionMemoryBytes;
        }

        public long trackedRetainedStateMemoryBytes() {
            return trackedRetainedStateMemoryBytes;
        }

        public RetainedStateBreakdownStats retainedStateBreakdown() {
            return retainedStateBreakdown;
        }

        public long sessionMemoryHighThresholdBytes() {
            return sessionMemoryHighThresholdBytes;
        }

        public long sessionMemoryHardCapBytes() {
            return sessionMemoryHardCapBytes;
        }

        public boolean memoryPressureHigh() {
            return memoryPressureHigh;
        }

        public long bufferedReceiveBytes() {
            return bufferedReceiveBytes;
        }

        public long bufferedReceiveStorageBytes() {
            return bufferedReceiveStorageBytes;
        }

        public long recvSessionAdvertisedBytes() {
            return recvSessionAdvertisedBytes;
        }

        public long recvSessionReceivedBytes() {
            return recvSessionReceivedBytes;
        }

        public long recvSessionPendingBytes() {
            return recvSessionPendingBytes;
        }

        public long outstandingPingBytes() {
            return outstandingPingBytes;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof PressureStats)) {
                return false;
            }
            PressureStats that = (PressureStats) other;
            return trackedSessionMemoryBytes == that.trackedSessionMemoryBytes
                    && trackedRetainedStateMemoryBytes == that.trackedRetainedStateMemoryBytes
                    && sessionMemoryHighThresholdBytes == that.sessionMemoryHighThresholdBytes
                    && sessionMemoryHardCapBytes == that.sessionMemoryHardCapBytes
                    && memoryPressureHigh == that.memoryPressureHigh
                    && bufferedReceiveBytes == that.bufferedReceiveBytes
                    && bufferedReceiveStorageBytes == that.bufferedReceiveStorageBytes
                    && recvSessionAdvertisedBytes == that.recvSessionAdvertisedBytes
                    && recvSessionReceivedBytes == that.recvSessionReceivedBytes
                    && recvSessionPendingBytes == that.recvSessionPendingBytes
                    && outstandingPingBytes == that.outstandingPingBytes
                    && Objects.equals(retainedStateBreakdown, that.retainedStateBreakdown);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    trackedSessionMemoryBytes,
                    trackedRetainedStateMemoryBytes,
                    retainedStateBreakdown,
                    sessionMemoryHighThresholdBytes,
                    sessionMemoryHardCapBytes,
                    memoryPressureHigh,
                    bufferedReceiveBytes,
                    bufferedReceiveStorageBytes,
                    recvSessionAdvertisedBytes,
                    recvSessionReceivedBytes,
                    recvSessionPendingBytes,
                    outstandingPingBytes
            );
        }
    }
}
