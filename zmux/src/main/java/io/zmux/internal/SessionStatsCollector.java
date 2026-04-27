package io.zmux.internal;

import io.zmux.SessionState;
import io.zmux.SessionStats;

import java.util.Map;
import java.util.Objects;

final class SessionStatsSurfaceSnapshot {
    private final long sentFrames;
    private final long receivedFrames;
    private final long sentDataBytes;
    private final long receivedDataBytes;
    private final long openStreams;
    private final long acceptedStreams;
    private final long retainedOpenInfoBytes;
    private final long retainedPeerReasonBytes;
    private final long sessionQueuedDataBytes;
    private final long sessionReservedSendBytes;
    private final long writerHeldRetainedBytes;

    SessionStatsSurfaceSnapshot(long sentFrames,
                                long receivedFrames,
                                long sentDataBytes,
                                long receivedDataBytes,
                                long openStreams,
                                long acceptedStreams,
                                long retainedOpenInfoBytes,
                                long retainedPeerReasonBytes,
                                long sessionQueuedDataBytes,
                                long sessionReservedSendBytes,
                                long writerHeldRetainedBytes) {
        this.sentFrames = sentFrames;
        this.receivedFrames = receivedFrames;
        this.sentDataBytes = sentDataBytes;
        this.receivedDataBytes = receivedDataBytes;
        this.openStreams = openStreams;
        this.acceptedStreams = acceptedStreams;
        this.retainedOpenInfoBytes = retainedOpenInfoBytes;
        this.retainedPeerReasonBytes = retainedPeerReasonBytes;
        this.sessionQueuedDataBytes = sessionQueuedDataBytes;
        this.sessionReservedSendBytes = sessionReservedSendBytes;
        this.writerHeldRetainedBytes = writerHeldRetainedBytes;
    }

    long sentFrames() {
        return sentFrames;
    }

    long receivedFrames() {
        return receivedFrames;
    }

    long sentDataBytes() {
        return sentDataBytes;
    }

    long receivedDataBytes() {
        return receivedDataBytes;
    }

    long openStreams() {
        return openStreams;
    }

    long acceptedStreams() {
        return acceptedStreams;
    }

    long retainedOpenInfoBytes() {
        return retainedOpenInfoBytes;
    }

    long retainedPeerReasonBytes() {
        return retainedPeerReasonBytes;
    }

    long sessionQueuedDataBytes() {
        return sessionQueuedDataBytes;
    }

    long sessionReservedSendBytes() {
        return sessionReservedSendBytes;
    }

    long writerHeldRetainedBytes() {
        return writerHeldRetainedBytes;
    }
}

final class SessionStatsDiagnosticSnapshot {
    private final long droppedPriorityUpdates;
    private final long droppedLocalPriorityUpdates;
    private final long coalescedTerminalSignals;
    private final long supersededTerminalSignals;
    private final long lateDataAfterCloseRead;
    private final long lateDataAfterReset;
    private final long lateDataAfterAbort;
    private final long visibleTerminalChurnEvents;
    private final long groupRebucketEvents;
    private final long skippedCloseOnDeadIo;
    private final long closeFrameFlushErrors;
    private final long hiddenStreamsRefused;
    private final long hiddenStreamsReaped;
    private final long hiddenUnreadBytesDiscarded;

    SessionStatsDiagnosticSnapshot(long droppedPriorityUpdates,
                                   long droppedLocalPriorityUpdates,
                                   long coalescedTerminalSignals,
                                   long supersededTerminalSignals,
                                   long lateDataAfterCloseRead,
                                   long lateDataAfterReset,
                                   long lateDataAfterAbort,
                                   long visibleTerminalChurnEvents,
                                   long groupRebucketEvents,
                                   long skippedCloseOnDeadIo,
                                   long closeFrameFlushErrors,
                                   long hiddenStreamsRefused,
                                   long hiddenStreamsReaped,
                                   long hiddenUnreadBytesDiscarded) {
        this.droppedPriorityUpdates = droppedPriorityUpdates;
        this.droppedLocalPriorityUpdates = droppedLocalPriorityUpdates;
        this.coalescedTerminalSignals = coalescedTerminalSignals;
        this.supersededTerminalSignals = supersededTerminalSignals;
        this.lateDataAfterCloseRead = lateDataAfterCloseRead;
        this.lateDataAfterReset = lateDataAfterReset;
        this.lateDataAfterAbort = lateDataAfterAbort;
        this.visibleTerminalChurnEvents = visibleTerminalChurnEvents;
        this.groupRebucketEvents = groupRebucketEvents;
        this.skippedCloseOnDeadIo = skippedCloseOnDeadIo;
        this.closeFrameFlushErrors = closeFrameFlushErrors;
        this.hiddenStreamsRefused = hiddenStreamsRefused;
        this.hiddenStreamsReaped = hiddenStreamsReaped;
        this.hiddenUnreadBytesDiscarded = hiddenUnreadBytesDiscarded;
    }

    long droppedPriorityUpdates() {
        return droppedPriorityUpdates;
    }

    long droppedLocalPriorityUpdates() {
        return droppedLocalPriorityUpdates;
    }

    long coalescedTerminalSignals() {
        return coalescedTerminalSignals;
    }

    long supersededTerminalSignals() {
        return supersededTerminalSignals;
    }

    long lateDataAfterCloseRead() {
        return lateDataAfterCloseRead;
    }

    long lateDataAfterReset() {
        return lateDataAfterReset;
    }

    long lateDataAfterAbort() {
        return lateDataAfterAbort;
    }

    long visibleTerminalChurnEvents() {
        return visibleTerminalChurnEvents;
    }

    long groupRebucketEvents() {
        return groupRebucketEvents;
    }

    long skippedCloseOnDeadIo() {
        return skippedCloseOnDeadIo;
    }

    long closeFrameFlushErrors() {
        return closeFrameFlushErrors;
    }

    long hiddenStreamsRefused() {
        return hiddenStreamsRefused;
    }

    long hiddenStreamsReaped() {
        return hiddenStreamsReaped;
    }

    long hiddenUnreadBytesDiscarded() {
        return hiddenUnreadBytesDiscarded;
    }
}

final class SessionStatsReasonSnapshot {
    private final Map<Long, Long> reset;
    private final long resetOverflow;
    private final Map<Long, Long> abort;
    private final long abortOverflow;

    SessionStatsReasonSnapshot(Map<Long, Long> reset, long resetOverflow, Map<Long, Long> abort, long abortOverflow) {
        this.reset = reset;
        this.resetOverflow = resetOverflow;
        this.abort = abort;
        this.abortOverflow = abortOverflow;
    }

    Map<Long, Long> reset() {
        return reset;
    }

    long resetOverflow() {
        return resetOverflow;
    }

    Map<Long, Long> abort() {
        return abort;
    }

    long abortOverflow() {
        return abortOverflow;
    }
}

final class SessionStatsReceiveSnapshot {
    private final long bufferedReceiveBytes;
    private final long bufferedReceiveStorageBytes;
    private final long recvSessionAdvertisedBytes;
    private final long recvSessionReceivedBytes;
    private final long recvSessionPendingBytes;

    SessionStatsReceiveSnapshot(long bufferedReceiveBytes,
                                long bufferedReceiveStorageBytes,
                                long recvSessionAdvertisedBytes,
                                long recvSessionReceivedBytes,
                                long recvSessionPendingBytes) {
        this.bufferedReceiveBytes = bufferedReceiveBytes;
        this.bufferedReceiveStorageBytes = bufferedReceiveStorageBytes;
        this.recvSessionAdvertisedBytes = recvSessionAdvertisedBytes;
        this.recvSessionReceivedBytes = recvSessionReceivedBytes;
        this.recvSessionPendingBytes = recvSessionPendingBytes;
    }

    long bufferedReceiveBytes() {
        return bufferedReceiveBytes;
    }

    long bufferedReceiveStorageBytes() {
        return bufferedReceiveStorageBytes;
    }

    long recvSessionAdvertisedBytes() {
        return recvSessionAdvertisedBytes;
    }

    long recvSessionReceivedBytes() {
        return recvSessionReceivedBytes;
    }

    long recvSessionPendingBytes() {
        return recvSessionPendingBytes;
    }
}

final class SessionStatsCollector {
    private final SessionRuntime runtime;

    SessionStatsCollector(SessionRuntime runtime) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
    }

    private static SessionStats.RetainedBucketStats retainedBucketStats(long count, long unitBytes) {
        long safeCount = Math.max(0L, count);
        return new SessionStats.RetainedBucketStats(
                safeCount,
                saturatingMultiply(safeCount, Math.max(0L, unitBytes))
        );
    }

    private static long saturatingMultiply(long left, long right) {
        if (left == 0L || right == 0L) {
            return 0L;
        }
        try {
            return Math.multiplyExact(left, right);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    SessionStats collectLocked() {
        SessionState publicState = this.runtime.publicStateLocked();
        if (publicState == SessionState.INVALID || this.runtime.negotiatedInternal() == null) {
            return SessionStats.empty(SessionState.INVALID);
        }

        SessionTelemetryState telemetry = this.runtime.telemetryInternal();
        SessionOutboundQueueBookkeeping outboundQueueBookkeeping = this.runtime.outboundQueueBookkeepingInternal();
        SessionLocalOpenTracker localOpenTracker = this.runtime.localOpenTrackerInternal();
        SessionStatsSurfaceSnapshot surface = this.runtime.statsSurfaceSnapshotLocked();
        SessionStatsDiagnosticSnapshot diagnosticsSnapshot = this.runtime.statsDiagnosticSnapshotLocked();
        SessionStatsReasonSnapshot reasonSnapshot = this.runtime.statsReasonSnapshotLocked();
        SessionStatsReceiveSnapshot receiveSnapshot = this.runtime.statsReceiveSnapshotLocked();

        int pendingAcceptedCount = this.runtime.pendingAcceptedCountLocked();
        long pendingAcceptedBytes = this.runtime.pendingAcceptedBytesLocked();
        int backlogHardCap = this.runtime.visibleAcceptBacklogHardCapLocked();
        long backlogBytesHardCap = this.runtime.visibleAcceptBacklogBytesHardCapLocked();
        int provisionalBidiCount = localOpenTracker.provisionalCountLocked(true);
        int provisionalUniCount = localOpenTracker.provisionalCountLocked(false);
        int provisionalSoftCap = this.runtime.provisionalOpenSoftCapLocked();
        int provisionalHardCap = this.runtime.provisionalOpenHardCapLocked();
        int hiddenRetained = this.runtime.hiddenControlStateRetainedLocked();
        int hiddenSoftCap = this.runtime.hiddenControlStateSoftCapLocked();
        int hiddenHardCap = this.runtime.hiddenControlStateHardCapLocked();
        int visibleTombstones = this.runtime.visibleTombstoneRetainedLocked();
        int markerOnly = this.runtime.markerOnlyRetainedLocked();
        long trackedRetainedStateMemory = this.runtime.trackedRetainedStateMemoryLocked();
        long trackedSessionMemory = this.runtime.trackedSessionMemoryLocked();
        long sessionMemoryHighThreshold = this.runtime.sessionMemoryHighThresholdLocked();
        long sessionMemoryHardCap = this.runtime.sessionMemoryHardCapLocked();
        long retainedStateUnit = this.runtime.retainedStateUnitLocked();

        SessionStats.AcceptBacklogStats acceptBacklog = new SessionStats.AcceptBacklogStats(
                pendingAcceptedCount,
                pendingAcceptedBytes,
                backlogHardCap,
                backlogBytesHardCap,
                backlogHardCap > 0 && pendingAcceptedCount >= backlogHardCap,
                backlogBytesHardCap > 0L && pendingAcceptedBytes >= backlogBytesHardCap,
                this.runtime.acceptRegistryInternal().visibleAcceptRefusedLocked()
        );
        SessionStats.QueueStats queues = new SessionStats.QueueStats(
                this.runtime.urgentQueueInternal().size(),
                this.runtime.advisoryQueueInternal().size(),
                this.runtime.dataQueueInternal().size(),
                surface.sessionQueuedDataBytes(),
                surface.sessionReservedSendBytes(),
                outboundQueueBookkeeping.urgentQueuedControlBytesLocked(),
                outboundQueueBookkeeping.ordinaryQueuedControlBytesLocked(),
                outboundQueueBookkeeping.pendingControlBytesLocked(),
                outboundQueueBookkeeping.pendingPriorityBytesLocked(),
                surface.writerHeldRetainedBytes()
        );
        SessionStats.ProvisionalStats provisionals = new SessionStats.ProvisionalStats(
                provisionalBidiCount,
                provisionalUniCount,
                provisionalSoftCap,
                provisionalHardCap,
                this.runtime.provisionalOpenMaxAgeNanosLocked(),
                provisionalBidiCount >= provisionalSoftCap,
                provisionalUniCount >= provisionalSoftCap,
                provisionalBidiCount >= provisionalHardCap,
                provisionalUniCount >= provisionalHardCap,
                this.runtime.provisionalOpenLimitedCountLocked(),
                this.runtime.provisionalOpenExpiredCountLocked()
        );
        SessionStats.HiddenStateStats hiddenState = new SessionStats.HiddenStateStats(
                hiddenRetained,
                hiddenSoftCap,
                hiddenHardCap,
                hiddenRetained >= hiddenSoftCap,
                hiddenRetained >= hiddenHardCap,
                visibleTombstones,
                markerOnly,
                diagnosticsSnapshot.hiddenStreamsRefused(),
                diagnosticsSnapshot.hiddenStreamsReaped(),
                diagnosticsSnapshot.hiddenUnreadBytesDiscarded()
        );
        SessionStats.ReasonStats reasons = new SessionStats.ReasonStats(
                reasonSnapshot.reset(),
                reasonSnapshot.resetOverflow(),
                reasonSnapshot.abort(),
                reasonSnapshot.abortOverflow()
        );
        SessionStats.DiagnosticStats diagnostics = new SessionStats.DiagnosticStats(
                diagnosticsSnapshot.droppedPriorityUpdates(),
                diagnosticsSnapshot.droppedLocalPriorityUpdates(),
                diagnosticsSnapshot.lateDataAfterCloseRead(),
                diagnosticsSnapshot.lateDataAfterReset(),
                diagnosticsSnapshot.lateDataAfterAbort(),
                diagnosticsSnapshot.visibleTerminalChurnEvents(),
                diagnosticsSnapshot.groupRebucketEvents(),
                outboundQueueBookkeeping.protocolBacklogBlockedCountLocked(),
                diagnosticsSnapshot.skippedCloseOnDeadIo(),
                diagnosticsSnapshot.closeFrameFlushErrors(),
                telemetry.closeCompletionTimeoutCountLocked(),
                telemetry.gracefulCloseTimeoutCountLocked(),
                telemetry.keepaliveTimeoutCountLocked(),
                diagnosticsSnapshot.coalescedTerminalSignals(),
                diagnosticsSnapshot.supersededTerminalSignals(),
                0L,
                0L,
                this.runtime.markerOnlyRangeCountLocked()
        );
        SessionStats.RetainedStateBreakdownStats retainedStateBreakdown = new SessionStats.RetainedStateBreakdownStats(
                retainedBucketStats(hiddenRetained, retainedStateUnit),
                retainedBucketStats(pendingAcceptedCount, retainedStateUnit),
                retainedBucketStats(localOpenTracker.totalProvisionalCountLocked(), retainedStateUnit),
                retainedBucketStats(visibleTombstones, retainedStateUnit),
                retainedBucketStats(markerOnly, retainedStateUnit)
        );
        SessionStats.PressureStats pressure = new SessionStats.PressureStats(
                trackedSessionMemory,
                trackedRetainedStateMemory,
                retainedStateBreakdown,
                sessionMemoryHighThreshold,
                sessionMemoryHardCap,
                this.runtime.sessionMemoryPressureHighLocked(),
                receiveSnapshot.bufferedReceiveBytes(),
                receiveSnapshot.bufferedReceiveStorageBytes(),
                receiveSnapshot.recvSessionAdvertisedBytes(),
                receiveSnapshot.recvSessionReceivedBytes(),
                receiveSnapshot.recvSessionPendingBytes(),
                telemetry.outstandingPingBytesLocked()
        );
        return new SessionStats(
                publicState,
                surface.sentFrames(),
                surface.receivedFrames(),
                surface.sentDataBytes(),
                surface.receivedDataBytes(),
                surface.openStreams(),
                surface.acceptedStreams(),
                this.runtime.streamBookkeepingInternal().activeStreamStatsLocked(),
                acceptBacklog,
                surface.retainedOpenInfoBytes(),
                this.runtime.retainedOpenInfoBudgetLocked(),
                surface.retainedPeerReasonBytes(),
                this.runtime.retainedPeerReasonBudgetLocked(),
                telemetry.keepaliveStatsLocked(),
                telemetry.progressStatsLocked(),
                telemetry.flushStatsLocked(),
                telemetry.blockedWriteTotalNanosLocked(),
                telemetry.lastOpenLatencyNanosLocked(),
                queues,
                provisionals,
                hiddenState,
                reasons,
                diagnostics,
                pressure
        );
    }
}
