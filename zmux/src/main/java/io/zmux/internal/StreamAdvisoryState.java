package io.zmux.internal;

final class StreamAdvisoryState {
    private long sendStopReasonBytes;
    private long recvResetReasonBytes;
    private long recvAbortReasonBytes;
    private long stopSendingGracefulDeadlineNanos;
    private boolean schedulingGroupTracked;
    private long trackedSchedulingGroup;

    void recordSendStopReasonBytes(long reasonBytes) {
        sendStopReasonBytes = Math.max(0L, reasonBytes);
    }

    void recordRecvResetReasonBytes(long reasonBytes) {
        recvResetReasonBytes = Math.max(0L, reasonBytes);
    }

    void recordRecvAbortReasonBytes(long reasonBytes) {
        recvAbortReasonBytes = Math.max(0L, reasonBytes);
    }

    long retainedPeerReasonBytes() {
        return SessionRuntime.saturatingAdd(
                SessionRuntime.saturatingAdd(sendStopReasonBytes, recvResetReasonBytes),
                recvAbortReasonBytes
        );
    }

    long sendStopReasonBytes() {
        return sendStopReasonBytes;
    }

    long recvResetReasonBytes() {
        return recvResetReasonBytes;
    }

    long recvAbortReasonBytes() {
        return recvAbortReasonBytes;
    }

    void clearRetainedPeerReasonBytes() {
        sendStopReasonBytes = 0L;
        recvResetReasonBytes = 0L;
        recvAbortReasonBytes = 0L;
    }

    void armStopSendingGracefulDrainLocked(SessionRuntime session,
                                           StreamRuntime streamRuntime,
                                           long deadlineNanos,
                                           boolean sendTerminal) {
        if (deadlineNanos <= 0L || sendTerminal) {
            clearStopSendingGracefulDrainLocked(session, streamRuntime);
            return;
        }
        stopSendingGracefulDeadlineNanos = deadlineNanos;
        session.updateStopSendingGracefulDeadlineLocked(streamRuntime, deadlineNanos);
    }

    void clearStopSendingGracefulDrainLocked(SessionRuntime session, StreamRuntime streamRuntime) {
        if (stopSendingGracefulDeadlineNanos == 0L) {
            return;
        }
        stopSendingGracefulDeadlineNanos = 0L;
        session.updateStopSendingGracefulDeadlineLocked(streamRuntime, 0L);
    }

    long stopSendingGracefulDeadlineNanos() {
        return stopSendingGracefulDeadlineNanos;
    }

    boolean stopSendingGracefulExpired(long nowNanos) {
        return stopSendingGracefulDeadlineNanos > 0L && nowNanos >= stopSendingGracefulDeadlineNanos;
    }

    boolean schedulingGroupTracked() {
        return schedulingGroupTracked;
    }

    long trackedSchedulingGroup() {
        return trackedSchedulingGroup;
    }

    void markSchedulingGroupTracked(long bucket) {
        schedulingGroupTracked = bucket != 0L;
        trackedSchedulingGroup = bucket;
    }

    void clearSchedulingGroupTracked() {
        schedulingGroupTracked = false;
        trackedSchedulingGroup = 0L;
    }
}
