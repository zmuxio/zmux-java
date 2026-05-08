package io.zmux.runtime;

import io.zmux.*;
import io.zmux.protocol.*;

import java.io.IOException;
import java.util.Objects;

final class SessionReaderCoordinatorOwner implements SessionReaderCoordinator.Owner {
    private final SessionRuntime owner;

    SessionReaderCoordinatorOwner(SessionRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    @Override
    public FrameCodec.Decoder input() {
        return this.owner.inputInternal();
    }

    @Override
    public Limits limits() {
        return this.owner.localSettings().limits();
    }

    @Override
    public InboundPayloadPool inboundPayloadPool() {
        return this.owner.inboundPayloadPoolInternal();
    }

    @Override
    public Object lock() {
        return this.owner.lock();
    }

    @Override
    public SessionState state() {
        return this.owner.stateInternal();
    }

    @Override
    public void setState(SessionState state) {
        this.owner.setStateInternal(state);
    }

    @Override
    public void incrementReceivedFramesLocked() {
        this.owner.incrementReceivedFramesLocked();
    }

    @Override
    public void recordInboundBudgetsLocked(FrameEnvelopeCodec.InboundFrame frame) throws IOException {
        this.owner.recordInboundBudgetsLocked(frame);
    }

    @Override
    public void recordInboundPingFloodLocked() throws IOException {
        this.owner.recordInboundPingFloodLocked();
    }

    @Override
    public void noteInboundFrameLocked(long nowNanos) {
        this.owner.noteInboundFrameLocked(nowNanos);
    }

    @Override
    public void reapExpiredHiddenControlStateLocked(long nowNanos) {
        this.owner.reapExpiredHiddenControlStateLocked(nowNanos);
    }

    @Override
    public void emitPendingEvents() {
        this.owner.emitPendingEvents();
    }

    @Override
    public void finishSessionLocked(IOException error, SessionState sessionState) {
        this.owner.finishSessionLocked(error, sessionState);
    }

    @Override
    public void failSession(IOException error) {
        this.owner.failSession(error);
    }

    @Override
    public boolean ignorePeerNonCloseFrameLocked(FrameType frameType) {
        return this.owner.ignorePeerNonCloseFrameLocked(frameType);
    }

    @Override
    public boolean ignorePeerCloseFrameLocked() {
        return this.owner.ignorePeerCloseFrameLocked();
    }

    @Override
    public long capabilities() {
        return this.owner.capabilities();
    }

    @Override
    public Role localRole() {
        return this.owner.localRole();
    }

    @Override
    public long localGoAwayBidi() {
        return this.owner.localGoAwayBidiInternal();
    }

    @Override
    public long localGoAwayUni() {
        return this.owner.localGoAwayUniInternal();
    }

    @Override
    public StreamRuntime liveStreamLocked(long streamId) {
        return this.owner.liveStreamLocked(streamId);
    }

    @Override
    public Iterable<StreamRuntime> liveStreamsLocked() {
        return this.owner.liveStreamsLocked();
    }

    @Override
    public boolean hasTerminalMarkerLocked(long streamId) {
        return this.owner.hasTerminalMarkerLocked(streamId);
    }

    @Override
    public SessionTerminalBookkeeping.TerminalDataDisposition terminalDataDispositionForLocked(long streamId) {
        return this.owner.terminalDataDispositionForLocked(streamId);
    }

    @Override
    public void refusePeerOpeningStreamLocked(long streamId, boolean recordTombstone, boolean hidden)
            throws IOException {
        this.owner.refusePeerOpeningStreamLocked(streamId, recordTombstone, hidden);
    }

    @Override
    public StreamRuntime createPeerOpenedStreamLocked(long streamId) {
        return this.owner.createPeerOpenedStreamLocked(streamId);
    }

    @Override
    public void recordAcceptedPeerStreamLocked(long streamId) {
        this.owner.recordAcceptedPeerStreamLocked(streamId);
    }

    @Override
    public boolean peerStreamWithinLimitLocked(boolean bidirectional) {
        return this.owner.peerStreamWithinLimitLocked(bidirectional);
    }

    @Override
    public void reclaimUnseenLocalStreamsLocked() {
        this.owner.reclaimUnseenLocalStreamsLocked();
    }

    @Override
    public void reclaimProvisionalsLocked() {
        this.owner.reclaimProvisionalsLocked();
    }

    @Override
    public void markPeerVisibleLocked(StreamRuntime streamRuntime) throws IOException {
        this.owner.markPeerVisibleLocked(streamRuntime);
    }

    @Override
    public void maybeCompactStreamLocked(StreamRuntime streamRuntime) {
        this.owner.maybeCompactStreamLocked(streamRuntime);
    }

    @Override
    public byte[] buildControlErrorPayloadLocked(long code, String reason) throws IOException {
        return this.owner.buildControlErrorPayloadLocked(code, reason);
    }

    @Override
    public long controlPayloadLimitLocked() {
        return this.owner.controlPayloadLimitLocked();
    }

    @Override
    public void enqueueReadLoopAbortLocked(StreamRuntime streamRuntime, long code, byte[] payload) throws IOException {
        this.owner.enqueueReadLoopAbortLocked(streamRuntime, code, payload);
    }

    @Override
    public void enqueueResetLocked(StreamRuntime streamRuntime, long code, byte[] payload) throws IOException {
        this.owner.enqueueResetLocked(streamRuntime, code, payload);
    }

    @Override
    public void enqueueControlLocked(FrameCodec.Frame frame) throws IOException {
        this.owner.enqueueControlLocked(frame);
    }

    @Override
    public void enqueuePongLocked(byte[] payload) throws IOException {
        this.owner.enqueuePongLocked(payload);
    }

    @Override
    public boolean handlePongLocked(byte[] payload, long nowNanos) {
        return this.owner.handlePongLocked(payload, nowNanos);
    }

    @Override
    public void recordNoOpZeroDataLocked() throws IOException {
        this.owner.recordNoOpZeroDataLocked();
    }

    @Override
    public void clearNoOpZeroDataLocked() {
        this.owner.clearNoOpZeroDataLocked();
    }

    @Override
    public void recordNoOpMaxDataLocked() throws IOException {
        this.owner.recordNoOpMaxDataLocked();
    }

    @Override
    public void clearNoOpMaxDataLocked() {
        this.owner.clearNoOpMaxDataLocked();
    }

    @Override
    public void recordNoOpControlLocked(String description) throws IOException {
        this.owner.recordNoOpControlLocked(description);
    }

    @Override
    public void clearNoOpControlLocked() {
        this.owner.clearNoOpControlLocked();
    }

    @Override
    public void recordNoOpBlockedLocked() throws IOException {
        this.owner.recordNoOpBlockedLocked();
    }

    @Override
    public void clearNoOpBlockedLocked() {
        this.owner.clearNoOpBlockedLocked();
    }

    @Override
    public void recordDroppedPriorityUpdateLocked() {
        this.owner.recordDroppedPriorityUpdateLocked();
    }

    @Override
    public void recordNoOpPriorityUpdateLocked() throws IOException {
        this.owner.recordNoOpPriorityUpdateLocked();
    }

    @Override
    public void clearNoOpPriorityUpdateLocked() {
        this.owner.clearNoOpPriorityUpdateLocked();
    }

    @Override
    public void recordGroupRebucketLocked(StreamRuntime streamRuntime, Long previousGroup) throws IOException {
        this.owner.recordGroupRebucketLocked(streamRuntime, previousGroup);
    }

    @Override
    public void noteResetReasonLocked(long code) {
        this.owner.noteResetReasonLocked(code);
    }

    @Override
    public void noteAbortReasonLocked(long code) {
        this.owner.noteAbortReasonLocked(code);
    }

    @Override
    public void retainHiddenAbortTombstoneLocked(long streamId, long code, String reason, long nowNanos) {
        this.owner.retainHiddenAbortTombstoneLocked(streamId, code, reason, nowNanos);
    }

    @Override
    public void recordHiddenAbortChurnLocked(long nowNanos) throws IOException {
        this.owner.recordHiddenAbortChurnLocked(nowNanos);
    }

    @Override
    public void recordVisibleTerminalChurnLocked(StreamRuntime streamRuntime) throws IOException {
        this.owner.recordVisibleTerminalChurnLocked(streamRuntime);
    }

    @Override
    public void recordLocalAbortTerminalChurnLocked(StreamRuntime streamRuntime) throws IOException {
        this.owner.recordLocalAbortTerminalChurnLocked(streamRuntime);
    }

    @Override
    public void enforceVisibleAcceptBacklogLocked() throws IOException {
        this.owner.enforceVisibleAcceptBacklogLocked();
    }

    @Override
    public IOException sessionMemoryCapErrorLocked(String operation) {
        return this.owner.sessionMemoryCapErrorLocked(operation);
    }

    @Override
    public void enqueueAcceptedLocked(StreamRuntime streamRuntime) {
        this.owner.enqueueAcceptedLocked(streamRuntime);
    }

    @Override
    public boolean tryGracefulStopSendingLocked(StreamRuntime streamRuntime) throws IOException {
        return this.owner.tryGracefulStopSendingLocked(streamRuntime);
    }

    @Override
    public String retainPeerReasonLocked(long oldBytes, String reason) {
        return this.owner.retainPeerReasonLocked(oldBytes, reason);
    }

    @Override
    public void releasePeerReasonBytesLocked(long bytes) {
        this.owner.releasePeerReasonBytesLocked(bytes);
    }

    @Override
    public void addAcceptQueuedBytesLocked(StreamRuntime streamRuntime, long queuedBytes) {
        this.owner.addAcceptQueuedBytesLocked(streamRuntime, queuedBytes);
    }

    @Override
    public void releaseAcceptQueuedBytesLocked(StreamRuntime streamRuntime, long releasedBytes) {
        this.owner.releaseAcceptQueuedBytesLocked(streamRuntime, releasedBytes);
    }

    @Override
    public long sessionWindowTargetLocked() {
        return this.owner.sessionWindowTargetLocked();
    }

    @Override
    public long sessionEmergencyThresholdLocked() {
        return this.owner.sessionEmergencyThresholdLocked();
    }

    @Override
    public long sessionReplenishMinPendingLocked(long target) {
        return this.owner.sessionReplenishMinPendingLocked(target);
    }

    @Override
    public boolean sessionStandingGrowthAllowedLocked() {
        return this.owner.sessionStandingGrowthAllowedLocked();
    }

    @Override
    public long streamWindowTargetLocked(StreamRuntime streamRuntime) {
        return this.owner.streamWindowTargetLocked(streamRuntime);
    }

    @Override
    public long streamEmergencyThresholdLocked(long target) {
        return this.owner.streamEmergencyThresholdLocked(target);
    }

    @Override
    public long streamReplenishMinPendingLocked(long target) {
        return this.owner.streamReplenishMinPendingLocked(target);
    }

    @Override
    public boolean streamStandingGrowthAllowedLocked(StreamRuntime streamRuntime) {
        return this.owner.streamStandingGrowthAllowedLocked(streamRuntime);
    }

    @Override
    public boolean queueSessionMaxDataLocked(long desiredOffset) {
        return this.owner.queueSessionMaxDataLocked(desiredOffset);
    }

    @Override
    public boolean queueStreamMaxDataLocked(long streamId, long desiredOffset) {
        return this.owner.queueStreamMaxDataLocked(streamId, desiredOffset);
    }

    @Override
    public boolean flushPendingWindowUpdatesLocked(Long preferredStreamId) throws IOException {
        return this.owner.flushPendingWindowUpdatesLocked(preferredStreamId);
    }

    @Override
    public void noteLateDataDiscardLocked(int length, LateDataCause cause) {
        this.owner.noteLateDataDiscardLocked(length, cause);
    }

    @Override
    public void onHiddenUnreadBytesDiscardedLocked(long bytes) {
        this.owner.onHiddenUnreadBytesDiscardedLocked(bytes);
    }

    @Override
    public boolean recordTerminalLateDataLocked(long streamId, int length) {
        return this.owner.recordTerminalLateDataLocked(streamId, length);
    }

    @Override
    public long aggregateLateDataCap() {
        return this.owner.aggregateLateDataCap();
    }

    @Override
    public long lateDataPerStreamCap(StreamRuntime streamRuntime) {
        return this.owner.lateDataPerStreamCap(streamRuntime);
    }

    @Override
    public SessionState terminalStateForSessionError(IOException error) {
        return this.owner.terminalStateForSessionError(error);
    }

    @Override
    public IOException sessionError(ErrorCode code,
                                    String operation,
                                    String message,
                                    ZmuxErrorSource source,
                                    ZmuxErrorDirection direction) {
        return SessionRuntime.sessionError(code, operation, message, source, direction);
    }

    @Override
    public long recvSessionReceivedBytes() {
        return this.owner.recvSessionReceivedBytesInternal();
    }

    @Override
    public void setRecvSessionReceivedBytes(long value) {
        this.owner.setRecvSessionReceivedBytesInternal(value);
    }

    @Override
    public long recvSessionAdvertised() {
        return this.owner.recvSessionAdvertisedInternal();
    }

    @Override
    public void setRecvSessionAdvertised(long value) {
        this.owner.setRecvSessionAdvertisedInternal(value);
    }

    @Override
    public long recvSessionPending() {
        return this.owner.recvSessionPendingInternal();
    }

    @Override
    public void setRecvSessionPending(long value) {
        this.owner.setRecvSessionPendingInternal(value);
    }

    @Override
    public boolean receiveReplenishRetryLocked() {
        return this.owner.receiveReplenishRetryLocked();
    }

    @Override
    public void setReceiveReplenishRetryLocked(boolean value) {
        this.owner.setReceiveReplenishRetryLocked(value);
    }

    @Override
    public void addReceivedDataBytes(long value) {
        this.owner.addReceivedDataBytesInternal(value);
    }

    @Override
    public long sessionSendLimit() {
        return this.owner.sessionSendLimitLocked();
    }

    @Override
    public void setSessionSendLimit(long value) {
        this.owner.setSessionSendLimitInternal(value);
    }

    @Override
    public void clearBlockedFrameLocked(long streamId) {
        this.owner.clearBlockedFrameLocked(streamId);
    }

    @Override
    public long peerGoAwayBidi() {
        return this.owner.peerGoAwayBidiInternal();
    }

    @Override
    public void setPeerGoAwayBidi(long value) {
        this.owner.setPeerGoAwayBidiInternal(value);
    }

    @Override
    public long peerGoAwayUni() {
        return this.owner.peerGoAwayUniInternal();
    }

    @Override
    public void setPeerGoAwayUni(long value) {
        this.owner.setPeerGoAwayUniInternal(value);
    }

    @Override
    public ApplicationError peerGoAwayError() {
        return this.owner.peerGoAwayErrorInternal();
    }

    @Override
    public void setPeerGoAwayError(ApplicationError error) {
        this.owner.setPeerGoAwayErrorInternal(error);
    }

    @Override
    public ApplicationError peerCloseError() {
        return this.owner.peerCloseErrorInternal();
    }

    @Override
    public void setPeerCloseError(ApplicationError error) {
        this.owner.setPeerCloseErrorInternal(error);
    }

    @Override
    public long nextPeerBidi() {
        return this.owner.nextPeerBidiInternal();
    }

    @Override
    public void setNextPeerBidi(long value) {
        this.owner.setNextPeerBidiInternal(value);
    }

    @Override
    public long nextPeerUni() {
        return this.owner.nextPeerUniInternal();
    }

    @Override
    public void setNextPeerUni(long value) {
        this.owner.setNextPeerUniInternal(value);
    }

    @Override
    public long bufferedReceiveBytes() {
        return this.owner.bufferedReceiveBytesInternal();
    }

    @Override
    public void setBufferedReceiveBytes(long value) {
        this.owner.setBufferedReceiveBytesInternal(value);
    }

    @Override
    public long bufferedReceiveStorageBytes() {
        return this.owner.bufferedReceiveStorageBytesInternal();
    }

    @Override
    public void setBufferedReceiveStorageBytes(long value) {
        this.owner.setBufferedReceiveStorageBytesInternal(value);
    }

    @Override
    public long aggregateLateDataReceived() {
        return this.owner.aggregateLateDataReceivedInternal();
    }

    @Override
    public void setAggregateLateDataReceived(long value) {
        this.owner.setAggregateLateDataReceivedInternal(value);
    }

    @Override
    public long trackedSessionMemoryLocked() {
        return this.owner.trackedSessionMemoryLocked();
    }

    @Override
    public boolean sessionMemoryWakeNeededLocked(long previousTracked) {
        return this.owner.sessionMemoryWakeNeededLocked(previousTracked);
    }

    @Override
    public void notifyAcceptWaiters() {
        this.owner.notifyAcceptWaitersLocked();
    }

    @Override
    public void notifyStreamWaiters() {
        this.owner.notifyStreamWaitersLocked();
    }

    @Override
    public void notifyStreamReadWaiters() {
        this.owner.notifyStreamReadWaitersLocked();
    }

    @Override
    public void notifyStreamWriteWaiters() {
        this.owner.notifyStreamWriteWaitersLocked();
    }

    @Override
    public void notifyWriterWaiters() {
        this.owner.notifyWriterWaitersLocked();
    }

    @Override
    public void notifyLockWaiters() {
        this.owner.notifyLockWaitersLocked();
    }
}
