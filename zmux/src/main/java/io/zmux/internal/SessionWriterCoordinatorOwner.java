package io.zmux.internal;

import io.zmux.SessionState;
import io.zmux.Settings;

import java.io.IOException;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

final class SessionWriterCoordinatorOwner implements SessionWriterCoordinator.Owner {
    private final SessionRuntime owner;

    SessionWriterCoordinatorOwner(SessionRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    @Override
    public Object lock() {
        return this.owner.lock();
    }

    @Override
    public void emitPendingEvents() {
        this.owner.emitPendingEvents();
    }

    @Override
    public SessionState state() {
        return this.owner.stateInternal();
    }

    @Override
    public boolean closeFrameQueued() {
        return this.owner.closeFrameQueuedInternal();
    }

    @Override
    public Deque<SessionRuntime.OutboundFrame> urgentQueue() {
        return this.owner.urgentQueueInternal();
    }

    @Override
    public Deque<StreamRuntime> advisoryQueue() {
        return this.owner.advisoryQueueInternal();
    }

    @Override
    public Deque<SessionRuntime.OutboundFrame> dataQueue() {
        return this.owner.dataQueueInternal();
    }

    @Override
    public List<SessionRuntime.OutboundFrame> inflightBatch() {
        return this.owner.inflightBatchInternal();
    }

    @Override
    public void setInflightBatch(List<SessionRuntime.OutboundFrame> batch) {
        this.owner.setInflightBatchInternal(batch);
    }

    @Override
    public void finishSessionLocked(IOException error, SessionState sessionState) {
        this.owner.finishSessionLocked(error, sessionState);
    }

    @Override
    public boolean batchContainsCloseFrameLocked(List<SessionRuntime.OutboundFrame> batch) {
        return this.owner.batchContainsCloseFrameLocked(batch);
    }

    @Override
    public void recordCloseFrameFlushErrorLocked() {
        this.owner.recordCloseFrameFlushErrorLocked();
    }

    @Override
    public void recordSkippedCloseOnDeadIOLocked() {
        this.owner.recordSkippedCloseOnDeadIOLocked();
    }

    @Override
    public void retainWriterHeldFramesLocked(List<SessionRuntime.OutboundFrame> batch) {
        this.owner.retainWriterHeldFramesLocked(batch);
    }

    @Override
    public void appendPendingWindowUpdatesLocked(List<SessionRuntime.OutboundFrame> batch,
                                                 int maxFrames,
                                                 Long preferredStreamId,
                                                 boolean trackWriterHeld) throws IOException {
        this.owner.appendPendingWindowUpdatesLocked(batch, maxFrames, preferredStreamId, trackWriterHeld);
    }

    @Override
    public boolean hasPendingWindowUpdatesLocked() {
        return this.owner.hasPendingWindowUpdatesLocked();
    }

    @Override
    public boolean hasPendingMaxDataLocked() {
        return this.owner.hasPendingMaxDataLocked();
    }

    @Override
    public void expireStopSendingGracefulDrainsLocked() {
        this.owner.expireStopSendingGracefulDrainsLocked();
    }

    @Override
    public void addBatchFrameLocked(List<SessionRuntime.OutboundFrame> batch,
                                    SessionRuntime.OutboundFrame outboundFrame,
                                    boolean trackWriterHeld) {
        this.owner.addBatchFrameLocked(batch, outboundFrame, trackWriterHeld);
    }

    @Override
    public Long outboundSchedulingGroupLocked(StreamRuntime streamRuntime) {
        return this.owner.outboundSchedulingGroupLocked(streamRuntime);
    }

    @Override
    public Settings peerSettings() {
        return this.owner.peerSettings();
    }

    @Override
    public OrdinaryBatchOrderer.RetainedBias ordinaryBatchBias() {
        return this.owner.ordinaryBatchBiasInternal();
    }

    @Override
    public long sendRateEstimateLocked() {
        return this.owner.sendRateEstimateLocked();
    }

    @Override
    public long nextStopSendingGracefulDeadlineLocked() {
        return this.owner.nextStopSendingGracefulDeadlineLocked();
    }

    @Override
    public long nextKeepaliveWakeNanosLocked(long nowNanos) {
        return this.owner.nextKeepaliveWakeNanosLocked(nowNanos);
    }

    @Override
    public boolean processKeepaliveScheduledWorkLocked(long nowNanos) throws IOException {
        return this.owner.processKeepaliveScheduledWorkLocked(nowNanos);
    }

    @Override
    public void retryReceiveReplenishLocked() {
        this.owner.retryReceiveReplenishLocked();
    }

    @Override
    public SessionRuntime.OutboundFrame pollQueuedOutboundLocked(Deque<SessionRuntime.OutboundFrame> deque) {
        return this.owner.pollQueuedOutboundLocked(deque);
    }

    @Override
    public void releaseEmptyAdvisoryQueueStorageLocked() {
        this.owner.releaseEmptyAdvisoryQueueStorageLocked();
    }

    @Override
    public void releaseQueuedDataLocked(SessionRuntime.OutboundFrame outboundFrame) {
        this.owner.releaseQueuedDataLocked(outboundFrame);
    }

    @Override
    public void releaseWriterHeldFrameLocked(SessionRuntime.OutboundFrame outboundFrame) {
        this.owner.releaseWriterHeldFrameLocked(outboundFrame);
    }

    @Override
    public void maybeCompactStreamLocked(StreamRuntime streamRuntime) {
        this.owner.maybeCompactStreamLocked(streamRuntime);
    }

    @Override
    public boolean shouldEmitPriorityUpdateLocked(StreamRuntime streamRuntime) {
        return this.owner.shouldEmitPriorityUpdateLocked(streamRuntime);
    }

    @Override
    public SessionRuntime.OutboundFrame takePendingPriorityUpdateForBatchLocked(StreamRuntime streamRuntime)
            throws IOException {
        return this.owner.takePendingPriorityUpdateForBatchLocked(streamRuntime);
    }

    @Override
    public void afterWriteBatchLocked(List<SessionRuntime.OutboundFrame> batch,
                                      long batchBytes,
                                      long batchStartedAtNanos,
                                      long batchCompletedAtNanos) throws IOException {
        this.owner.afterWriteBatchLocked(batch, batchBytes, batchStartedAtNanos, batchCompletedAtNanos);
    }

    @Override
    public void emitKeepaliveTimeoutClose() throws IOException {
        this.owner.emitKeepaliveTimeoutClose();
    }

    @Override
    public void notifyLockWaiters() {
        this.owner.notifyWriterWaitersLocked();
    }

    @Override
    public void waitOnLock() throws InterruptedException {
        this.owner.waitOnLock(SessionRuntime.LockWaitKind.WRITER);
    }

    @Override
    public void waitOnLockNanos(long waitNanos) throws InterruptedException {
        this.owner.waitOnLockNanos(waitNanos, SessionRuntime.LockWaitKind.WRITER);
    }
}
