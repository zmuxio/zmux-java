package io.zmux.runtime;

import io.zmux.ApplicationError;
import io.zmux.SessionState;
import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

final class SessionLifecycleCoordinatorOwner implements SessionLifecycleCoordinator.Owner {
    private final SessionRuntime owner;

    SessionLifecycleCoordinatorOwner(SessionRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    @Override
    public Object lock() {
        return this.owner.lock();
    }

    @Override
    public IOException terminalError() {
        return this.owner.terminalErrorInternal();
    }

    @Override
    public void setTerminalError(IOException error) {
        this.owner.setTerminalErrorInternal(error);
    }

    @Override
    public ApplicationError peerCloseError() {
        return this.owner.peerCloseErrorInternal();
    }

    @Override
    public boolean closeFrameQueued() {
        return this.owner.closeFrameQueuedInternal();
    }

    @Override
    public void setCloseFrameQueued(boolean value) {
        this.owner.setCloseFrameQueuedInternal(value);
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
    public void setGracefulCloseActive(boolean value) {
        this.owner.setGracefulCloseActiveInternal(value);
    }

    @Override
    public void clearKeepaliveSchedulesLocked() {
        this.owner.clearKeepaliveSchedulesLockedInternal();
    }

    @Override
    public void setInflightBatch(List<SessionRuntime.OutboundFrame> batch) {
        this.owner.setInflightBatchInternal(batch);
    }

    @Override
    public boolean terminalCleanupApplied() {
        return this.owner.terminalCleanupAppliedInternal();
    }

    @Override
    public void setTerminalCleanupApplied(boolean value) {
        this.owner.setTerminalCleanupAppliedInternal(value);
    }

    @Override
    public void discardPendingOutboundLocked() {
        this.owner.discardPendingOutboundLocked();
    }

    @Override
    public List<SessionRuntime.OutboundFrame> takeQueuedGoAwayFramesLocked() {
        return this.owner.takeQueuedGoAwayFramesLocked();
    }

    @Override
    public void failActivePingLocked(IOException error) {
        this.owner.failActivePingLocked(error);
    }

    @Override
    public ApplicationError sessionCloseStreamErrorLocked(SessionState sessionState) {
        return this.owner.sessionCloseStreamErrorLocked(sessionState);
    }

    @Override
    public void releaseAllStreamsForSessionCloseLocked(ApplicationError error) {
        this.owner.releaseAllStreamsForSessionCloseLocked(error);
    }

    @Override
    public void clearSessionCloseStateLocked() {
        this.owner.clearSessionCloseStateLocked();
    }

    @Override
    public void restoreQueuedGoAwayFramesLocked(List<SessionRuntime.OutboundFrame> frames) {
        this.owner.restoreQueuedGoAwayFramesLocked(frames);
    }

    @Override
    public boolean hasActivePingLocked() {
        return this.owner.hasActivePingLockedInternal();
    }

    @Override
    public byte[] buildControlErrorPayloadLocked(long code, String reason) throws IOException {
        return this.owner.buildControlErrorPayloadLocked(code, reason);
    }

    @Override
    public void enqueueCloseFrameLocked(byte[] payload) throws IOException {
        this.owner.enqueueControlLocked(new FrameCodec.Frame(FrameType.CLOSE, 0, 0L, payload));
    }

    @Override
    public void enqueueSessionClosedEventLocked(IOException error) {
        this.owner.enqueueSessionClosedEventLocked(error);
    }

    @Override
    public void notifyLockWaiters() {
        this.owner.notifyLockWaitersLocked();
    }

    @Override
    public void terminatedCountDown() {
        this.owner.terminatedCountDownInternal();
    }

    @Override
    public boolean closedTransport() {
        return this.owner.closedTransportInternal();
    }

    @Override
    public void setClosedTransport(boolean value) {
        this.owner.setClosedTransportInternal(value);
    }

    @Override
    public void closeConnection() throws IOException {
        this.owner.connection().close();
    }

    @Override
    public boolean hasGracefulClosePendingWorkLocked() {
        return this.owner.hasGracefulClosePendingWorkLocked();
    }

    @Override
    public long goAwayDrainIntervalNanosLocked() {
        return this.owner.goAwayDrainIntervalNanosLocked();
    }

    @Override
    public boolean awaitTermination(Duration duration) throws InterruptedException {
        return this.owner.awaitTermination(duration);
    }

    @Override
    public void waitOnLockNanos(long waitNanos) throws InterruptedException {
        this.owner.waitOnLockNanos(waitNanos, SessionRuntime.LockWaitKind.LIFECYCLE);
    }

    @Override
    public void recordCloseCompletionTimeoutLocked() {
        this.owner.recordCloseCompletionTimeoutLocked();
    }
}
