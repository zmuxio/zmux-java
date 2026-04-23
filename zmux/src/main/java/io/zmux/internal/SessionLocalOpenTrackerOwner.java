package io.zmux.internal;

import java.util.Objects;

final class SessionLocalOpenTrackerOwner implements SessionLocalOpenTracker.Owner {
    private final SessionRuntime owner;

    SessionLocalOpenTrackerOwner(SessionRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    @Override
    public void onStreamOpenInfoUpdatedLocked(int previousLength, int nextLength) {
        this.owner.onStreamOpenInfoUpdatedLocked(previousLength, nextLength);
    }

    @Override
    public void releaseStreamPeerReasonBudgetLocked(StreamRuntime streamRuntime) {
        this.owner.releaseStreamPeerReasonBudgetLocked(streamRuntime);
    }

    @Override
    public void maybeCompactStreamLocked(StreamRuntime streamRuntime) {
        this.owner.maybeCompactStreamLocked(streamRuntime);
    }

    @Override
    public void recordProvisionalOpenExpiredLocked() {
        this.owner.recordProvisionalOpenExpiredLocked();
    }

    @Override
    public void notifyLockWaiters() {
        this.owner.notifyOpenWaitersLocked();
    }
}
