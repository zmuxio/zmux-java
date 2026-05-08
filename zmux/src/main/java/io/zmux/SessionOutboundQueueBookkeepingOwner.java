package io.zmux;

import java.util.Objects;

final class SessionOutboundQueueBookkeepingOwner implements SessionOutboundQueueBookkeeping.Owner {
    private final SessionRuntime owner;

    SessionOutboundQueueBookkeepingOwner(SessionRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
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
    public long sessionMemoryHardCapLocked() {
        return this.owner.sessionMemoryHardCapLocked();
    }

    @Override
    public long pendingControlBytesBudgetLocked() {
        return this.owner.pendingControlBytesBudgetLocked();
    }

    @Override
    public long pendingPriorityBytesBudgetLocked() {
        return this.owner.pendingPriorityBytesBudgetLocked();
    }

    @Override
    public void notifyLockWaiters() {
        this.owner.notifyStreamWriteWaitersLocked();
    }
}
