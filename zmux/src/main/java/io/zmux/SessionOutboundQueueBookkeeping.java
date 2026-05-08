package io.zmux;

import java.util.Objects;

final class SessionOutboundQueueBookkeeping {
    private final Owner owner;
    private long urgentQueuedControlBytes;
    private long ordinaryQueuedControlBytes;
    private long pendingControlBytes;
    private long pendingPriorityBytes;
    private long protocolBacklogBlockedCount;

    SessionOutboundQueueBookkeeping(Owner owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    long urgentQueuedControlBytesLocked() {
        return this.urgentQueuedControlBytes;
    }

    long ordinaryQueuedControlBytesLocked() {
        return this.ordinaryQueuedControlBytes;
    }

    long pendingControlBytesLocked() {
        return this.pendingControlBytes;
    }

    long pendingPriorityBytesLocked() {
        return this.pendingPriorityBytes;
    }

    long protocolBacklogBlockedCountLocked() {
        return this.protocolBacklogBlockedCount;
    }

    void noteUrgentFrameEnqueuedLocked(int retainedBytes) {
        if (retainedBytes > 0) {
            this.urgentQueuedControlBytes = SessionRuntime.saturatingAdd(this.urgentQueuedControlBytes, retainedBytes);
        }
    }

    void noteOrdinaryFrameEnqueuedLocked(int retainedBytes) {
        if (retainedBytes > 0) {
            this.ordinaryQueuedControlBytes = SessionRuntime.saturatingAdd(this.ordinaryQueuedControlBytes, retainedBytes);
        }
    }

    void noteUrgentFrameDequeuedLocked(int retainedBytes) {
        this.releaseQueuedControlBytesLocked(retainedBytes, true);
    }

    void noteOrdinaryFrameDequeuedLocked(int retainedBytes) {
        this.releaseQueuedControlBytesLocked(retainedBytes, false);
    }

    boolean replacePendingControlBytesLocked(long oldBytes, long newBytes) {
        long previousTracked = this.owner.trackedSessionMemoryLocked();
        long current = this.pendingControlBytes;
        long projected = this.replaceBucket(this.pendingControlBytes, oldBytes, newBytes);
        if (projected > this.owner.pendingControlBytesBudgetLocked()) {
            this.recordProtocolBacklogBlockedLocked();
            return false;
        }
        if (this.projectedTrackedSessionMemoryLocked(this.pendingControlBytes, projected) > this.owner.sessionMemoryHardCapLocked()) {
            this.recordProtocolBacklogBlockedLocked();
            return false;
        }
        this.pendingControlBytes = projected;
        this.notifyIfReplacementReleasedMemory(previousTracked, current, projected);
        return true;
    }

    PendingPriorityReplaceResult replacePendingPriorityBytesLocked(long oldBytes, long newBytes) {
        long previousTracked = this.owner.trackedSessionMemoryLocked();
        long current = this.pendingPriorityBytes;
        long projected = this.replaceBucket(this.pendingPriorityBytes, oldBytes, newBytes);
        if (projected > this.owner.pendingPriorityBytesBudgetLocked()) {
            this.recordProtocolBacklogBlockedLocked();
            return PendingPriorityReplaceResult.DROPPED_BUDGET;
        }
        if (this.projectedTrackedSessionMemoryLocked(this.pendingPriorityBytes, projected) > this.owner.sessionMemoryHardCapLocked()) {
            this.recordProtocolBacklogBlockedLocked();
            return PendingPriorityReplaceResult.DROPPED_MEMORY;
        }
        this.pendingPriorityBytes = projected;
        this.notifyIfReplacementReleasedMemory(previousTracked, current, projected);
        return PendingPriorityReplaceResult.ACCEPTED;
    }

    void releasePendingControlBytesLocked(long bytes) {
        this.releasePendingControlBytesLocked(bytes, false);
    }

    void releasePendingControlBytesAndNotifyLocked(long bytes) {
        this.releasePendingControlBytesLocked(bytes, true);
    }

    private void releasePendingControlBytesLocked(long bytes, boolean notifyMemoryRelease) {
        if (bytes <= 0L) {
            return;
        }
        long previousTracked = this.owner.trackedSessionMemoryLocked();
        this.pendingControlBytes = Math.max(0L, this.pendingControlBytes - bytes);
        if (notifyMemoryRelease && this.owner.sessionMemoryWakeNeededLocked(previousTracked)) {
            this.owner.notifyLockWaiters();
        }
    }

    void releasePendingPriorityBytesLocked(long bytes) {
        this.releasePendingPriorityBytesLocked(bytes, false);
    }

    void releasePendingPriorityBytesAndNotifyLocked(long bytes) {
        this.releasePendingPriorityBytesLocked(bytes, true);
    }

    private void releasePendingPriorityBytesLocked(long bytes, boolean notifyMemoryRelease) {
        if (bytes <= 0L) {
            return;
        }
        long previousTracked = this.owner.trackedSessionMemoryLocked();
        this.pendingPriorityBytes = Math.max(0L, this.pendingPriorityBytes - bytes);
        if (notifyMemoryRelease && this.owner.sessionMemoryWakeNeededLocked(previousTracked)) {
            this.owner.notifyLockWaiters();
        }
    }

    void clear() {
        this.urgentQueuedControlBytes = 0L;
        this.ordinaryQueuedControlBytes = 0L;
        this.pendingControlBytes = 0L;
        this.pendingPriorityBytes = 0L;
    }

    void recordProtocolBacklogBlockedLocked() {
        this.protocolBacklogBlockedCount = SessionRuntime.saturatingAdd(this.protocolBacklogBlockedCount, 1L);
    }

    private void releaseQueuedControlBytesLocked(int retainedBytes, boolean urgent) {
        if (retainedBytes <= 0) {
            return;
        }
        long previousTracked = this.owner.trackedSessionMemoryLocked();
        if (urgent) {
            this.urgentQueuedControlBytes = Math.max(0L, this.urgentQueuedControlBytes - retainedBytes);
        } else {
            this.ordinaryQueuedControlBytes = Math.max(0L, this.ordinaryQueuedControlBytes - retainedBytes);
        }
        if (this.owner.sessionMemoryWakeNeededLocked(previousTracked)) {
            this.owner.notifyLockWaiters();
        }
    }

    private long projectedTrackedSessionMemoryLocked(long oldBucket, long newBucket) {
        long tracked = this.owner.trackedSessionMemoryLocked();
        if (oldBucket > 0L) {
            tracked = Math.max(0L, tracked - oldBucket);
        }
        return SessionRuntime.saturatingAdd(tracked, newBucket);
    }

    private long replaceBucket(long currentBucket, long oldBytes, long newBytes) {
        long projected = currentBucket;
        if (oldBytes > 0L) {
            projected = Math.max(0L, projected - oldBytes);
        }
        return SessionRuntime.saturatingAdd(projected, newBytes);
    }

    private void notifyIfReplacementReleasedMemory(long previousTracked, long previousBucket, long nextBucket) {
        if (nextBucket < previousBucket && this.owner.sessionMemoryWakeNeededLocked(previousTracked)) {
            this.owner.notifyLockWaiters();
        }
    }

    enum PendingPriorityReplaceResult {
        ACCEPTED,
        DROPPED_BUDGET,
        DROPPED_MEMORY
    }

    interface Owner {
        long trackedSessionMemoryLocked();

        boolean sessionMemoryWakeNeededLocked(long previousTracked);

        long sessionMemoryHardCapLocked();

        long pendingControlBytesBudgetLocked();

        long pendingPriorityBytesBudgetLocked();

        void notifyLockWaiters();
    }
}
