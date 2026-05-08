package io.zmux;

import java.util.Objects;

final class SessionReceiveWindowUpdater {
    private final SessionReaderCoordinator.Owner owner;

    SessionReceiveWindowUpdater(SessionReaderCoordinator.Owner owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    boolean maybeReplenishReceiveLocked(StreamRuntime streamRuntime, boolean force) {
        boolean sessionQueued = this.maybeReplenishSessionLocked(force);
        boolean streamQueued = this.maybeReplenishStreamLocked(streamRuntime, force);
        return sessionQueued || streamQueued;
    }

    boolean maybeReplenishSessionLocked(boolean force) {
        if (this.owner.recvSessionPending() == 0L) {
            return false;
        }
        long target = this.owner.sessionWindowTargetLocked();
        if (!force && !SessionRuntime.shouldReplenishPendingWindow(
                RuntimeFlow.windowRemaining(this.owner.recvSessionAdvertised(), this.owner.recvSessionReceivedBytes()),
                target,
                this.owner.recvSessionAdvertised(),
                this.owner.recvSessionPending(),
                this.owner.sessionEmergencyThresholdLocked(),
                this.owner.sessionReplenishMinPendingLocked(target)
        )) {
            return false;
        }
        return this.replenishSessionLocked(target);
    }

    boolean maybeReplenishStreamLocked(StreamRuntime streamRuntime, boolean force) {
        if (streamRuntime == null || streamRuntime.recvPendingLocked() == 0L) {
            return false;
        }
        if (streamRuntime.streamIdInternal() <= 0L
                || !streamRuntime.localReceive()
                || streamRuntime.recvStoppedOrTerminal()) {
            streamRuntime.clearRecvPendingLocked();
            return false;
        }
        long target = this.owner.streamWindowTargetLocked(streamRuntime);
        if (!force && !SessionRuntime.shouldReplenishPendingWindow(
                RuntimeFlow.windowRemaining(streamRuntime.recvAdvertisedLimit(), streamRuntime.recvReceivedBytes()),
                target,
                streamRuntime.recvAdvertisedLimit(),
                streamRuntime.recvPendingLocked(),
                this.owner.streamEmergencyThresholdLocked(target),
                this.owner.streamReplenishMinPendingLocked(target)
        )) {
            return false;
        }
        return this.replenishStreamLocked(streamRuntime, target);
    }

    void retryReceiveReplenishLocked() {
        if (!this.owner.receiveReplenishRetryLocked()) {
            return;
        }
        this.owner.setReceiveReplenishRetryLocked(false);
        boolean pendingRemains = false;
        if (this.owner.recvSessionPending() != 0L) {
            this.maybeReplenishSessionLocked(true);
            pendingRemains = this.owner.recvSessionPending() != 0L;
        }
        for (StreamRuntime streamRuntime : this.owner.liveStreamsLocked()) {
            if (streamRuntime != null && streamRuntime.recvPendingLocked() != 0L) {
                this.maybeReplenishStreamLocked(streamRuntime, true);
                if (streamRuntime.recvPendingLocked() != 0L) {
                    pendingRemains = true;
                }
            }
        }
        this.owner.setReceiveReplenishRetryLocked(pendingRemains);
    }

    private boolean replenishSessionLocked(long target) {
        long desired = SessionRuntime.saturatingAdd(this.owner.recvSessionAdvertised(), this.owner.recvSessionPending());
        if (this.owner.sessionStandingGrowthAllowedLocked()) {
            long targetDesired = SessionRuntime.saturatingAdd(this.owner.recvSessionReceivedBytes(), target);
            if (targetDesired > desired) {
                desired = targetDesired;
            }
        }
        desired = SessionRuntime.clampVarint62(desired);
        if (!this.owner.queueSessionMaxDataLocked(desired)) {
            this.owner.setReceiveReplenishRetryLocked(true);
            return false;
        }
        this.owner.setRecvSessionAdvertised(desired);
        this.owner.setRecvSessionPending(0L);
        return true;
    }

    private boolean replenishStreamLocked(StreamRuntime streamRuntime, long target) {
        long desired = SessionRuntime.saturatingAdd(streamRuntime.recvAdvertisedLimit(), streamRuntime.recvPendingLocked());
        if (this.owner.streamStandingGrowthAllowedLocked(streamRuntime)) {
            long targetDesired = SessionRuntime.saturatingAdd(streamRuntime.recvReceivedBytes(), target);
            if (targetDesired > desired) {
                desired = targetDesired;
            }
        }
        desired = SessionRuntime.clampVarint62(desired);
        if (!this.owner.queueStreamMaxDataLocked(streamRuntime.streamIdInternal(), desired)) {
            this.owner.setReceiveReplenishRetryLocked(true);
            return false;
        }
        streamRuntime.raiseRecvAdvertisedLimitLocked(desired);
        streamRuntime.clearRecvPendingLocked();
        return true;
    }
}
