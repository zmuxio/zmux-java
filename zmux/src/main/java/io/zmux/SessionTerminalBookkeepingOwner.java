package io.zmux;

import java.io.IOException;
import java.util.Objects;

final class SessionTerminalBookkeepingOwner implements SessionTerminalBookkeeping.Owner {
    private final SessionRuntime owner;

    SessionTerminalBookkeepingOwner(SessionRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    @Override
    public int tombstoneLimitLocked() {
        return this.owner.tombstoneLimitLocked();
    }

    @Override
    public int markerOnlyUsedStreamHardCapLocked() {
        return this.owner.markerOnlyUsedStreamHardCapLocked();
    }

    @Override
    public int hiddenControlStateHardCapLocked() {
        return this.owner.hiddenControlStateHardCapLocked();
    }

    @Override
    public long trackedSessionMemoryLocked() {
        return this.owner.trackedSessionMemoryLocked();
    }

    @Override
    public long sessionMemoryHardCapLocked() {
        return this.owner.sessionMemoryHardCapLocked();
    }

    @Override
    public long retainedStateUnitLocked() {
        return this.owner.retainedStateUnitLocked();
    }

    @Override
    public boolean sessionMemoryWakeNeededLocked(long previousTracked) {
        return this.owner.sessionMemoryWakeNeededLocked(previousTracked);
    }

    @Override
    public void notifyStreamWriteWaitersLocked() {
        this.owner.notifyStreamWriteWaitersLocked();
    }

    @Override
    public boolean sessionTerminalLocked() {
        return this.owner.stateInternal().terminal();
    }

    @Override
    public IOException sessionInternalError(String operation, String message) {
        return SessionRuntime.sessionInternalError(operation, message);
    }

    @Override
    public IOException sessionMemoryCapErrorLocked(String operation) {
        return this.owner.sessionMemoryCapErrorLocked(operation);
    }

    @Override
    public void failSession(IOException error) {
        this.owner.failSession(error);
    }

    @Override
    public void failSessionAsync(IOException error) {
        this.owner.failSessionAsync(error);
    }
}
