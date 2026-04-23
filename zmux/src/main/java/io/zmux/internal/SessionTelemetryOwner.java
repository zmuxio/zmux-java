package io.zmux.internal;

import io.zmux.FrameType;
import io.zmux.SessionState;

import java.io.IOException;
import java.util.Objects;

final class SessionTelemetryOwner implements SessionTelemetryState.Owner {
    private final SessionRuntime owner;

    SessionTelemetryOwner(SessionRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
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
    public boolean closeFrameQueued() {
        return this.owner.closeFrameQueuedInternal();
    }

    @Override
    public IOException sessionErrorLocked() {
        return this.owner.sessionErrorLocked();
    }

    @Override
    public byte[] buildPingPayloadLocked(byte[] payload) throws IOException {
        return this.owner.buildPingPayloadLocked(payload);
    }

    @Override
    public void enqueuePingLocked(byte[] payload) throws IOException {
        this.owner.enqueueControlLocked(new FrameCodec.Frame(FrameType.PING, 0, 0L, payload));
    }

    @Override
    public void notifyTelemetryWaitersLocked() {
        this.owner.notifyTelemetryWaitersLocked();
    }

    @Override
    public void notifyWriterWaitersLocked() {
        this.owner.notifyWriterWaitersLocked();
    }

    @Override
    public void notifyStreamWriteWaitersLocked() {
        this.owner.notifyStreamWriteWaitersLocked();
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
    public void waitOnLock() throws InterruptedException {
        this.owner.waitOnLock(SessionRuntime.LockWaitKind.TELEMETRY);
    }

    @Override
    public void waitOnLockNanos(long waitNanos) throws InterruptedException {
        this.owner.waitOnLockNanos(waitNanos, SessionRuntime.LockWaitKind.TELEMETRY);
    }

    @Override
    public boolean allowLocalNonCloseControlLocked() {
        return this.owner.allowLocalNonCloseControlLocked();
    }
}
