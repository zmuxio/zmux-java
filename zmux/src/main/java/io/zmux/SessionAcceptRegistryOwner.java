package io.zmux;

import java.io.IOException;
import java.util.Objects;

final class SessionAcceptRegistryOwner implements SessionAcceptRegistry.Owner {
    private final SessionRuntime owner;

    SessionAcceptRegistryOwner(SessionRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    @Override
    public long retainedOpenInfoBytesLocked() {
        return this.owner.retainedOpenInfoBytesLocked();
    }

    @Override
    public long retainedOpenInfoBudgetLocked() {
        return this.owner.retainedOpenInfoBudgetLocked();
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
    public int visibleAcceptBacklogHardCapLocked() {
        return this.owner.visibleAcceptBacklogHardCapLocked();
    }

    @Override
    public long visibleAcceptBacklogBytesHardCapLocked() {
        return this.owner.visibleAcceptBacklogBytesHardCapLocked();
    }

    @Override
    public byte[] buildControlErrorPayloadLocked(long code, String reason) throws IOException {
        return this.owner.buildControlErrorPayloadLocked(code, reason);
    }

    @Override
    public void enqueueAbortLocked(StreamRuntime streamRuntime, long code, byte[] payload) throws IOException {
        this.owner.enqueueAbortLocked(streamRuntime, code, payload);
    }

    @Override
    public void enqueueAbortLocked(StreamRuntime streamRuntime,
                                   long code,
                                   byte[] payload,
                                   boolean notifyWaiters) throws IOException {
        this.owner.enqueueAbortLocked(streamRuntime, code, payload, notifyWaiters);
    }

    @Override
    public void maybeCompactStreamLocked(StreamRuntime streamRuntime) {
        this.owner.maybeCompactStreamLocked(streamRuntime);
    }

    @Override
    public void notifyAcceptWaitersLocked() {
        this.owner.notifyAcceptWaitersLocked();
    }

    @Override
    public void notifyWriterWaitersLocked() {
        this.owner.notifyWriterWaitersLocked();
    }
}
