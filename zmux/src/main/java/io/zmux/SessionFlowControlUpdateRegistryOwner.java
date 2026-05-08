package io.zmux;

import java.util.Objects;

final class SessionFlowControlUpdateRegistryOwner implements SessionFlowControlUpdateRegistry.Owner {
    private final SessionRuntime owner;

    SessionFlowControlUpdateRegistryOwner(SessionRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    @Override
    public long pendingControlFrameBytesLocked(long streamId, long value) {
        return this.owner.pendingControlFrameBytesLocked(streamId, value);
    }

    @Override
    public boolean replacePendingControlBytesLocked(long oldBytes, long newBytes) {
        return this.owner.replacePendingControlBytesLocked(oldBytes, newBytes);
    }

    @Override
    public void releasePendingControlBytesLocked(long bytes) {
        this.owner.releasePendingControlBytesLocked(bytes);
    }

    @Override
    public void releasePendingControlBytesForHandoffLocked(long bytes) {
        this.owner.releasePendingControlBytesForHandoffLocked(bytes);
    }

    @Override
    public boolean allowLocalNonCloseControlLocked() {
        return this.owner.allowLocalNonCloseControlLocked();
    }

    @Override
    public StreamRuntime liveStreamLocked(long streamId) {
        return this.owner.liveStreamLocked(streamId);
    }
}
