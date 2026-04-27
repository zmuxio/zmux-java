package io.zmux.internal;

import io.zmux.Limits;

import java.io.OutputStream;
import java.nio.channels.GatheringByteChannel;
import java.util.Objects;

final class SessionWriterTransportOwner implements SessionWriterTransport.Owner {
    private final SessionRuntime owner;

    SessionWriterTransportOwner(SessionRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    @Override
    public OutputStream output() {
        return this.owner.outputInternal();
    }

    @Override
    public GatheringByteChannel gatheringOutput() {
        return this.owner.gatheringOutputInternal();
    }

    @Override
    public Limits limits() {
        return this.owner.peerSettings().limits();
    }
}
