package io.zmux.runtime;

import io.zmux.protocol.Limits;
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
        return this.owner.connection().gatheringOutput();
    }

    @Override
    public Limits limits() {
        return this.owner.peerSettings().limits();
    }
}
