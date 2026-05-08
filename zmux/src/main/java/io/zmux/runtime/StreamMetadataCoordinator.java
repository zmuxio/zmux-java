package io.zmux.runtime;

import io.zmux.EmptyMetadataUpdateException;
import io.zmux.MetadataUpdate;
import io.zmux.PriorityUpdateUnavailableException;
import io.zmux.protocol.Protocol;

import java.io.IOException;
import java.util.Objects;

final class StreamMetadataCoordinator {
    private final StreamRuntime owner;

    StreamMetadataCoordinator(StreamRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    void updateMetadata(MetadataUpdate update) throws IOException {
        if (update == null || update.isEmpty()) {
            throw new EmptyMetadataUpdateException();
        }
        synchronized (this.owner.lockInternal()) {
            this.owner.ensureWritableLocked();
            long capabilities = this.owner.sessionInternal().capabilities();
            LocalOpenPhase phase = this.owner.localOpenPhaseLocked();
            if (phase.needsLocalOpener()) {
                this.requireOpenMetadataCapability(capabilities, update);
                this.owner.metadataStateInternal().applyMetadataUpdateLocked(this.owner.sessionInternal(), this.owner, update);
                return;
            }
            if (phase.shouldEmitOpenerFrame() && this.canCarryOnOpen(capabilities, update)) {
                this.owner.metadataStateInternal().validateMetadataUpdateAsOpenLocked(this.owner.sessionInternal(), update);
                this.owner.metadataStateInternal().applyMetadataUpdateLocked(this.owner.sessionInternal(), this.owner, update);
                return;
            }
            if (!this.owner.sessionInternal().allowLocalNonCloseControlLocked()) {
                throw this.owner.sessionInternal().sessionOperationErrorLocked(
                        "write",
                        this.owner.sessionInternal().currentErrorLocked()
                );
            }
            if ((update.priority() != null && !Protocol.canCarryPriorityInUpdate(capabilities))
                    || (update.group() != null && !Protocol.canCarryGroupInUpdate(capabilities))) {
                throw new PriorityUpdateUnavailableException();
            }
            this.owner.sessionInternal().enqueuePriorityUpdateLocked(this.owner, update.priority(), update.group());
            this.owner.metadataStateInternal().applyMetadataUpdateLocked(this.owner.sessionInternal(), this.owner, update);
        }
    }

    private void requireOpenMetadataCapability(long capabilities, MetadataUpdate update) throws IOException {
        if (!this.canCarryOnOpen(capabilities, update)) {
            throw new PriorityUpdateUnavailableException();
        }
    }

    private boolean canCarryOnOpen(long capabilities, MetadataUpdate update) {
        return (update.priority() == null || Protocol.canCarryPriorityOnOpen(capabilities))
                && (update.group() == null || Protocol.canCarryGroupOnOpen(capabilities));
    }
}
