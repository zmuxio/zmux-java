package io.zmux.internal;

enum LocalOpenPhase {
    NONE,
    NEEDS_COMMIT,
    NEEDS_EMIT,
    QUEUED,
    PEER_VISIBLE;

    static LocalOpenPhase from(boolean localOpened,
                               boolean sendCommitted,
                               boolean peerVisible,
                               boolean openerQueued) {
        if (!localOpened) {
            return NONE;
        }
        if (peerVisible) {
            return PEER_VISIBLE;
        }
        if (!sendCommitted) {
            return NEEDS_COMMIT;
        }
        if (openerQueued) {
            return QUEUED;
        }
        return NEEDS_EMIT;
    }

    boolean isLocal() {
        return this != NONE;
    }

    boolean needsLocalOpener() {
        return this == NEEDS_COMMIT;
    }

    boolean awaitingPeerVisibility() {
        return this == NEEDS_COMMIT || this == NEEDS_EMIT || this == QUEUED;
    }

    boolean shouldEmitOpenerFrame() {
        return this == NEEDS_COMMIT || this == NEEDS_EMIT;
    }

    boolean shouldMarkPeerVisible() {
        return isLocal() && this != PEER_VISIBLE;
    }

    boolean canTakePendingPriorityUpdate() {
        return !awaitingPeerVisibility();
    }

    boolean shouldQueueStreamBlocked(long availableStream) {
        return availableStream == 0L && this == PEER_VISIBLE;
    }
}
