package io.zmux.internal;

import java.io.IOException;
import java.util.Objects;

final class StreamAdvisoryCoordinator {
    private final StreamRuntime owner;

    StreamAdvisoryCoordinator(StreamRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    void prepareLocalControlOpenerLocked(boolean replaceQueuedPayload, boolean preserveAfterSendClose) throws IOException {
        LocalOpenPhase phase = this.owner.localOpenPhaseLocked();
        if (!phase.awaitingPeerVisibility()) {
            return;
        }
        byte[] openingPrefix = phase.shouldEmitOpenerFrame()
                ? this.owner.metadataStateInternal().buildOpeningPrefixLocked(this.owner.sessionInternal())
                : StreamRuntime.EMPTY_BYTES;
        this.owner.sessionInternal().prepareLocalControlOpenerLocked(
                this.owner,
                openingPrefix,
                replaceQueuedPayload,
                preserveAfterSendClose
        );
    }

    void onFrameWrittenLocked(FrameCodec.Frame frame, boolean openingFrame) {
        if (openingFrame) {
            this.owner.metadataStateInternal().clearOpeningFramePending();
        }
        if (frame.type() == io.zmux.FrameType.EXT) {
            this.owner.metadataStateInternal().clearPriorityUpdateQueued();
        }
        if (frame.type() == io.zmux.FrameType.DATA
                && (frame.flags() & io.zmux.Protocol.FRAME_FLAG_FIN) != 0
                && this.owner.halfStateInternal().sendFinQueued()) {
            StreamHalfState.SendState previousSendState = this.owner.halfStateInternal().markSendFinFromQueued();
            this.clearWriteAdvisoryLocked();
            this.owner.notifySendTerminalTransitionLocked(previousSendState);
            this.refreshGracefulCloseBlockingLocked();
        }
    }

    boolean awaitingPeerVisibilityLocked() {
        return this.owner.metadataStateInternal().awaitingPeerVisibility(
                this.owner.openedLocally(),
                this.owner.lifecycleStateInternal().idAssigned(),
                this.owner.fullyTerminalLocked()
        );
    }

    boolean blocksGracefulSessionCloseLocked() {
        return this.owner.gracefulCloseBlockingInternal();
    }

    void refreshGracefulCloseBlockingLocked() {
        boolean previous = this.owner.gracefulCloseBlockingInternal();
        boolean current = this.computeGracefulCloseBlockingLocked();
        this.owner.setGracefulCloseBlockingInternal(current);
        this.owner.sessionInternal().onStreamGracefulCloseBlockingChangedLocked(this.owner, previous, current);
    }

    boolean computeGracefulCloseBlockingLocked() {
        if (this.owner.fullyTerminalLocked()) {
            return false;
        }
        if (this.owner.openedLocally()) {
            return true;
        }
        if (!this.owner.localSend()) {
            return false;
        }
        if (this.owner.halfStateInternal().sendFinQueued()) {
            return true;
        }
        if (!this.owner.halfStateInternal().effectiveSendOpen()) {
            return false;
        }
        return this.owner.sentBytes() > 0L
                || this.owner.reservedSendBytes() > 0L
                || this.owner.queuedDataBytesLocked() > 0L;
    }

    void armStopSendingGracefulDrainLocked(long deadlineNanos) {
        this.owner.advisoryStateInternal().armStopSendingGracefulDrainLocked(
                this.owner.sessionInternal(),
                this.owner,
                deadlineNanos,
                this.owner.sendTerminalLocked()
        );
    }

    void clearStopSendingGracefulDrainLocked() {
        this.owner.advisoryStateInternal().clearStopSendingGracefulDrainLocked(this.owner.sessionInternal(), this.owner);
    }

    long stopSendingGracefulDeadlineNanosLocked() {
        return this.owner.advisoryStateInternal().stopSendingGracefulDeadlineNanos();
    }

    boolean stopSendingGracefulExpiredLocked(long nowNanos) {
        return this.owner.advisoryStateInternal().stopSendingGracefulExpired(nowNanos);
    }

    long retainedPeerReasonBytesLocked() {
        return this.owner.advisoryStateInternal().retainedPeerReasonBytes();
    }

    long sendStopReasonBytesLocked() {
        return this.owner.advisoryStateInternal().sendStopReasonBytes();
    }

    long recvResetReasonBytesLocked() {
        return this.owner.advisoryStateInternal().recvResetReasonBytes();
    }

    long recvAbortReasonBytesLocked() {
        return this.owner.advisoryStateInternal().recvAbortReasonBytes();
    }

    void clearRetainedPeerReasonBytesLocked() {
        this.owner.advisoryStateInternal().clearRetainedPeerReasonBytes();
    }

    void queuePeerStopGracefulFinishLocked() throws IOException {
        this.owner.sessionInternal().flushPendingPriorityUpdateLocked(this.owner);
        if (this.owner.shouldEmitOpenerFrameLocked()) {
            byte[] prefix = this.owner.metadataStateInternal().buildOpeningPrefixLocked(this.owner.sessionInternal());
            this.owner.sessionInternal().queueOpeningDataLocked(this.owner, prefix, StreamRuntime.EMPTY_BYTES, true);
        } else {
            this.owner.sessionInternal().queueDataLocked(this.owner, StreamRuntime.EMPTY_BYTES, true);
        }
    }

    void stagePriorityUpdateLocked(Long priority, Long group, byte[] payload) {
        this.owner.metadataStateInternal().stagePriorityUpdate(priority, group, payload);
    }

    void clearWriteAdvisoryLocked() {
        this.clearStopSendingGracefulDrainLocked();
        this.owner.sessionInternal().discardPendingPriorityUpdateLocked(this.owner);
    }

    boolean hasPendingPriorityUpdateLocked() {
        return this.owner.metadataStateInternal().hasPendingPriorityUpdate();
    }

    boolean priorityUpdateQueuedLocked() {
        return this.owner.metadataStateInternal().priorityUpdateQueued();
    }

    Long pendingPriorityUpdatePriorityLocked() {
        return this.owner.metadataStateInternal().pendingPriorityUpdatePriority();
    }

    Long pendingPriorityUpdateGroupLocked() {
        return this.owner.metadataStateInternal().pendingPriorityUpdateGroup();
    }

    byte[] pendingPriorityUpdatePayloadLocked() {
        return this.owner.metadataStateInternal().pendingPriorityUpdatePayload();
    }

    void markPriorityUpdateQueuedLocked() {
        this.owner.metadataStateInternal().markPriorityUpdateQueued();
    }

    void clearPriorityUpdateQueuedLocked() {
        this.owner.metadataStateInternal().clearPriorityUpdateQueued();
    }

    void clearPendingPriorityUpdateLocked() {
        this.owner.metadataStateInternal().clearPendingPriorityUpdate();
    }

    boolean schedulingGroupTrackedLocked() {
        return this.owner.advisoryStateInternal().schedulingGroupTracked();
    }

    long trackedSchedulingGroupLocked() {
        return this.owner.advisoryStateInternal().trackedSchedulingGroup();
    }

    void markSchedulingGroupTrackedLocked(long bucket) {
        this.owner.advisoryStateInternal().markSchedulingGroupTracked(bucket);
    }

    void clearSchedulingGroupTrackedLocked() {
        this.owner.advisoryStateInternal().clearSchedulingGroupTracked();
    }
}
