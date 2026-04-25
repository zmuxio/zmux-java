package io.zmux.internal;

import io.zmux.ApplicationError;

import java.io.IOException;
import java.util.Objects;

final class StreamTerminalCoordinator {
    private final StreamRuntime owner;

    StreamTerminalCoordinator(StreamRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    boolean stopSendingFromPeerLocked(long code, String reason, long reasonBytes) {
        this.owner.terminalStateInternal().recordPeerStopSending(code, reason);
        this.owner.advisoryStateInternal().recordSendStopReasonBytes(reasonBytes);
        this.owner.clearWriteAdvisoryLocked();
        boolean sendOpen = this.owner.halfStateInternal().sendOpen();
        this.owner.halfStateInternal().markSendStopSeen();
        this.owner.sessionInternal().clearBlockedFrameLocked(this.owner.streamIdInternal());
        this.owner.refreshGracefulCloseBlockingLocked();
        return sendOpen;
    }

    void concludeStopSendingWithResetLocked() {
        this.owner.clearWriteAdvisoryLocked();
        StreamHalfState.SendState previousSendState = this.owner.halfStateInternal().concludeStopSendingWithReset();
        this.owner.sessionInternal().discardQueuedStreamDataLocked(this.owner, false);
        this.owner.notifySendTerminalTransitionLocked(previousSendState);
        this.owner.refreshGracefulCloseBlockingLocked();
    }

    void resetFromPeerLocked(long code, String reason, long reasonBytes) {
        this.owner.terminalStateInternal().recordPeerReset(code, reason);
        this.owner.advisoryStateInternal().recordRecvResetReasonBytes(reasonBytes);
        this.owner.halfStateInternal().markRecvReset();
        this.owner.clearStopSendingGracefulDrainLocked();
        this.owner.discardReadBufferLocked();
        this.owner.refreshGracefulCloseBlockingLocked();
    }

    void abortFromPeerLocked(long code, String reason, long reasonBytes) {
        this.owner.terminalStateInternal().recordPeerAbort(code, reason);
        this.owner.advisoryStateInternal().recordRecvAbortReasonBytes(reasonBytes);
        this.abortBothAndDiscardLocked();
    }

    void abortFromLocalLocked(long code, String reason) {
        this.owner.terminalStateInternal().recordLocalAbort(code, reason);
        this.abortBothAndDiscardLocked();
    }

    void failLocallyLocked(IOException error) {
        this.owner.terminalStateInternal().recordLocalFailure(error);
        this.abortBothAndDiscardLocked();
    }

    private void abortBothAndDiscardLocked() {
        this.owner.clearWriteAdvisoryLocked();
        StreamHalfState.SendState previousSendState = this.owner.halfStateInternal().abortBoth();
        this.owner.sessionInternal().discardQueuedStreamDataLocked(this.owner, false);
        this.owner.notifySendTerminalTransitionLocked(previousSendState);
        this.owner.discardReadBufferLocked();
        this.owner.refreshGracefulCloseBlockingLocked();
    }

    void closeForSessionLocked(ApplicationError error) {
        boolean sendTerminal = this.owner.sendTerminalLocked();
        boolean recvTerminal = this.owner.halfStateInternal().recvTerminal();
        StreamHalfState.SendState previousSendState = this.owner.halfStateInternal().sendState();
        boolean closeWriteHalf = this.owner.localSend() && !sendTerminal && !this.owner.halfStateInternal().sendAbsent();
        boolean closeReadHalf = this.owner.localReceive() && !recvTerminal && !this.owner.halfStateInternal().recvAbsent();
        this.owner.clearWriteAdvisoryLocked();
        if (error == null) {
            this.owner.halfStateInternal().closeForSession(this.owner.localSend(), this.owner.localReceive(), true);
        } else {
            this.owner.halfStateInternal().closeForSession(this.owner.localSend(), this.owner.localReceive(), false);
            this.owner.terminalStateInternal().recordSessionClose(error, closeWriteHalf, closeReadHalf);
        }
        this.owner.notifySendTerminalTransitionLocked(previousSendState);
        this.clearSessionPendingStateLocked();
    }

    void clearSessionPendingStateLocked() {
        this.owner.discardReadBufferLocked();
        this.owner.halfStateInternal().clearLocalReadSignalPending();
        this.owner.metadataStateInternal().clearOpeningFramePending();
        this.owner.sessionInternal().discardPendingPriorityUpdateLocked(this.owner);
        this.owner.clearStopSendingGracefulDrainLocked();
        this.owner.receiveAccountingStateInternal().clearRecvPending();
        this.owner.lifecycleStateInternal().clearPendingTracking();
        this.owner.sendAccountingStateInternal().clearPendingBufferedState();
        this.owner.refreshGracefulCloseBlockingLocked();
    }
}
