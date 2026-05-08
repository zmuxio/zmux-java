package io.zmux;

final class StreamHalfState {
    private SendState sendState;
    private RecvState recvState;
    private boolean localReadStop;
    private boolean localReadSignalPending;
    private boolean remoteWriteStop;
    private boolean sendResetFromPeerStop;

    StreamHalfState(boolean localSend, boolean localReceive) {
        initialize(localSend, localReceive);
    }

    static boolean isSendTerminal(SendState state) {
        return state == SendState.ABSENT
                || state == SendState.FIN
                || state == SendState.RESET
                || state == SendState.ABORTED;
    }

    private static boolean effectivelySendTerminal(EffectiveSendState state) {
        return state == EffectiveSendState.ABSENT
                || state == EffectiveSendState.FIN
                || state == EffectiveSendState.RESET
                || state == EffectiveSendState.ABORTED;
    }

    private static boolean effectivelyRecvTerminal(EffectiveRecvState state) {
        return state == EffectiveRecvState.ABSENT
                || state == EffectiveRecvState.FIN
                || state == EffectiveRecvState.RESET
                || state == EffectiveRecvState.ABORTED;
    }

    void initialize(boolean localSend, boolean localReceive) {
        sendState = localSend ? SendState.OPEN : SendState.ABSENT;
        recvState = localReceive ? RecvState.OPEN : RecvState.ABSENT;
        localReadStop = false;
        localReadSignalPending = false;
        remoteWriteStop = false;
        sendResetFromPeerStop = false;
    }

    SendState sendState() {
        return sendState;
    }

    boolean sendOpen() {
        return sendState == SendState.OPEN;
    }

    boolean sendAbsent() {
        return sendState == SendState.ABSENT;
    }

    boolean sendFinQueued() {
        return sendState == SendState.FIN_QUEUED;
    }

    boolean sendFin() {
        return sendState == SendState.FIN;
    }

    boolean sendReset() {
        return sendState == SendState.RESET;
    }

    boolean sendResetOrAborted() {
        return sendState == SendState.RESET || sendState == SendState.ABORTED;
    }

    boolean sendResetFromPeerStop() {
        return sendState == SendState.RESET && sendResetFromPeerStop;
    }

    boolean sendAborted() {
        return sendState == SendState.ABORTED;
    }

    boolean localAbortNoOp() {
        return sendState == SendState.ABORTED || recvState == RecvState.ABORTED;
    }

    EffectiveSendState effectiveSendState() {
        switch (sendState) {
            case ABSENT:
                return EffectiveSendState.ABSENT;
            case OPEN:
                return remoteWriteStop ? EffectiveSendState.STOP_SEEN : EffectiveSendState.OPEN;
            case FIN_QUEUED:
            case FIN:
                return EffectiveSendState.FIN;
            case RESET:
                return EffectiveSendState.RESET;
            case ABORTED:
                return EffectiveSendState.ABORTED;
            default:
                throw new IllegalStateException("unexpected send state: " + sendState);
        }
    }

    boolean effectiveSendOpen() {
        return effectiveSendState() == EffectiveSendState.OPEN;
    }

    boolean sendStopSeen() {
        return effectiveSendState() == EffectiveSendState.STOP_SEEN;
    }

    EffectiveRecvState effectiveRecvState() {
        if (localReadStop) {
            return recvState == RecvState.ABSENT
                    ? EffectiveRecvState.ABSENT
                    : EffectiveRecvState.STOPPED;
        }
        switch (recvState) {
            case ABSENT:
                return EffectiveRecvState.ABSENT;
            case OPEN:
                return EffectiveRecvState.OPEN;
            case FIN:
                return EffectiveRecvState.FIN;
            case STOP_SENT:
                return EffectiveRecvState.STOPPED;
            case RESET:
                return EffectiveRecvState.RESET;
            case ABORTED:
                return EffectiveRecvState.ABORTED;
            default:
                throw new IllegalStateException("unexpected recv state: " + recvState);
        }
    }

    SendState markFinQueuedIfOpen() {
        SendState previous = sendState;
        if (sendState == SendState.OPEN) {
            sendState = SendState.FIN_QUEUED;
        }
        return previous;
    }

    SendState markSendReset() {
        SendState previous = sendState;
        sendState = SendState.RESET;
        sendResetFromPeerStop = false;
        return previous;
    }

    SendState markSendFinFromQueued() {
        SendState previous = sendState;
        if (sendState == SendState.FIN_QUEUED) {
            sendState = SendState.FIN;
        }
        return previous;
    }

    SendState clearFinQueuedIfQueued() {
        SendState previous = sendState;
        if (sendState == SendState.FIN_QUEUED) {
            sendState = SendState.OPEN;
        }
        return previous;
    }

    SendState concludeStopSendingWithReset() {
        SendState previous = sendState;
        if (sendState == SendState.OPEN || sendState == SendState.FIN_QUEUED) {
            sendState = SendState.RESET;
            sendResetFromPeerStop = remoteWriteStop;
        }
        return previous;
    }

    void markSendStopSeen() {
        remoteWriteStop = true;
    }

    SendState abortBoth() {
        SendState previous = sendState;
        sendState = sendState == SendState.ABSENT ? SendState.ABSENT : SendState.ABORTED;
        recvState = recvState == RecvState.ABSENT ? RecvState.ABSENT : RecvState.ABORTED;
        sendResetFromPeerStop = false;
        return previous;
    }

    boolean shouldEmitQueuedData(boolean preserveAfterSendClose) {
        if (sendState == SendState.OPEN || sendState == SendState.FIN_QUEUED) {
            return true;
        }
        return preserveAfterSendClose && sendState == SendState.RESET;
    }

    boolean sendTerminal() {
        return isSendTerminal(sendState);
    }

    RecvState recvState() {
        return recvState;
    }

    boolean recvOpen() {
        return recvState == RecvState.OPEN;
    }

    boolean recvAbsent() {
        return recvState == RecvState.ABSENT;
    }

    boolean recvFin() {
        return recvState == RecvState.FIN;
    }

    boolean recvStopSent() {
        return recvState == RecvState.STOP_SENT;
    }

    boolean readStopSent() {
        return effectiveRecvState() == EffectiveRecvState.STOPPED;
    }

    boolean localReadSignalPending() {
        return localReadSignalPending;
    }

    boolean recvReset() {
        return recvState == RecvState.RESET;
    }

    boolean recvAbortive() {
        return recvState == RecvState.RESET || recvState == RecvState.ABORTED;
    }

    boolean recvAborted() {
        return recvState == RecvState.ABORTED;
    }

    boolean recvStoppedOrTerminal() {
        return effectiveRecvState() != EffectiveRecvState.OPEN;
    }

    boolean recvTerminal() {
        return recvState == RecvState.ABSENT
                || recvState == RecvState.FIN
                || recvState == RecvState.RESET
                || recvState == RecvState.ABORTED;
    }

    boolean fullyTerminal() {
        return sendTerminal() && recvTerminal();
    }

    boolean effectivelyFullyTerminal() {
        EffectiveSendState send = effectiveSendState();
        EffectiveRecvState recv = effectiveRecvState();
        if (send == EffectiveSendState.ABORTED || recv == EffectiveRecvState.ABORTED) {
            return true;
        }
        return effectivelySendTerminal(send) && effectivelyRecvTerminal(recv);
    }

    boolean receiveGraceful() {
        return effectiveRecvState() == EffectiveRecvState.FIN;
    }

    void finishReceiveIfActive() {
        if (recvState == RecvState.OPEN || recvState == RecvState.STOP_SENT) {
            recvState = RecvState.FIN;
        }
    }

    void markRecvReset() {
        recvState = RecvState.RESET;
    }

    void markLocalReadStop() {
        localReadStop = true;
        localReadSignalPending = true;
        recvState = RecvState.STOP_SENT;
    }

    void clearLocalReadSignalPending() {
        localReadSignalPending = false;
    }

    void closeForSession(boolean localSend, boolean localReceive, boolean graceful) {
        if (graceful) {
            if (localSend && !sendTerminal()) {
                sendState = SendState.FIN;
                sendResetFromPeerStop = false;
            }
            if (localReceive && !recvTerminal()) {
                recvState = RecvState.FIN;
            }
            return;
        }
        if (localSend && !sendTerminal()) {
            sendState = SendState.ABORTED;
            sendResetFromPeerStop = false;
        }
        if (localReceive && !recvTerminal()) {
            recvState = RecvState.ABORTED;
        }
    }

    StreamRuntime.PeerDataAction peerDataAction(boolean localReceive, boolean fullyTerminal, boolean fin) {
        if (!localReceive) {
            return fullyTerminal ? StreamRuntime.PeerDataAction.IGNORE : StreamRuntime.PeerDataAction.ABORT_STREAM_STATE;
        }
        switch (effectiveRecvState()) {
            case RESET:
            case ABORTED:
                return StreamRuntime.PeerDataAction.IGNORE;
            case STOPPED:
                return fin ? StreamRuntime.PeerDataAction.IGNORE_AND_FIN : StreamRuntime.PeerDataAction.IGNORE;
            case FIN:
                return fullyTerminal ? StreamRuntime.PeerDataAction.IGNORE : StreamRuntime.PeerDataAction.ABORT_STREAM_CLOSED;
            case OPEN:
                return fullyTerminal ? StreamRuntime.PeerDataAction.IGNORE : StreamRuntime.PeerDataAction.ACCEPT;
            case ABSENT:
                return fullyTerminal ? StreamRuntime.PeerDataAction.IGNORE : StreamRuntime.PeerDataAction.ABORT_STREAM_STATE;
            default:
                throw new IllegalStateException("unexpected recv state: " + effectiveRecvState());
        }
    }

    boolean readClosed(boolean localReceive) {
        return !localReceive || effectiveRecvState() != EffectiveRecvState.OPEN;
    }

    boolean tracksLatePeerData() {
        switch (effectiveRecvState()) {
            case STOPPED:
            case RESET:
            case ABORTED:
                return true;
            default:
                return false;
        }
    }

    boolean writeClosed() {
        return effectiveSendState() != EffectiveSendState.OPEN;
    }

    boolean closeWriteNoOpAfterPeerStop() {
        if (!remoteWriteStop) {
            return false;
        }
        return sendState == SendState.FIN_QUEUED
                || sendState == SendState.FIN
                || sendState == SendState.RESET;
    }

    boolean shouldIgnorePeerStopSending(boolean fullyTerminal) {
        if (fullyTerminal) {
            return true;
        }
        EffectiveSendState state = effectiveSendState();
        return state == EffectiveSendState.STOP_SEEN
                || state == EffectiveSendState.FIN
                || state == EffectiveSendState.RESET
                || state == EffectiveSendState.ABORTED;
    }

    TerminalErrorChoice terminalErrorPriority() {
        EffectiveSendState effectiveSendState = effectiveSendState();
        EffectiveRecvState effectiveRecvState = effectiveRecvState();
        if (effectiveSendState == EffectiveSendState.ABORTED) {
            return TerminalErrorChoice.SEND_ABORT;
        }
        if (effectiveRecvState == EffectiveRecvState.ABORTED) {
            return TerminalErrorChoice.RECV_ABORT;
        }
        if (effectiveSendState == EffectiveSendState.RESET) {
            return TerminalErrorChoice.SEND_RESET;
        }
        if (effectiveRecvState == EffectiveRecvState.RESET) {
            return TerminalErrorChoice.RECV_RESET;
        }
        if (effectiveSendState == EffectiveSendState.FIN || effectiveSendState == EffectiveSendState.STOP_SEEN) {
            return TerminalErrorChoice.SEND_CLOSED;
        }
        return effectiveRecvState == EffectiveRecvState.STOPPED || effectiveRecvState == EffectiveRecvState.FIN
                ? TerminalErrorChoice.RECV_CLOSED
                : TerminalErrorChoice.NONE;
    }

    enum EffectiveSendState {
        ABSENT,
        OPEN,
        STOP_SEEN,
        FIN,
        RESET,
        ABORTED
    }

    enum EffectiveRecvState {
        ABSENT,
        OPEN,
        STOPPED,
        FIN,
        RESET,
        ABORTED
    }

    enum TerminalErrorChoice {
        NONE,
        SEND_ABORT,
        RECV_ABORT,
        SEND_RESET,
        RECV_RESET,
        SEND_CLOSED,
        RECV_CLOSED
    }

    enum SendState {
        ABSENT,
        OPEN,
        FIN_QUEUED,
        FIN,
        RESET,
        ABORTED
    }

    enum RecvState {
        ABSENT,
        OPEN,
        FIN,
        STOP_SENT,
        RESET,
        ABORTED
    }
}
