package io.zmux.runtime;

import io.zmux.*;

import java.io.IOException;
import java.util.Objects;

final class StreamTerminalState {
    private long terminalCode;
    private String terminalReason = "";
    private IOException localError;
    private IOException sendCloseError;
    private IOException recvCloseError;
    private ApplicationError sendStopError;
    private ApplicationError recvResetError;
    private ApplicationError recvAbortError;

    private static ApplicationError sessionCloseHalfError(ApplicationError error, ZmuxErrorDirection direction) {
        Objects.requireNonNull(error, "error");
        ZmuxErrorSource source = ZmuxErrorSource.LOCAL;
        ZmuxErrorDetails details = ZmuxErrors.details(error);
        if (details != null && details.source() != ZmuxErrorSource.UNKNOWN) {
            source = details.source();
        }
        return new ApplicationError(
                error.code(),
                error.reason(),
                ZmuxErrorScope.STREAM,
                source,
                direction,
                ZmuxTerminationKind.SESSION_TERMINATION
        );
    }

    IOException localError() {
        return localError;
    }

    IOException sendCloseError() {
        return sendCloseError;
    }

    IOException recvCloseError() {
        return recvCloseError;
    }

    ApplicationError recvResetError() {
        return recvResetError;
    }

    ApplicationError recvAbortError() {
        return recvAbortError;
    }

    boolean localAbort() {
        if (!(localError instanceof ApplicationError)) {
            return false;
        }
        ApplicationError applicationError = (ApplicationError) localError;
        return applicationError.source() == ZmuxErrorSource.LOCAL
                && applicationError.terminationKind() == ZmuxTerminationKind.ABORT;
    }

    long terminalCode() {
        return terminalCode;
    }

    String terminalReason() {
        return terminalReason;
    }

    void recordLocalWriteReset(long code) {
        sendCloseError = new ApplicationError(
                code,
                "",
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.WRITE,
                ZmuxTerminationKind.RESET
        );
        setTerminal(code, "");
    }

    void recordLocalReadStop(long code) {
        setTerminal(code, "");
    }

    void recordPeerStopSending(long code, String reason) {
        sendStopError = new ApplicationError(
                code,
                reason,
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.REMOTE,
                ZmuxErrorDirection.WRITE,
                ZmuxTerminationKind.STOPPED
        );
        setTerminal(code, reason);
    }

    void recordPeerReset(long code, String reason) {
        recvResetError = new ApplicationError(
                code,
                reason,
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.REMOTE,
                ZmuxErrorDirection.READ,
                ZmuxTerminationKind.RESET
        );
        setTerminal(code, reason);
    }

    void recordPeerAbort(long code, String reason) {
        recvAbortError = new ApplicationError(
                code,
                reason,
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.REMOTE,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.ABORT
        );
        setTerminal(code, reason);
    }

    void recordLocalAbort(long code, String reason) {
        localError = new ApplicationError(
                code,
                reason,
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.ABORT
        );
        setTerminal(code, reason);
    }

    void recordLocalFailure(IOException error) {
        localError = error == null
                ? new ZmuxException(
                ErrorCode.INTERNAL.code(),
                "stream",
                "zmux: stream failed locally",
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.ABORT
        )
                : error;
        if (localError instanceof ApplicationError) {
            ApplicationError appError = (ApplicationError) localError;
            setTerminal(appError.code(), appError.reason());
            return;
        }
        setTerminal(
                ZmuxErrors.code(localError, ErrorCode.INTERNAL.code()),
                ZmuxErrors.reason(localError)
        );
    }

    void recordSessionClose(ApplicationError error, boolean closeWriteHalf, boolean closeReadHalf) {
        if (error == null) {
            return;
        }
        if (closeWriteHalf) {
            sendCloseError = sessionCloseHalfError(error, ZmuxErrorDirection.WRITE);
        }
        if (closeReadHalf) {
            recvCloseError = sessionCloseHalfError(error, ZmuxErrorDirection.READ);
        }
        setTerminal(error.code(), error.reason());
    }

    WriteClosedException peerStopWriteClosed() {
        return new WriteClosedException(
                ZmuxErrorSource.REMOTE,
                ZmuxTerminationKind.STOPPED,
                sendStopError
        );
    }

    IOException operationError(StreamHalfState halfState) {
        if (localError != null) {
            return localError;
        }
        if (sendCloseError != null) {
            return sendCloseError;
        }
        if (recvCloseError != null) {
            return recvCloseError;
        }
        switch (halfState.terminalErrorPriority()) {
            case SEND_ABORT:
                return localErrorOrFallback();
            case RECV_ABORT:
                return recvAbortErrorOrFallback();
            case SEND_RESET:
                if (halfState.sendResetFromPeerStop() && sendStopError != null) {
                    return peerStopWriteClosed();
                }
                return localWriteResetError();
            case RECV_RESET:
                return recvResetErrorOrFallback();
            case SEND_CLOSED:
                return sendClosedError(halfState);
            case RECV_CLOSED:
                return recvClosedError(halfState);
            case NONE:
                return genericClosedError();
            default:
                throw new IllegalStateException("unexpected terminal error priority: " + halfState.terminalErrorPriority());
        }
    }

    private IOException localErrorOrFallback() {
        if (localError != null) {
            return localError;
        }
        if (recvAbortError != null) {
            return recvAbortError;
        }
        return genericClosedError();
    }

    private IOException recvAbortErrorOrFallback() {
        if (recvAbortError != null) {
            return recvAbortError;
        }
        return genericClosedError();
    }

    private IOException recvResetErrorOrFallback() {
        if (recvResetError != null) {
            return recvResetError;
        }
        return genericClosedError();
    }

    private IOException sendClosedError(StreamHalfState halfState) {
        if (halfState.sendFinQueued() || halfState.sendFin()) {
            return new WriteClosedException(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.GRACEFUL);
        }
        if (halfState.sendStopSeen()) {
            return peerStopWriteClosed();
        }
        return new WriteClosedException(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.GRACEFUL);
    }

    private IOException recvClosedError(StreamHalfState halfState) {
        switch (halfState.effectiveRecvState()) {
            case FIN:
                return new ReadClosedException(ZmuxErrorSource.REMOTE, ZmuxTerminationKind.GRACEFUL);
            case STOPPED:
                return localReadStoppedError();
            default:
                return unknownReadClosedError();
        }
    }

    private IOException unknownReadClosedError() {
        if (terminalCode != 0L || !terminalReason.isEmpty()) {
            return localReadStoppedError();
        }
        return new ReadClosedException(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.UNKNOWN);
    }

    private IOException genericClosedError() {
        if (terminalCode != 0L || !terminalReason.isEmpty()) {
            return new ApplicationError(
                    terminalCode,
                    terminalReason,
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorSource.LOCAL,
                    ZmuxErrorDirection.BOTH,
                    ZmuxTerminationKind.UNKNOWN
            );
        }
        return new ZmuxException(
                ErrorCode.INTERNAL.code(),
                "stream",
                "zmux: stream is closed",
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.UNKNOWN
        );
    }

    private ApplicationError localReadStoppedError() {
        return new ApplicationError(
                terminalCode,
                terminalReason,
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.READ,
                ZmuxTerminationKind.STOPPED
        );
    }

    private ApplicationError localWriteResetError() {
        return new ApplicationError(
                terminalCode,
                terminalReason,
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.WRITE,
                ZmuxTerminationKind.RESET
        );
    }

    private void setTerminal(long code, String reason) {
        terminalCode = code;
        terminalReason = reason == null ? "" : reason;
    }
}
