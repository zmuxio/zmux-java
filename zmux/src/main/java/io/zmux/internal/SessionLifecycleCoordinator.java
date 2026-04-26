package io.zmux.internal;

import io.zmux.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

final class SessionLifecycleCoordinator {
    private final Owner owner;

    SessionLifecycleCoordinator(Owner owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    private static IOException sessionTerminationError(IOException error) {
        if (error == null) {
            return null;
        }
        if (ZmuxErrors.terminationKind(error) != ZmuxTerminationKind.UNKNOWN) {
            return error;
        }
        return new ZmuxException(
                ZmuxErrors.code(error, ErrorCode.INTERNAL.code()),
                defaultOperation(ZmuxErrors.operation(error)),
                defaultReason(error),
                error,
                ZmuxErrorScope.SESSION,
                defaultSource(ZmuxErrors.operation(error), ZmuxErrors.source(error)),
                defaultDirection(ZmuxErrors.operation(error), ZmuxErrors.direction(error)),
                ZmuxTerminationKind.SESSION_TERMINATION
        );
    }

    static IOException sessionOperationError(String operation, IOException error) {
        if (error == null) {
            return null;
        }
        String op = operation == null ? "" : operation;
        if (op.isEmpty()) {
            return error;
        }
        IOException terminalError = sessionTerminationError(error);
        IOException sourceError = terminalError != null ? terminalError : error;
        ZmuxErrorSource source = defaultSource(ZmuxErrors.operation(sourceError), ZmuxErrors.source(sourceError));
        if (source == ZmuxErrorSource.UNKNOWN) {
            source = ZmuxErrors.source(error);
        }
        if (sourceError instanceof SessionClosedException) {
            return new SessionClosedException(source, sourceError, op);
        }
        if (sourceError instanceof ApplicationError) {
            ApplicationError applicationError = (ApplicationError) sourceError;
            return new ApplicationError(
                    applicationError.code(),
                    applicationError.reason(),
                    ZmuxErrorScope.SESSION,
                    source,
                    ZmuxErrorDirection.BOTH,
                    ZmuxTerminationKind.SESSION_TERMINATION,
                    op
            );
        }
        return new ZmuxException(
                ZmuxErrors.code(sourceError, ErrorCode.INTERNAL.code()),
                op,
                defaultReason(sourceError),
                sourceError,
                ZmuxErrorScope.SESSION,
                source,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        );
    }

    private static String defaultOperation(String operation) {
        return operation == null || operation.isEmpty() ? "session" : operation;
    }

    private static String defaultReason(IOException error) {
        String reason = ZmuxErrors.reason(error);
        if (reason != null && !reason.isEmpty()) {
            return reason;
        }
        String message = error.getMessage();
        return message == null || message.isEmpty() ? error.getClass().getSimpleName() : message;
    }

    private static ZmuxErrorSource defaultSource(String operation, ZmuxErrorSource source) {
        if (source != null && source != ZmuxErrorSource.UNKNOWN) {
            return source;
        }
        return operation != null && operation.startsWith("validate ")
                ? ZmuxErrorSource.REMOTE
                : ZmuxErrorSource.UNKNOWN;
    }

    private static ZmuxErrorDirection defaultDirection(String operation, ZmuxErrorDirection direction) {
        if (operation != null
                && operation.startsWith("validate ")
                && (direction == null
                || direction == ZmuxErrorDirection.UNKNOWN
                || direction == ZmuxErrorDirection.BOTH)) {
            return ZmuxErrorDirection.READ;
        }
        if (direction != null && direction != ZmuxErrorDirection.UNKNOWN) {
            return direction;
        }
        return ZmuxErrorDirection.BOTH;
    }

    IOException sessionErrorLocked() {
        IOException error = this.owner.terminalError() != null ? this.owner.terminalError() : this.owner.peerCloseError();
        if (error instanceof ApplicationError && ((ApplicationError) error).code() == ErrorCode.NO_ERROR.code()) {
            ApplicationError applicationError = (ApplicationError) error;
            ZmuxErrorSource source = applicationError.source() == ZmuxErrorSource.UNKNOWN
                    ? (this.owner.peerCloseError() != null ? ZmuxErrorSource.REMOTE : ZmuxErrorSource.LOCAL)
                    : applicationError.source();
            return new SessionClosedException(source);
        }
        if (error != null) {
            return error;
        }
        if (this.owner.closeFrameQueued() || this.owner.state() == SessionState.CLOSING) {
            return this.localClosingErrorLocked();
        }
        return new SessionClosedException(this.owner.peerCloseError() != null ? ZmuxErrorSource.REMOTE : ZmuxErrorSource.LOCAL);
    }

    IOException localClosingErrorLocked() {
        return new SessionClosedException(ZmuxErrorSource.LOCAL);
    }

    IOException currentErrorLocked() {
        return this.sessionErrorLocked();
    }

    void failSession(IOException error) {
        IOException terminalError = SessionLifecycleCoordinator.sessionTerminationError(error);
        synchronized (this.owner.lock()) {
            if (this.owner.state().terminal()) {
                this.finishSessionLocked(terminalError, this.owner.state());
                return;
            }

            this.owner.setTerminalError(terminalError);
            if (!this.owner.closeFrameQueued()) {
                String reason = ZmuxErrors.reason(terminalError);
                long code = ZmuxErrors.code(terminalError, ErrorCode.INTERNAL.code());

                this.beginSessionTerminationLocked(terminalError, SessionState.FAILED);
                this.owner.setCloseFrameQueued(true);
                try {
                    this.owner.enqueueCloseFrameLocked(this.owner.buildControlErrorPayloadLocked(code, reason));
                } catch (IOException ignored) {
                    this.finishSessionLocked(terminalError, SessionState.FAILED);
                }
                this.owner.notifyLockWaiters();
                return;
            }

            this.finishSessionLocked(terminalError, SessionState.FAILED);
        }
    }

    void finishSessionLocked(IOException error, SessionState sessionState) {
        boolean terminalAlreadyFinalized = this.owner.state().terminal() && this.owner.terminalCleanupApplied();
        IOException terminalError = terminalAlreadyFinalized
                ? null
                : SessionLifecycleCoordinator.sessionTerminationError(error);
        SessionState targetState = terminalAlreadyFinalized ? this.owner.state() : sessionState;
        this.beginSessionTerminationLocked(terminalError, targetState);
        IOException eventError = terminalError != null
                ? terminalError
                : this.owner.terminalError() != null ? this.owner.terminalError() : this.owner.peerCloseError();
        this.owner.enqueueSessionClosedEventLocked(eventError);
        this.closeTransport();
        this.owner.notifyLockWaiters();
        this.owner.terminatedCountDown();
    }

    void beginSessionTerminationLocked(IOException error, SessionState sessionState) {
        IOException terminalError = SessionLifecycleCoordinator.sessionTerminationError(error);
        if (terminalError != null && this.owner.terminalError() == null) {
            this.owner.setTerminalError(terminalError);
        }
        this.owner.setGracefulCloseActive(false);
        this.owner.setState(sessionState);
        this.owner.clearKeepaliveSchedulesLocked();
        this.owner.setInflightBatch(Collections.emptyList());
        if (!this.owner.terminalCleanupApplied()) {
            List<SessionRuntime.OutboundFrame> retainedQueuedGoAwayFrames =
                    sessionState == SessionState.CLOSING
                            ? this.owner.takeQueuedGoAwayFramesLocked()
                            : Collections.emptyList();
            this.owner.discardPendingOutboundLocked();
            this.owner.failActivePingLocked(this.sessionErrorLocked());
            this.owner.releaseAllStreamsForSessionCloseLocked(this.owner.sessionCloseStreamErrorLocked(sessionState));
            this.owner.clearSessionCloseStateLocked();
            if (!retainedQueuedGoAwayFrames.isEmpty()) {
                this.owner.restoreQueuedGoAwayFramesLocked(retainedQueuedGoAwayFrames);
            }
            this.owner.setTerminalCleanupApplied(true);
        } else if (this.owner.hasActivePingLocked()) {
            this.owner.failActivePingLocked(this.sessionErrorLocked());
        }
        this.owner.notifyLockWaiters();
    }

    void closeTransport() {
        if (this.owner.closedTransport()) {
            return;
        }
        this.owner.setClosedTransport(true);
        try {
            this.owner.closeConnection();
        } catch (IOException ignored) {
            // Transport is already terminating.
        }
    }

    SessionState terminalStateForSessionError(IOException error) {
        if (error instanceof ApplicationError && ((ApplicationError) error).code() == ErrorCode.NO_ERROR.code()) {
            ApplicationError applicationError = (ApplicationError) error;
            return applicationError.source() == ZmuxErrorSource.REMOTE
                    ? SessionState.CLOSED
                    : SessionState.CLOSING;
        }
        return SessionState.FAILED;
    }

    boolean waitForGracefulCloseDrain(Duration duration) throws IOException {
        TimeoutBudget budget = TimeoutBudget.fromTimeout(duration);
        synchronized (this.owner.lock()) {
            while (this.owner.hasGracefulClosePendingWorkLocked()) {
                if (this.owner.state().terminal()) {
                    return true;
                }
                long remainingNanos = budget.remainingNanos();
                if (remainingNanos <= 0L) {
                    return false;
                }
                try {
                    this.owner.waitOnLockNanos(remainingNanos);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw SessionRuntime.interruptedIo(
                            "zmux: interrupted while waiting for graceful close drain",
                            "close",
                            io.zmux.ZmuxErrorScope.SESSION,
                            io.zmux.ZmuxErrorDirection.BOTH,
                            interruptedException
                    );
                }
            }
        }
        return true;
    }

    void awaitGoAwayDrainInterval() throws IOException {
        synchronized (this.owner.lock()) {
            if (this.owner.state().terminal()) {
                return;
            }
            try {
                this.owner.waitOnLockNanos(this.owner.goAwayDrainIntervalNanosLocked());
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw SessionRuntime.interruptedIo(
                        "zmux: interrupted while waiting for GOAWAY drain interval",
                        "close",
                        io.zmux.ZmuxErrorScope.SESSION,
                        io.zmux.ZmuxErrorDirection.BOTH,
                        interruptedException
                );
            }
        }
    }

    void awaitCloseCompletion(Duration duration) throws IOException {
        boolean terminated;
        try {
            terminated = this.owner.awaitTermination(duration);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw SessionRuntime.interruptedIo(
                    "zmux: interrupted while waiting for close",
                    "close",
                    io.zmux.ZmuxErrorScope.SESSION,
                    io.zmux.ZmuxErrorDirection.BOTH,
                    interruptedException
            );
        }
        if (!terminated) {
            synchronized (this.owner.lock()) {
                this.owner.recordCloseCompletionTimeoutLocked();
            }
            throw new GracefulCloseTimeoutException();
        }
    }

    interface Owner {
        Object lock();

        IOException terminalError();

        void setTerminalError(IOException error);

        ApplicationError peerCloseError();

        boolean closeFrameQueued();

        void setCloseFrameQueued(boolean value);

        SessionState state();

        void setState(SessionState state);

        void setGracefulCloseActive(boolean value);

        void clearKeepaliveSchedulesLocked();

        void setInflightBatch(List<SessionRuntime.OutboundFrame> batch);

        boolean terminalCleanupApplied();

        void setTerminalCleanupApplied(boolean value);

        void discardPendingOutboundLocked();

        List<SessionRuntime.OutboundFrame> takeQueuedGoAwayFramesLocked();

        void failActivePingLocked(IOException error);

        ApplicationError sessionCloseStreamErrorLocked(SessionState sessionState);

        void releaseAllStreamsForSessionCloseLocked(ApplicationError error);

        void clearSessionCloseStateLocked();

        void restoreQueuedGoAwayFramesLocked(List<SessionRuntime.OutboundFrame> frames);

        boolean hasActivePingLocked();

        byte[] buildControlErrorPayloadLocked(long code, String reason) throws IOException;

        void enqueueCloseFrameLocked(byte[] payload) throws IOException;

        void enqueueSessionClosedEventLocked(IOException error);

        void notifyLockWaiters();

        void terminatedCountDown();

        boolean closedTransport();

        void setClosedTransport(boolean value);

        void closeConnection() throws IOException;

        boolean hasGracefulClosePendingWorkLocked();

        long goAwayDrainIntervalNanosLocked();

        boolean awaitTermination(Duration duration) throws InterruptedException;

        void waitOnLockNanos(long waitNanos) throws InterruptedException;

        void recordCloseCompletionTimeoutLocked();
    }
}
