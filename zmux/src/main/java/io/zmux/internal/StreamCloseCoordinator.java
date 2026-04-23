package io.zmux.internal;

import io.zmux.*;

import java.io.IOException;
import java.util.Objects;

final class StreamCloseCoordinator {
    private final StreamRuntime owner;

    StreamCloseCoordinator(StreamRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    private static boolean isBenignCloseError(IOException error) {
        return error instanceof StreamNotWritableException
                || error instanceof StreamNotReadableException
                || error instanceof WriteClosedException
                || error instanceof ReadClosedException;
    }

    private static boolean isBenignCancelAfterTimeoutError(IOException error) {
        return isBenignCloseError(error) || error instanceof SessionClosedException;
    }

    void close() throws IOException {
        boolean closeWriteNeeded;
        boolean closeReadNeeded;
        synchronized (this.owner.lockInternal()) {
            closeWriteNeeded = this.owner.localSend()
                    && this.owner.terminalStateInternal().sendCloseError() == null
                    && this.owner.terminalStateInternal().localError() == null
                    && !this.owner.halfStateInternal().sendFinQueued()
                    && !this.owner.halfStateInternal().sendFin()
                    && !this.owner.halfStateInternal().sendResetOrAborted();
            closeReadNeeded = this.owner.localReceive()
                    && this.owner.halfStateInternal().recvOpen()
                    && !this.owner.halfStateInternal().readStopSent();
            if (this.owner.sessionInternal().shouldFailSessionOperationsLocked()) {
                throw this.owner.sessionInternal().sessionOperationErrorLocked(
                        "close",
                        this.owner.sessionInternal().currentErrorLocked()
                );
            }
        }
        IOException error = null;
        try {
            if (closeWriteNeeded) {
                this.owner.closeWrite();
            }
        } catch (IOException e) {
            if (!isBenignCloseError(e)) {
                error = e;
                if (e instanceof WriteTimeoutException) {
                    try {
                        this.owner.cancelWrite(ErrorCode.CANCELLED.code());
                    } catch (IOException cancelError) {
                        if (!isBenignCancelAfterTimeoutError(cancelError)) {
                            error.addSuppressed(cancelError);
                        }
                    }
                }
            }
        }
        try {
            if (closeReadNeeded) {
                this.owner.closeRead();
            }
        } catch (IOException e) {
            if (!isBenignCloseError(e)) {
                if (error == null) {
                    error = e;
                } else {
                    error.addSuppressed(e);
                }
            }
        }
        if (error != null) {
            throw error;
        }
    }
}
