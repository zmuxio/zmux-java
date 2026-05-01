package io.zmux.internal;

import io.zmux.*;

import java.io.IOException;
import java.util.Objects;

final class StreamReadCoordinator {
    private final StreamRuntime owner;

    StreamReadCoordinator(StreamRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    int read(byte[] dst, int offset, int length) throws IOException {
        Objects.requireNonNull(dst, "dst");
        RangeChecks.checkFromIndexSize(offset, length, dst.length);
        if (length == 0) {
            return 0;
        }
        synchronized (this.owner.lockInternal()) {
            if (!this.owner.localReceive()) {
                throw new StreamNotReadableException();
            }
            while (true) {
                if (!this.owner.readBufferInternal().isEmpty()) {
                    ByteArrayQueue readBuffer = this.owner.readBufferInternal();
                    int readBytes = readBuffer.readAndTrackReleasedStorage(dst, offset, length);
                    this.owner.onReadBufferReleasedLocked(readBytes, readBuffer.lastReadReleasedStorageBytes());
                    this.owner.onReadDiscardLocked(this.owner.halfStateInternal().recvOpen());
                    this.owner.noteReadPayloadProgressLocked(readBytes);
                    this.owner.sessionInternal().maybeCompactStreamLocked(this.owner);
                    return readBytes;
                }
                this.owner.sessionInternal().maybeCompactStreamLocked(this.owner);
                if (this.owner.halfStateInternal().readStopSent()) {
                    throw new ReadClosedException(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.STOPPED);
                }
                if (this.owner.terminalStateInternal().localError() != null) {
                    throw this.owner.terminalStateInternal().localError();
                }
                if (this.owner.terminalStateInternal().recvCloseError() != null) {
                    throw this.owner.terminalStateInternal().recvCloseError();
                }
                if (this.owner.terminalStateInternal().recvAbortError() != null) {
                    throw this.owner.terminalStateInternal().recvAbortError();
                }
                if (this.owner.terminalStateInternal().recvResetError() != null) {
                    throw this.owner.terminalStateInternal().recvResetError();
                }
                if (this.owner.halfStateInternal().recvFin()) {
                    return -1;
                }
                if (this.owner.sessionInternal().shouldFailSessionOperationsLocked()) {
                    throw this.owner.sessionInternal().sessionOperationErrorLocked(
                            "read",
                            this.owner.sessionInternal().currentErrorLocked()
                    );
                }
                try {
                    long remainingNanos = StreamRuntime.remainingDeadlineNanosLocked(this.owner.readDeadlineNanosInternal());
                    if (remainingNanos < 0L) {
                        throw new ReadTimeoutException();
                    }
                    if (remainingNanos == 0L) {
                        this.owner.waitOnLock();
                    } else {
                        this.owner.waitOnLockNanos(remainingNanos);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw SessionRuntime.interruptedIo(
                            "zmux: interrupted while reading stream",
                            "read",
                            ZmuxErrorScope.STREAM,
                            ZmuxErrorDirection.READ,
                            e
                    );
                }
            }
        }
    }

    void cancelRead(long code) throws IOException {
        try {
            synchronized (this.owner.lockInternal()) {
                if (this.owner.sessionInternal().shouldFailSessionOperationsLocked()) {
                    throw this.owner.sessionInternal().sessionOperationErrorLocked(
                            "close",
                            this.owner.sessionInternal().currentErrorLocked()
                    );
                }
                if (!this.owner.localReceive()) {
                    throw new StreamNotReadableException();
                }
                boolean signalPending = this.owner.halfStateInternal().localReadSignalPending();
                if (!this.owner.halfStateInternal().recvOpen() && !signalPending) {
                    throw this.closeReadErrorLocked();
                }
                byte[] stopPayload = this.owner.sessionInternal().buildControlErrorPayloadLocked(code, "");
                if (!signalPending) {
                    this.owner.commitLocalReadStopLocked(code);
                }
                boolean notifyAfterAttempt = true;
                try {
                    this.owner.prepareLocalControlOpenerLocked(false, false);
                    this.owner.sessionInternal().enqueueStopSendingLocked(this.owner, code, stopPayload);
                    this.owner.halfStateInternal().clearLocalReadSignalPending();
                    notifyAfterAttempt = false;
                } finally {
                    if (notifyAfterAttempt) {
                        this.owner.notifyLockWaitersLocked();
                    }
                }
            }
        } finally {
            this.owner.emitPendingEvents();
        }
    }

    IOException closeReadErrorLocked() {
        if (this.owner.terminalStateInternal().localError() != null) {
            return this.owner.terminalStateInternal().localError();
        }
        if (this.owner.terminalStateInternal().recvCloseError() != null) {
            return this.owner.terminalStateInternal().recvCloseError();
        }
        if (this.owner.halfStateInternal().readStopSent()) {
            return new ReadClosedException(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.STOPPED);
        }
        if (this.owner.terminalStateInternal().recvAbortError() != null) {
            return this.owner.terminalStateInternal().recvAbortError();
        }
        if (this.owner.terminalStateInternal().recvResetError() != null) {
            return this.owner.terminalStateInternal().recvResetError();
        }
        if (this.owner.halfStateInternal().recvFin()) {
            return new ReadClosedException(ZmuxErrorSource.REMOTE, ZmuxTerminationKind.GRACEFUL);
        }
        if (this.owner.sessionInternal().shouldFailSessionOperationsLocked()) {
            return this.owner.sessionInternal().sessionOperationErrorLocked(
                    "close",
                    this.owner.sessionInternal().currentErrorLocked()
            );
        }
        return new ReadClosedException(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.UNKNOWN);
    }
}
