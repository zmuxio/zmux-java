package io.zmux.internal;

import io.zmux.*;

import java.io.IOException;
import java.util.Objects;

final class StreamWriteCoordinator {
    private final StreamRuntime owner;

    StreamWriteCoordinator(StreamRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    void closeWrite() throws IOException {
        try {
            synchronized (this.owner.lockInternal()) {
                this.throwIfCloseWriteSessionTerminalLocked();
                if (this.owner.halfStateInternal().closeWriteNoOpAfterPeerStop()) {
                    return;
                }
                this.ensureCloseWritableLocked();
                this.owner.sessionInternal().flushPendingPriorityUpdateLocked(this.owner);
                if (this.owner.shouldEmitOpenerFrameLocked()) {
                    byte[] prefix = this.buildOpeningPrefixLocked();
                    this.owner.sessionInternal().queueOpeningFinLocked(this.owner, prefix);
                } else {
                    this.owner.sessionInternal().queueDataLocked(this.owner, StreamRuntime.EMPTY_BYTES, true);
                }
                this.owner.notifyLockWaitersLocked();
            }
        } finally {
            this.owner.emitPendingEvents();
        }
    }

    void cancelWrite(long code) throws IOException {
        try {
            synchronized (this.owner.lockInternal()) {
                this.throwIfSessionTerminalLocked("close");
                this.ensureResettableLocked();
                if (this.owner.needsLocalOpenerLocked()) {
                    if (!this.owner.lifecycleStateInternal().idAssigned()) {
                        this.owner.sessionInternal().buildControlErrorPayloadLocked(code, "");
                        this.owner.sessionInternal().failProvisionalLocalAbortLocked(this.owner, code, "");
                    } else {
                        byte[] abortPayload = this.owner.sessionInternal().buildControlErrorPayloadLocked(code, "");
                        this.owner.sessionInternal().markLocalStreamOpeningCommittedLocked(this.owner);
                        this.owner.abortFromLocalLocked(code, "");
                        this.owner.sessionInternal().enqueueAbortLocked(this.owner, code, abortPayload, false);
                        this.owner.sessionInternal().maybeCompactStreamLocked(this.owner);
                    }
                    this.owner.notifyLockWaitersLocked();
                    return;
                }
                byte[] resetPayload = this.owner.sessionInternal().buildControlErrorPayloadLocked(code, "");
                this.owner.prepareLocalControlOpenerLocked(true, true);
                StreamHalfState.SendState previousSendState = this.owner.halfStateInternal().markSendReset();
                this.owner.terminalStateInternal().recordLocalWriteReset(code);
                this.owner.clearWriteAdvisoryLocked();
                this.owner.sessionInternal().discardQueuedStreamDataLocked(this.owner, true);
                this.owner.sessionInternal().dropOrdinaryBatchStateLocked(this.owner);
                this.owner.notifySendTerminalTransitionLocked(previousSendState);
                this.owner.refreshGracefulCloseBlockingLocked();
                this.owner.sessionInternal().enqueueResetLocked(this.owner, code, resetPayload, false);
                this.owner.sessionInternal().maybeCompactStreamLocked(this.owner);
                this.owner.notifyLockWaitersLocked();
            }
        } finally {
            this.owner.emitPendingEvents();
        }
    }

    void closeWithError(long code, String reason) throws IOException {
        try {
            synchronized (this.owner.lockInternal()) {
                this.throwIfSessionTerminalLocked("close");
                if (this.owner.halfStateInternal().localAbortNoOp()) {
                    return;
                }
                if (!this.owner.lifecycleStateInternal().idAssigned()) {
                    this.owner.sessionInternal().buildControlErrorPayloadLocked(code, reason);
                    this.owner.sessionInternal().failProvisionalLocalAbortLocked(this.owner, code, reason);
                    this.owner.notifyLockWaitersLocked();
                    return;
                }
                byte[] abortPayload = this.owner.sessionInternal().buildControlErrorPayloadLocked(code, reason);
                this.owner.sessionInternal().markLocalStreamOpeningCommittedLocked(this.owner);
                this.owner.abortFromLocalLocked(code, reason);
                this.owner.sessionInternal().enqueueAbortLocked(this.owner, code, abortPayload, false);
                this.owner.sessionInternal().maybeCompactStreamLocked(this.owner);
                this.owner.notifyLockWaitersLocked();
            }
        } finally {
            this.owner.emitPendingEvents();
        }
    }

    void write(byte[] src, int offset, int length, boolean fin) throws IOException {
        this.write(src, offset, length, fin, true, SessionRuntime.PayloadOwnership.BORROWED);
    }

    void writeOwned(byte[] src, int offset, int length, boolean fin) throws IOException {
        this.write(src, offset, length, fin, true, SessionRuntime.PayloadOwnership.OWNED);
    }

    void queueWrite(byte[] src, int offset, int length, boolean fin) throws IOException {
        this.write(src, offset, length, fin, false, SessionRuntime.PayloadOwnership.BORROWED);
    }

    private void write(byte[] src,
                       int offset,
                       int length,
                       boolean fin,
                       boolean waitForTransportWrite,
                       SessionRuntime.PayloadOwnership payloadOwnership) throws IOException {
        Objects.requireNonNull(src, "src");
        RangeChecks.checkFromIndexSize(offset, length, src.length);
        if (length == 0 && !fin) {
            return;
        }
        StreamWriteCompletion completion = waitForTransportWrite ? new StreamWriteCompletion() : null;
        boolean waitForCompletion = false;
        try {
            synchronized (this.owner.lockInternal()) {
                this.ensureWritableLocked();
                boolean openingPending = this.owner.shouldEmitOpenerFrameLocked();
                byte[] openingPrefix = openingPending ? this.buildOpeningPrefixLocked() : StreamRuntime.EMPTY_BYTES;
                int position = offset;
                int end = offset + length;

                if (openingPending && length > 0 && this.owner.initialPeerSendLimitForPendingOpenLocked() == 0L) {
                    this.owner.sessionInternal().queueOpeningDataLocked(this.owner, openingPrefix, StreamRuntime.EMPTY_BYTES, false);
                    openingPrefix = StreamRuntime.EMPTY_BYTES;
                    openingPending = false;
                }

                while (position < end) {
                    int prefixBudget = openingPending ? openingPrefix.length : 0;
                    int chunkLimit = this.fragmentChunkLimitLocked(prefixBudget);
                    int remaining = end - position;
                    int chunkSize = Math.min(remaining, chunkLimit);
                    boolean frameFin = fin && position + chunkSize >= end;

                    if (chunkSize == 0) {
                        if (!openingPending) {
                            throw StreamRuntime.streamLocalError(
                                    ErrorCode.FRAME_SIZE,
                                    "write",
                                    "zmux: peer max_frame_payload does not allow DATA payload",
                                    ZmuxErrorDirection.WRITE
                            );
                        }
                        this.owner.sessionInternal().queueOpeningDataLocked(this.owner, openingPrefix, StreamRuntime.EMPTY_BYTES, false);
                        openingPrefix = StreamRuntime.EMPTY_BYTES;
                        openingPending = false;
                        continue;
                    }

                    if (openingPending) {
                        this.owner.sessionInternal().queueOpeningDataLocked(
                                this.owner,
                                openingPrefix,
                                src,
                                position,
                                chunkSize,
                                frameFin,
                                payloadOwnership,
                                completion
                        );
                        openingPrefix = StreamRuntime.EMPTY_BYTES;
                        openingPending = false;
                    } else {
                        this.owner.sessionInternal().queueDataLocked(
                                this.owner,
                                src,
                                position,
                                chunkSize,
                                frameFin,
                                payloadOwnership,
                                completion
                        );
                    }
                    position += chunkSize;
                }

                if (fin && length == 0) {
                    this.queueEmptyFinalFrameLocked(openingPending, openingPrefix, completion);
                }
                this.owner.noteWritePayloadProgressLocked(length);
                this.owner.notifyLockWaitersLocked();
                if (completion != null && completion.hasFrames()) {
                    this.owner.registerWriteCompletionWaiterLocked(completion);
                    waitForCompletion = true;
                }
            }
        } finally {
            this.owner.emitPendingEvents();
        }
        if (waitForCompletion) {
            this.awaitTransportWrite(completion);
        }
    }

    int writev(byte[][] parts, boolean fin) throws IOException {
        return this.writev(parts, fin, true);
    }

    int queueWritev(byte[][] parts, boolean fin) throws IOException {
        return this.writev(parts, fin, false);
    }

    private int writev(byte[][] parts, boolean fin, boolean waitForTransportWrite) throws IOException {
        Objects.requireNonNull(parts, "parts");
        int totalLength = StreamIoSupport.checkedWritevTotalLength(parts, "writevFinal");
        if (totalLength == 0 && !fin) {
            return 0;
        }

        StreamWriteCompletion completion = waitForTransportWrite ? new StreamWriteCompletion() : null;
        boolean waitForCompletion = false;
        try {
            synchronized (this.owner.lockInternal()) {
                this.ensureWritableLocked();
                boolean openingPending = this.owner.shouldEmitOpenerFrameLocked();
                byte[] openingPrefix = openingPending ? this.buildOpeningPrefixLocked() : StreamRuntime.EMPTY_BYTES;
                int partIndex = 0;
                int partOffset = 0;
                int remainingTotal = totalLength;

                if (openingPending && totalLength > 0 && this.owner.initialPeerSendLimitForPendingOpenLocked() == 0L) {
                    this.owner.sessionInternal().queueOpeningDataLocked(this.owner, openingPrefix, StreamRuntime.EMPTY_BYTES, false);
                    openingPrefix = StreamRuntime.EMPTY_BYTES;
                    openingPending = false;
                }

                while (remainingTotal > 0) {
                    while (partIndex < parts.length && partOffset >= parts[partIndex].length) {
                        partIndex++;
                        partOffset = 0;
                    }

                    int prefixBudget = openingPending ? openingPrefix.length : 0;
                    int chunkLimit = this.fragmentChunkLimitLocked(prefixBudget);
                    int chunkSize = Math.min(remainingTotal, chunkLimit);
                    boolean frameFin = fin && chunkSize == remainingTotal;

                    if (chunkSize == 0) {
                        if (!openingPending) {
                            throw StreamRuntime.streamLocalError(
                                    ErrorCode.FRAME_SIZE,
                                    "writevFinal",
                                    "zmux: peer max_frame_payload does not allow DATA payload",
                                    ZmuxErrorDirection.WRITE
                            );
                        }
                        this.owner.sessionInternal().queueOpeningDataLocked(this.owner, openingPrefix, StreamRuntime.EMPTY_BYTES, false);
                        openingPrefix = StreamRuntime.EMPTY_BYTES;
                        openingPending = false;
                        continue;
                    }

                    byte[] current = parts[partIndex];
                    int currentAvailable = current.length - partOffset;
                    boolean singleSegment = chunkSize <= currentAvailable;
                    if (openingPending) {
                        if (singleSegment) {
                            this.owner.sessionInternal().queueOpeningDataLocked(this.owner, openingPrefix, current, partOffset, chunkSize, frameFin, completion);
                        } else {
                            this.owner.sessionInternal().queueOpeningDataLocked(
                                    this.owner,
                                    openingPrefix,
                                    parts,
                                    partIndex,
                                    partOffset,
                                    chunkSize,
                                    frameFin,
                                    SessionRuntime.PayloadOwnership.BORROWED,
                                    completion
                            );
                        }
                        openingPrefix = StreamRuntime.EMPTY_BYTES;
                        openingPending = false;
                    } else if (singleSegment) {
                        this.owner.sessionInternal().queueDataLocked(this.owner, current, partOffset, chunkSize, frameFin, completion);
                    } else {
                        this.owner.sessionInternal().queueDataLocked(
                                this.owner,
                                parts,
                                partIndex,
                                partOffset,
                                chunkSize,
                                frameFin,
                                SessionRuntime.PayloadOwnership.BORROWED,
                                completion
                        );
                    }

                    int toAdvance = chunkSize;
                    while (toAdvance > 0 && partIndex < parts.length) {
                        int available = parts[partIndex].length - partOffset;
                        int step = Math.min(available, toAdvance);
                        partOffset += step;
                        toAdvance -= step;
                        if (partOffset >= parts[partIndex].length) {
                            partIndex++;
                            partOffset = 0;
                        }
                    }
                    remainingTotal -= chunkSize;
                }

                if (fin && totalLength == 0) {
                    this.queueEmptyFinalFrameLocked(openingPending, openingPrefix, completion);
                }
                this.owner.noteWritePayloadProgressLocked(totalLength);
                this.owner.notifyLockWaitersLocked();
                if (completion != null && completion.hasFrames()) {
                    this.owner.registerWriteCompletionWaiterLocked(completion);
                    waitForCompletion = true;
                }
            }
        } finally {
            this.owner.emitPendingEvents();
        }
        if (waitForCompletion) {
            this.awaitTransportWrite(completion);
        }
        return totalLength;
    }

    private void awaitTransportWrite(StreamWriteCompletion completion) throws IOException {
        boolean deadlineCancellationAttempted = false;
        try {
            while (true) {
                if (completion.done()) {
                    completion.throwIfFailed();
                    return;
                }
                long waitNanos;
                synchronized (this.owner.lockInternal()) {
                    waitNanos = deadlineCancellationAttempted ? 0L : this.owner.remainingWriteDeadlineNanosLocked();
                }
                if (!deadlineCancellationAttempted && waitNanos < 0L) {
                    WriteTimeoutException timeout = new WriteTimeoutException();
                    if (this.cancelQueuedWriteAfterTimeout(completion, timeout)) {
                        throw timeout;
                    }
                    deadlineCancellationAttempted = true;
                    continue;
                }
                completion.awaitSignal(waitNanos);
            }
        } finally {
            synchronized (this.owner.lockInternal()) {
                this.owner.unregisterWriteCompletionWaiterLocked(completion);
            }
        }
    }

    private boolean cancelQueuedWriteAfterTimeout(StreamWriteCompletion completion, IOException timeout) {
        synchronized (this.owner.lockInternal()) {
            if (!this.owner.sessionInternal().cancelQueuedWriteCompletionLocked(completion)) {
                return false;
            }
            if (!completion.completeFailureIfPending(timeout)) {
                return false;
            }
            this.owner.notifyLockWaitersLocked();
        }
        this.owner.emitPendingEvents();
        return true;
    }

    void ensureWritableLocked() throws IOException {
        this.ensureLocalWriteSurfaceLocked("write");
        if (this.owner.halfStateInternal().sendStopSeen()) {
            throw this.owner.terminalStateInternal().peerStopWriteClosed();
        }
        this.throwIfResetOrAbortedLocked();
        if (!this.owner.halfStateInternal().sendOpen()) {
            throw new WriteClosedException(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.GRACEFUL);
        }
    }

    void ensureCloseWritableLocked() throws IOException {
        this.ensureLocalWriteSurfaceLocked("close");
        this.throwIfResetOrAbortedLocked();
        this.throwIfGracefullyClosedOrNotOpenLocked();
    }

    void ensureResettableLocked() throws IOException {
        this.ensureLocalWriteSurfaceLocked("close");
        if (this.owner.halfStateInternal().sendStopSeen()) {
            if (this.owner.halfStateInternal().sendResetOrAborted() || this.owner.halfStateInternal().sendFin()) {
                throw this.owner.terminalStateInternal().peerStopWriteClosed();
            }
            if (!this.owner.halfStateInternal().sendOpen() && !this.owner.halfStateInternal().sendFinQueued()) {
                throw this.owner.terminalStateInternal().peerStopWriteClosed();
            }
            return;
        }
        this.throwIfResetOrAbortedLocked();
        this.throwIfGracefullyClosedOrNotOpenLocked();
    }

    private void ensureLocalWriteSurfaceLocked(String operation) throws IOException {
        if (!this.owner.localSend()) {
            throw new StreamNotWritableException();
        }
        this.throwIfSessionTerminalLocked(operation);
        if (this.owner.terminalStateInternal().localError() != null) {
            throw this.owner.terminalStateInternal().localError();
        }
        if (this.owner.terminalStateInternal().sendCloseError() != null) {
            throw this.owner.terminalStateInternal().sendCloseError();
        }
    }

    private void throwIfResetOrAbortedLocked() throws IOException {
        if (!this.owner.halfStateInternal().sendResetOrAborted()) {
            return;
        }
        if (this.owner.halfStateInternal().sendResetFromPeerStop()) {
            throw this.owner.terminalStateInternal().peerStopWriteClosed();
        }
        if (this.owner.terminalStateInternal().recvAbortError() != null) {
            throw this.owner.terminalStateInternal().recvAbortError();
        }
        if (this.owner.terminalStateInternal().recvResetError() != null) {
            throw this.owner.terminalStateInternal().recvResetError();
        }
        throw new WriteClosedException(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.GRACEFUL);
    }

    private void throwIfGracefullyClosedOrNotOpenLocked() throws IOException {
        if (this.owner.halfStateInternal().sendFin() || this.owner.halfStateInternal().sendFinQueued()) {
            throw new WriteClosedException(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.GRACEFUL);
        }
        if (!this.owner.halfStateInternal().sendOpen()) {
            throw new WriteClosedException(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.GRACEFUL);
        }
    }

    private void queueEmptyFinalFrameLocked(boolean openingPending,
                                           byte[] openingPrefix,
                                           StreamWriteCompletion completion) throws IOException {
        if (openingPending) {
            this.owner.sessionInternal().queueOpeningDataLocked(this.owner, openingPrefix, StreamRuntime.EMPTY_BYTES, true, completion);
        } else {
            this.owner.sessionInternal().queueDataLocked(this.owner, StreamRuntime.EMPTY_BYTES, true, completion);
        }
    }

    private byte[] buildOpeningPrefixLocked() throws IOException {
        return this.owner.metadataStateInternal().buildOpeningPrefixLocked(this.owner.sessionInternal());
    }

    private int fragmentChunkLimitLocked(int prefixBudget) {
        long cap = this.owner.txFragmentCapLocked(Math.max(0L, prefixBudget));
        if (cap <= 0L) {
            return 0;
        }
        return (int) Math.min(Integer.MAX_VALUE, cap);
    }

    private void throwIfSessionTerminalLocked(String operation) throws IOException {
        IOException localRefused = this.owner.localGracefulCloseRefusedErrorLocked();
        if (localRefused != null) {
            throw localRefused;
        }
        if (this.owner.sessionInternal().shouldFailSessionOperationsLocked()) {
            throw this.owner.sessionInternal().sessionOperationErrorLocked(
                    operation,
                    this.owner.sessionInternal().currentErrorLocked()
            );
        }
    }

    private void throwIfCloseWriteSessionTerminalLocked() throws IOException {
        if (!this.owner.sessionInternal().shouldFailSessionOperationsLocked()) {
            return;
        }
        IOException localRefused = this.owner.localGracefulCloseRefusedErrorLocked();
        if (localRefused != null) {
            throw localRefused;
        }
        if (this.owner.sessionInternal().gracefulCloseActiveLocked()
                && this.owner.sessionInternal().stateInternal() == SessionState.DRAINING) {
            return;
        }
        throw this.owner.sessionInternal().sessionOperationErrorLocked(
                "close",
                this.owner.sessionInternal().currentErrorLocked()
        );
    }
}
