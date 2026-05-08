package io.zmux.runtime;

import io.zmux.Settings;
import io.zmux.WriteTimeoutException;
import io.zmux.ZmuxErrorDirection;
import io.zmux.ZmuxErrorScope;
import io.zmux.protocol.Frame;
import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;
import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.Objects;

@SuppressWarnings("resource")
final class SessionOutboundDataCoordinator {
    private final SessionRuntime owner;

    SessionOutboundDataCoordinator(SessionRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    private static long streamSendCreditLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null) {
            return 0L;
        }
        return RuntimeFlow.windowRemaining(
                streamRuntime.peerSendLimit(),
                SessionRuntime.saturatingAdd(streamRuntime.sentBytes(), streamRuntime.reservedSendBytes())
        );
    }

    static long queuedDataTrackedBytes(int dataBytes, int retainedQueueBytes) {
        return SessionRuntime.saturatingAdd(Math.max(0L, dataBytes), Math.max(0L, retainedQueueBytes));
    }

    void queueDataLocked(StreamRuntime streamRuntime,
                         byte[] payload,
                         int payloadOffset,
                         int payloadLength,
                         boolean fin,
                         SessionRuntime.PayloadOwnership payloadOwnership,
                         StreamWriteCompletion completion) throws IOException {
        int frameFlags = 0;
        if (fin) {
            frameFlags |= 0x40;
        }
        this.reserveSendLocked(streamRuntime, payloadLength, queuedDataTrackedBytes(payloadLength, 1));
        byte[] retainedPayload = this.owner.retainPayload(payload, payloadOffset, payloadLength, payloadOwnership);
        SessionRuntime.OutboundFrame outboundFrame = new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.DATA, frameFlags, streamRuntime.streamIdInternal(), retainedPayload),
                streamRuntime,
                payloadLength,
                false,
                false,
                completion
        );
        this.enqueueReservedDataLocked(streamRuntime, outboundFrame, payloadLength, fin);
    }

    void queueDataLocked(StreamRuntime streamRuntime,
                         byte[][] parts,
                         int partIndex,
                         int partOffset,
                         int length,
                         boolean fin,
                         SessionRuntime.PayloadOwnership payloadOwnership,
                         StreamWriteCompletion completion) throws IOException {
        int flags = 0;
        if (fin) {
            flags |= 0x40;
        }
        this.reserveSendLocked(streamRuntime, length, queuedDataTrackedBytes(length, 1));
        byte[][] payloadParts = this.owner.retainPayloadParts(parts, partIndex, partOffset, length, payloadOwnership);
        SessionRuntime.OutboundFrame outboundFrame;
        if (payloadParts.length <= 1) {
            byte[] payload = payloadParts.length == 0 ? StreamRuntime.EMPTY_BYTES : payloadParts[0];
            outboundFrame = new SessionRuntime.OutboundFrame(
                    new FrameCodec.Frame(FrameType.DATA, flags, streamRuntime.streamIdInternal(), payload),
                    streamRuntime,
                    length,
                    false,
                    false,
                    completion
            );
        } else {
            outboundFrame = new SessionRuntime.OutboundFrame(
                    new FrameCodec.Frame(FrameType.DATA, flags, streamRuntime.streamIdInternal(), StreamRuntime.EMPTY_BYTES),
                    streamRuntime,
                    length,
                    false,
                    false,
                    completion,
                    null,
                    null,
                    0,
                    length,
                    payloadParts,
                    0,
                    0
            );
        }
        this.enqueueReservedDataLocked(streamRuntime, outboundFrame, length, fin);
    }

    void reserveSendLocked(StreamRuntime streamRuntime, int bytes) throws IOException {
        this.reserveSendLocked(streamRuntime, bytes, bytes);
    }

    void reserveSendLocked(StreamRuntime streamRuntime, int bytes, long trackedAdditional) throws IOException {
        long memoryAdditional = Math.max(0L, trackedAdditional);
        while (true) {
            long streamCredit = streamSendCreditLocked(streamRuntime);
            long sessionCredit = this.owner.sessionRemainingSendCreditLocked();
            boolean flowBlocked = streamCredit < (long) bytes || sessionCredit < (long) bytes;
            boolean watermarkBlocked = !this.withinQueuedDataWatermarkLocked(streamRuntime, bytes);
            IOException memoryError = this.owner.streamWriteQueueMemoryErrorLocked(memoryAdditional);
            if (memoryError != null) {
                this.owner.failSession(memoryError);
                throw this.owner.sessionOperationErrorLocked("write", memoryError);
            }
            boolean memoryBlocked = this.owner.sessionWriteMemoryBlockedLocked(memoryAdditional);
            if (!flowBlocked && !watermarkBlocked && !memoryBlocked) {
                streamRuntime.reserveSendBytesLocked(bytes);
                this.owner.reserveSessionSendBytesLocked(bytes);
                return;
            }
            if (this.owner.shouldFailSessionOperationsLocked()) {
                throw this.owner.sessionOperationErrorLocked("write", this.owner.currentErrorLocked());
            }
            if (flowBlocked || watermarkBlocked) {
                this.maybeQueueBlockedLocked(streamRuntime, bytes);
            }
            if (!streamRuntime.shouldEmitQueuedDataLocked()) {
                throw streamRuntime.operationErrorLocked();
            }
            long blockedStartedAtNanos = System.nanoTime();
            try {
                long remainingNanos = streamRuntime.remainingWriteDeadlineNanosLocked();
                if (remainingNanos < 0L) {
                    throw new WriteTimeoutException();
                }
                if (remainingNanos == 0L) {
                    this.owner.waitOnLock(SessionRuntime.LockWaitKind.WRITE_STREAM);
                } else {
                    this.owner.waitOnLockNanos(remainingNanos, SessionRuntime.LockWaitKind.WRITE_STREAM);
                }
            } catch (InterruptedException interruptedException) {
                this.owner.noteBlockedWriteLocked(RuntimeFlow.elapsedNanos(
                        System.nanoTime(),
                        blockedStartedAtNanos
                ));
                Thread.currentThread().interrupt();
                throw SessionRuntime.interruptedIo(
                        "zmux: interrupted while waiting for send credit",
                        "write",
                        io.zmux.ZmuxErrorScope.STREAM,
                        io.zmux.ZmuxErrorDirection.WRITE,
                        interruptedException
                );
            }
            this.owner.noteBlockedWriteLocked(RuntimeFlow.elapsedNanos(System.nanoTime(), blockedStartedAtNanos));
        }
    }

    long queuedDataBytesForStreamLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null) {
            return 0L;
        }
        return streamRuntime.queuedDataBytesLocked();
    }

    long inflightQueuedBytesForStreamLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null) {
            return 0L;
        }
        return Math.max(0L, streamRuntime.reservedSendBytes() - this.queuedDataBytesForStreamLocked(streamRuntime));
    }

    long fragmentCapLocked(StreamRuntime streamRuntime) {
        if (streamRuntime != null) {
            long fragmentCap = streamRuntime.txFragmentCapLocked(0L);
            if (fragmentCap > 0L) {
                return fragmentCap;
            }
        }
        long maxFramePayload = this.owner.peerSettings().maxFramePayload();
        if (maxFramePayload <= 0L) {
            maxFramePayload = Settings.defaults().maxFramePayload();
        }
        return maxFramePayload;
    }

    void releaseQueuedDataLocked(SessionRuntime.OutboundFrame outboundFrame) {
        if (outboundFrame == null || outboundFrame.stream() == null || outboundFrame.dataBytes() <= 0) {
            return;
        }
        long previousTracked = this.owner.trackedSessionMemoryLocked();
        long previousSessionCredit = this.owner.sessionRemainingSendCreditLocked();
        long previousStreamCredit = streamSendCreditLocked(outboundFrame.stream());
        this.owner.releaseQueuedDataAccountingLocked(outboundFrame.stream(), outboundFrame.dataBytes());
        outboundFrame.stream().releaseReservedSendBytesLocked(outboundFrame.dataBytes());
        this.owner.releaseSessionReservedSendBytesLocked(outboundFrame.dataBytes());
        boolean sessionCreditGained = RuntimeFlow.gainedCredit(
                previousSessionCredit,
                this.owner.sessionRemainingSendCreditLocked()
        );
        boolean streamCreditGained = RuntimeFlow.gainedCredit(
                previousStreamCredit,
                streamSendCreditLocked(outboundFrame.stream())
        );
        if (sessionCreditGained) {
            this.owner.clearBlockedFrameLocked(0L);
        }
        if (streamCreditGained) {
            this.owner.clearBlockedFrameLocked(outboundFrame.stream().streamIdInternal());
        }
        if (this.owner.sessionMemoryWakeNeededLocked(previousTracked) || sessionCreditGained || streamCreditGained) {
            this.owner.notifyStreamWriteWaitersLocked();
        }
    }

    void discardQueuedStreamDataLocked(Deque<SessionRuntime.OutboundFrame> deque,
                                       StreamRuntime streamRuntime,
                                       boolean preserveAfterSendClose) {
        Iterator<SessionRuntime.OutboundFrame> iterator = deque.iterator();
        while (iterator.hasNext()) {
            SessionRuntime.OutboundFrame outboundFrame = iterator.next();
            if (outboundFrame.stream() != streamRuntime || outboundFrame.frame().type() != FrameType.DATA) {
                continue;
            }
            if (preserveAfterSendClose && outboundFrame.preserveAfterSendClose()) {
                continue;
            }
            iterator.remove();
            this.owner.onQueuedFrameDequeuedLocked(deque, outboundFrame);
            this.owner.failWriteCompletionLocked(outboundFrame, null);
            this.releaseQueuedDataLocked(outboundFrame);
        }
    }

    private void maybeQueueBlockedLocked(StreamRuntime streamRuntime, int requestedBytes) {
        if (requestedBytes <= 0) {
            return;
        }
        if (streamRuntime != null) {
            this.owner.moveOpeningFrameToUrgentLocked(streamRuntime);
        }
        if (this.owner.sessionRemainingSendCreditLocked() < (long) requestedBytes) {
            this.queueBlockedFrameLocked(0L, this.owner.sessionSendLimitLocked());
        }
        if (streamRuntime != null
                && streamSendCreditLocked(streamRuntime) < (long) requestedBytes) {
            this.queueBlockedFrameLocked(streamRuntime.streamIdInternal(), streamRuntime.peerSendLimit());
        }
    }

    private void queueBlockedFrameLocked(long streamId, long offset) {
        if (this.owner.flowControlUpdateRegistryInternal().queueBlockedFrameLocked(streamId, offset)) {
            this.owner.notifyWriterWaitersLocked();
        }
    }

    void ensureStreamWriteQueueMemoryLocked(SessionRuntime.OutboundFrame outboundFrame) throws IOException {
        IOException memoryError = this.owner.streamWriteQueueMemoryErrorLocked(
                SessionRuntime.retainedQueueBytes(outboundFrame)
        );
        if (memoryError == null) {
            return;
        }
        this.owner.failSession(memoryError);
        throw this.owner.sessionOperationErrorLocked("write", memoryError);
    }

    void releaseReservedSendLocked(StreamRuntime streamRuntime, int bytes) {
        if (streamRuntime == null || bytes <= 0) {
            return;
        }
        streamRuntime.releaseReservedSendBytesLocked(bytes);
        this.owner.releaseSessionReservedSendBytesLocked(bytes);
        this.owner.notifyStreamWriteWaitersLocked();
    }

    private void enqueueReservedDataLocked(StreamRuntime streamRuntime,
                                           SessionRuntime.OutboundFrame outboundFrame,
                                           int dataBytes,
                                           boolean fin) throws IOException {
        try {
            this.ensureStreamWriteQueueMemoryLocked(outboundFrame);
            streamRuntime.markLocalSendStartedLocked();
            if (fin) {
                streamRuntime.markFinQueuedLocked();
            }
            this.owner.enqueueQueuedOutboundLocked(this.owner.dataQueueInternal(), outboundFrame);
            outboundFrame.retainWriteCompletion();
        } catch (IOException error) {
            this.releaseReservedSendLocked(streamRuntime, dataBytes);
            throw error;
        }
    }

    boolean withinQueuedDataWatermarkLocked(StreamRuntime streamRuntime, int bytes) {
        if (bytes <= 0) {
            return true;
        }
        long perStreamLimit = this.owner.perStreamQueuedDataHighWatermarkLocked();
        long sessionLimit = this.owner.sessionQueuedDataHighWatermarkLocked();
        return !RuntimeFlow.queueWouldBlock(
                false,
                this.owner.sessionQueuedDataBytesLocked(),
                streamRuntime.queuedDataBytesLocked(),
                bytes,
                sessionLimit,
                perStreamLimit
        );
    }
}
