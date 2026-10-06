package io.zmux.runtime;

import io.zmux.Settings;
import io.zmux.WriteTimeoutException;
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

    /**
     * Queues one DATA frame carrying at most {@code payloadLength} bytes and returns how many were queued.
     * The frame is capped to the currently available stream and session credit; the call only waits while
     * that credit (or queue/memory admission) is exhausted. FIN is set only when the whole payload fits.
     */
    int queueDataLocked(StreamRuntime streamRuntime,
                        byte[] payload,
                        int payloadOffset,
                        int payloadLength,
                        boolean fin,
                        SessionRuntime.PayloadOwnership payloadOwnership,
                        StreamWriteCompletion completion) throws IOException {
        int reserved = this.reserveSendUpToLocked(streamRuntime, payloadLength, 1);
        boolean frameFin = fin && reserved == payloadLength;
        int frameFlags = 0;
        if (frameFin) {
            frameFlags |= 0x40;
        }
        byte[] retainedPayload = this.owner.retainPayload(payload, payloadOffset, reserved, payloadOwnership);
        SessionRuntime.OutboundFrame outboundFrame = new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.DATA, frameFlags, streamRuntime.streamIdInternal(), retainedPayload),
                streamRuntime,
                reserved,
                false,
                false,
                completion
        );
        this.enqueueReservedDataLocked(streamRuntime, outboundFrame, reserved, frameFin);
        return reserved;
    }

    int queueDataLocked(StreamRuntime streamRuntime,
                        byte[][] parts,
                        int partIndex,
                        int partOffset,
                        int requestedLength,
                        boolean fin,
                        SessionRuntime.PayloadOwnership payloadOwnership,
                        StreamWriteCompletion completion) throws IOException {
        int length = this.reserveSendUpToLocked(streamRuntime, requestedLength, 1);
        boolean frameFin = fin && length == requestedLength;
        int flags = 0;
        if (frameFin) {
            flags |= 0x40;
        }
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
        this.enqueueReservedDataLocked(streamRuntime, outboundFrame, length, frameFin);
        return length;
    }

    /**
     * Reserves between 1 and {@code maxBytes} bytes of stream and session send credit (exactly 0 when
     * {@code maxBytes} is 0), waiting only while no credit at all is available or queue/memory admission
     * blocks. Writes are fragmented to the available windows rather than waiting for a fixed chunk size to
     * fit, which a peer with small windows might never grant.
     */
    int reserveSendUpToLocked(StreamRuntime streamRuntime, int maxBytes, int retainedOverhead) throws IOException {
        int requested = Math.max(0, maxBytes);
        boolean firstWait = true;
        while (true) {
            int bytes = this.availableSendBytesLocked(streamRuntime, requested);
            boolean flowBlocked = requested > 0 && bytes == 0;
            boolean watermarkBlocked = !flowBlocked && !this.withinQueuedDataWatermarkLocked(streamRuntime, bytes);
            if (this.tryReserveLocked(
                    streamRuntime,
                    bytes,
                    queuedDataTrackedBytes(bytes, retainedOverhead),
                    flowBlocked,
                    watermarkBlocked
            )) {
                return bytes;
            }
            this.awaitSendAdmissionLocked(streamRuntime, firstWait);
            firstWait = false;
        }
    }

    /**
     * Non-blocking reservation for a stream's opening frame. The caller has already committed the stream
     * ID, so it must not release the session monitor before the opener is queued: a later same-class stream
     * could otherwise put its opener on the wire first (SPEC §3.1). Returns the payload bytes the opener
     * may carry, possibly 0 (a zero-length opener); the rest of the write waits for credit afterwards.
     */
    int reserveOpeningSendLocked(StreamRuntime streamRuntime, int maxBytes, int retainedOverhead) throws IOException {
        IOException memoryError = this.owner.streamWriteQueueMemoryErrorLocked(
                queuedDataTrackedBytes(0, retainedOverhead)
        );
        if (memoryError != null) {
            this.owner.failSession(memoryError);
            throw this.owner.sessionOperationErrorLocked("write", memoryError);
        }
        int bytes = this.availableSendBytesLocked(streamRuntime, Math.max(0, maxBytes));
        if (bytes > 0) {
            long tracked = queuedDataTrackedBytes(bytes, retainedOverhead);
            if (!this.withinQueuedDataWatermarkLocked(streamRuntime, bytes)
                    || this.owner.streamWriteQueueMemoryErrorLocked(tracked) != null
                    || this.owner.sessionWriteMemoryBlockedLocked(tracked)) {
                bytes = 0;
            }
        }
        if (bytes > 0) {
            streamRuntime.reserveSendBytesLocked(bytes);
            this.owner.reserveSessionSendBytesLocked(bytes);
        }
        return bytes;
    }

    private int availableSendBytesLocked(StreamRuntime streamRuntime, int maxBytes) {
        if (maxBytes <= 0) {
            return 0;
        }
        long credit = Math.min(streamSendCreditLocked(streamRuntime), this.owner.sessionRemainingSendCreditLocked());
        return (int) Math.max(0L, Math.min(maxBytes, credit));
    }

    private boolean tryReserveLocked(StreamRuntime streamRuntime,
                                     int bytes,
                                     long memoryAdditional,
                                     boolean flowBlocked,
                                     boolean watermarkBlocked) throws IOException {
        IOException memoryError = this.owner.streamWriteQueueMemoryErrorLocked(memoryAdditional);
        if (memoryError != null) {
            this.owner.failSession(memoryError);
            throw this.owner.sessionOperationErrorLocked("write", memoryError);
        }
        boolean memoryBlocked = this.owner.sessionWriteMemoryBlockedLocked(memoryAdditional);
        if (!flowBlocked && !watermarkBlocked && !memoryBlocked) {
            streamRuntime.reserveSendBytesLocked(bytes);
            this.owner.reserveSessionSendBytesLocked(bytes);
            return true;
        }
        if (this.owner.shouldFailSessionOperationsLocked()) {
            throw this.owner.sessionOperationErrorLocked("write", this.owner.currentErrorLocked());
        }
        if (flowBlocked) {
            this.maybeQueueBlockedLocked(streamRuntime);
        }
        if (!streamRuntime.shouldEmitQueuedDataLocked()) {
            throw streamRuntime.operationErrorLocked();
        }
        return false;
    }

    private void awaitSendAdmissionLocked(StreamRuntime streamRuntime, boolean wakeWriter) throws IOException {
        if (wakeWriter) {
            // Frames this write already queued must be able to drain while it waits; otherwise a write
            // larger than the queued-data watermark stalls until some unrelated writer wakeup. Only the
            // first wait notifies, so blocked writers cannot wake each other in a loop.
            this.owner.notifyWriterWaitersLocked();
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

    private void maybeQueueBlockedLocked(StreamRuntime streamRuntime) {
        if (streamRuntime != null) {
            this.owner.moveOpeningFrameToUrgentLocked(streamRuntime);
        }
        // BLOCKED reports that the applicable limit has been reached (SPEC §6.6), so only a scope whose
        // credit is exhausted is advertised; a write merely larger than the remaining credit is fragmented.
        if (this.owner.sessionRemainingSendCreditLocked() <= 0L) {
            this.queueBlockedFrameLocked(0L, this.owner.sessionSendLimitLocked());
        }
        if (streamRuntime != null && streamSendCreditLocked(streamRuntime) <= 0L) {
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
