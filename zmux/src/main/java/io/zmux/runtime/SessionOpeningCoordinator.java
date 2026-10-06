package io.zmux.runtime;

import io.zmux.*;
import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;

import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("resource")
final class SessionOpeningCoordinator {
    private final SessionRuntime owner;

    SessionOpeningCoordinator(SessionRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    private static boolean hasQueuedOpeningFrameLocked(Deque<SessionRuntime.OutboundFrame> deque,
                                                       StreamRuntime streamRuntime) {
        for (SessionRuntime.OutboundFrame outboundFrame : deque) {
            if (outboundFrame.stream() == streamRuntime && outboundFrame.openingFrame()) {
                return true;
            }
        }
        return false;
    }

    private static ApplicationError refusedLocalOpenError(ZmuxTerminationKind terminationKind) {
        return new ApplicationError(
                io.zmux.ErrorCode.REFUSED_STREAM.code(),
                "",
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.REMOTE,
                ZmuxErrorDirection.BOTH,
                terminationKind,
                "open"
        );
    }

    StreamRuntime newLocalStreamLocked(boolean bidirectional, OpenOptions openOptions) throws IOException {
        return this.newLocalStreamLocked(bidirectional, openOptions, TimeoutBudget.unbounded());
    }

    StreamRuntime newLocalStreamLocked(boolean bidirectional, OpenOptions openOptions, TimeoutBudget budget)
            throws IOException {
        OpenOptions effectiveOptions = openOptions == null ? OpenOptions.empty() : openOptions;
        this.throwIfNewLocalOpenDisallowedLocked();
        if (budget != null && budget.expired()) {
            throw new OpenTimeoutException();
        }
        byte[] openInfo = effectiveOptions.openInfoLength() == 0 ? StreamRuntime.EMPTY_BYTES : effectiveOptions.openInfo();
        try {
            FrameCodec.buildOpenMetadataPrefix(
                    this.owner.capabilities(),
                    effectiveOptions.initialPriority(),
                    effectiveOptions.initialGroup(),
                    openInfo,
                    this.owner.peerSettings().maxFramePayload()
            );
        } catch (OpenInfoUnavailableException error) {
            throw new OpenInfoUnavailableException(
                    "open",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorSource.LOCAL,
                    ZmuxErrorDirection.WRITE,
                    error
            );
        } catch (OpenMetadataTooLargeException error) {
            throw new OpenMetadataTooLargeException(
                    "open",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorSource.LOCAL,
                    ZmuxErrorDirection.WRITE,
                    error
            );
        }
        this.checkLocalOpenCapacityLocked(bidirectional, effectiveOptions.openInfoLength());
        StreamRuntime streamRuntime = new StreamRuntime(this.owner, true, bidirectional, effectiveOptions);
        this.owner.localOpenTrackerInternal().appendProvisionalLocked(streamRuntime);
        this.owner.onStreamOpenInfoUpdatedLocked(0, streamRuntime.openInfoLengthLocked());
        return streamRuntime;
    }

    long beginLocalOpenLocked(StreamRuntime streamRuntime) throws IOException {
        return this.beginLocalOpenLocked(streamRuntime, LocalOpenSurface.OPEN);
    }

    long beginLocalOpenForWriteLocked(StreamRuntime streamRuntime) throws IOException {
        return this.beginLocalOpenLocked(streamRuntime, LocalOpenSurface.WRITE);
    }

    long beginLocalOpenForCloseLocked(StreamRuntime streamRuntime) throws IOException {
        return this.beginLocalOpenLocked(streamRuntime, LocalOpenSurface.CLOSE);
    }

    private long beginLocalOpenLocked(StreamRuntime streamRuntime, LocalOpenSurface surface) throws IOException {
        if (streamRuntime.idAssigned()) {
            return streamRuntime.streamIdInternal();
        }
        boolean bidirectional = streamRuntime.bidirectional();
        while (true) {
            if (!streamRuntime.provisionalTracked()) {
                throw streamRuntime.operationErrorLocked();
            }
            IOException blockedError = this.provisionalCommitErrorLocked(streamRuntime, surface);
            if (blockedError != null) {
                throw blockedError;
            }
            long nowNanos = System.nanoTime();
            this.owner.localOpenTrackerInternal().reapExpiredProvisionalsLocked(
                    bidirectional,
                    nowNanos,
                    this.owner.provisionalOpenMaxAgeNanosLocked()
            );
            if (!streamRuntime.provisionalTracked()) {
                throw streamRuntime.operationErrorLocked();
            }
            blockedError = this.provisionalCommitErrorLocked(streamRuntime, surface);
            if (blockedError != null) {
                throw blockedError;
            }
            if (streamRuntime.idAssigned()) {
                return streamRuntime.streamIdInternal();
            }
            StreamRuntime provisionalHead = this.owner.localOpenTrackerInternal().provisionalHeadLocked(bidirectional);
            if (provisionalHead == streamRuntime) {
                break;
            }
            OpenTurnWait wait = this.provisionalOpenTurnWaitLocked(streamRuntime, provisionalHead, nowNanos);
            if (wait.kind() == OpenTurnWaitKind.RETRY) {
                continue;
            }
            if (wait.kind() == OpenTurnWaitKind.WRITE_TIMEOUT) {
                throw new WriteTimeoutException();
            }
            // Waiting for the commit turn does not age the stream (see SessionLocalOpenTracker#provisionalExpired).
            streamRuntime.beginProvisionalCommitWaitLocked(System.nanoTime());
            try {
                if (wait.kind() == OpenTurnWaitKind.UNBOUNDED) {
                    this.owner.waitOnLock(SessionRuntime.LockWaitKind.OPEN);
                } else {
                    this.owner.waitOnLockNanos(wait.waitNanos(), SessionRuntime.LockWaitKind.OPEN);
                }
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                IOException interruptedIo = surface.interruptedIOException(interruptedException);
                this.owner.localOpenTrackerInternal().failProvisionalLocked(streamRuntime, interruptedIo, false);
                throw interruptedIo;
            } finally {
                streamRuntime.endProvisionalCommitWaitLocked(System.nanoTime());
            }
        }
        long assignedStreamId = this.owner.nextLocalStreamIdLocked(bidirectional);
        try {
            this.checkLocalOpenAllowedLocked(assignedStreamId, bidirectional, 0);
        } catch (IOException error) {
            this.owner.localOpenTrackerInternal().failProvisionalLocked(
                    streamRuntime,
                    error,
                    this.owner.isRefusedStreamError(error)
            );
            if (!surface.directOpenSurface() && this.owner.isRefusedStreamError(error)) {
                throw streamRuntime.operationErrorLocked();
            }
            throw error;
        }
        this.owner.commitLocalOpenAssignmentLocked(streamRuntime, assignedStreamId);
        return assignedStreamId;
    }

    private OpenTurnWait provisionalOpenTurnWaitLocked(StreamRuntime streamRuntime,
                                                       StreamRuntime provisionalHead,
                                                       long nowNanos) {
        long writeRemainingNanos = streamRuntime.remainingWriteDeadlineNanosLocked();
        if (writeRemainingNanos < 0L) {
            return OpenTurnWait.writeTimeout();
        }
        if (provisionalHead == null) {
            return writeRemainingNanos == 0L ? OpenTurnWait.unbounded() : OpenTurnWait.timed(writeRemainingNanos);
        }
        // A head that is itself waiting for its commit turn does not age; it commits (and notifies) as soon
        // as it can.
        long headAgeOriginNanos = provisionalHead.provisionalCommitWaitingLocked()
                ? 0L
                : provisionalHead.provisionalAgeOriginNanos();
        long provisionalOpenMaxAgeNanos = this.owner.provisionalOpenMaxAgeNanosLocked();
        if (headAgeOriginNanos != 0L && provisionalOpenMaxAgeNanos > 0L) {
            long headRemainingNanos = TimeoutBudget.remainingNanosUntil(
                    SessionRuntime.saturatingAdd(headAgeOriginNanos, provisionalOpenMaxAgeNanos),
                    nowNanos
            );
            if (headRemainingNanos <= 0L) {
                return OpenTurnWait.retry();
            }
            if (writeRemainingNanos == 0L) {
                return OpenTurnWait.timed(headRemainingNanos);
            }
            return OpenTurnWait.timed(Math.min(headRemainingNanos, writeRemainingNanos));
        }
        return writeRemainingNanos == 0L ? OpenTurnWait.unbounded() : OpenTurnWait.timed(writeRemainingNanos);
    }

    void moveOpeningFrameToUrgentLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null) {
            return;
        }
        if (SessionOpeningCoordinator.hasQueuedOpeningFrameLocked(this.owner.urgentQueueInternal(), streamRuntime)) {
            streamRuntime.markOpeningFramePendingLocked();
            return;
        }
        SessionRuntime.OutboundFrame queuedOpener = null;
        for (SessionRuntime.OutboundFrame outboundFrame : this.owner.dataQueueInternal()) {
            if (outboundFrame.stream() == streamRuntime && outboundFrame.openingFrame()) {
                queuedOpener = outboundFrame;
                break;
            }
        }
        // Without urgent capacity the opener keeps its place: re-appending it behind later openers of
        // the same class would put their stream IDs on the wire first.
        if (queuedOpener == null || !this.owner.canAdmitUrgentOutboundLocked(queuedOpener)) {
            return;
        }
        SessionRuntime.OutboundFrame opener =
                this.removeQueuedOpeningFrameLocked(this.owner.dataQueueInternal(), streamRuntime);
        if (opener != null) {
            this.owner.enqueueAdmittedExistingUrgentOutboundLocked(opener);
            streamRuntime.markOpeningFramePendingLocked();
        }
    }

    void prepareLocalControlOpenerLocked(StreamRuntime streamRuntime,
                                         byte[] openingPrefix,
                                         boolean replaceQueuedPayload,
                                         boolean preserveAfterSendClose) throws IOException {
        if (streamRuntime == null) {
            return;
        }
        LocalOpenPhase phase = streamRuntime.localOpenPhaseLocked();
        if (!phase.awaitingPeerVisibility()) {
            return;
        }
        SessionRuntime.OutboundFrame existingOpeningFrame = this.removeQueuedOpeningFrameLocked(streamRuntime);
        if (existingOpeningFrame != null) {
            if (replaceQueuedPayload) {
                this.owner.failWriteCompletionLocked(existingOpeningFrame, null);
                this.owner.releaseQueuedDataLocked(existingOpeningFrame);
                SessionRuntime.OutboundFrame replacement = this.newOpeningFrameLocked(
                        streamRuntime,
                        openingPrefix,
                        false,
                        0,
                        preserveAfterSendClose
                );
                this.owner.enqueueQueuedOutboundLocked(this.owner.urgentQueueInternal(), replacement);
            } else {
                this.owner.enqueueExistingOutboundLocked(
                        this.owner.urgentQueueInternal(),
                        existingOpeningFrame.withPreserveAfterSendClose(
                                preserveAfterSendClose || existingOpeningFrame.preserveAfterSendClose()
                        )
                );
            }
            streamRuntime.markOpeningFramePendingLocked();
            this.owner.markLocalStreamOpeningCommittedLocked(streamRuntime);
            this.owner.flushPendingPriorityUpdateLocked(streamRuntime);
            return;
        }
        if (!phase.shouldEmitOpenerFrame()) {
            return;
        }
        if (!streamRuntime.idAssigned()) {
            this.owner.beginLocalOpenForCloseLocked(streamRuntime);
        }
        SessionRuntime.OutboundFrame opener = this.newOpeningFrameLocked(
                streamRuntime,
                openingPrefix,
                false,
                0,
                preserveAfterSendClose
        );
        this.owner.enqueueQueuedOutboundLocked(this.owner.urgentQueueInternal(), opener);
        streamRuntime.markOpeningFramePendingLocked();
        this.owner.markLocalStreamOpeningCommittedLocked(streamRuntime);
        this.owner.flushPendingPriorityUpdateLocked(streamRuntime);
    }

    /**
     * Queues the stream's opening DATA frame carrying at most {@code payloadLength} bytes and returns how
     * many it carries. Committing the stream ID and queueing the opener happen in one monitor hold: the
     * opener only takes credit that is available right now (possibly none), so no later same-class
     * stream can get its opener into the writer order first.
     */
    int queueOpeningDataLocked(StreamRuntime streamRuntime,
                               byte[] openingPrefix,
                               byte[] payload,
                               int payloadOffset,
                               int payloadLength,
                               boolean fin,
                               SessionRuntime.PayloadOwnership payloadOwnership,
                               StreamWriteCompletion completion) throws IOException {
        int reserved = this.prepareOpeningWriteFrameLocked(streamRuntime, openingPrefix, payloadLength);
        boolean frameFin = fin && reserved == payloadLength;
        byte[] retainedPayload = this.owner.retainPayload(payload, payloadOffset, reserved, payloadOwnership);
        SessionRuntime.OutboundFrame outboundFrame = new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.DATA, openingFrameFlags(openingPrefix, frameFin), streamRuntime.streamIdInternal(), openingPrefix),
                streamRuntime,
                reserved,
                true,
                false,
                completion,
                openingPrefix,
                retainedPayload,
                0,
                retainedPayload.length,
                null,
                0,
                0
        );
        this.enqueueReservedOpeningDataLocked(streamRuntime, outboundFrame, reserved, frameFin);
        return reserved;
    }

    void queueOpeningFinLocked(StreamRuntime streamRuntime, byte[] openingPrefix) throws IOException {
        if (!streamRuntime.idAssigned()) {
            this.owner.beginLocalOpenForCloseLocked(streamRuntime);
        }
        this.owner.reserveOpeningSendLocked(streamRuntime, 0, openingPrefix.length + 1);
        SessionRuntime.OutboundFrame outboundFrame = new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.DATA, openingPrefix.length == 0 ? 0x40 : 0x60, streamRuntime.streamIdInternal(), openingPrefix),
                streamRuntime,
                0,
                true,
                false,
                openingPrefix,
                StreamRuntime.EMPTY_BYTES,
                0,
                0,
                null,
                0,
                0
        );
        this.owner.ensureStreamWriteQueueMemoryLocked(outboundFrame);
        streamRuntime.markLocalSendStartedLocked();
        streamRuntime.markFinQueuedLocked();
        this.owner.enqueueQueuedOutboundLocked(this.owner.dataQueueInternal(), outboundFrame);
        streamRuntime.markOpeningFramePendingLocked();
        this.owner.markLocalStreamOpeningCommittedLocked(streamRuntime);
        this.owner.flushPendingPriorityUpdateLocked(streamRuntime);
    }

    int queueOpeningDataLocked(StreamRuntime streamRuntime,
                               byte[] prefix,
                               byte[][] parts,
                               int partIndex,
                               int partOffset,
                               int requestedLength,
                               boolean fin,
                               SessionRuntime.PayloadOwnership payloadOwnership,
                               StreamWriteCompletion completion) throws IOException {
        int length = this.prepareOpeningWriteFrameLocked(streamRuntime, prefix, requestedLength);
        boolean frameFin = fin && length == requestedLength;
        int flags = openingFrameFlags(prefix, frameFin);
        byte[][] payloadParts = this.owner.retainPayloadParts(parts, partIndex, partOffset, length, payloadOwnership);
        SessionRuntime.OutboundFrame outboundFrame;
        if (payloadParts.length <= 1) {
            byte[] payload = payloadParts.length == 0 ? StreamRuntime.EMPTY_BYTES : payloadParts[0];
            outboundFrame = new SessionRuntime.OutboundFrame(
                    new FrameCodec.Frame(FrameType.DATA, flags, streamRuntime.streamIdInternal(), prefix),
                    streamRuntime,
                    length,
                    true,
                    false,
                    completion,
                    prefix,
                    payload,
                    0,
                    payload.length,
                    null,
                    0,
                    0
            );
        } else {
            outboundFrame = new SessionRuntime.OutboundFrame(
                    new FrameCodec.Frame(FrameType.DATA, flags, streamRuntime.streamIdInternal(), prefix),
                    streamRuntime,
                    length,
                    true,
                    false,
                    completion,
                    prefix,
                    null,
                    0,
                    length,
                    payloadParts,
                    0,
                    0
            );
        }
        this.enqueueReservedOpeningDataLocked(streamRuntime, outboundFrame, length, frameFin);
        return length;
    }

    void markPeerVisibleLocked(StreamRuntime streamRuntime) throws IOException {
        if (streamRuntime == null || !streamRuntime.shouldMarkPeerVisibleLocked()) {
            return;
        }
        streamRuntime.markPeerVisibleLocked();
        this.owner.localOpenTrackerInternal().removeUnseenLocalLocked(streamRuntime);
        this.owner.enqueueStreamEventLocked(streamRuntime, ZmuxEventType.STREAM_OPENED, null);
        this.owner.flushPendingPriorityUpdateLocked(streamRuntime);
    }

    SessionRuntime.OutboundFrame removeQueuedOpeningFrameLocked(StreamRuntime streamRuntime) {
        SessionRuntime.OutboundFrame outboundFrame =
                this.removeQueuedOpeningFrameLocked(this.owner.urgentQueueInternal(), streamRuntime);
        if (outboundFrame != null) {
            return outboundFrame;
        }
        return this.removeQueuedOpeningFrameLocked(this.owner.dataQueueInternal(), streamRuntime);
    }

    SessionRuntime.OutboundFrame removeQueuedOpeningFrameLocked(Deque<SessionRuntime.OutboundFrame> deque,
                                                                StreamRuntime streamRuntime) {
        SessionRuntime.OutboundFrame first = deque.peekFirst();
        if (first != null && first.stream() == streamRuntime && first.openingFrame()) {
            deque.pollFirst();
            this.owner.onQueuedFrameDequeuedLocked(deque, first);
            return first;
        }
        SessionRuntime.OutboundFrame last = deque.peekLast();
        if (last != null && last.stream() == streamRuntime && last.openingFrame()) {
            deque.pollLast();
            this.owner.onQueuedFrameDequeuedLocked(deque, last);
            return last;
        }
        Iterator<SessionRuntime.OutboundFrame> iterator = deque.iterator();
        while (iterator.hasNext()) {
            SessionRuntime.OutboundFrame outboundFrame = iterator.next();
            if (outboundFrame.stream() != streamRuntime || !outboundFrame.openingFrame()) {
                continue;
            }
            iterator.remove();
            this.owner.onQueuedFrameDequeuedLocked(deque, outboundFrame);
            return outboundFrame;
        }
        return null;
    }

    boolean hasQueuedOpeningFrameLocked(StreamRuntime streamRuntime) {
        return SessionOpeningCoordinator.hasQueuedOpeningFrameLocked(this.owner.urgentQueueInternal(), streamRuntime)
                || SessionOpeningCoordinator.hasQueuedOpeningFrameLocked(this.owner.dataQueueInternal(), streamRuntime);
    }

    boolean hasInflightOpeningFrameLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null) {
            return false;
        }
        return hasOpeningFrameLocked(this.owner.inflightBatchInternal(), streamRuntime)
                || hasOpeningFrameLocked(this.owner.stagedOrdinaryBatchInternal(), streamRuntime);
    }

    private static boolean hasOpeningFrameLocked(List<SessionRuntime.OutboundFrame> batch, StreamRuntime streamRuntime) {
        for (SessionRuntime.OutboundFrame outboundFrame : batch) {
            if (outboundFrame.stream() == streamRuntime && outboundFrame.openingFrame()) {
                return true;
            }
        }
        return false;
    }

    /**
     * An urgent batch may carry a stream's opening frame (moved ahead of its BLOCKED or RESET, or an opening
     * ABORT) while lower-ID openers of the same class still wait in the queues. Those openers are pulled into
     * the batch so the writer emits them first: within a class the peer requires new stream IDs in
     * assignment order (SPEC §3.1). Frames the writer staged or has in flight are already ahead.
     */
    void pullEarlierOpeningFramesLocked(List<SessionRuntime.OutboundFrame> batch, boolean trackWriterHeld) {
        long[] highestOpenerByClass = null;
        for (SessionRuntime.OutboundFrame outboundFrame : batch) {
            if (!outboundFrame.openingFrame() || outboundFrame.stream() == null) {
                continue;
            }
            long streamId = outboundFrame.frame().streamId();
            if (highestOpenerByClass == null) {
                highestOpenerByClass = new long[]{-1L, -1L, -1L, -1L};
            }
            int streamClass = (int) (streamId & 3L);
            highestOpenerByClass[streamClass] = Math.max(highestOpenerByClass[streamClass], streamId);
        }
        if (highestOpenerByClass == null) {
            return;
        }
        this.pullEarlierOpeningFramesLocked(this.owner.urgentQueueInternal(), batch, highestOpenerByClass, trackWriterHeld);
        this.pullEarlierOpeningFramesLocked(this.owner.dataQueueInternal(), batch, highestOpenerByClass, trackWriterHeld);
    }

    private void pullEarlierOpeningFramesLocked(Deque<SessionRuntime.OutboundFrame> deque,
                                                List<SessionRuntime.OutboundFrame> batch,
                                                long[] highestOpenerByClass,
                                                boolean trackWriterHeld) {
        Iterator<SessionRuntime.OutboundFrame> iterator = deque.iterator();
        while (iterator.hasNext()) {
            SessionRuntime.OutboundFrame outboundFrame = iterator.next();
            if (!outboundFrame.openingFrame() || outboundFrame.stream() == null) {
                continue;
            }
            long streamId = outboundFrame.frame().streamId();
            if (streamId >= highestOpenerByClass[(int) (streamId & 3L)]) {
                continue;
            }
            iterator.remove();
            this.owner.onQueuedFrameDequeuedLocked(deque, outboundFrame);
            this.owner.addBatchFrameLocked(batch, outboundFrame, trackWriterHeld);
        }
    }

    /**
     * Replaces a not-yet-written opening DATA frame whose payload can no longer be sent (the send half was
     * reset or aborted) with a zero-length opener that keeps the OPEN_METADATA prefix, so the stream still
     * opens in its place in the writer order. The dropped payload is released like any filtered DATA.
     */
    SessionRuntime.OutboundFrame zeroLengthOpenerReplacementLocked(SessionRuntime.OutboundFrame openingFrame) {
        this.owner.failWriteCompletionLocked(openingFrame, null);
        this.owner.releaseWriterHeldFrameLocked(openingFrame);
        this.owner.releaseQueuedDataLocked(openingFrame);
        SessionRuntime.OutboundFrame replacement = this.zeroLengthOpeningFrameLocked(openingFrame, true);
        this.owner.retainWriterHeldFrameLocked(replacement);
        return replacement;
    }

    SessionRuntime.OutboundFrame zeroLengthOpeningFrameLocked(SessionRuntime.OutboundFrame openingFrame,
                                                             boolean preserveAfterSendClose) {
        byte[] prefix = openingFrame.payloadPrefix() == null ? StreamRuntime.EMPTY_BYTES : openingFrame.payloadPrefix();
        return this.newOpeningFrameLocked(openingFrame.stream(), prefix, false, 0, preserveAfterSendClose);
    }

    private SessionRuntime.OutboundFrame newOpeningFrameLocked(StreamRuntime streamRuntime,
                                                               byte[] openingPrefix,
                                                               boolean fin,
                                                               int dataBytes,
                                                               boolean preserveAfterSendClose) {
        int frameFlags = openingPrefix.length == 0 ? 0 : 0x20;
        if (fin) {
            frameFlags |= 0x40;
        }
        return new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.DATA, frameFlags, streamRuntime.streamIdInternal(), openingPrefix),
                streamRuntime,
                dataBytes,
                true,
                preserveAfterSendClose,
                openingPrefix,
                StreamRuntime.EMPTY_BYTES,
                0,
                0,
                null,
                0,
                0
        );
    }

    private void enqueueReservedOpeningDataLocked(StreamRuntime streamRuntime,
                                                  SessionRuntime.OutboundFrame outboundFrame,
                                                  int dataBytes,
                                                  boolean fin) throws IOException {
        boolean queued = false;
        try {
            this.owner.ensureStreamWriteQueueMemoryLocked(outboundFrame);
            streamRuntime.markLocalSendStartedLocked();
            if (fin) {
                streamRuntime.markFinQueuedLocked();
            }
            this.owner.enqueueQueuedOutboundLocked(this.owner.dataQueueInternal(), outboundFrame);
            outboundFrame.retainWriteCompletion();
            queued = true;
            streamRuntime.markOpeningFramePendingLocked();
            this.owner.markLocalStreamOpeningCommittedLocked(streamRuntime);
            this.owner.flushPendingPriorityUpdateLocked(streamRuntime);
        } catch (IOException error) {
            if (!queued) {
                this.owner.releaseReservedSendLocked(streamRuntime, dataBytes);
            }
            throw error;
        }
    }

    private int prepareOpeningWriteFrameLocked(StreamRuntime streamRuntime,
                                               byte[] openingPrefix,
                                               int payloadLength) throws IOException {
        if (!streamRuntime.idAssigned()) {
            // May wait for this stream's provisional turn; no stream ID is committed while waiting.
            this.owner.beginLocalOpenForWriteLocked(streamRuntime);
        }
        // The ID is committed from here on, so the opener must be queued without waiting.
        return this.owner.reserveOpeningSendLocked(streamRuntime, payloadLength, openingPrefix.length + 1);
    }

    private static int openingFrameFlags(byte[] openingPrefix, boolean fin) {
        int frameFlags = openingPrefix.length == 0 ? 0 : 0x20;
        if (fin) {
            frameFlags |= 0x40;
        }
        return frameFlags;
    }

    private void checkLocalOpenPossibleLocked(boolean bidirectional, int openInfoBytes) throws IOException {
        this.throwIfNewLocalOpenDisallowedLocked();
        this.checkLocalOpenCapacityLocked(bidirectional, openInfoBytes);
    }

    private void checkLocalOpenCapacityLocked(boolean bidirectional, int openInfoBytes) throws IOException {
        this.owner.localOpenTrackerInternal().reapExpiredProvisionalsLocked(
                bidirectional,
                System.nanoTime(),
                this.owner.provisionalOpenMaxAgeNanosLocked()
        );
        int provisionalCount = this.owner.localOpenTrackerInternal().provisionalCountLocked(bidirectional);
        if (provisionalCount >= this.owner.provisionalOpenHardCapLocked()) {
            this.owner.recordProvisionalOpenLimitedLocked();
            throw new OpenLimitedException(SessionRuntime.openLimitedError("zmux: provisional open hard cap reached"));
        }
        long openInfoBudget = this.owner.retainedOpenInfoBudgetLocked();
        if (openInfoBytes > 0
                && openInfoBudget > 0L
                && SessionRuntime.saturatingAdd(this.owner.retainedOpenInfoBytesLocked(), openInfoBytes) > openInfoBudget) {
            this.owner.recordProvisionalOpenLimitedLocked();
            throw new OpenLimitedException(SessionRuntime.openLimitedError("zmux: open_info budget exceeded"));
        }
        long additionalTrackedBytes = SessionRuntime.saturatingAdd(this.owner.retainedStateUnitLocked(), openInfoBytes);
        if (this.owner.projectedTrackedSessionMemoryWithAdditionalLocked(additionalTrackedBytes)
                > this.owner.sessionMemoryHardCapLocked()) {
            this.owner.recordProvisionalOpenLimitedLocked();
            throw new OpenLimitedException(SessionRuntime.openLimitedError("zmux: local open limited by session memory cap"));
        }
        long projectedStreamId = this.owner.localOpenTrackerInternal().projectedLocalOpenId(
                this.owner.nextLocalStreamIdLocked(bidirectional),
                provisionalCount
        );
        this.checkLocalOpenAllowedLocked(projectedStreamId, bidirectional, provisionalCount);
    }

    private IOException provisionalCommitErrorLocked(StreamRuntime streamRuntime, LocalOpenSurface surface) {
        if (this.owner.gracefulCloseActiveLocked()) {
            this.owner.localOpenTrackerInternal().failProvisionalLocked(
                    streamRuntime,
                    new ApplicationError(
                            io.zmux.ErrorCode.REFUSED_STREAM.code(),
                            "",
                            ZmuxErrorScope.STREAM,
                            ZmuxErrorSource.LOCAL,
                            ZmuxErrorDirection.BOTH,
                            ZmuxTerminationKind.SESSION_TERMINATION
                    ),
                    false
            );
            return streamRuntime.operationErrorLocked();
        }
        if (this.owner.shouldFailSessionOperationsLocked()) {
            return this.owner.sessionOperationErrorLocked(surface.operation, this.owner.currentErrorLocked());
        }
        return null;
    }

    private void throwIfNewLocalOpenDisallowedLocked() throws IOException {
        if (this.owner.gracefulCloseActiveLocked() || this.owner.shouldFailSessionOperationsLocked()) {
            throw this.owner.sessionOperationErrorLocked("open", this.owner.currentErrorLocked());
        }
    }

    private void checkLocalOpenAllowedLocked(long projectedStreamId, boolean bidirectional, int provisionalAhead)
            throws IOException {
        if (projectedStreamId > 0x3FFFFFFFFFFFFFFFL) {
            // A local limit, not a peer violation: no ID is wrapped or reused and nothing reaches the wire.
            this.owner.onLocalStreamIdsExhaustedLocked();
            throw new OpenLimitedException(SessionRuntime.openLimitedError("zmux: local stream ID space exhausted"));
        }
        long peerGoAwayWatermark = this.owner.peerGoAwayWatermarkLocked(bidirectional);
        if (projectedStreamId > peerGoAwayWatermark) {
            throw refusedLocalOpenError(ZmuxTerminationKind.GRACEFUL);
        }
        long peerIncomingLimit = bidirectional
                ? this.owner.peerSettings().maxIncomingStreamsBidi()
                : this.owner.peerSettings().maxIncomingStreamsUni();
        long activeLocalCount = this.owner.streamBookkeepingInternal().activeLocalCountLocked(bidirectional);
        if (activeLocalCount >= peerIncomingLimit
                || SessionRuntime.saturatingAdd(activeLocalCount, provisionalAhead) >= peerIncomingLimit) {
            throw refusedLocalOpenError(ZmuxTerminationKind.UNKNOWN);
        }
    }

    private enum LocalOpenSurface {
        OPEN(
                "open",
                ZmuxErrorScope.SESSION,
                ZmuxErrorDirection.BOTH,
                "zmux: interrupted while waiting for open turn"
        ),
        WRITE(
                "write",
                ZmuxErrorScope.STREAM,
                ZmuxErrorDirection.WRITE,
                "zmux: interrupted while writing stream"
        ),
        CLOSE(
                "close",
                ZmuxErrorScope.STREAM,
                ZmuxErrorDirection.BOTH,
                "zmux: interrupted while closing stream"
        );

        private final String operation;
        private final ZmuxErrorScope scope;
        private final ZmuxErrorDirection direction;
        private final String interruptedMessage;

        LocalOpenSurface(String operation,
                         ZmuxErrorScope scope,
                         ZmuxErrorDirection direction,
                         String interruptedMessage) {
            this.operation = operation;
            this.scope = scope;
            this.direction = direction;
            this.interruptedMessage = interruptedMessage;
        }

        IOException interruptedIOException(InterruptedException cause) {
            return SessionRuntime.interruptedIo(
                    this.interruptedMessage,
                    this.operation,
                    this.scope,
                    this.direction,
                    cause
            );
        }

        boolean directOpenSurface() {
            return this == OPEN;
        }
    }

    private enum OpenTurnWaitKind {
        RETRY,
        UNBOUNDED,
        TIMED,
        WRITE_TIMEOUT
    }

    private static final class OpenTurnWait {
        private final OpenTurnWaitKind kind;
        private final long waitNanos;

        private OpenTurnWait(OpenTurnWaitKind kind, long waitNanos) {
            this.kind = kind;
            this.waitNanos = waitNanos;
        }

        static OpenTurnWait retry() {
            return new OpenTurnWait(OpenTurnWaitKind.RETRY, 0L);
        }

        static OpenTurnWait unbounded() {
            return new OpenTurnWait(OpenTurnWaitKind.UNBOUNDED, 0L);
        }

        static OpenTurnWait timed(long waitNanos) {
            return new OpenTurnWait(OpenTurnWaitKind.TIMED, Math.max(1L, waitNanos));
        }

        static OpenTurnWait writeTimeout() {
            return new OpenTurnWait(OpenTurnWaitKind.WRITE_TIMEOUT, 0L);
        }

        OpenTurnWaitKind kind() {
            return kind;
        }

        long waitNanos() {
            return waitNanos;
        }
    }
}
