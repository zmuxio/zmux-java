package io.zmux.internal;

import io.zmux.*;

import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("resource")
final class SessionPriorityUpdateCoordinator {
    private final SessionRuntime owner;

    SessionPriorityUpdateCoordinator(SessionRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    private static IOException priorityUpdateWriteError(ErrorCode code, String message, Throwable cause) {
        return new ZmuxException(
                code.code(),
                "write",
                message,
                cause,
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.WRITE,
                ZmuxTerminationKind.UNKNOWN
        );
    }

    private static boolean queueContainsStreamDataLocked(Deque<SessionRuntime.OutboundFrame> deque, StreamRuntime streamRuntime) {
        for (SessionRuntime.OutboundFrame outboundFrame : deque) {
            if (outboundFrame.stream() == streamRuntime && outboundFrame.frame().type() == FrameType.DATA) {
                return true;
            }
        }
        return false;
    }

    private static long replaceBucket(long currentBucket, long oldBytes, long newBytes) {
        long projected = currentBucket;
        if (oldBytes > 0L) {
            projected = Math.max(0L, projected - oldBytes);
        }
        return SessionRuntime.saturatingAdd(projected, newBytes);
    }

    void enqueuePriorityUpdateLocked(StreamRuntime streamRuntime, Long priority, Long group) throws IOException {
        if (streamRuntime == null) {
            return;
        }
        Long nextPriority = priority != null ? priority : streamRuntime.pendingPriorityUpdatePriorityLocked();
        Long nextGroup = group != null || streamRuntime.pendingPriorityUpdateGroupLocked() == null
                ? group
                : streamRuntime.pendingPriorityUpdateGroupLocked();
        byte[] payload = this.reuseOrBuildPriorityUpdatePayloadLocked(streamRuntime, nextPriority, nextGroup);
        long oldBytes = this.pendingPriorityBytesForLocked(streamRuntime);
        SessionOutboundQueueBookkeeping.PendingPriorityReplaceResult replaceResult =
                this.owner.replacePendingPriorityBytesLocked(oldBytes, payload.length);
        if (replaceResult != SessionOutboundQueueBookkeeping.PendingPriorityReplaceResult.ACCEPTED) {
            this.owner.recordDroppedLocalPriorityUpdateLocked();
            if (replaceResult == SessionOutboundQueueBookkeeping.PendingPriorityReplaceResult.DROPPED_MEMORY) {
                throw this.priorityUpdateQueueMemoryCapErrorLocked(oldBytes, payload.length);
            }
            throw priorityUpdateWriteError(ErrorCode.INTERNAL, "pending priority update budget exceeded", null);
        }
        streamRuntime.stagePriorityUpdateLocked(priority, group, payload);
        if (!this.canRetainPriorityUpdateLocked(streamRuntime)) {
            this.discardPendingPriorityUpdateLocked(streamRuntime);
            return;
        }
        if (!streamRuntime.canTakePendingPriorityUpdateLocked()) {
            this.removeQueuedPriorityUpdateLocked(streamRuntime);
            return;
        }
        boolean openerPending = streamRuntime.openingFramePendingLocked();
        boolean committedStreamData = this.hasCommittedStreamDataLocked(streamRuntime);
        if (streamRuntime.priorityUpdateQueuedLocked() && !openerPending && !committedStreamData) {
            return;
        }
        this.removeQueuedPriorityUpdateLocked(streamRuntime);
        if (openerPending || committedStreamData) {
            SessionRuntime.OutboundFrame outboundFrame = this.takePendingPriorityUpdateForBatchLocked(streamRuntime);
            if (outboundFrame != null) {
                this.owner.orderStreamControlLocked(outboundFrame);
            }
        } else {
            this.owner.advisoryQueueInternal().offerLast(streamRuntime);
            streamRuntime.markPriorityUpdateQueuedLocked();
        }
        this.owner.notifyWriterWaitersLocked();
    }

    void discardPendingPriorityUpdateLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null) {
            return;
        }
        boolean hadPending = streamRuntime.hasPendingPriorityUpdateLocked() || streamRuntime.priorityUpdateQueuedLocked();
        this.removeQueuedPriorityUpdateLocked(streamRuntime);
        this.clearPendingPriorityUpdateRetainedLocked(streamRuntime, true);
        if (hadPending) {
            this.owner.recordDroppedLocalPriorityUpdateLocked();
        }
    }

    boolean shouldEmitPriorityUpdateLocked(StreamRuntime streamRuntime) {
        return this.canRetainPriorityUpdateLocked(streamRuntime)
                && streamRuntime.canTakePendingPriorityUpdateLocked();
    }

    private SessionRuntime.OutboundFrame newPriorityUpdateFrameLocked(StreamRuntime streamRuntime, byte[] payload) {
        return new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(
                        FrameType.EXT,
                        0,
                        streamRuntime.streamIdInternal(),
                        payload
                ),
                streamRuntime,
                0,
                false,
                false
        );
    }

    SessionRuntime.OutboundFrame takePendingPriorityUpdateForBatchLocked(StreamRuntime streamRuntime) throws IOException {
        if (streamRuntime == null || !streamRuntime.hasPendingPriorityUpdateLocked()) {
            return null;
        }
        if (!this.shouldEmitPriorityUpdateLocked(streamRuntime)) {
            this.discardPendingPriorityUpdateLocked(streamRuntime);
            return null;
        }
        this.removeQueuedPriorityUpdateLocked(streamRuntime);
        SessionRuntime.OutboundFrame outboundFrame = this.newPriorityUpdateFrameLocked(
                streamRuntime,
                streamRuntime.pendingPriorityUpdatePayloadLocked()
        );
        if (this.priorityUpdateHandoffWouldExceedMemoryCapLocked(streamRuntime, outboundFrame)) {
            this.discardPendingPriorityUpdateLocked(streamRuntime);
            return null;
        }
        this.clearPendingPriorityUpdateRetainedLocked(streamRuntime, false);
        return outboundFrame;
    }

    private void clearPendingPriorityUpdateRetainedLocked(StreamRuntime streamRuntime, boolean notifyMemoryRelease) {
        if (streamRuntime == null) {
            return;
        }
        long retainedBytes = this.pendingPriorityBytesForLocked(streamRuntime);
        if (notifyMemoryRelease) {
            this.owner.outboundQueueBookkeepingInternal().releasePendingPriorityBytesAndNotifyLocked(retainedBytes);
        } else {
            this.owner.outboundQueueBookkeepingInternal().releasePendingPriorityBytesLocked(retainedBytes);
        }
        streamRuntime.clearPendingPriorityUpdateLocked();
    }

    void flushPendingPriorityUpdateLocked(StreamRuntime streamRuntime) throws IOException {
        if (streamRuntime == null
                || !streamRuntime.hasPendingPriorityUpdateLocked()
                || !streamRuntime.canTakePendingPriorityUpdateLocked()) {
            return;
        }
        if (streamRuntime.priorityUpdateQueuedLocked()) {
            return;
        }
        if (!this.shouldEmitPriorityUpdateLocked(streamRuntime)) {
            this.discardPendingPriorityUpdateLocked(streamRuntime);
            return;
        }
        this.enqueuePriorityUpdateLocked(
                streamRuntime,
                streamRuntime.pendingPriorityUpdatePriorityLocked(),
                streamRuntime.pendingPriorityUpdateGroupLocked()
        );
    }

    void discardPendingPriorityQueueLocked() {
        StreamRuntime streamRuntime;
        boolean drained = false;
        while ((streamRuntime = this.owner.advisoryQueueInternal().pollFirst()) != null) {
            this.clearPendingPriorityUpdateRetainedLocked(streamRuntime, true);
            streamRuntime.clearPriorityUpdateQueuedLocked();
            drained = true;
        }
        if (drained) {
            this.owner.releaseEmptyAdvisoryQueueStorageLocked();
        }
    }

    long pendingPriorityBytesForLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null || !streamRuntime.hasPendingPriorityUpdateLocked()) {
            return 0L;
        }
        return streamRuntime.pendingPriorityUpdatePayloadLocked().length;
    }

    byte[] buildPriorityUpdatePayloadLocked(Long priority, Long group) throws IOException {
        if (priority == null && group == null) {
            throw new EmptyMetadataUpdateException();
        }
        if (priority != null) {
            if (!Protocol.canCarryPriorityInUpdate(this.owner.capabilities())) {
                throw new PriorityUpdateUnavailableException();
            }
        }
        if (group != null) {
            if (!Protocol.canCarryGroupInUpdate(this.owner.capabilities())) {
                throw new PriorityUpdateUnavailableException();
            }
        }
        try {
            return FrameCodec.buildPriorityUpdatePayload(
                    this.owner.capabilities(),
                    priority,
                    group,
                    this.owner.extensionPayloadLimitLocked()
            );
        } catch (PriorityUpdateTooLargeException error) {
            throw new PriorityUpdateTooLargeException(
                    "write",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorSource.LOCAL,
                    ZmuxErrorDirection.WRITE,
                    error
            );
        } catch (ZmuxException error) {
            throw priorityUpdateWriteError(ErrorCode.PROTOCOL, error.getMessage(), error);
        }
    }

    void removeQueuedPriorityUpdateLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null) {
            return;
        }
        this.removeQueuedPriorityUpdateLocked(this.owner.urgentQueueInternal(), streamRuntime);
        this.removeQueuedPriorityUpdateLocked(this.owner.dataQueueInternal(), streamRuntime);
        this.removeQueuedPriorityUpdateAdvisoryLocked(streamRuntime);
        streamRuntime.clearPriorityUpdateQueuedLocked();
    }

    private void removeQueuedPriorityUpdateLocked(Deque<SessionRuntime.OutboundFrame> deque, StreamRuntime streamRuntime) {
        SessionRuntime.OutboundFrame first = deque.peekFirst();
        if (first != null && first.stream() == streamRuntime && first.frame().type() == FrameType.EXT) {
            deque.pollFirst();
            this.owner.onQueuedFrameDequeuedLocked(deque, first);
            return;
        }
        SessionRuntime.OutboundFrame last = deque.peekLast();
        if (last != null && last.stream() == streamRuntime && last.frame().type() == FrameType.EXT) {
            deque.pollLast();
            this.owner.onQueuedFrameDequeuedLocked(deque, last);
            return;
        }
        Iterator<SessionRuntime.OutboundFrame> iterator = deque.iterator();
        while (iterator.hasNext()) {
            SessionRuntime.OutboundFrame outboundFrame = iterator.next();
            if (outboundFrame.stream() != streamRuntime || outboundFrame.frame().type() != FrameType.EXT) {
                continue;
            }
            this.owner.onQueuedFrameDequeuedLocked(deque, outboundFrame);
            iterator.remove();
        }
    }

    private void removeQueuedPriorityUpdateAdvisoryLocked(StreamRuntime streamRuntime) {
        Deque<StreamRuntime> advisoryQueue = this.owner.advisoryQueueInternal();
        if (advisoryQueue.peekFirst() == streamRuntime) {
            advisoryQueue.pollFirst();
            this.owner.releaseEmptyAdvisoryQueueStorageLocked();
            return;
        }
        if (advisoryQueue.peekLast() == streamRuntime) {
            advisoryQueue.pollLast();
            this.owner.releaseEmptyAdvisoryQueueStorageLocked();
            return;
        }
        Iterator<StreamRuntime> iterator = advisoryQueue.iterator();
        boolean removed = false;
        while (iterator.hasNext()) {
            StreamRuntime queuedStream = iterator.next();
            if (queuedStream != streamRuntime) {
                continue;
            }
            iterator.remove();
            removed = true;
        }
        if (removed) {
            this.owner.releaseEmptyAdvisoryQueueStorageLocked();
        }
    }

    private boolean hasCommittedStreamDataLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null) {
            return false;
        }
        if (queueContainsStreamDataLocked(this.owner.urgentQueueInternal(), streamRuntime)
                || queueContainsStreamDataLocked(this.owner.dataQueueInternal(), streamRuntime)) {
            return true;
        }
        List<SessionRuntime.OutboundFrame> inflightBatch = this.owner.inflightBatchInternal();
        for (SessionRuntime.OutboundFrame outboundFrame : inflightBatch) {
            if (outboundFrame.stream() == streamRuntime && outboundFrame.frame().type() == FrameType.DATA) {
                return true;
            }
        }
        return false;
    }

    private boolean canRetainPriorityUpdateLocked(StreamRuntime streamRuntime) {
        return streamRuntime != null
                && Protocol.supportsPriorityUpdate(this.owner.capabilities())
                && this.owner.allowLocalNonCloseControlLocked()
                && streamRuntime.localSend()
                && !streamRuntime.sendTerminalLocked();
    }

    private byte[] reuseOrBuildPriorityUpdatePayloadLocked(StreamRuntime streamRuntime,
                                                           Long priority,
                                                           Long group) throws IOException {
        if (streamRuntime != null
                && streamRuntime.hasPendingPriorityUpdateLocked()
                && Objects.equals(priority, streamRuntime.pendingPriorityUpdatePriorityLocked())
                && Objects.equals(group, streamRuntime.pendingPriorityUpdateGroupLocked())) {
            byte[] existing = streamRuntime.pendingPriorityUpdatePayloadLocked();
            if (existing.length > 0) {
                return existing;
            }
        }
        return this.buildPriorityUpdatePayloadLocked(priority, group);
    }

    private IOException priorityUpdateQueueMemoryCapErrorLocked(long oldBytes, long newBytes) {
        long currentPending = this.owner.outboundQueueBookkeepingInternal().pendingPriorityBytesLocked();
        long projectedPending = replaceBucket(currentPending, oldBytes, newBytes);
        long tracked = this.owner.trackedSessionMemoryLocked();
        if (currentPending > 0L) {
            tracked = Math.max(0L, tracked - currentPending);
        }
        tracked = SessionRuntime.saturatingAdd(tracked, projectedPending);
        return priorityUpdateWriteError(
                ErrorCode.INTERNAL,
                "session memory cap exceeded: tracked=" + tracked + " cap=" + this.owner.sessionMemoryHardCapLocked(),
                null
        );
    }

    private boolean priorityUpdateHandoffWouldExceedMemoryCapLocked(
            StreamRuntime streamRuntime,
            SessionRuntime.OutboundFrame outboundFrame
    ) {
        long removedPendingBytes = this.pendingPriorityBytesForLocked(streamRuntime);
        long addedQueuedBytes = SessionRuntime.retainedQueueBytes(outboundFrame);
        if (addedQueuedBytes <= removedPendingBytes) {
            return false;
        }
        long projectedTracked = this.owner.trackedSessionMemoryLocked();
        if (removedPendingBytes > 0L) {
            projectedTracked = Math.max(0L, projectedTracked - removedPendingBytes);
        }
        projectedTracked = SessionRuntime.saturatingAdd(projectedTracked, addedQueuedBytes);
        return projectedTracked > this.owner.sessionMemoryHardCapLocked();
    }
}
