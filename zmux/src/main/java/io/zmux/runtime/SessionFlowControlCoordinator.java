package io.zmux.runtime;

import io.zmux.protocol.Frame;
import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

final class SessionFlowControlCoordinator {
    private final SessionRuntime owner;
    private final ArrayList<SessionRuntime.OutboundFrame> flushPendingWindowUpdateBatch = new ArrayList<>(8);
    private final PendingWindowUpdateBatchCollector pendingWindowUpdateCollector =
            new PendingWindowUpdateBatchCollector();

    SessionFlowControlCoordinator(SessionRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    boolean queueSessionMaxDataLocked(long desiredOffset) {
        return this.owner.flowControlUpdateRegistryInternal().queueSessionMaxDataLocked(desiredOffset);
    }

    boolean queueStreamMaxDataLocked(long streamId, long desiredOffset) {
        return this.owner.flowControlUpdateRegistryInternal().queueStreamMaxDataLocked(streamId, desiredOffset);
    }

    boolean hasPendingWindowUpdatesLocked() {
        return this.owner.flowControlUpdateRegistryInternal().hasPendingWindowUpdatesLocked();
    }

    boolean hasPendingMaxDataLocked() {
        return this.owner.flowControlUpdateRegistryInternal().hasPendingMaxDataLocked();
    }

    boolean flushPendingWindowUpdatesLocked(Long preferredStreamId) throws java.io.IOException {
        ArrayList<SessionRuntime.OutboundFrame> pending = this.flushPendingWindowUpdateBatch;
        pending.clear();
        PendingWindowUpdateBatchCollector collector = this.pendingWindowUpdateCollector.reset(
                pending,
                this.owner.outboundQueueBookkeepingInternal().urgentQueuedControlBytesLocked(),
                this.owner.urgentQueuedBytesHardCapLocked(),
                false
        );
        try {
            this.owner.flowControlUpdateRegistryInternal().takePendingWindowUpdatesLocked(
                    collector,
                    32,
                    preferredStreamId
            );
            if (collector.retainedBytes() > 0L) {
                IOException memoryError = this.owner.urgentControlWriterBatchMemoryErrorLocked(collector.retainedBytes());
                if (memoryError != null) {
                    this.owner.recordProtocolBacklogBlockedLocked();
                    this.owner.failSession(memoryError);
                    throw memoryError;
                }
            }
            for (SessionRuntime.OutboundFrame outboundFrame : pending) {
                this.owner.orderStreamControlLocked(outboundFrame);
            }
            return !pending.isEmpty();
        } finally {
            pending.clear();
            collector.clear();
        }
    }

    void appendPendingWindowUpdatesLocked(List<SessionRuntime.OutboundFrame> batch,
                                          int maxFrames,
                                          Long preferredStreamId,
                                          boolean trackWriterHeld) throws IOException {
        if (batch == null || maxFrames <= 0 || batch.size() >= maxFrames) {
            return;
        }
        int previousSize = batch.size();
        PendingWindowUpdateBatchCollector collector = this.pendingWindowUpdateCollector.reset(
                batch,
                SessionRuntime.retainedUrgentControlBytes(batch),
                this.owner.urgentQueuedBytesHardCapLocked(),
                true
        );
        try {
            this.owner.flowControlUpdateRegistryInternal().takePendingWindowUpdatesLocked(
                    collector,
                    maxFrames - previousSize,
                    preferredStreamId
            );
            if (trackWriterHeld && collector.retainedBytes() > 0L) {
                IOException memoryError = this.owner.urgentControlWriterBatchMemoryErrorLocked(collector.retainedBytes());
                if (memoryError != null) {
                    this.owner.recordProtocolBacklogBlockedLocked();
                    this.owner.failSession(memoryError);
                    throw memoryError;
                }
            }
            if (trackWriterHeld) {
                for (int i = previousSize; i < batch.size(); ++i) {
                    this.owner.retainWriterHeldFrameLocked(batch.get(i));
                }
            }
        } finally {
            collector.clear();
        }
    }

    private int flowControlPendingFrameRetainedBytes(long value) {
        long bytes = SessionRuntime.saturatingAdd(1L, this.owner.pendingControlValueBytesLocked(value));
        return bytes >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) bytes;
    }

    private SessionRuntime.OutboundFrame flowControlPendingFrame(FrameType type, long streamId, long value) {
        switch (type) {
            case MAX_DATA:
                return this.maxDataFrame(streamId, value);
            case BLOCKED:
                return this.blockedFrame(streamId, value);
            default:
                throw new IllegalStateException("unexpected flow-control pending frame: " + type);
        }
    }

    private SessionRuntime.OutboundFrame maxDataFrame(long streamId, long limit) {
        return new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.MAX_DATA, 0, streamId, this.owner.encodeVarint(limit)),
                this.owner.liveStreamLocked(streamId),
                0,
                false,
                false
        );
    }

    private SessionRuntime.OutboundFrame blockedFrame(long streamId, long offset) {
        return new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.BLOCKED, 0, streamId, this.owner.encodeVarint(offset)),
                this.owner.liveStreamLocked(streamId),
                0,
                false,
                false
        );
    }

    private final class PendingWindowUpdateBatchCollector implements SessionFlowControlUpdateRegistry.PendingFrameSink {
        private List<SessionRuntime.OutboundFrame> batch;
        private long initialUrgentControlBytes;
        private long urgentLaneCap;
        private boolean allowFirstOverCap;
        private long retainedBytes;
        private int accepted;

        private PendingWindowUpdateBatchCollector reset(List<SessionRuntime.OutboundFrame> batch,
                                                        long initialUrgentControlBytes,
                                                        long urgentLaneCap,
                                                        boolean allowFirstOverCap) {
            this.batch = batch;
            this.initialUrgentControlBytes = Math.max(0L, initialUrgentControlBytes);
            this.urgentLaneCap = Math.max(0L, urgentLaneCap);
            this.allowFirstOverCap = allowFirstOverCap;
            this.retainedBytes = 0L;
            this.accepted = 0;
            return this;
        }

        private void clear() {
            this.batch = null;
            this.initialUrgentControlBytes = 0L;
            this.urgentLaneCap = 0L;
            this.allowFirstOverCap = false;
            this.retainedBytes = 0L;
            this.accepted = 0;
        }

        @Override
        public boolean accept(FrameType type, long streamId, long value) {
            int retainedBytes = SessionFlowControlCoordinator.this.flowControlPendingFrameRetainedBytes(value);
            if (!this.canAccept(retainedBytes)) {
                return false;
            }
            this.batch.add(SessionFlowControlCoordinator.this.flowControlPendingFrame(type, streamId, value));
            this.retainedBytes = SessionRuntime.saturatingAdd(this.retainedBytes, retainedBytes);
            ++this.accepted;
            return true;
        }

        private boolean canAccept(int bytes) {
            if (bytes <= 0 || this.urgentLaneCap <= 0L) {
                return true;
            }
            long current = SessionRuntime.saturatingAdd(this.initialUrgentControlBytes, this.retainedBytes);
            if (this.allowFirstOverCap && current == 0L && this.accepted == 0) {
                return true;
            }
            return SessionRuntime.saturatingAdd(current, bytes) <= this.urgentLaneCap;
        }

        private long retainedBytes() {
            return this.retainedBytes;
        }
    }
}
