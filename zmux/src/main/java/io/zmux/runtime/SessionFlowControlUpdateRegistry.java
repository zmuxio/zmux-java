package io.zmux.runtime;

import io.zmux.protocol.FrameType;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("resource")
final class SessionFlowControlUpdateRegistry {
    private final Owner owner;
    private final LongLongSortedMap pendingStreamMaxData = new LongLongSortedMap();
    private final LongLongSortedMap streamBlockedOffsets = new LongLongSortedMap();
    private long pendingSessionMaxData;
    private boolean pendingSessionMaxDataSet;
    private long sessionBlockedAt = -1L;
    private long sessionBlockedOffset = -1L;

    SessionFlowControlUpdateRegistry(Owner owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    boolean queueSessionMaxDataLocked(long desiredOffset) {
        if (!this.ensurePendingNonCloseControlLocked()) {
            return false;
        }
        desiredOffset = SessionRuntime.clampVarint62(desiredOffset);
        if (this.pendingSessionMaxDataSet && desiredOffset <= this.pendingSessionMaxData) {
            return true;
        }
        long oldBytes = this.pendingControlBytesIfPresentLocked(
                0L,
                this.pendingSessionMaxDataSet,
                this.pendingSessionMaxData
        );
        long newBytes = this.pendingControlBytesLocked(0L, desiredOffset);
        if (!this.owner.replacePendingControlBytesLocked(oldBytes, newBytes)) {
            return false;
        }
        this.pendingSessionMaxData = desiredOffset;
        this.pendingSessionMaxDataSet = true;
        return true;
    }

    boolean queueStreamMaxDataLocked(long streamId, long desiredOffset) {
        if (!this.ensurePendingNonCloseControlLocked()) {
            return false;
        }
        desiredOffset = SessionRuntime.clampVarint62(desiredOffset);
        int previousIndex = this.pendingStreamMaxData.indexOf(streamId);
        long previous = previousIndex >= 0 ? this.pendingStreamMaxData.valueAt(previousIndex) : -1L;
        if (previousIndex >= 0 && desiredOffset <= previous) {
            return true;
        }
        long oldBytes = this.pendingControlBytesIfPresentLocked(streamId, previousIndex >= 0, previous);
        long newBytes = this.pendingControlBytesLocked(streamId, desiredOffset);
        if (!this.owner.replacePendingControlBytesLocked(oldBytes, newBytes)) {
            return false;
        }
        this.pendingStreamMaxData.put(streamId, desiredOffset);
        return true;
    }

    boolean queueBlockedFrameLocked(long streamId, long offset) {
        if (!this.ensurePendingNonCloseControlLocked()) {
            return false;
        }
        if (streamId == 0L) {
            if (this.sessionBlockedOffset == offset || this.sessionBlockedAt == offset) {
                return false;
            }
            long oldBytes = this.pendingControlBytesIfPresentLocked(
                    0L,
                    this.sessionBlockedOffset >= 0L,
                    this.sessionBlockedOffset
            );
            long newBytes = this.pendingControlBytesLocked(0L, offset);
            if (!this.owner.replacePendingControlBytesLocked(oldBytes, newBytes)) {
                return false;
            }
            this.sessionBlockedOffset = offset;
            return true;
        }

        StreamRuntime streamRuntime = this.owner.liveStreamLocked(streamId);
        if (streamRuntime != null && streamRuntime.blockedAtLocked() == offset) {
            return false;
        }
        int previousIndex = this.streamBlockedOffsets.indexOf(streamId);
        long previous = previousIndex >= 0 ? this.streamBlockedOffsets.valueAt(previousIndex) : -1L;
        if (previousIndex >= 0 && previous == offset) {
            return false;
        }
        long oldBytes = this.pendingControlBytesIfPresentLocked(streamId, previousIndex >= 0, previous);
        long newBytes = this.pendingControlBytesLocked(streamId, offset);
        if (!this.owner.replacePendingControlBytesLocked(oldBytes, newBytes)) {
            return false;
        }
        this.streamBlockedOffsets.put(streamId, offset);
        if (streamRuntime != null) {
            streamRuntime.markBlockedQueuedLocked(offset);
        }
        return true;
    }

    void clearBlockedFrameLocked(long streamId) {
        if (streamId == 0L) {
            if (this.sessionBlockedOffset >= 0L) {
                this.owner.releasePendingControlBytesLocked(
                        this.pendingControlBytesLocked(0L, this.sessionBlockedOffset)
                );
            }
            this.sessionBlockedAt = -1L;
            this.sessionBlockedOffset = -1L;
            return;
        }

        int removedIndex = this.streamBlockedOffsets.indexOf(streamId);
        if (removedIndex >= 0) {
            long removed = this.streamBlockedOffsets.valueAt(removedIndex);
            this.streamBlockedOffsets.removeAt(removedIndex);
            this.owner.releasePendingControlBytesLocked(this.pendingControlBytesLocked(streamId, removed));
        }
        StreamRuntime streamRuntime = this.owner.liveStreamLocked(streamId);
        if (streamRuntime != null) {
            streamRuntime.clearBlockedLocked();
        }
    }

    void clearStreamStateLocked(long streamId) {
        int pendingMaxDataIndex = this.pendingStreamMaxData.indexOf(streamId);
        if (pendingMaxDataIndex >= 0) {
            long pendingMaxData = this.pendingStreamMaxData.valueAt(pendingMaxDataIndex);
            this.pendingStreamMaxData.removeAt(pendingMaxDataIndex);
            this.owner.releasePendingControlBytesLocked(this.pendingControlBytesLocked(streamId, pendingMaxData));
        }
        this.clearBlockedFrameLocked(streamId);
    }

    boolean hasPendingWindowUpdatesLocked() {
        return this.pendingSessionMaxDataSet
                || !this.pendingStreamMaxData.isEmpty()
                || this.sessionBlockedOffset >= 0L
                || !this.streamBlockedOffsets.isEmpty();
    }

    boolean hasPendingMaxDataLocked() {
        return this.pendingSessionMaxDataSet || !this.pendingStreamMaxData.isEmpty();
    }

    void takePendingWindowUpdatesLocked(List<PendingFrame> batch, int maxFrames, Long preferredStreamId) {
        if (batch == null || maxFrames <= 0 || batch.size() >= maxFrames) {
            return;
        }
        this.takePendingWindowUpdatesLocked((type, streamId, value) -> {
            batch.add(new PendingFrame(type, streamId, value));
            return true;
        }, maxFrames - batch.size(), preferredStreamId);
    }

    void takePendingWindowUpdatesLocked(PendingFrameSink sink, int maxFrames, Long preferredStreamId) {
        if (sink == null || maxFrames <= 0) {
            return;
        }
        PendingDrain drain = new PendingDrain(sink, maxFrames);
        if (this.pendingSessionMaxDataSet) {
            long value = this.pendingSessionMaxData;
            if (!drain.accept(FrameType.MAX_DATA, 0L, value)) {
                return;
            }
            this.pendingSessionMaxDataSet = false;
            this.owner.releasePendingControlBytesForHandoffLocked(this.pendingControlBytesLocked(0L, value));
            if (drain.full()) {
                return;
            }
        }
        this.drainPendingStreamMaxDataLocked(drain, preferredStreamId);
        if (drain.full()) {
            return;
        }
        if (this.sessionBlockedOffset >= 0L) {
            long offset = this.sessionBlockedOffset;
            if (!drain.accept(FrameType.BLOCKED, 0L, offset)) {
                return;
            }
            this.sessionBlockedAt = offset;
            this.sessionBlockedOffset = -1L;
            this.owner.releasePendingControlBytesForHandoffLocked(this.pendingControlBytesLocked(0L, offset));
            if (drain.full()) {
                return;
            }
        }
        this.drainPendingStreamBlockedLocked(drain, preferredStreamId);
    }

    void clear() {
        long releaseBytes = 0L;
        if (this.pendingSessionMaxDataSet) {
            releaseBytes = SessionRuntime.saturatingAdd(
                    releaseBytes,
                    this.pendingControlBytesLocked(0L, this.pendingSessionMaxData)
            );
        }
        for (int i = 0; i < this.pendingStreamMaxData.size(); ++i) {
            long streamId = this.pendingStreamMaxData.keyAt(i);
            long value = this.pendingStreamMaxData.valueAt(i);
            releaseBytes = SessionRuntime.saturatingAdd(
                    releaseBytes,
                    this.pendingControlBytesLocked(streamId, value)
            );
        }
        if (this.sessionBlockedOffset >= 0L) {
            releaseBytes = SessionRuntime.saturatingAdd(
                    releaseBytes,
                    this.pendingControlBytesLocked(0L, this.sessionBlockedOffset)
            );
        }
        for (int i = 0; i < this.streamBlockedOffsets.size(); ++i) {
            long streamId = this.streamBlockedOffsets.keyAt(i);
            long value = this.streamBlockedOffsets.valueAt(i);
            releaseBytes = SessionRuntime.saturatingAdd(
                    releaseBytes,
                    this.pendingControlBytesLocked(streamId, value)
            );
            StreamRuntime streamRuntime = this.owner.liveStreamLocked(streamId);
            if (streamRuntime != null) {
                streamRuntime.clearBlockedLocked();
            }
        }
        this.owner.releasePendingControlBytesLocked(releaseBytes);
        this.pendingStreamMaxData.clear();
        this.streamBlockedOffsets.clear();
        this.pendingSessionMaxData = 0L;
        this.pendingSessionMaxDataSet = false;
        this.sessionBlockedAt = -1L;
        this.sessionBlockedOffset = -1L;
    }

    private void takePendingStreamMaxDataLocked(PendingDrain drain, long streamId) {
        if (drain.full()) {
            return;
        }
        int index = this.pendingStreamMaxData.indexOf(streamId);
        if (index < 0) {
            return;
        }
        long value = this.pendingStreamMaxData.valueAt(index);
        StreamRuntime streamRuntime = this.owner.liveStreamLocked(streamId);
        if (streamRuntime != null && streamRuntime.shouldFlushPendingStreamMaxDataLocked()) {
            if (!drain.accept(FrameType.MAX_DATA, streamId, value)) {
                return;
            }
            this.pendingStreamMaxData.removeAt(index);
            this.owner.releasePendingControlBytesForHandoffLocked(this.pendingControlBytesLocked(streamId, value));
            return;
        }
        if (streamRuntime == null || !streamRuntime.shouldRetainPendingStreamMaxDataLocked()) {
            this.pendingStreamMaxData.removeAt(index);
            this.owner.releasePendingControlBytesLocked(this.pendingControlBytesLocked(streamId, value));
        }
    }

    private void takePendingStreamBlockedLocked(PendingDrain drain, long streamId) {
        if (drain.full()) {
            return;
        }
        int index = this.streamBlockedOffsets.indexOf(streamId);
        if (index < 0) {
            return;
        }
        long offset = this.streamBlockedOffsets.valueAt(index);
        StreamRuntime streamRuntime = this.owner.liveStreamLocked(streamId);
        if (streamRuntime != null && streamRuntime.shouldFlushPendingStreamBlockedLocked()) {
            if (!drain.accept(FrameType.BLOCKED, streamId, offset)) {
                return;
            }
            this.streamBlockedOffsets.removeAt(index);
            this.owner.releasePendingControlBytesForHandoffLocked(this.pendingControlBytesLocked(streamId, offset));
            return;
        }
        if (streamRuntime == null || !streamRuntime.shouldRetainPendingStreamBlockedLocked()) {
            this.streamBlockedOffsets.removeAt(index);
            this.owner.releasePendingControlBytesLocked(this.pendingControlBytesLocked(streamId, offset));
            if (streamRuntime != null) {
                streamRuntime.clearBlockedLocked();
            }
        }
    }

    private void drainPendingStreamMaxDataLocked(PendingDrain drain, Long preferred) {
        if (preferred != null) {
            this.takePendingStreamMaxDataLocked(drain, preferred);
        }
        if (drain.full() || this.pendingStreamMaxData.isEmpty()) {
            return;
        }
        long preferredValue = preferred == null ? Long.MIN_VALUE : preferred;
        for (int index = 0; index < this.pendingStreamMaxData.size(); ) {
            if (drain.full()) {
                return;
            }
            long streamId = this.pendingStreamMaxData.keyAt(index);
            if (preferred != null && streamId == preferredValue) {
                ++index;
                continue;
            }
            long value = this.pendingStreamMaxData.valueAt(index);
            if (!this.takePendingStreamMaxDataLocked(drain, index, streamId, value)) {
                ++index;
            }
        }
    }

    private void drainPendingStreamBlockedLocked(PendingDrain drain, Long preferred) {
        if (preferred != null) {
            this.takePendingStreamBlockedLocked(drain, preferred);
        }
        if (drain.full() || this.streamBlockedOffsets.isEmpty()) {
            return;
        }
        long preferredValue = preferred == null ? Long.MIN_VALUE : preferred;
        for (int index = 0; index < this.streamBlockedOffsets.size(); ) {
            if (drain.full()) {
                return;
            }
            long streamId = this.streamBlockedOffsets.keyAt(index);
            if (preferred != null && streamId == preferredValue) {
                ++index;
                continue;
            }
            long value = this.streamBlockedOffsets.valueAt(index);
            if (!this.takePendingStreamBlockedLocked(drain, index, streamId, value)) {
                ++index;
            }
        }
    }

    private boolean ensurePendingNonCloseControlLocked() {
        if (this.owner.allowLocalNonCloseControlLocked()) {
            return true;
        }
        this.clear();
        return false;
    }

    private boolean takePendingStreamMaxDataLocked(PendingDrain drain, int index, long streamId, long value) {
        if (drain.full()) {
            return false;
        }
        StreamRuntime streamRuntime = this.owner.liveStreamLocked(streamId);
        if (streamRuntime != null && streamRuntime.shouldFlushPendingStreamMaxDataLocked()) {
            if (!drain.accept(FrameType.MAX_DATA, streamId, value)) {
                return false;
            }
            this.pendingStreamMaxData.removeAt(index);
            this.owner.releasePendingControlBytesForHandoffLocked(this.pendingControlBytesLocked(streamId, value));
            return true;
        }
        if (streamRuntime == null || !streamRuntime.shouldRetainPendingStreamMaxDataLocked()) {
            this.pendingStreamMaxData.removeAt(index);
            this.owner.releasePendingControlBytesLocked(this.pendingControlBytesLocked(streamId, value));
            return true;
        }
        return false;
    }

    private boolean takePendingStreamBlockedLocked(PendingDrain drain, int index, long streamId, long offset) {
        if (drain.full()) {
            return false;
        }
        StreamRuntime streamRuntime = this.owner.liveStreamLocked(streamId);
        if (streamRuntime != null && streamRuntime.shouldFlushPendingStreamBlockedLocked()) {
            if (!drain.accept(FrameType.BLOCKED, streamId, offset)) {
                return false;
            }
            this.streamBlockedOffsets.removeAt(index);
            this.owner.releasePendingControlBytesForHandoffLocked(this.pendingControlBytesLocked(streamId, offset));
            return true;
        }
        if (streamRuntime == null || !streamRuntime.shouldRetainPendingStreamBlockedLocked()) {
            this.streamBlockedOffsets.removeAt(index);
            this.owner.releasePendingControlBytesLocked(this.pendingControlBytesLocked(streamId, offset));
            if (streamRuntime != null) {
                streamRuntime.clearBlockedLocked();
            }
            return true;
        }
        return false;
    }

    private long pendingControlBytesIfPresentLocked(long streamId, boolean present, long value) {
        return present ? this.pendingControlBytesLocked(streamId, value) : 0L;
    }

    private long pendingControlBytesLocked(long streamId, long value) {
        return this.owner.pendingControlFrameBytesLocked(streamId, value);
    }

    interface Owner {
        long pendingControlFrameBytesLocked(long streamId, long value);

        boolean replacePendingControlBytesLocked(long oldBytes, long newBytes);

        void releasePendingControlBytesLocked(long bytes);

        void releasePendingControlBytesForHandoffLocked(long bytes);

        boolean allowLocalNonCloseControlLocked();

        StreamRuntime liveStreamLocked(long streamId);
    }

    interface PendingFrameSink {
        boolean accept(FrameType type, long streamId, long value);
    }

    static final class PendingFrame {
        private final FrameType type;
        private final long streamId;
        private final long value;

        PendingFrame(FrameType type, long streamId, long value) {
            this.type = type;
            this.streamId = streamId;
            this.value = value;
        }

        FrameType type() {
            return type;
        }

        long streamId() {
            return streamId;
        }

        long value() {
            return value;
        }
    }

    private static final class PendingDrain {
        private final PendingFrameSink sink;
        private final int limit;
        private int accepted;
        private boolean stopped;

        private PendingDrain(PendingFrameSink sink, int limit) {
            this.sink = sink;
            this.limit = Math.max(0, limit);
        }

        private boolean accept(FrameType type, long streamId, long value) {
            if (this.full()) {
                return false;
            }
            if (!this.sink.accept(type, streamId, value)) {
                this.stopped = true;
                return false;
            }
            ++this.accepted;
            return true;
        }

        private boolean full() {
            return this.stopped || this.accepted >= this.limit;
        }
    }
}
