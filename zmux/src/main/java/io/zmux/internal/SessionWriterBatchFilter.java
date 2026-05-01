package io.zmux.internal;

import io.zmux.FrameType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("resource")
final class SessionWriterBatchFilter {
    private final SessionWriterCoordinator.Owner owner;

    SessionWriterBatchFilter(SessionWriterCoordinator.Owner owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    private static void removeTail(ArrayList<SessionRuntime.OutboundFrame> batch, int fromIndex, int size) {
        for (int index = size - 1; index >= fromIndex; --index) {
            batch.remove(index);
        }
    }

    List<SessionRuntime.OutboundFrame> filterWritableBatchLocked(List<SessionRuntime.OutboundFrame> batch) {
        if (batch == null || batch.isEmpty()) {
            return Collections.emptyList();
        }
        if (batch instanceof ArrayList<?>) {
            @SuppressWarnings("unchecked")
            ArrayList<SessionRuntime.OutboundFrame> typed = (ArrayList<SessionRuntime.OutboundFrame>) batch;
            return this.filterWritableArrayListLocked(typed);
        }

        ArrayList<SessionRuntime.OutboundFrame> filtered = null;
        boolean dataDropped = false;
        int index = 0;
        for (SessionRuntime.OutboundFrame outboundFrame : batch) {
            FilterResult result = this.filterResultLocked(outboundFrame);
            if (!result.keep()) {
                if (result.dataDropped()) {
                    dataDropped = true;
                }
                if (filtered == null) {
                    filtered = new ArrayList<>(batch.size());
                    for (int prefix = 0; prefix < index; ++prefix) {
                        filtered.add(batch.get(prefix));
                    }
                }
            } else if (filtered != null) {
                filtered.add(outboundFrame);
            }
            ++index;
        }
        if (dataDropped) {
            this.owner.notifyLockWaiters();
        }
        return filtered != null ? filtered : batch;
    }

    void discardCollectedBatchLocked(List<SessionRuntime.OutboundFrame> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        for (SessionRuntime.OutboundFrame outboundFrame : batch) {
            if (outboundFrame.stream() != null && outboundFrame.openingFrame()) {
                outboundFrame.stream().clearOpeningFramePendingLocked();
            }
            this.owner.failWriteCompletionLocked(outboundFrame, null);
            this.owner.releaseQueuedDataLocked(outboundFrame);
            this.owner.releaseWriterHeldFrameLocked(outboundFrame);
            if (outboundFrame.stream() != null) {
                this.owner.maybeCompactStreamLocked(outboundFrame.stream());
            }
        }
    }

    private ArrayList<SessionRuntime.OutboundFrame> filterWritableArrayListLocked(
            ArrayList<SessionRuntime.OutboundFrame> batch) {
        boolean dataDropped = false;
        int size = batch.size();
        int writeIndex = 0;
        for (int readIndex = 0; readIndex < size; ++readIndex) {
            SessionRuntime.OutboundFrame outboundFrame = batch.get(readIndex);
            FilterResult result = this.filterResultLocked(outboundFrame);
            if (!result.keep()) {
                if (result.dataDropped()) {
                    dataDropped = true;
                }
                continue;
            }
            if (writeIndex != readIndex) {
                batch.set(writeIndex, outboundFrame);
            }
            ++writeIndex;
        }
        if (writeIndex < size) {
            removeTail(batch, writeIndex, size);
        }
        if (dataDropped) {
            this.owner.notifyLockWaiters();
        }
        return batch;
    }

    private FilterResult filterResultLocked(SessionRuntime.OutboundFrame outboundFrame) {
        if (outboundFrame.frame().type() == FrameType.DATA
                && outboundFrame.stream() != null
                && !outboundFrame.stream().shouldEmitQueuedDataLocked(outboundFrame.preserveAfterSendClose())) {
            this.owner.failWriteCompletionLocked(outboundFrame, null);
            this.owner.releaseWriterHeldFrameLocked(outboundFrame);
            this.owner.releaseQueuedDataLocked(outboundFrame);
            this.owner.maybeCompactStreamLocked(outboundFrame.stream());
            return FilterResult.DROPPED_DATA;
        }
        if (outboundFrame.frame().type() == FrameType.EXT
                && outboundFrame.stream() != null
                && !this.owner.shouldEmitPriorityUpdateLocked(outboundFrame.stream())) {
            this.owner.releaseWriterHeldFrameLocked(outboundFrame);
            outboundFrame.stream().clearPriorityUpdateQueuedLocked();
            this.owner.maybeCompactStreamLocked(outboundFrame.stream());
            return FilterResult.DROPPED_PRIORITY_UPDATE;
        }
        return FilterResult.KEEP;
    }

    private enum FilterResult {
        KEEP(true, false),
        DROPPED_DATA(false, true),
        DROPPED_PRIORITY_UPDATE(false, false);

        private final boolean keep;
        private final boolean dataDropped;

        FilterResult(boolean keep, boolean dataDropped) {
            this.keep = keep;
            this.dataDropped = dataDropped;
        }

        boolean keep() {
            return this.keep;
        }

        boolean dataDropped() {
            return this.dataDropped;
        }
    }
}
