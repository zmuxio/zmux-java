package io.zmux.runtime;

import io.zmux.protocol.FrameType;

import java.util.*;

@SuppressWarnings("resource")
final class SessionWriterBatchFilter {
    private final SessionWriterCoordinator.Owner owner;

    SessionWriterBatchFilter(SessionWriterCoordinator.Owner owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    private static void removeTail(ArrayList<SessionRuntime.OutboundFrame> batch, int fromIndex, int size) {
        batch.subList(fromIndex, size).clear();
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
        for (SessionRuntime.OutboundFrame outboundFrame : batch) {
            if (this.filterResultLocked(outboundFrame) != FilterResult.KEEP) {
                return this.filterWritableArrayListLocked(new ArrayList<>(batch));
            }
        }
        return batch;
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
        int size = batch.size();
        FilterResult[] results = null;
        // Decided back to front: whether a dropped opener must still open its stream depends on which
        // later frames of the batch reach the wire.
        for (int index = size - 1; index >= 0; --index) {
            FilterResult result = this.filterResultLocked(batch.get(index));
            if (result == FilterResult.KEEP) {
                if (results != null) {
                    results[index] = result;
                }
                continue;
            }
            if (results == null) {
                results = new FilterResult[size];
                Arrays.fill(results, index + 1, size, FilterResult.KEEP);
            }
            if (result == FilterResult.DROPPED_DATA && this.droppedOpenerStillOpensStreamLocked(batch, index, results)) {
                result = FilterResult.REPLACED_OPENER;
            }
            results[index] = result;
        }
        if (results == null) {
            return batch;
        }

        boolean dataDropped = false;
        int writeIndex = 0;
        for (int readIndex = 0; readIndex < size; ++readIndex) {
            SessionRuntime.OutboundFrame outboundFrame = batch.get(readIndex);
            FilterResult result = results[readIndex];
            if (result == FilterResult.REPLACED_OPENER) {
                outboundFrame = this.owner.zeroLengthOpenerReplacementLocked(outboundFrame);
                dataDropped = true;
            } else if (!result.keep()) {
                this.dropLocked(outboundFrame, result);
                if (result.dataDropped()) {
                    dataDropped = true;
                }
                continue;
            }
            if (writeIndex != readIndex || result == FilterResult.REPLACED_OPENER) {
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

    /**
     * A local stream's not-yet-written opening DATA can become unsendable when the send half is reset or
     * aborted while the writer holds it. Dropping it is only safe when the stream's own opening-eligible
     * ABORT reaches the wire before any other opener of the same class. Otherwise the stream must still
     * open in its place (as a zero-length opener): RESET may not be a stream's first frame (SPEC §6.7), and
     * a later stream ID may not reach the peer first (SPEC §3.1).
     */
    private boolean droppedOpenerStillOpensStreamLocked(List<SessionRuntime.OutboundFrame> batch,
                                                        int index,
                                                        FilterResult[] results) {
        SessionRuntime.OutboundFrame opener = batch.get(index);
        StreamRuntime streamRuntime = opener.stream();
        if (!opener.openingFrame()
                || streamRuntime == null
                || !streamRuntime.openedLocally()
                || streamRuntime.peerVisibleLocked()) {
            return false;
        }
        if (streamRuntime.halfStateInternal().sendReset()) {
            return true;
        }
        long streamId = opener.frame().streamId();
        for (int later = index + 1; later < batch.size(); ++later) {
            SessionRuntime.OutboundFrame candidate = batch.get(later);
            if (results[later] == FilterResult.DROPPED_DATA || !candidate.openingFrame() || candidate.stream() == null) {
                continue;
            }
            long candidateId = candidate.frame().streamId();
            if (candidateId == streamId) {
                return false;
            }
            if ((candidateId & 3L) == (streamId & 3L)) {
                return true;
            }
        }
        return false;
    }

    private FilterResult filterResultLocked(SessionRuntime.OutboundFrame outboundFrame) {
        if (outboundFrame.frame().type() == FrameType.DATA
                && outboundFrame.stream() != null
                && !outboundFrame.stream().shouldEmitQueuedDataLocked(outboundFrame.preserveAfterSendClose())) {
            return FilterResult.DROPPED_DATA;
        }
        if (outboundFrame.frame().type() == FrameType.EXT
                && outboundFrame.stream() != null
                && !this.owner.shouldEmitPriorityUpdateLocked(outboundFrame.stream())) {
            return FilterResult.DROPPED_PRIORITY_UPDATE;
        }
        return FilterResult.KEEP;
    }

    private void dropLocked(SessionRuntime.OutboundFrame outboundFrame, FilterResult result) {
        if (result == FilterResult.DROPPED_DATA) {
            if (outboundFrame.openingFrame()) {
                outboundFrame.stream().clearOpeningFramePendingLocked();
            }
            this.owner.failWriteCompletionLocked(outboundFrame, null);
            this.owner.releaseWriterHeldFrameLocked(outboundFrame);
            this.owner.releaseQueuedDataLocked(outboundFrame);
            this.owner.maybeCompactStreamLocked(outboundFrame.stream());
            return;
        }
        this.owner.releaseWriterHeldFrameLocked(outboundFrame);
        outboundFrame.stream().clearPriorityUpdateQueuedLocked();
        this.owner.maybeCompactStreamLocked(outboundFrame.stream());
    }

    private enum FilterResult {
        KEEP(true, false),
        DROPPED_DATA(false, true),
        DROPPED_PRIORITY_UPDATE(false, false),
        REPLACED_OPENER(true, true);

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
