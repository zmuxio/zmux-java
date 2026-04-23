package io.zmux.internal;

import io.zmux.FrameType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

final class SessionWriterBatchOrderer {
    private final SessionWriterCoordinator.Owner owner;
    @SuppressWarnings("FieldCanBeLocal")
    private final OrdinaryBatchOrderer.Workspace ordinaryOrderWorkspace = new OrdinaryBatchOrderer.Workspace();
    @SuppressWarnings("FieldCanBeLocal")
    private final ArrayList<OrdinaryBatchOrderer.BatchFrame> ordinaryOrderItems;
    @SuppressWarnings("FieldCanBeLocal")
    private final ArrayList<SessionRuntime.OutboundFrame> orderedBatch;

    SessionWriterBatchOrderer(SessionWriterCoordinator.Owner owner, int maxBatchFrames) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.ordinaryOrderItems = new ArrayList<>(maxBatchFrames);
        this.orderedBatch = new ArrayList<>(maxBatchFrames);
    }

    private static ArrayList<SessionRuntime.OutboundFrame> reusableBatchList(List<SessionRuntime.OutboundFrame> batch) {
        if (batch == null) {
            return new ArrayList<>(0);
        }
        if (batch instanceof ArrayList<?>) {
            @SuppressWarnings("unchecked")
            ArrayList<SessionRuntime.OutboundFrame> arrayList = (ArrayList<SessionRuntime.OutboundFrame>) batch;
            return arrayList;
        }
        return new ArrayList<>(batch);
    }

    void orderUrgent(ArrayList<SessionRuntime.OutboundFrame> batch) {
        if (batch == null || batch.size() < 2) {
            return;
        }
        for (int i = 1; i < batch.size(); ++i) {
            SessionRuntime.OutboundFrame current = batch.get(i);
            int insert = i;
            while (insert > 0 && SessionWriterBatchPolicy.urgentOutboundPrecedes(current, batch.get(insert - 1))) {
                batch.set(insert, batch.get(insert - 1));
                --insert;
            }
            if (insert != i) {
                batch.set(insert, current);
            }
        }
    }

    ArrayList<SessionRuntime.OutboundFrame> orderOrdinary(List<SessionRuntime.OutboundFrame> batch) {
        if (batch == null || batch.size() < 2 || this.sameStreamOrdinaryBatchKeepsOrder(batch)) {
            return reusableBatchList(batch);
        }

        this.ordinaryOrderItems.ensureCapacity(batch.size());
        int batchIndex = 0;
        for (SessionRuntime.OutboundFrame outboundFrame : batch) {
            StreamRuntime streamRuntime = outboundFrame.stream();
            long streamId = outboundFrame.frame().streamId();
            boolean streamScoped = streamId != 0L;
            boolean priorityUpdate = streamScoped && outboundFrame.frame().type() == FrameType.EXT;
            Long group = streamRuntime == null ? null : this.owner.outboundSchedulingGroupLocked(streamRuntime);
            if (batchIndex == this.ordinaryOrderItems.size()) {
                this.ordinaryOrderItems.add(new OrdinaryBatchOrderer.BatchFrame(
                        streamId,
                        streamScoped,
                        priorityUpdate,
                        outboundFrame.openingFrame(),
                        SessionWriterBatchPolicy.outboundBatchCost(outboundFrame),
                        streamRuntime == null ? 0L : streamRuntime.priorityLocked(),
                        group
                ));
            } else {
                this.ordinaryOrderItems.get(batchIndex).reset(
                        streamId,
                        streamScoped,
                        priorityUpdate,
                        outboundFrame.openingFrame(),
                        SessionWriterBatchPolicy.outboundBatchCost(outboundFrame),
                        streamRuntime == null ? 0L : streamRuntime.priorityLocked(),
                        group
                );
            }
            ++batchIndex;
        }

        OrdinaryBatchOrderer.OrderView order = OrdinaryBatchOrderer.orderView(
                this.ordinaryOrderItems.subList(0, batchIndex),
                this.owner.peerSettings().schedulerHints(),
                this.owner.peerSettings().maxFramePayload(),
                this.owner.ordinaryBatchBias(),
                this.ordinaryOrderWorkspace
        );
        if (order.isIdentity()) {
            return reusableBatchList(batch);
        }
        boolean identity = order.size() == batch.size();
        for (int i = 0; i < order.size(); ++i) {
            if (order.indexAt(i) != i) {
                identity = false;
                break;
            }
        }
        if (identity) {
            return reusableBatchList(batch);
        }

        this.orderedBatch.clear();
        this.orderedBatch.ensureCapacity(order.size());
        for (int i = 0; i < order.size(); ++i) {
            int index = order.indexAt(i);
            if (index < 0 || index >= batch.size()) {
                return reusableBatchList(batch);
            }
            this.orderedBatch.add(batch.get(index));
        }
        return this.orderedBatch;
    }

    void clearRetainedBatchRefs() {
        this.orderedBatch.clear();
        for (OrdinaryBatchOrderer.BatchFrame item : this.ordinaryOrderItems) {
            item.reset(0L, false, false, false, 0L, 0L, null);
        }
    }

    private boolean sameStreamOrdinaryBatchKeepsOrder(List<SessionRuntime.OutboundFrame> batch) {
        if (batch == null || batch.isEmpty()) {
            return false;
        }
        long streamId = this.ordinaryBatchStreamId(batch.get(0));
        if (streamId == 0L) {
            return false;
        }
        for (int i = 1; i < batch.size(); ++i) {
            if (this.ordinaryBatchStreamId(batch.get(i)) != streamId) {
                return false;
            }
        }
        return true;
    }

    private long ordinaryBatchStreamId(SessionRuntime.OutboundFrame outboundFrame) {
        if (outboundFrame == null || outboundFrame.frame().type() == FrameType.EXT) {
            return 0L;
        }
        return outboundFrame.frame().streamId();
    }
}
