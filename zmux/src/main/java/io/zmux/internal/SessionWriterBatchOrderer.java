package io.zmux.internal;

import io.zmux.FrameType;
import io.zmux.Protocol;

import java.io.IOException;
import java.util.*;

final class SessionWriterBatchOrderer {
    private final SessionWriterCoordinator.Owner owner;
    @SuppressWarnings("FieldCanBeLocal")
    private final OrdinaryBatchOrderer.Workspace ordinaryOrderWorkspace = new OrdinaryBatchOrderer.Workspace();
    @SuppressWarnings("FieldCanBeLocal")
    private final ArrayList<OrdinaryBatchOrderer.BatchFrame> ordinaryOrderItems;
    private final BatchFrameWindow ordinaryOrderWindow = new BatchFrameWindow();
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

    private static boolean priorityUpdateFrame(SessionRuntime.OutboundFrame outboundFrame) {
        if (outboundFrame == null
                || outboundFrame.frame().type() != FrameType.EXT
                || outboundFrame.frame().streamId() == 0L) {
            return false;
        }
        try {
            return Varint62.decode(outboundFrame.frame().payload(), 0).value() == Protocol.EXT_PRIORITY_UPDATE;
        } catch (IOException invalidExtPayload) {
            return false;
        }
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
        int size = batch == null ? 0 : batch.size();
        if (size < 2 || this.sameStreamOrdinaryBatchKeepsOrder(batch, size)) {
            return reusableBatchList(batch);
        }

        this.ordinaryOrderItems.ensureCapacity(size);
        int batchIndex = 0;
        for (int i = 0; i < size; ++i) {
            SessionRuntime.OutboundFrame outboundFrame = batch.get(i);
            StreamRuntime streamRuntime = outboundFrame.stream();
            long streamId = outboundFrame.frame().streamId();
            boolean streamScoped = streamId != 0L;
            boolean priorityUpdate = streamScoped && priorityUpdateFrame(outboundFrame);
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
                this.ordinaryOrderWindow.reset(this.ordinaryOrderItems, batchIndex),
                this.owner.peerSettings().schedulerHints(),
                this.owner.peerSettings().maxFramePayload(),
                this.owner.ordinaryBatchBias(),
                this.ordinaryOrderWorkspace
        );
        if (order.isIdentity()) {
            return reusableBatchList(batch);
        }
        boolean identity = order.size() == size;
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
            if (index < 0 || index >= size) {
                return reusableBatchList(batch);
            }
            this.orderedBatch.add(batch.get(index));
        }
        return this.orderedBatch;
    }

    void clearRetainedBatchRefs() {
        this.orderedBatch.clear();
        int size = this.ordinaryOrderItems.size();
        for (int i = 0; i < size; ++i) {
            OrdinaryBatchOrderer.BatchFrame item = this.ordinaryOrderItems.get(i);
            item.reset(0L, false, false, false, 0L, 0L, null);
        }
    }

    private boolean sameStreamOrdinaryBatchKeepsOrder(List<SessionRuntime.OutboundFrame> batch, int size) {
        if (batch == null || size == 0) {
            return false;
        }
        long streamId = this.ordinaryBatchStreamId(batch.get(0));
        if (streamId == 0L) {
            return false;
        }
        for (int i = 1; i < size; ++i) {
            if (this.ordinaryBatchStreamId(batch.get(i)) != streamId) {
                return false;
            }
        }
        return true;
    }

    private long ordinaryBatchStreamId(SessionRuntime.OutboundFrame outboundFrame) {
        if (outboundFrame == null || priorityUpdateFrame(outboundFrame)) {
            return 0L;
        }
        return outboundFrame.frame().streamId();
    }

    private static final class BatchFrameWindow
            extends AbstractList<OrdinaryBatchOrderer.BatchFrame>
            implements RandomAccess {
        private ArrayList<OrdinaryBatchOrderer.BatchFrame> source;
        private int size;

        private BatchFrameWindow reset(ArrayList<OrdinaryBatchOrderer.BatchFrame> source, int size) {
            this.source = Objects.requireNonNull(source, "source");
            this.size = size;
            return this;
        }

        @Override
        public OrdinaryBatchOrderer.BatchFrame get(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException("index " + index + " size " + size);
            }
            return source.get(index);
        }

        @Override
        public int size() {
            return size;
        }
    }
}
