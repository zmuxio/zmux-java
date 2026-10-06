package io.zmux.runtime;

import io.zmux.protocol.FrameType;
import io.zmux.protocol.Protocol;
import io.zmux.protocol.Varint62;

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

    private static boolean localOpeningFrame(SessionRuntime.OutboundFrame outboundFrame) {
        return outboundFrame != null
                && outboundFrame.openingFrame()
                && outboundFrame.stream() != null
                && outboundFrame.frame().streamId() != 0L;
    }

    /**
     * Within one stream class the peer must observe new stream IDs in assignment order (SPEC §3.1), so no
     * reordering stage (urgent ranking, WFQ) may put a stream's opening frame ahead of a lower-ID opener of
     * the same class. This restores that order in place with minimal movement: an opener that would jump
     * ahead, and every later frame of its stream, is held back until the lower opener has been placed.
     */
    static void keepOpeningFramesInStreamIdOrder(List<SessionRuntime.OutboundFrame> batch) {
        if (batch == null || batch.size() < 2 || openingFramesInStreamIdOrder(batch)) {
            return;
        }
        Map<Long, ArrayDeque<Long>> pendingOpenersByClass = new HashMap<>();
        Set<Long> pendingOpenerStreams = new HashSet<>();
        for (SessionRuntime.OutboundFrame outboundFrame : batch) {
            if (localOpeningFrame(outboundFrame) && pendingOpenerStreams.add(outboundFrame.frame().streamId())) {
                pendingOpenersByClass
                        .computeIfAbsent(outboundFrame.frame().streamId() & 3L, ignored -> new ArrayDeque<>())
                        .add(outboundFrame.frame().streamId());
            }
        }
        for (ArrayDeque<Long> openers : pendingOpenersByClass.values()) {
            ArrayList<Long> sorted = new ArrayList<>(openers);
            Collections.sort(sorted);
            openers.clear();
            openers.addAll(sorted);
        }

        ArrayList<SessionRuntime.OutboundFrame> source = new ArrayList<>(batch);
        ArrayList<SessionRuntime.OutboundFrame> held = new ArrayList<>();
        batch.clear();
        for (SessionRuntime.OutboundFrame outboundFrame : source) {
            if (heldForOpeningOrder(outboundFrame, pendingOpenersByClass, pendingOpenerStreams)) {
                held.add(outboundFrame);
                continue;
            }
            emitInOpeningOrder(batch, outboundFrame, pendingOpenersByClass, pendingOpenerStreams);
            boolean progressed = true;
            while (progressed && !held.isEmpty()) {
                progressed = false;
                for (int i = 0; i < held.size(); ++i) {
                    SessionRuntime.OutboundFrame candidate = held.get(i);
                    if (!heldForOpeningOrder(candidate, pendingOpenersByClass, pendingOpenerStreams)) {
                        held.remove(i);
                        emitInOpeningOrder(batch, candidate, pendingOpenersByClass, pendingOpenerStreams);
                        progressed = true;
                        break;
                    }
                }
            }
        }
        batch.addAll(held);
    }

    private static boolean openingFramesInStreamIdOrder(List<SessionRuntime.OutboundFrame> batch) {
        long[] lastOpenerByClass = null;
        for (SessionRuntime.OutboundFrame outboundFrame : batch) {
            if (!localOpeningFrame(outboundFrame)) {
                continue;
            }
            long streamId = outboundFrame.frame().streamId();
            int streamClass = (int) (streamId & 3L);
            if (lastOpenerByClass == null) {
                lastOpenerByClass = new long[]{-1L, -1L, -1L, -1L};
            }
            if (streamId < lastOpenerByClass[streamClass]) {
                return false;
            }
            lastOpenerByClass[streamClass] = streamId;
        }
        return true;
    }

    private static boolean heldForOpeningOrder(SessionRuntime.OutboundFrame outboundFrame,
                                               Map<Long, ArrayDeque<Long>> pendingOpenersByClass,
                                               Set<Long> pendingOpenerStreams) {
        long streamId = outboundFrame.frame().streamId();
        if (streamId == 0L || !pendingOpenerStreams.contains(streamId)) {
            return false;
        }
        if (!localOpeningFrame(outboundFrame)) {
            return true;
        }
        ArrayDeque<Long> openers = pendingOpenersByClass.get(streamId & 3L);
        return openers == null || openers.isEmpty() || openers.peekFirst() != streamId;
    }

    private static void emitInOpeningOrder(List<SessionRuntime.OutboundFrame> batch,
                                           SessionRuntime.OutboundFrame outboundFrame,
                                           Map<Long, ArrayDeque<Long>> pendingOpenersByClass,
                                           Set<Long> pendingOpenerStreams) {
        batch.add(outboundFrame);
        long streamId = outboundFrame.frame().streamId();
        if (localOpeningFrame(outboundFrame) && pendingOpenerStreams.remove(streamId)) {
            ArrayDeque<Long> openers = pendingOpenersByClass.get(streamId & 3L);
            if (openers != null) {
                openers.remove(streamId);
            }
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
        keepOpeningFramesInStreamIdOrder(batch);
    }

    ArrayList<SessionRuntime.OutboundFrame> orderOrdinary(List<SessionRuntime.OutboundFrame> batch) {
        ArrayList<SessionRuntime.OutboundFrame> ordered = this.orderOrdinaryByScheduler(batch);
        keepOpeningFramesInStreamIdOrder(ordered);
        return ordered;
    }

    private ArrayList<SessionRuntime.OutboundFrame> orderOrdinaryByScheduler(List<SessionRuntime.OutboundFrame> batch) {
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
