package io.zmux.internal;

import io.zmux.SchedulerHint;

import java.util.*;

final class OrdinaryBatchOrderer {
    private static final int BULK_QUANTUM_MULTIPLIER = 4;
    private static final int BATCH_SCRATCH_RETAIN_FACTOR = 4;
    private static final int MIN_BATCH_SCRATCH_RETAIN_HINT = 256;

    private OrdinaryBatchOrderer() {
    }

    static int[] order(List<BatchFrame> batch, SchedulerHint hint, long maxFramePayload) {
        return order(batch, hint, maxFramePayload, null);
    }

    static int[] order(List<BatchFrame> batch, SchedulerHint hint, long maxFramePayload, RetainedBias retainedBias) {
        return order(batch, hint, maxFramePayload, retainedBias, null);
    }

    static int[] order(List<BatchFrame> batch,
                       SchedulerHint hint,
                       long maxFramePayload,
                       RetainedBias retainedBias,
                       Workspace workspace) {
        return orderView(batch, hint, maxFramePayload, retainedBias, workspace).toArray();
    }

    static OrderView orderView(List<BatchFrame> batch,
                               SchedulerHint hint,
                               long maxFramePayload,
                               RetainedBias retainedBias,
                               Workspace workspace) {
        int size = batch == null ? 0 : batch.size();
        if (size < 2 || sameStreamBurstKeepsOrder(batch)) {
            return OrderView.identity(size);
        }

        SchedulerHint schedulerHint = hint == null ? SchedulerHint.UNSPECIFIED_OR_BALANCED : hint;
        long quantum = schedulerQuantum(maxFramePayload);
        long feedbackWindow = feedbackWindow(schedulerHint, quantum);
        Workspace state = workspace == null ? new Workspace() : workspace;
        BatchBuild build = build(batch, schedulerHint, state);
        return OrdinaryBatchDriver.order(state, build, schedulerHint, quantum, feedbackWindow, retainedBias, size);
    }

    private static BatchBuild build(List<BatchFrame> batch, SchedulerHint hint, Workspace workspace) {
        return OrdinaryBatchBuildPlanner.build(batch, hint, workspace);
    }

    private static boolean sameStreamBurstKeepsOrder(List<BatchFrame> batch) {
        if (batch == null || batch.isEmpty()) {
            return false;
        }
        BatchFrame first = batch.get(0);
        if (!first.streamScoped() || first.priorityUpdate()) {
            return false;
        }
        long streamId = first.streamId();
        for (int i = 1; i < batch.size(); ++i) {
            BatchFrame frame = batch.get(i);
            if (!frame.streamScoped() || frame.priorityUpdate() || frame.streamId() != streamId) {
                return false;
            }
        }
        return true;
    }

    static long streamWeight(long priority, long queuedBytes, SchedulerHint hint, long maxPayload) {
        return OrdinaryBatchSchedulingPolicy.streamWeight(priority, queuedBytes, hint, maxPayload);
    }

    static long groupWeight(GroupKey groupKey, long streamWeight, SchedulerHint hint) {
        return OrdinaryBatchSchedulingPolicy.groupWeight(groupKey, streamWeight, hint);
    }

    private static long schedulerQuantum(long maxPayload) {
        return OrdinaryBatchSchedulingPolicy.schedulerQuantum(maxPayload);
    }

    static long feedbackWindow(SchedulerHint hint, long maxPayload) {
        return OrdinaryBatchSchedulingPolicy.feedbackWindow(hint, maxPayload);
    }

    static long adjustWeightForLag(long base, long lag, long window, boolean fresh) {
        return OrdinaryBatchSchedulingPolicy.adjustWeightForLag(base, lag, window, fresh);
    }

    private static long normalizeCost(long cost) {
        return OrdinaryBatchSchedulingPolicy.normalizeCost(cost);
    }

    private static int[] identityOrder(int size) {
        int[] order = new int[size];
        for (int i = 0; i < size; ++i) {
            order[i] = i;
        }
        return order;
    }

    private static int batchScratchRetainLimit(int hint) {
        int normalizedHint = Math.max(MIN_BATCH_SCRATCH_RETAIN_HINT, hint);
        if (normalizedHint > Integer.MAX_VALUE / BATCH_SCRATCH_RETAIN_FACTOR) {
            return Integer.MAX_VALUE;
        }
        return normalizedHint * BATCH_SCRATCH_RETAIN_FACTOR;
    }

    private static boolean batchScratchOversized(int retainedCap, int hint) {
        if (retainedCap <= 0) {
            return false;
        }
        return retainedCap > batchScratchRetainLimit(hint);
    }

    enum TrafficClass {
        INTERACTIVE,
        BULK
    }

    static final class BatchFrame {
        private long streamId;
        private boolean streamScoped;
        private boolean priorityUpdate;
        private boolean openingFrame;
        private long cost;
        private long priority;
        private Long group;

        BatchFrame(long streamId,
                   boolean streamScoped,
                   boolean priorityUpdate,
                   boolean openingFrame,
                   long cost,
                   long priority,
                   Long group) {
            reset(streamId, streamScoped, priorityUpdate, openingFrame, cost, priority, group);
        }

        BatchFrame reset(long streamId,
                         boolean streamScoped,
                         boolean priorityUpdate,
                         boolean openingFrame,
                         long cost,
                         long priority,
                         Long group) {
            this.streamId = streamId;
            this.streamScoped = streamScoped;
            this.priorityUpdate = priorityUpdate;
            this.openingFrame = openingFrame;
            this.cost = cost;
            this.priority = priority;
            this.group = group;
            return this;
        }

        long streamId() {
            return streamId;
        }

        boolean streamScoped() {
            return streamScoped;
        }

        boolean priorityUpdate() {
            return priorityUpdate;
        }

        boolean openingFrame() {
            return openingFrame;
        }

        long cost() {
            return cost;
        }

        long priority() {
            return priority;
        }

        Long group() {
            return group;
        }
    }

    static final class Workspace {
        private LinkedHashMap<GroupKey, BatchGroup> groups = new LinkedHashMap<>();
        private final ArrayList<BatchGroup> groupsInOrder = new ArrayList<>();
        private HashMap<Long, GroupKey> explicitGroups = new HashMap<>();
        private final OrdinaryBatchRetainedState retainedState = new OrdinaryBatchRetainedState();
        private LongIntCounterMap bypassSelections = new LongIntCounterMap();
        private final int[] activeSelectionCounts = new int[2];
        private HashMap<GroupKey, Long> nextPreferredStreamHeads = new HashMap<>();
        private HashSet<GroupKey> recordedGroupHeads = new HashSet<>();
        private final ArrayList<GroupCandidate> interactiveCandidates = new ArrayList<>();
        private final ArrayList<GroupCandidate> bulkCandidates = new ArrayList<>();
        private final ArrayList<BatchGroup> batchGroupPool = new ArrayList<>();
        private final ArrayList<BatchStreamState> batchStreamPool = new ArrayList<>();
        private final ArrayList<BatchEntry> batchEntryPool = new ArrayList<>();
        private final BatchBuild batchBuild = new BatchBuild(groupsInOrder);
        private int[] ordered = new int[0];
        private boolean[] selected = new boolean[0];
        private int orderedSize;
        private int batchGroupCursor;
        private int batchStreamCursor;
        private int batchEntryCursor;
        private int lastBuildCapHint;

        private static <T> void trimScratchList(ArrayList<T> list) {
            list.clear();
            list.trimToSize();
        }

        private static <T> void trimPooledScratch(ArrayList<T> list, int capHint) {
            int retainLimit = batchScratchRetainLimit(capHint);
            if (list.size() > retainLimit) {
                list.subList(retainLimit, list.size()).clear();
            }
            list.trimToSize();
        }

        void resetOrderScratch(int size) {
            if (ordered.length < size) {
                ordered = new int[size];
            }
            if (selected.length < size) {
                selected = new boolean[size];
            }
            Arrays.fill(selected, 0, size, false);
            orderedSize = 0;
        }

        void addOrderedIndex(int index) {
            if (orderedSize >= ordered.length) {
                throw new IllegalStateException("batch order scratch overflow");
            }
            if (index < 0 || index >= selected.length) {
                throw new IllegalStateException("batch order index out of range");
            }
            if (selected[index]) {
                throw new IllegalStateException("duplicate batch order index");
            }
            ordered[orderedSize++] = index;
            selected[index] = true;
        }

        void appendRemainingInInputOrder(int size) {
            if (orderedSize >= size) {
                return;
            }
            for (int index = 0; index < size; ++index) {
                if (!selected[index]) {
                    ordered[orderedSize++] = index;
                    selected[index] = true;
                }
            }
        }

        OrderView toOrderView(int size) {
            if (orderedSize != size) {
                throw new IllegalStateException("incomplete batch order");
            }
            return new OrderView(ordered, size, false);
        }

        int orderedSize() {
            return orderedSize;
        }

        void prepareBuildScratch(int capHint) {
            if (batchScratchOversized(lastBuildCapHint, capHint)) {
                ordered = new int[0];
                selected = new boolean[0];
                groups = new LinkedHashMap<>();
                trimScratchList(groupsInOrder);
                explicitGroups = new HashMap<>();
                bypassSelections = new LongIntCounterMap();
                nextPreferredStreamHeads = new HashMap<>();
                recordedGroupHeads = new HashSet<>();
                trimScratchList(interactiveCandidates);
                trimScratchList(bulkCandidates);
                trimPooledScratch(batchGroupPool, capHint);
                trimPooledScratch(batchStreamPool, capHint);
                trimPooledScratch(batchEntryPool, capHint);
            }
            lastBuildCapHint = Math.max(0, capHint);
            batchGroupCursor = 0;
            batchStreamCursor = 0;
            batchEntryCursor = 0;
        }

        BatchGroup nextBatchGroup(GroupKey key, int order) {
            if (batchGroupCursor >= batchGroupPool.size()) {
                batchGroupPool.add(new BatchGroup());
            }
            return batchGroupPool.get(batchGroupCursor++).reset(key, order);
        }

        BatchStreamState nextBatchStream(long streamKey,
                                         boolean streamScoped,
                                         long priority,
                                         int order) {
            if (batchStreamCursor >= batchStreamPool.size()) {
                batchStreamPool.add(new BatchStreamState());
            }
            return batchStreamPool.get(batchStreamCursor++).reset(streamKey, streamScoped, priority, order);
        }

        BatchEntry nextBatchEntry(int index, BatchFrame frame, long cost) {
            if (batchEntryCursor >= batchEntryPool.size()) {
                batchEntryPool.add(new BatchEntry());
            }
            return batchEntryPool.get(batchEntryCursor++).reset(index, frame, cost);
        }

        void loadRetainedBias(RetainedBias retainedBias) {
            retainedState.loadFrom(retainedBias == null ? null : retainedBias.state());
        }

        OrdinaryBatchRetainedState retainedState() {
            return retainedState;
        }

        LinkedHashMap<GroupKey, BatchGroup> groups() {
            return groups;
        }

        ArrayList<BatchGroup> groupsInOrder() {
            return groupsInOrder;
        }

        HashMap<Long, GroupKey> explicitGroups() {
            return explicitGroups;
        }

        BatchBuild batchBuild() {
            return batchBuild;
        }

        LongIntCounterMap bypassSelections() {
            return bypassSelections;
        }

        int[] activeSelectionCounts() {
            return activeSelectionCounts;
        }

        HashMap<GroupKey, Long> nextPreferredStreamHeads() {
            return nextPreferredStreamHeads;
        }

        HashSet<GroupKey> recordedGroupHeads() {
            return recordedGroupHeads;
        }

        ArrayList<GroupCandidate> interactiveCandidates() {
            return interactiveCandidates;
        }

        ArrayList<GroupCandidate> bulkCandidates() {
            return bulkCandidates;
        }
    }

    static final class OrderView {
        private final int[] order;
        private final int size;
        private final boolean identity;

        private OrderView(int[] order, int size, boolean identity) {
            this.order = order;
            this.size = Math.max(0, size);
            this.identity = identity;
        }

        static OrderView identity(int size) {
            return new OrderView(null, size, true);
        }

        int size() {
            return size;
        }

        boolean isIdentity() {
            return identity;
        }

        int indexAt(int position) {
            if (position < 0 || position >= size) {
                throw new IndexOutOfBoundsException("order position out of range: " + position + " size=" + size);
            }
            return identity ? position : order[position];
        }

        int[] toArray() {
            if (identity) {
                return identityOrder(size);
            }
            return Arrays.copyOf(order, size);
        }
    }

    static final class RetainedBias {
        private final OrdinaryBatchRetainedState state = new OrdinaryBatchRetainedState();

        void replace(GroupKey nextGroupHead,
                     Map<GroupKey, Long> nextStreamHeads,
                     long nextRootVirtualTime,
                     long nextServiceSeq,
                     Map<GroupKey, Long> nextGroupVirtualTime,
                     Map<GroupKey, Long> nextGroupFinishTag,
                     Map<GroupKey, Long> nextGroupLastServed,
                     Map<GroupKey, Long> nextGroupLag,
                     Map<Long, Long> nextStreamFinishTag,
                     Map<Long, Long> nextStreamLastServed,
                     Map<Long, Long> nextStreamLag,
                     Map<Long, TrafficClass> nextStreamClass,
                     Map<Long, Long> nextStreamLastSeenBatch,
                     Set<Long> nextSmallBurstDisarmed,
                     long nextBatchSeq,
                     int nextInteractiveStreak,
                     int nextClassSelectionsSinceBulk) {
            state.replace(
                    nextGroupHead,
                    nextStreamHeads,
                    nextRootVirtualTime,
                    nextServiceSeq,
                    nextGroupVirtualTime,
                    nextGroupFinishTag,
                    nextGroupLastServed,
                    nextGroupLag,
                    nextStreamFinishTag,
                    nextStreamLastServed,
                    nextStreamLag,
                    nextStreamClass,
                    nextStreamLastSeenBatch,
                    nextSmallBurstDisarmed,
                    nextBatchSeq,
                    nextInteractiveStreak,
                    nextClassSelectionsSinceBulk
            );
        }

        void clear() {
            state.clear();
        }

        void release() {
            state.release();
        }

        void dropStream(long streamId) {
            state.dropStream(streamId);
        }

        void dropExplicitGroup(long group) {
            state.dropExplicitGroup(group);
        }

        OrdinaryBatchRetainedState state() {
            return state;
        }
    }

    static final class GroupKey {
        private final int kind;
        private final long value;

        GroupKey(int kind, long value) {
            this.kind = kind;
            this.value = value;
        }

        int kind() {
            return kind;
        }

        long value() {
            return value;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof GroupKey)) {
                return false;
            }
            GroupKey that = (GroupKey) other;
            return kind == that.kind && value == that.value;
        }

        @Override
        public int hashCode() {
            return 31 * Integer.hashCode(kind) + Long.hashCode(value);
        }
    }

    static final class BatchBuild {
        private final ArrayList<BatchGroup> groupsInOrder;
        private boolean hasRealStreamScoped;
        private boolean hasPriorityUpdate;

        private BatchBuild(ArrayList<BatchGroup> groupsInOrder) {
            this.groupsInOrder = groupsInOrder;
        }

        BatchBuild reset(boolean hasRealStreamScoped, boolean hasPriorityUpdate) {
            this.hasRealStreamScoped = hasRealStreamScoped;
            this.hasPriorityUpdate = hasPriorityUpdate;
            return this;
        }

        ArrayList<BatchGroup> groupsInOrder() {
            return groupsInOrder;
        }

        boolean hasRealStreamScoped() {
            return hasRealStreamScoped;
        }

        boolean hasPriorityUpdate() {
            return hasPriorityUpdate;
        }
    }

    static final class BatchGroup {
        private final LinkedHashMap<Long, BatchStreamState> streams = new LinkedHashMap<>();
        private GroupKey key;
        private int order;

        private BatchGroup() {
        }

        private BatchGroup reset(GroupKey key, int order) {
            this.key = key;
            this.order = order;
            streams.clear();
            return this;
        }

        GroupKey key() {
            return key;
        }

        int order() {
            return order;
        }

        LinkedHashMap<Long, BatchStreamState> streams() {
            return streams;
        }
    }

    static final class BatchStreamState {
        private final ArrayList<BatchEntry> entries = new ArrayList<>();
        private long streamKey;
        private boolean streamScoped;
        private long priority;
        private int order;
        private long remainingCost;
        private TrafficClass trafficClass = TrafficClass.INTERACTIVE;
        private boolean smallBurstBonusArmed;
        private BatchEntry selectedEntry;
        private int selectedQueuePos = -1;
        private long selectedBaseWeight;

        private BatchStreamState() {
        }

        private BatchStreamState reset(long streamKey,
                                       boolean streamScoped,
                                       long priority,
                                       int order) {
            this.streamKey = streamKey;
            this.streamScoped = streamScoped;
            this.priority = priority;
            this.order = order;
            entries.clear();
            remainingCost = 0L;
            trafficClass = TrafficClass.INTERACTIVE;
            smallBurstBonusArmed = false;
            clearSelection();
            return this;
        }

        void addRemainingCost(long cost) {
            remainingCost = OrdinaryBatchSchedulingPolicy.saturatingAdd(remainingCost, cost);
        }

        void consumeCost(long cost) {
            remainingCost = Math.max(0L, remainingCost - cost);
        }

        void removeEntry(int queuePos) {
            if (queuePos >= 0 && queuePos < entries.size()) {
                entries.remove(queuePos);
            }
        }

        void clearSelection() {
            selectedEntry = null;
            selectedQueuePos = -1;
            selectedBaseWeight = 0L;
        }

        void selectEntry(BatchEntry entry, int queuePos, long baseWeight) {
            selectedEntry = entry;
            selectedQueuePos = queuePos;
            selectedBaseWeight = baseWeight;
        }

        long streamKey() {
            return streamKey;
        }

        boolean streamScoped() {
            return streamScoped;
        }

        long priority() {
            return priority;
        }

        int order() {
            return order;
        }

        ArrayList<BatchEntry> entries() {
            return entries;
        }

        long remainingCost() {
            return remainingCost;
        }

        TrafficClass trafficClass() {
            return trafficClass;
        }

        void setTrafficClass(TrafficClass trafficClass) {
            this.trafficClass = trafficClass == null ? TrafficClass.INTERACTIVE : trafficClass;
        }

        boolean smallBurstBonusArmed() {
            return smallBurstBonusArmed;
        }

        void setSmallBurstBonusArmed(boolean smallBurstBonusArmed) {
            this.smallBurstBonusArmed = smallBurstBonusArmed;
        }

        boolean hasSelection() {
            return selectedEntry != null;
        }

        BatchEntry selectedEntry() {
            return selectedEntry;
        }

        int selectedQueuePos() {
            return selectedQueuePos;
        }

        long selectedBaseWeight() {
            return selectedBaseWeight;
        }
    }

    static final class BatchEntry {
        private int index;
        private BatchFrame frame;
        private long cost;

        private BatchEntry() {
        }

        private BatchEntry reset(int index, BatchFrame frame, long cost) {
            this.index = index;
            this.frame = frame;
            this.cost = cost;
            return this;
        }

        int index() {
            return index;
        }

        BatchFrame frame() {
            return frame;
        }

        long cost() {
            return cost;
        }
    }

    static final class GroupCandidatePair {
        private final GroupCandidate interactive;
        private final GroupCandidate bulk;

        GroupCandidatePair(GroupCandidate interactive, GroupCandidate bulk) {
            this.interactive = interactive;
            this.bulk = bulk;
        }

        static GroupCandidatePair empty() {
            return new GroupCandidatePair(null, null);
        }

        GroupCandidate interactive() {
            return interactive;
        }

        GroupCandidate bulk() {
            return bulk;
        }
    }

    static final class TransientHead {
        private final BatchGroup group;
        private final BatchStreamState stream;
        private final BatchEntry entry;

        TransientHead(BatchGroup group, BatchStreamState stream, BatchEntry entry) {
            this.group = group;
            this.stream = stream;
            this.entry = entry;
        }

        BatchGroup group() {
            return group;
        }

        BatchStreamState stream() {
            return stream;
        }

        BatchEntry entry() {
            return entry;
        }
    }

    static final class GroupCandidate {
        private final BatchGroup group;
        private final BatchStreamState streamState;
        private final BatchEntry entry;
        private final long baseWeight;
        private final long baseGroupWeight;
        private final int queuePos;
        private final long groupWeight;
        private final long totalBaseStreamWeight;
        private final long totalStreamWeight;
        private final long streamStart;
        private final long streamFinish;
        private final long streamLastServed;
        private final long groupStart;
        private final long groupFinish;
        private final long groupLastServed;
        private final boolean eligible;

        GroupCandidate(BatchGroup group,
                       BatchStreamState streamState,
                       BatchEntry entry,
                       long baseWeight,
                       long baseGroupWeight,
                       int queuePos,
                       long groupWeight,
                       long totalBaseStreamWeight,
                       long totalStreamWeight,
                       long streamStart,
                       long streamFinish,
                       long streamLastServed,
                       long groupStart,
                       long groupFinish,
                       long groupLastServed,
                       boolean eligible) {
            this.group = group;
            this.streamState = streamState;
            this.entry = entry;
            this.baseWeight = baseWeight;
            this.baseGroupWeight = baseGroupWeight;
            this.queuePos = queuePos;
            this.groupWeight = groupWeight;
            this.totalBaseStreamWeight = totalBaseStreamWeight;
            this.totalStreamWeight = totalStreamWeight;
            this.streamStart = streamStart;
            this.streamFinish = streamFinish;
            this.streamLastServed = streamLastServed;
            this.groupStart = groupStart;
            this.groupFinish = groupFinish;
            this.groupLastServed = groupLastServed;
            this.eligible = eligible;
        }

        BatchGroup group() {
            return group;
        }

        BatchStreamState streamState() {
            return streamState;
        }

        BatchEntry entry() {
            return entry;
        }

        long baseWeight() {
            return baseWeight;
        }

        long baseGroupWeight() {
            return baseGroupWeight;
        }

        int queuePos() {
            return queuePos;
        }

        long groupWeight() {
            return groupWeight;
        }

        long totalBaseStreamWeight() {
            return totalBaseStreamWeight;
        }

        long totalStreamWeight() {
            return totalStreamWeight;
        }

        long streamStart() {
            return streamStart;
        }

        long streamFinish() {
            return streamFinish;
        }

        long streamLastServed() {
            return streamLastServed;
        }

        long groupStart() {
            return groupStart;
        }

        long groupFinish() {
            return groupFinish;
        }

        long groupLastServed() {
            return groupLastServed;
        }

        boolean eligible() {
            return eligible;
        }

        private GroupKey groupKey() {
            return group.key();
        }

        private long streamKey() {
            return streamState.streamKey();
        }

        TrafficClass trafficClass() {
            return streamState.trafficClass();
        }
    }
}
