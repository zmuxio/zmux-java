package io.zmux.internal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class OrdinaryBatchRetainedState {
    private static final long REBASE_THRESHOLD = 1L << 48;
    HashMap<OrdinaryBatchOrderer.GroupKey, Long> preferredStreamHeads = new HashMap<>();
    HashMap<OrdinaryBatchOrderer.GroupKey, Long> groupVirtualTime = new HashMap<>();
    HashMap<OrdinaryBatchOrderer.GroupKey, Long> groupFinishTag = new HashMap<>();
    HashMap<OrdinaryBatchOrderer.GroupKey, Long> groupLastServed = new HashMap<>();
    HashMap<OrdinaryBatchOrderer.GroupKey, Long> groupLag = new HashMap<>();
    HashMap<Long, Long> streamFinishTag = new HashMap<>();
    HashMap<Long, Long> streamLastServed = new HashMap<>();
    HashMap<Long, Long> streamLag = new HashMap<>();
    HashMap<Long, OrdinaryBatchOrderer.TrafficClass> streamClass = new HashMap<>();
    HashMap<Long, Long> streamLastSeenBatch = new HashMap<>();
    HashSet<Long> smallBurstDisarmed = new HashSet<>();
    OrdinaryBatchOrderer.GroupKey preferredGroupHead;
    long rootVirtualTime;
    long serviceSeq;
    long batchSeq;
    int interactiveStreak;
    int classSelectionsSinceBulk;

    private static void copyGroupLongMap(HashMap<OrdinaryBatchOrderer.GroupKey, Long> target,
                                         Map<OrdinaryBatchOrderer.GroupKey, Long> source,
                                         boolean filterTransient) {
        target.clear();
        if (source == null || source.isEmpty()) {
            return;
        }
        for (Map.Entry<OrdinaryBatchOrderer.GroupKey, Long> entry : source.entrySet()) {
            OrdinaryBatchOrderer.GroupKey key = entry.getKey();
            if (key == null || (filterTransient && isTransientGroupKey(key))) {
                continue;
            }
            target.put(key, entry.getValue());
        }
    }

    private static void copyStreamLongMap(HashMap<Long, Long> target, Map<Long, Long> source) {
        target.clear();
        if (source == null || source.isEmpty()) {
            return;
        }
        for (Map.Entry<Long, Long> entry : source.entrySet()) {
            Long key = entry.getKey();
            if (key == null || isSyntheticStreamKey(key)) {
                continue;
            }
            target.put(key, entry.getValue());
        }
    }

    private static void copyStreamClassMap(HashMap<Long, OrdinaryBatchOrderer.TrafficClass> target,
                                           Map<Long, OrdinaryBatchOrderer.TrafficClass> source) {
        target.clear();
        if (source == null || source.isEmpty()) {
            return;
        }
        for (Map.Entry<Long, OrdinaryBatchOrderer.TrafficClass> entry : source.entrySet()) {
            Long key = entry.getKey();
            OrdinaryBatchOrderer.TrafficClass value = entry.getValue();
            if (key == null || isSyntheticStreamKey(key) || value == null) {
                continue;
            }
            target.put(key, value);
        }
    }

    private static void copyStreamIdSet(HashSet<Long> target, Set<Long> source) {
        target.clear();
        if (source == null || source.isEmpty()) {
            return;
        }
        for (Long value : source) {
            if (value == null || isSyntheticStreamKey(value)) {
                continue;
            }
            target.add(value);
        }
    }

    private static boolean isTransientGroupKey(OrdinaryBatchOrderer.GroupKey groupKey) {
        return groupKey != null && groupKey.kind() == 2;
    }

    private static boolean isSyntheticStreamKey(long streamKey) {
        return streamKey <= 0L;
    }

    private static void rebaseGroupMap(Map<OrdinaryBatchOrderer.GroupKey, Long> values, long floor) {
        for (Map.Entry<OrdinaryBatchOrderer.GroupKey, Long> entry : values.entrySet()) {
            entry.setValue(entry.getValue() - floor);
        }
    }

    private static void rebaseLongMap(Map<Long, Long> values, long floor) {
        for (Map.Entry<Long, Long> entry : values.entrySet()) {
            entry.setValue(entry.getValue() - floor);
        }
    }

    void loadFrom(OrdinaryBatchRetainedState source) {
        clear();
        if (source == null) {
            return;
        }
        preferredGroupHead = source.preferredGroupHead;
        copyGroupLongMap(preferredStreamHeads, source.preferredStreamHeads, false);
        rootVirtualTime = source.rootVirtualTime;
        serviceSeq = source.serviceSeq;
        copyGroupLongMap(groupVirtualTime, source.groupVirtualTime, true);
        copyGroupLongMap(groupFinishTag, source.groupFinishTag, true);
        copyGroupLongMap(groupLastServed, source.groupLastServed, true);
        copyGroupLongMap(groupLag, source.groupLag, true);
        copyStreamLongMap(streamFinishTag, source.streamFinishTag);
        copyStreamLongMap(streamLastServed, source.streamLastServed);
        copyStreamLongMap(streamLag, source.streamLag);
        copyStreamClassMap(streamClass, source.streamClass);
        copyStreamLongMap(streamLastSeenBatch, source.streamLastSeenBatch);
        copyStreamIdSet(smallBurstDisarmed, source.smallBurstDisarmed);
        batchSeq = source.batchSeq;
        interactiveStreak = source.interactiveStreak;
        classSelectionsSinceBulk = source.classSelectionsSinceBulk;
    }

    void replace(OrdinaryBatchOrderer.GroupKey nextGroupHead,
                 Map<OrdinaryBatchOrderer.GroupKey, Long> nextStreamHeads,
                 long nextRootVirtualTime,
                 long nextServiceSeq,
                 Map<OrdinaryBatchOrderer.GroupKey, Long> nextGroupVirtualTime,
                 Map<OrdinaryBatchOrderer.GroupKey, Long> nextGroupFinishTag,
                 Map<OrdinaryBatchOrderer.GroupKey, Long> nextGroupLastServed,
                 Map<OrdinaryBatchOrderer.GroupKey, Long> nextGroupLag,
                 Map<Long, Long> nextStreamFinishTag,
                 Map<Long, Long> nextStreamLastServed,
                 Map<Long, Long> nextStreamLag,
                 Map<Long, OrdinaryBatchOrderer.TrafficClass> nextStreamClass,
                 Map<Long, Long> nextStreamLastSeenBatch,
                 Set<Long> nextSmallBurstDisarmed,
                 long nextBatchSeq,
                 int nextInteractiveStreak,
                 int nextClassSelectionsSinceBulk) {
        preferredGroupHead = nextGroupHead;
        copyGroupLongMap(preferredStreamHeads, nextStreamHeads, false);
        rootVirtualTime = nextRootVirtualTime;
        serviceSeq = nextServiceSeq;
        copyGroupLongMap(groupVirtualTime, nextGroupVirtualTime, true);
        copyGroupLongMap(groupFinishTag, nextGroupFinishTag, true);
        copyGroupLongMap(groupLastServed, nextGroupLastServed, true);
        copyGroupLongMap(groupLag, nextGroupLag, true);
        copyStreamLongMap(streamFinishTag, nextStreamFinishTag);
        copyStreamLongMap(streamLastServed, nextStreamLastServed);
        copyStreamLongMap(streamLag, nextStreamLag);
        copyStreamClassMap(streamClass, nextStreamClass);
        copyStreamLongMap(streamLastSeenBatch, nextStreamLastSeenBatch);
        copyStreamIdSet(smallBurstDisarmed, nextSmallBurstDisarmed);
        batchSeq = nextBatchSeq;
        interactiveStreak = nextInteractiveStreak;
        classSelectionsSinceBulk = nextClassSelectionsSinceBulk;
        maybeRebase();
    }

    void clear() {
        preferredGroupHead = null;
        preferredStreamHeads.clear();
        rootVirtualTime = 0L;
        serviceSeq = 0L;
        groupVirtualTime.clear();
        groupFinishTag.clear();
        groupLastServed.clear();
        groupLag.clear();
        streamFinishTag.clear();
        streamLastServed.clear();
        streamLag.clear();
        streamClass.clear();
        streamLastSeenBatch.clear();
        smallBurstDisarmed.clear();
        batchSeq = 0L;
        interactiveStreak = 0;
        classSelectionsSinceBulk = 0;
    }

    void release() {
        preferredGroupHead = null;
        preferredStreamHeads = new HashMap<>();
        groupVirtualTime = new HashMap<>();
        groupFinishTag = new HashMap<>();
        groupLastServed = new HashMap<>();
        groupLag = new HashMap<>();
        streamFinishTag = new HashMap<>();
        streamLastServed = new HashMap<>();
        streamLag = new HashMap<>();
        streamClass = new HashMap<>();
        streamLastSeenBatch = new HashMap<>();
        smallBurstDisarmed = new HashSet<>();
        rootVirtualTime = 0L;
        serviceSeq = 0L;
        batchSeq = 0L;
        interactiveStreak = 0;
        classSelectionsSinceBulk = 0;
    }

    void dropStream(long streamId) {
        if (streamId <= 0L) {
            return;
        }
        streamFinishTag.remove(streamId);
        streamLastServed.remove(streamId);
        streamLag.remove(streamId);
        streamClass.remove(streamId);
        streamLastSeenBatch.remove(streamId);
        smallBurstDisarmed.remove(streamId);
        dropGroupState(new OrdinaryBatchOrderer.GroupKey(0, streamId));
        scrubIdleState();
    }

    void dropExplicitGroup(long group) {
        if (group == 0L) {
            return;
        }
        dropGroupState(new OrdinaryBatchOrderer.GroupKey(1, group));
        scrubIdleState();
    }

    private void dropGroupState(OrdinaryBatchOrderer.GroupKey groupKey) {
        groupVirtualTime.remove(groupKey);
        groupFinishTag.remove(groupKey);
        groupLastServed.remove(groupKey);
        groupLag.remove(groupKey);
        preferredStreamHeads.remove(groupKey);
        if (groupKey.equals(preferredGroupHead)) {
            preferredGroupHead = null;
        }
    }

    private void scrubIdleState() {
        if (!groupVirtualTime.isEmpty()
                || !groupFinishTag.isEmpty()
                || !groupLastServed.isEmpty()
                || !groupLag.isEmpty()
                || !streamFinishTag.isEmpty()
                || !streamLastServed.isEmpty()
                || !streamLag.isEmpty()
                || !streamClass.isEmpty()
                || !streamLastSeenBatch.isEmpty()
                || !smallBurstDisarmed.isEmpty()
                || !preferredStreamHeads.isEmpty()
                || preferredGroupHead != null) {
            return;
        }
        release();
    }

    private void maybeRebase() {
        if (rootVirtualTime < REBASE_THRESHOLD) {
            return;
        }
        long floor = rootVirtualTime;
        for (long value : groupVirtualTime.values()) {
            if (value < floor) {
                floor = value;
            }
        }
        for (long value : groupFinishTag.values()) {
            if (value < floor) {
                floor = value;
            }
        }
        for (long value : streamFinishTag.values()) {
            if (value < floor) {
                floor = value;
            }
        }
        if (floor == 0L) {
            return;
        }
        rootVirtualTime -= floor;
        rebaseGroupMap(groupVirtualTime, floor);
        rebaseGroupMap(groupFinishTag, floor);
        rebaseLongMap(streamFinishTag, floor);
    }
}
