package io.zmux.internal;

import io.zmux.SchedulerHint;

import java.util.List;
import java.util.Map;

final class OrdinaryBatchCandidateSelector {
    private static final long WFQ_TAG_SCALE = 256L;
    private static final int AGING_ROUND_THRESHOLD = 2;

    private OrdinaryBatchCandidateSelector() {
    }

    static OrdinaryBatchOrderer.GroupCandidatePair topCandidates(OrdinaryBatchOrderer.BatchGroup group,
                                                                 SchedulerHint hint,
                                                                 long quantum,
                                                                 long preferredStreamHead,
                                                                 long rootVirtualTime,
                                                                 Map<OrdinaryBatchOrderer.GroupKey, Long> groupVirtualTime,
                                                                 Map<OrdinaryBatchOrderer.GroupKey, Long> groupFinishTag,
                                                                 Map<OrdinaryBatchOrderer.GroupKey, Long> groupLastServed,
                                                                 Map<OrdinaryBatchOrderer.GroupKey, Long> groupLag,
                                                                 Map<Long, Long> streamFinishTag,
                                                                 Map<Long, Long> streamLastServed,
                                                                 Map<Long, Long> streamLag,
                                                                 long feedbackWindow,
                                                                 int interactiveActiveStreams,
                                                                 int bulkActiveStreams,
                                                                 LongIntCounterMap bypassSelections,
                                                                 OrdinaryBatchOrderer.GroupCandidatePair out,
                                                                 TopCandidateScratch scratch) {
        if (out == null) {
            throw new NullPointerException("out");
        }
        if (scratch == null) {
            throw new NullPointerException("scratch");
        }
        if (group == null) {
            scratch.clearRetainedRefs();
            return out.clear();
        }

        scratch.reset();
        long interactiveBaseStreamWeight = 0L;
        long interactiveStreamWeight = 0L;
        long bulkBaseStreamWeight = 0L;
        long bulkStreamWeight = 0L;
        long groupVirtual = groupVirtualTime.getOrDefault(group.key(), 0L);
        List<OrdinaryBatchOrderer.BatchStreamState> streams = group.streamsInOrder();
        for (int i = 0; i < streams.size(); ++i) {
            OrdinaryBatchOrderer.BatchStreamState stream = streams.get(i);
            if (!stream.streamScoped()) {
                continue;
            }
            if (!stream.hasSelection()) {
                continue;
            }

            OrdinaryBatchOrderer.BatchEntry selected = stream.selectedEntry();
            long baseWeight = stream.selectedBaseWeight();
            long lagAdjustedWeight = OrdinaryBatchSchedulingPolicy.adjustWeightForLag(
                    baseWeight,
                    streamLag.getOrDefault(stream.streamKey(), 0L),
                    feedbackWindow,
                    isFreshStream(stream.streamKey(), streamFinishTag, streamLastServed, streamLag)
            );
            int activeClassCount = stream.trafficClass() == OrdinaryBatchOrderer.TrafficClass.BULK
                    ? bulkActiveStreams
                    : interactiveActiveStreams;
            boolean ageBoost = shouldApplyAging(stream, activeClassCount, bypassSelections);
            long effectiveWeight = OrdinaryBatchSchedulingPolicy.classAdjustedWeight(
                    stream,
                    baseWeight,
                    lagAdjustedWeight,
                    quantum,
                    ageBoost
            );
            long streamStart = Math.max(streamFinishTag.getOrDefault(stream.streamKey(), 0L), groupVirtual);
            long streamFinish = saturatingAdd(
                    streamStart,
                    serviceTag(selected.cost(), Math.max(1L, effectiveWeight))
            );
            StreamCandidate candidate = scratch.candidate.reset(
                    stream,
                    selected,
                    baseWeight,
                    effectiveWeight,
                    stream.selectedQueuePos(),
                    streamStart,
                    streamFinish,
                    streamLastServed.getOrDefault(stream.streamKey(), 0L),
                    streamStart <= groupVirtual
            );
            if (stream.trafficClass() == OrdinaryBatchOrderer.TrafficClass.BULK) {
                bulkBaseStreamWeight = saturatingAdd(bulkBaseStreamWeight, baseWeight);
                bulkStreamWeight = saturatingAdd(bulkStreamWeight, effectiveWeight);
                if (betterStreamCandidate(preferredStreamHead, candidate, scratch.bulkTop())) {
                    scratch.markBulkTop(candidate);
                }
                continue;
            }

            interactiveBaseStreamWeight = saturatingAdd(interactiveBaseStreamWeight, baseWeight);
            interactiveStreamWeight = saturatingAdd(interactiveStreamWeight, effectiveWeight);
            if (betterStreamCandidate(preferredStreamHead, candidate, scratch.interactiveTop())) {
                scratch.markInteractiveTop(candidate);
            }
        }

        return out.reset(
                buildGroupCandidate(
                        group,
                        scratch.interactiveTop(),
                        hint,
                        rootVirtualTime,
                        groupFinishTag,
                        groupLastServed,
                        groupLag,
                        feedbackWindow,
                        interactiveBaseStreamWeight,
                        interactiveStreamWeight
                ),
                buildGroupCandidate(
                        group,
                        scratch.bulkTop(),
                        hint,
                        rootVirtualTime,
                        groupFinishTag,
                        groupLastServed,
                        groupLag,
                        feedbackWindow,
                        bulkBaseStreamWeight,
                        bulkStreamWeight
                )
        );
    }

    static void refreshActiveSelections(List<OrdinaryBatchOrderer.BatchGroup> groups,
                                        boolean advisoryOnly,
                                        SchedulerHint hint,
                                        long quantum,
                                        int[] counts) {
        counts[0] = 0;
        counts[1] = 0;
        if (groups == null) {
            return;
        }
        for (OrdinaryBatchOrderer.BatchGroup group : groups) {
            if (group == null) {
                continue;
            }
            List<OrdinaryBatchOrderer.BatchStreamState> streams = group.streamsInOrder();
            for (int i = 0; i < streams.size(); ++i) {
                OrdinaryBatchOrderer.BatchStreamState stream = streams.get(i);
                if (stream != null) {
                    stream.clearSelection();
                }
                if (stream == null || !stream.streamScoped()) {
                    continue;
                }
                int selectedPos = selectStreamEntryPosition(stream, advisoryOnly);
                if (selectedPos < 0) {
                    continue;
                }
                stream.selectEntry(
                        stream.entries().get(selectedPos),
                        selectedPos,
                        OrdinaryBatchSchedulingPolicy.streamWeight(
                                stream.priority(),
                                stream.remainingCost(),
                                hint,
                                quantum
                        )
                );
                if (stream.trafficClass() == OrdinaryBatchOrderer.TrafficClass.BULK) {
                    counts[1]++;
                } else {
                    counts[0]++;
                }
            }
        }
    }

    static OrdinaryBatchOrderer.TransientHead pickNextTransientOrdinaryHead(List<OrdinaryBatchOrderer.BatchGroup> groups) {
        if (groups == null) {
            return null;
        }
        for (OrdinaryBatchOrderer.BatchGroup group : groups) {
            if (group == null || group.key().kind() != 2) {
                continue;
            }
            List<OrdinaryBatchOrderer.BatchStreamState> streams = group.streamsInOrder();
            for (int i = 0; i < streams.size(); ++i) {
                OrdinaryBatchOrderer.BatchStreamState stream = streams.get(i);
                if (stream.entries().isEmpty()) {
                    continue;
                }
                return new OrdinaryBatchOrderer.TransientHead(group, stream, stream.entries().get(0));
            }
        }
        return null;
    }

    static OrdinaryBatchOrderer.GroupKey nextRealGroupHead(List<OrdinaryBatchOrderer.BatchGroup> groups, int selectedOrder) {
        if (groups == null || groups.size() < 2 || selectedOrder < 0) {
            return null;
        }
        for (int offset = 1; offset < groups.size(); ++offset) {
            OrdinaryBatchOrderer.BatchGroup next = groups.get((selectedOrder + offset) % groups.size());
            if (next != null && next.key().kind() != 2) {
                return next.key();
            }
        }
        return null;
    }

    static Long nextRealStreamHead(OrdinaryBatchOrderer.BatchGroup group, int selectedOrder) {
        if (group == null || group.streamsInOrder().size() < 2 || selectedOrder < 0) {
            return null;
        }
        List<OrdinaryBatchOrderer.BatchStreamState> streams = group.streamsInOrder();
        int streamCount = streams.size();
        int bestOffset = Integer.MAX_VALUE;
        Long bestStreamKey = null;
        for (int i = 0; i < streamCount; ++i) {
            OrdinaryBatchOrderer.BatchStreamState stream = streams.get(i);
            if (stream == null || !stream.streamScoped()) {
                continue;
            }
            int offset = stream.order() - selectedOrder;
            if (offset <= 0) {
                offset += streamCount;
            }
            if (offset > 0 && offset < bestOffset) {
                bestOffset = offset;
                bestStreamKey = stream.streamKey();
            }
        }
        return bestStreamKey;
    }

    static OrdinaryBatchOrderer.GroupCandidate bestCandidate(OrdinaryBatchOrderer.GroupKey preferredGroupHead,
                                                             List<OrdinaryBatchOrderer.GroupCandidate> candidates) {
        OrdinaryBatchOrderer.GroupCandidate best = null;
        if (candidates == null) {
            return null;
        }
        for (OrdinaryBatchOrderer.GroupCandidate candidate : candidates) {
            if (OrdinaryBatchCandidateComparator.betterGroupCandidate(preferredGroupHead, candidate, best)) {
                best = candidate;
            }
        }
        return best;
    }

    static void updateLagFeedback(OrdinaryBatchOrderer.GroupCandidate chosen,
                                  List<OrdinaryBatchOrderer.GroupCandidate> candidates,
                                  long totalGroupWeight,
                                  long feedbackWindow,
                                  Map<OrdinaryBatchOrderer.GroupKey, Long> groupLag,
                                  Map<Long, Long> streamLag) {
        if (chosen == null || candidates == null || candidates.isEmpty() || feedbackWindow <= 0L) {
            return;
        }

        long cost = OrdinaryBatchSchedulingPolicy.normalizeCost(chosen.entry().cost());
        long effectiveTotalGroupWeight = Math.max(1L, totalGroupWeight);
        for (OrdinaryBatchOrderer.GroupCandidate candidate : candidates) {
            if (candidate == null || candidate.groupWeight() <= 0L) {
                continue;
            }
            long expected = fairShare(cost, candidate.baseGroupWeight(), effectiveTotalGroupWeight);
            long actual = candidate.group().key().equals(chosen.group().key()) ? cost : 0L;
            long nextLag = applyLagFeedback(
                    groupLag.getOrDefault(candidate.group().key(), 0L),
                    expected,
                    actual,
                    feedbackWindow
            );
            groupLag.put(candidate.group().key(), nextLag);
        }

        long effectiveTotalBaseStreamWeight = Math.max(1L, chosen.totalBaseStreamWeight());
        List<OrdinaryBatchOrderer.BatchStreamState> streams = chosen.group().streamsInOrder();
        for (int i = 0; i < streams.size(); ++i) {
            OrdinaryBatchOrderer.BatchStreamState stream = streams.get(i);
            if (stream.trafficClass() != chosen.trafficClass()) {
                continue;
            }
            if (!stream.streamScoped() || !stream.hasSelection()) {
                continue;
            }
            long expected = fairShare(cost, stream.selectedBaseWeight(), effectiveTotalBaseStreamWeight);
            long actual = stream.streamKey() == chosen.streamState().streamKey() ? cost : 0L;
            long nextLag = applyLagFeedback(
                    streamLag.getOrDefault(stream.streamKey(), 0L),
                    expected,
                    actual,
                    feedbackWindow
            );
            streamLag.put(stream.streamKey(), nextLag);
        }
    }

    static long serviceTag(long cost, long weight) {
        long normalizedCost = OrdinaryBatchSchedulingPolicy.normalizeCost(cost);
        long effectiveWeight = Math.max(1L, weight);
        return Math.max(1L, RuntimeFlow.saturatingMulDivCeil(normalizedCost, WFQ_TAG_SCALE, effectiveWeight));
    }

    private static OrdinaryBatchOrderer.GroupCandidate buildGroupCandidate(OrdinaryBatchOrderer.BatchGroup group,
                                                                           StreamCandidate top,
                                                                           SchedulerHint hint,
                                                                           long rootVirtualTime,
                                                                           Map<OrdinaryBatchOrderer.GroupKey, Long> groupFinishTag,
                                                                           Map<OrdinaryBatchOrderer.GroupKey, Long> groupLastServed,
                                                                           Map<OrdinaryBatchOrderer.GroupKey, Long> groupLag,
                                                                           long feedbackWindow,
                                                                           long totalBaseStreamWeight,
                                                                           long totalStreamWeight) {
        if (group == null || top == null) {
            return null;
        }

        long baseGroupWeight = OrdinaryBatchSchedulingPolicy.groupWeight(group.key(), top.baseWeight(), hint);
        long effectiveGroupWeight = OrdinaryBatchSchedulingPolicy.adjustWeightForLag(
                baseGroupWeight,
                groupLag.getOrDefault(group.key(), 0L),
                feedbackWindow,
                isFreshGroup(group.key(), groupFinishTag, groupLastServed, groupLag)
        );
        long groupStart = Math.max(groupFinishTag.getOrDefault(group.key(), 0L), rootVirtualTime);
        long groupFinish = saturatingAdd(
                groupStart,
                serviceTag(top.entry().cost(), Math.max(1L, effectiveGroupWeight))
        );
        return new OrdinaryBatchOrderer.GroupCandidate(
                group,
                top.stream(),
                top.entry(),
                top.baseWeight(),
                Math.max(1L, baseGroupWeight),
                top.queuePos(),
                Math.max(1L, effectiveGroupWeight),
                Math.max(1L, totalBaseStreamWeight),
                Math.max(1L, totalStreamWeight),
                top.streamStart(),
                top.streamFinish(),
                top.streamLastServed(),
                groupStart,
                groupFinish,
                groupLastServed.getOrDefault(group.key(), 0L),
                groupStart <= rootVirtualTime && top.eligible()
        );
    }

    private static int selectStreamEntryPosition(OrdinaryBatchOrderer.BatchStreamState stream,
                                                 boolean advisoryOnly) {
        if (stream == null || stream.entries().isEmpty()) {
            return -1;
        }

        OrdinaryBatchOrderer.BatchEntry head = stream.entries().get(0);
        if (head.frame().openingFrame()) {
            if (advisoryOnly && !head.frame().priorityUpdate()) {
                return -1;
            }
            return 0;
        }

        int priorityPos = firstPriorityUpdatePos(stream.entries());
        if (priorityPos >= 0) {
            return priorityPos;
        }
        if (advisoryOnly) {
            return -1;
        }
        return 0;
    }

    private static int firstPriorityUpdatePos(List<OrdinaryBatchOrderer.BatchEntry> entries) {
        for (int i = 0; i < entries.size(); ++i) {
            if (entries.get(i).frame().priorityUpdate()) {
                return i;
            }
        }
        return -1;
    }

    private static boolean betterStreamCandidate(long preferredStreamHead, StreamCandidate candidate, StreamCandidate currentBest) {
        if (candidate == null) {
            return false;
        }
        if (currentBest == null) {
            return true;
        }
        if (candidate.eligible() != currentBest.eligible()) {
            return candidate.eligible();
        }
        if (candidate.eligible()) {
            if (candidate.streamFinish() != currentBest.streamFinish()) {
                return candidate.streamFinish() < currentBest.streamFinish();
            }
            if (candidate.streamStart() != currentBest.streamStart()) {
                return candidate.streamStart() < currentBest.streamStart();
            }
        } else {
            if (candidate.streamStart() != currentBest.streamStart()) {
                return candidate.streamStart() < currentBest.streamStart();
            }
            if (candidate.streamFinish() != currentBest.streamFinish()) {
                return candidate.streamFinish() < currentBest.streamFinish();
            }
        }
        boolean candidatePreferred = preferredStreamHead != 0L && candidate.stream().streamKey() == preferredStreamHead;
        boolean currentBestPreferred = preferredStreamHead != 0L && currentBest.stream().streamKey() == preferredStreamHead;
        if (candidatePreferred != currentBestPreferred) {
            return candidatePreferred;
        }
        if (candidate.streamLastServed() != currentBest.streamLastServed()) {
            return candidate.streamLastServed() < currentBest.streamLastServed();
        }
        return candidate.stream().order() < currentBest.stream().order();
    }

    private static boolean shouldApplyAging(OrdinaryBatchOrderer.BatchStreamState stream,
                                            int activeClassStreams,
                                            LongIntCounterMap bypassSelections) {
        if (stream == null || !stream.streamScoped() || activeClassStreams <= 1 || bypassSelections == null) {
            return false;
        }
        int threshold = AGING_ROUND_THRESHOLD * activeClassStreams;
        return bypassSelections.getOrDefault(stream.streamKey(), 0) >= threshold;
    }

    private static long fairShare(long cost, long weight, long totalWeight) {
        if (cost <= 0L || weight <= 0L || totalWeight <= 0L) {
            return 0L;
        }
        return RuntimeFlow.saturatingMulDivCeil(cost, weight, totalWeight);
    }

    private static long clampLag(long value, long window) {
        if (window <= 0L) {
            return 0L;
        }
        long limit = window <= Long.MAX_VALUE / 2L ? window * 2L : Long.MAX_VALUE;
        return Math.max(-limit, Math.min(value, limit));
    }

    static long applyLagFeedback(long current, long expected, long actual, long window) {
        if (window <= 0L) {
            return 0L;
        }
        if (expected >= actual) {
            long delta = expected - actual;
            if (current > Long.MAX_VALUE - delta) {
                return clampLag(Long.MAX_VALUE, window);
            }
            return clampLag(current + delta, window);
        }
        long delta = actual - expected;
        long floor = -Long.MAX_VALUE;
        if (current < floor + delta) {
            return clampLag(floor, window);
        }
        return clampLag(current - delta, window);
    }

    private static boolean isFreshStream(long streamKey,
                                         Map<Long, Long> streamFinishTag,
                                         Map<Long, Long> streamLastServed,
                                         Map<Long, Long> streamLag) {
        if (isSyntheticStreamKey(streamKey)) {
            return false;
        }
        return !streamFinishTag.containsKey(streamKey)
                && !streamLastServed.containsKey(streamKey)
                && !streamLag.containsKey(streamKey);
    }

    private static boolean isFreshGroup(OrdinaryBatchOrderer.GroupKey groupKey,
                                        Map<OrdinaryBatchOrderer.GroupKey, Long> groupFinishTag,
                                        Map<OrdinaryBatchOrderer.GroupKey, Long> groupLastServed,
                                        Map<OrdinaryBatchOrderer.GroupKey, Long> groupLag) {
        if (isTransientGroupKey(groupKey)) {
            return false;
        }
        return !groupFinishTag.containsKey(groupKey)
                && !groupLastServed.containsKey(groupKey)
                && !groupLag.containsKey(groupKey);
    }

    private static boolean isTransientGroupKey(OrdinaryBatchOrderer.GroupKey groupKey) {
        return groupKey != null && groupKey.kind() == 2;
    }

    private static boolean isSyntheticStreamKey(long streamKey) {
        return streamKey <= 0L;
    }

    private static long saturatingAdd(long left, long right) {
        return OrdinaryBatchSchedulingPolicy.saturatingAdd(left, right);
    }

    static final class TopCandidateScratch {
        private final StreamCandidate candidate = new StreamCandidate();
        private final StreamCandidate interactiveTop = new StreamCandidate();
        private final StreamCandidate bulkTop = new StreamCandidate();
        private boolean hasInteractiveTop;
        private boolean hasBulkTop;

        private void reset() {
            hasInteractiveTop = false;
            hasBulkTop = false;
        }

        void clearRetainedRefs() {
            candidate.clearRetainedRefs();
            interactiveTop.clearRetainedRefs();
            bulkTop.clearRetainedRefs();
            hasInteractiveTop = false;
            hasBulkTop = false;
        }

        private StreamCandidate interactiveTop() {
            return hasInteractiveTop ? interactiveTop : null;
        }

        private StreamCandidate bulkTop() {
            return hasBulkTop ? bulkTop : null;
        }

        private void markInteractiveTop(StreamCandidate source) {
            interactiveTop.copyFrom(source);
            hasInteractiveTop = true;
        }

        private void markBulkTop(StreamCandidate source) {
            bulkTop.copyFrom(source);
            hasBulkTop = true;
        }
    }

    private static final class StreamCandidate {
        private OrdinaryBatchOrderer.BatchStreamState stream;
        private OrdinaryBatchOrderer.BatchEntry entry;
        private long baseWeight;
        private long weight;
        private int queuePos;
        private long streamStart;
        private long streamFinish;
        private long streamLastServed;
        private boolean eligible;

        private StreamCandidate reset(OrdinaryBatchOrderer.BatchStreamState stream,
                                      OrdinaryBatchOrderer.BatchEntry entry,
                                      long baseWeight,
                                      long weight,
                                      int queuePos,
                                      long streamStart,
                                      long streamFinish,
                                      long streamLastServed,
                                      boolean eligible) {
            this.stream = stream;
            this.entry = entry;
            this.baseWeight = baseWeight;
            this.weight = weight;
            this.queuePos = queuePos;
            this.streamStart = streamStart;
            this.streamFinish = streamFinish;
            this.streamLastServed = streamLastServed;
            this.eligible = eligible;
            return this;
        }

        private void clearRetainedRefs() {
            this.stream = null;
            this.entry = null;
            this.baseWeight = 0L;
            this.weight = 0L;
            this.queuePos = 0;
            this.streamStart = 0L;
            this.streamFinish = 0L;
            this.streamLastServed = 0L;
            this.eligible = false;
        }

        private void copyFrom(StreamCandidate source) {
            this.stream = source.stream;
            this.entry = source.entry;
            this.baseWeight = source.baseWeight;
            this.weight = source.weight;
            this.queuePos = source.queuePos;
            this.streamStart = source.streamStart;
            this.streamFinish = source.streamFinish;
            this.streamLastServed = source.streamLastServed;
            this.eligible = source.eligible;
        }

        private OrdinaryBatchOrderer.BatchStreamState stream() {
            return stream;
        }

        private OrdinaryBatchOrderer.BatchEntry entry() {
            return entry;
        }

        private long baseWeight() {
            return baseWeight;
        }

        private long weight() {
            return weight;
        }

        private int queuePos() {
            return queuePos;
        }

        private long streamStart() {
            return streamStart;
        }

        private long streamFinish() {
            return streamFinish;
        }

        private long streamLastServed() {
            return streamLastServed;
        }

        private boolean eligible() {
            return eligible;
        }
    }
}
