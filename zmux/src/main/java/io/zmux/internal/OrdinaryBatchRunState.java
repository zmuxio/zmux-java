package io.zmux.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

final class OrdinaryBatchRunState {
    private final OrdinaryBatchOrderer.Workspace workspace;
    private final OrdinaryBatchRetainedState retainedState;
    private final long quantum;
    private final long batchSeq;
    private final LongIntCounterMap bypassSelections;
    private final int[] activeSelectionCounts;
    private final ArrayList<OrdinaryBatchOrderer.GroupCandidate> interactiveCandidates;
    private final ArrayList<OrdinaryBatchOrderer.GroupCandidate> bulkCandidates;
    private final HashMap<OrdinaryBatchOrderer.GroupKey, Long> nextPreferredStreamHeads;
    private final HashSet<OrdinaryBatchOrderer.GroupKey> recordedGroupHeads;
    private long rootVirtualTime;
    private long serviceSeq;
    private int interactiveStreak;
    private int classSelectionsSinceBulk;
    private OrdinaryBatchOrderer.GroupKey nextPreferredGroupHead;
    private boolean recordedBatchHead;
    private boolean seenRealOpportunity;
    private boolean transientHeadUsed;
    private boolean advisoryHeadArmed;

    OrdinaryBatchRunState(OrdinaryBatchOrderer.Workspace workspace,
                          OrdinaryBatchRetainedState retainedState,
                          boolean advisoryHeadArmed,
                          long quantum) {
        this.workspace = workspace;
        this.retainedState = retainedState;
        this.quantum = quantum;
        this.batchSeq = OrdinaryBatchSchedulingPolicy.saturatingAdd(retainedState.batchSeq, 1L);
        this.rootVirtualTime = retainedState.rootVirtualTime;
        this.serviceSeq = retainedState.serviceSeq;
        this.interactiveStreak = retainedState.interactiveStreak;
        this.classSelectionsSinceBulk = retainedState.classSelectionsSinceBulk;
        this.advisoryHeadArmed = advisoryHeadArmed;
        this.bypassSelections = workspace.bypassSelections();
        this.bypassSelections.clear();
        this.activeSelectionCounts = workspace.activeSelectionCounts();
        this.interactiveCandidates = workspace.interactiveCandidates();
        this.interactiveCandidates.clear();
        this.bulkCandidates = workspace.bulkCandidates();
        this.bulkCandidates.clear();
        this.nextPreferredStreamHeads = workspace.nextPreferredStreamHeads();
        this.nextPreferredStreamHeads.clear();
        this.recordedGroupHeads = workspace.recordedGroupHeads();
        this.recordedGroupHeads.clear();
    }

    private static int saturatingIncrement(int value) {
        return value == Integer.MAX_VALUE ? Integer.MAX_VALUE : value + 1;
    }

    long batchSeq() {
        return batchSeq;
    }

    long rootVirtualTime() {
        return rootVirtualTime;
    }

    long serviceSeq() {
        return serviceSeq;
    }

    int interactiveStreak() {
        return interactiveStreak;
    }

    int classSelectionsSinceBulk() {
        return classSelectionsSinceBulk;
    }

    boolean advisoryHeadArmed() {
        return advisoryHeadArmed;
    }

    LongIntCounterMap bypassSelections() {
        return bypassSelections;
    }

    int[] activeSelectionCounts() {
        return activeSelectionCounts;
    }

    ArrayList<OrdinaryBatchOrderer.GroupCandidate> interactiveCandidates() {
        return interactiveCandidates;
    }

    ArrayList<OrdinaryBatchOrderer.GroupCandidate> bulkCandidates() {
        return bulkCandidates;
    }

    OrdinaryBatchOrderer.GroupCandidatePair candidatePair() {
        return workspace.candidatePair();
    }

    OrdinaryBatchCandidateSelector.TopCandidateScratch topCandidateScratch() {
        return workspace.topCandidateScratch();
    }

    OrdinaryBatchRoundPlanner.RoundSelection roundSelection() {
        return workspace.roundSelection();
    }

    OrdinaryBatchOrderer.GroupKey nextPreferredGroupHead() {
        return nextPreferredGroupHead;
    }

    HashMap<OrdinaryBatchOrderer.GroupKey, Long> nextPreferredStreamHeads() {
        return nextPreferredStreamHeads;
    }

    boolean tryConsumeTransientHead(List<OrdinaryBatchOrderer.BatchGroup> groupsInOrder) {
        if (seenRealOpportunity || transientHeadUsed) {
            return false;
        }
        OrdinaryBatchOrderer.TransientHead transientHead =
                OrdinaryBatchCandidateSelector.pickNextTransientOrdinaryHead(groupsInOrder);
        if (transientHead == null) {
            return false;
        }
        workspace.addOrderedIndex(transientHead.entry().index());
        transientHead.stream().removeEntry(0);
        transientHead.stream().consumeCost(transientHead.entry().cost());
        transientHeadUsed = true;
        return true;
    }

    boolean consumeAdvisoryRetry() {
        if (!advisoryHeadArmed) {
            return false;
        }
        advisoryHeadArmed = false;
        return true;
    }

    void appendRemainingInInputOrder(int size) {
        workspace.appendRemainingInInputOrder(size);
    }

    void recordChosenHeads(OrdinaryBatchOrderer.GroupCandidate chosen,
                           List<OrdinaryBatchOrderer.BatchGroup> groupsInOrder) {
        if (!recordedBatchHead && chosen.streamState().streamScoped()) {
            nextPreferredGroupHead =
                    OrdinaryBatchCandidateSelector.nextRealGroupHead(groupsInOrder, chosen.group().order());
            recordedBatchHead = true;
        }
        if (chosen.streamState().streamScoped() && recordedGroupHeads.add(chosen.group().key())) {
            Long nextHead = OrdinaryBatchCandidateSelector.nextRealStreamHead(
                    chosen.group(),
                    chosen.streamState().order()
            );
            if (nextHead != null) {
                nextPreferredStreamHeads.put(chosen.group().key(), nextHead);
            }
        }
    }

    void applyChosen(List<OrdinaryBatchOrderer.BatchGroup> groupsInOrder,
                     OrdinaryBatchOrderer.GroupCandidate chosen,
                     List<OrdinaryBatchOrderer.GroupCandidate> candidates,
                     long totalGroupWeight,
                     boolean bothClassesActive,
                     long feedbackWindow) {
        workspace.addOrderedIndex(chosen.entry().index());
        OrdinaryBatchCandidateSelector.updateLagFeedback(
                chosen,
                candidates,
                totalGroupWeight,
                feedbackWindow,
                retainedState.groupLag,
                retainedState.streamLag
        );
        OrdinaryBatchOrderer.BatchStreamState streamState = chosen.streamState();
        OrdinaryBatchClassStateTracker.updateBypassSelections(
                groupsInOrder,
                chosen.trafficClass(),
                streamState.streamKey(),
                bypassSelections
        );
        if (streamState.streamScoped()
                && streamState.smallBurstBonusArmed()
                && streamState.remainingCost() <= quantum) {
            streamState.setSmallBurstBonusArmed(false);
            retainedState.smallBurstDisarmed.add(streamState.streamKey());
        }
        streamState.removeEntry(chosen.queuePos());

        long cost = OrdinaryBatchSchedulingPolicy.normalizeCost(chosen.entry().cost());
        streamState.consumeCost(cost);

        serviceSeq = OrdinaryBatchSchedulingPolicy.saturatingAdd(serviceSeq, 1L);
        long activeGroupWeight = Math.max(1L, totalGroupWeight);
        long currentRoot = Math.max(rootVirtualTime, chosen.groupStart());
        rootVirtualTime = OrdinaryBatchSchedulingPolicy.saturatingAdd(
                currentRoot,
                OrdinaryBatchCandidateSelector.serviceTag(cost, activeGroupWeight)
        );

        long currentGroupVirtual = retainedState.groupVirtualTime.getOrDefault(chosen.group().key(), 0L);
        currentGroupVirtual = Math.max(currentGroupVirtual, chosen.streamStart());
        retainedState.groupVirtualTime.put(
                chosen.group().key(),
                OrdinaryBatchSchedulingPolicy.saturatingAdd(
                        currentGroupVirtual,
                        OrdinaryBatchCandidateSelector.serviceTag(cost, Math.max(1L, chosen.totalStreamWeight()))
                )
        );
        retainedState.groupFinishTag.put(chosen.group().key(), chosen.groupFinish());
        retainedState.groupLastServed.put(chosen.group().key(), serviceSeq);
        retainedState.streamFinishTag.put(streamState.streamKey(), chosen.streamFinish());
        retainedState.streamLastServed.put(streamState.streamKey(), serviceSeq);

        if (streamState.streamScoped()) {
            if (chosen.trafficClass() == OrdinaryBatchOrderer.TrafficClass.BULK) {
                interactiveStreak = 0;
                classSelectionsSinceBulk = 0;
            } else {
                interactiveStreak = saturatingIncrement(interactiveStreak);
                classSelectionsSinceBulk = bothClassesActive ? saturatingIncrement(classSelectionsSinceBulk) : 0;
            }
            seenRealOpportunity = true;
        }

        if (chosen.entry().frame().priorityUpdate()) {
            advisoryHeadArmed = false;
        }
    }
}
