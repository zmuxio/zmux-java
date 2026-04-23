package io.zmux.internal;

import io.zmux.SchedulerHint;

import java.util.Collections;
import java.util.List;
import java.util.Map;

final class OrdinaryBatchRoundPlanner {
    private OrdinaryBatchRoundPlanner() {
    }

    static RoundSelection plan(List<OrdinaryBatchOrderer.BatchGroup> groupsInOrder,
                               SchedulerHint hint,
                               long quantum,
                               OrdinaryBatchRetainedState retainedState,
                               long feedbackWindow,
                               OrdinaryBatchRunState runState) {
        List<OrdinaryBatchOrderer.GroupCandidate> interactiveCandidates = runState.interactiveCandidates();
        List<OrdinaryBatchOrderer.GroupCandidate> bulkCandidates = runState.bulkCandidates();
        interactiveCandidates.clear();
        bulkCandidates.clear();
        long interactiveGroupWeight = 0L;
        long bulkGroupWeight = 0L;
        OrdinaryBatchCandidateSelector.refreshActiveSelections(
                groupsInOrder,
                runState.advisoryHeadArmed(),
                hint,
                quantum,
                runState.activeSelectionCounts()
        );
        int interactiveActiveStreams = runState.activeSelectionCounts()[0];
        int bulkActiveStreams = runState.activeSelectionCounts()[1];
        for (OrdinaryBatchOrderer.BatchGroup group : groupsInOrder) {
            OrdinaryBatchOrderer.GroupCandidatePair candidatePair = OrdinaryBatchCandidateSelector.topCandidates(
                    group,
                    hint,
                    quantum,
                    preferredStreamHead(retainedState.preferredStreamHeads, group.key()),
                    runState.rootVirtualTime(),
                    retainedState.groupVirtualTime,
                    retainedState.groupFinishTag,
                    retainedState.groupLastServed,
                    retainedState.groupLag,
                    retainedState.streamFinishTag,
                    retainedState.streamLastServed,
                    retainedState.streamLag,
                    feedbackWindow,
                    interactiveActiveStreams,
                    bulkActiveStreams,
                    runState.bypassSelections()
            );
            OrdinaryBatchOrderer.GroupCandidate interactiveCandidate = candidatePair.interactive();
            if (interactiveCandidate != null) {
                interactiveGroupWeight = OrdinaryBatchSchedulingPolicy.saturatingAdd(
                        interactiveGroupWeight,
                        interactiveCandidate.groupWeight()
                );
                interactiveCandidates.add(interactiveCandidate);
            }
            OrdinaryBatchOrderer.GroupCandidate bulkCandidate = candidatePair.bulk();
            if (bulkCandidate != null) {
                bulkGroupWeight = OrdinaryBatchSchedulingPolicy.saturatingAdd(
                        bulkGroupWeight,
                        bulkCandidate.groupWeight()
                );
                bulkCandidates.add(bulkCandidate);
            }
        }

        if (interactiveCandidates.isEmpty() && bulkCandidates.isEmpty()) {
            return RoundSelection.empty();
        }

        OrdinaryBatchOrderer.GroupCandidate interactiveBest =
                OrdinaryBatchCandidateSelector.bestCandidate(retainedState.preferredGroupHead, interactiveCandidates);
        OrdinaryBatchOrderer.GroupCandidate bulkBest =
                OrdinaryBatchCandidateSelector.bestCandidate(retainedState.preferredGroupHead, bulkCandidates);
        OrdinaryBatchOrderer.TrafficClass selectedClass = OrdinaryBatchSchedulingPolicy.chooseClass(
                retainedState.preferredGroupHead,
                hint,
                interactiveBest,
                bulkBest,
                runState.interactiveStreak(),
                runState.classSelectionsSinceBulk()
        );
        if (selectedClass == OrdinaryBatchOrderer.TrafficClass.INTERACTIVE && interactiveBest == null && bulkBest != null) {
            selectedClass = OrdinaryBatchOrderer.TrafficClass.BULK;
        } else if (selectedClass == OrdinaryBatchOrderer.TrafficClass.BULK && bulkBest == null && interactiveBest != null) {
            selectedClass = OrdinaryBatchOrderer.TrafficClass.INTERACTIVE;
        }

        List<OrdinaryBatchOrderer.GroupCandidate> candidates;
        long totalGroupWeight;
        if (selectedClass == OrdinaryBatchOrderer.TrafficClass.INTERACTIVE) {
            candidates = interactiveCandidates;
            totalGroupWeight = interactiveGroupWeight;
        } else {
            candidates = bulkCandidates;
            totalGroupWeight = bulkGroupWeight;
        }
        boolean bothClassesActive = interactiveBest != null && bulkBest != null;
        OrdinaryBatchOrderer.GroupCandidate best = null;
        for (OrdinaryBatchOrderer.GroupCandidate candidate : candidates) {
            if (OrdinaryBatchCandidateComparator.betterGroupCandidate(retainedState.preferredGroupHead, candidate, best)) {
                best = candidate;
            }
        }

        OrdinaryBatchOrderer.GroupCandidate chosen = best;

        return new RoundSelection(chosen, candidates, totalGroupWeight, bothClassesActive);
    }

    private static long preferredStreamHead(Map<OrdinaryBatchOrderer.GroupKey, Long> preferredStreamHeads,
                                            OrdinaryBatchOrderer.GroupKey groupKey) {
        if (groupKey == null || preferredStreamHeads == null || preferredStreamHeads.isEmpty()) {
            return 0L;
        }
        Long preferred = preferredStreamHeads.get(groupKey);
        return preferred == null ? 0L : preferred;
    }

    static final class RoundSelection {
        private final OrdinaryBatchOrderer.GroupCandidate chosen;
        private final List<OrdinaryBatchOrderer.GroupCandidate> candidates;
        private final long totalGroupWeight;
        private final boolean bothClassesActive;

        RoundSelection(OrdinaryBatchOrderer.GroupCandidate chosen,
                       List<OrdinaryBatchOrderer.GroupCandidate> candidates,
                       long totalGroupWeight,
                       boolean bothClassesActive) {
            this.chosen = chosen;
            this.candidates = candidates;
            this.totalGroupWeight = totalGroupWeight;
            this.bothClassesActive = bothClassesActive;
        }

        static RoundSelection empty() {
            return new RoundSelection(null, Collections.emptyList(), 0L, false);
        }

        OrdinaryBatchOrderer.GroupCandidate chosen() {
            return chosen;
        }

        List<OrdinaryBatchOrderer.GroupCandidate> candidates() {
            return candidates;
        }

        long totalGroupWeight() {
            return totalGroupWeight;
        }

        boolean bothClassesActive() {
            return bothClassesActive;
        }

        boolean hasCandidates() {
            return !candidates.isEmpty();
        }
    }
}
