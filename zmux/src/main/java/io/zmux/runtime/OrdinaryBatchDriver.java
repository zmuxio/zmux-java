package io.zmux.runtime;

import io.zmux.SchedulerHint;

import java.util.List;

final class OrdinaryBatchDriver {
    private OrdinaryBatchDriver() {
    }

    static OrdinaryBatchOrderer.OrderView order(OrdinaryBatchOrderer.Workspace workspace,
                                                OrdinaryBatchOrderer.BatchBuild build,
                                                SchedulerHint schedulerHint,
                                                long quantum,
                                                long feedbackWindow,
                                                OrdinaryBatchOrderer.RetainedBias retainedBias,
                                                int size) {
        List<OrdinaryBatchOrderer.BatchGroup> groupsInOrder = build.groupsInOrder();
        workspace.loadRetainedBias(retainedBias);
        OrdinaryBatchRetainedState retainedState = workspace.retainedState();
        boolean retainedRealState = retainedState.hasRetainedRealState();
        if (!retainedRealState) {
            retainedState.scrubIdleRetainedState();
        }
        if (!build.hasRealStreamScoped()) {
            if (retainedBias != null && !retainedRealState) {
                retainedState.release();
                retainedBias.state().release();
            }
            return OrdinaryBatchOrderer.OrderView.identity(size);
        }

        workspace.resetOrderScratch(size);
        OrdinaryBatchRunState runState = new OrdinaryBatchRunState(
                workspace,
                retainedState,
                build.hasPriorityUpdate(),
                quantum
        );
        OrdinaryBatchClassStateTracker.applyClassState(
                groupsInOrder,
                schedulerHint,
                quantum,
                runState.batchSeq(),
                retainedState.streamClass,
                retainedState.streamLastSeenBatch,
                retainedState.smallBurstDisarmed
        );

        while (workspace.orderedSize() < size) {
            if (runState.tryConsumeTransientHead(groupsInOrder)) {
                continue;
            }

            OrdinaryBatchRoundPlanner.RoundSelection round = OrdinaryBatchRoundPlanner.plan(
                    groupsInOrder,
                    schedulerHint,
                    quantum,
                    retainedState,
                    feedbackWindow,
                    runState
            );

            if (!round.hasCandidates()) {
                if (runState.consumeAdvisoryRetry()) {
                    continue;
                }
                runState.appendRemainingInInputOrder(size);
                break;
            }

            OrdinaryBatchOrderer.GroupCandidate chosen = round.chosen();
            if (chosen == null) {
                runState.appendRemainingInInputOrder(size);
                break;
            }

            runState.recordChosenHeads(chosen, groupsInOrder);
            runState.applyChosen(
                    groupsInOrder,
                    chosen,
                    round.candidates(),
                    round.totalGroupWeight(),
                    round.bothClassesActive(),
                    feedbackWindow
            );
        }

        if (retainedBias != null) {
            OrdinaryBatchClassStateTracker.retainClassState(
                    groupsInOrder,
                    retainedState.streamClass,
                    retainedState.streamLastSeenBatch,
                    runState.batchSeq()
            );
            retainedBias.replace(
                    runState.nextPreferredGroupHead(),
                    runState.nextPreferredStreamHeads(),
                    runState.rootVirtualTime(),
                    runState.serviceSeq(),
                    retainedState.groupVirtualTime,
                    retainedState.groupFinishTag,
                    retainedState.groupLastServed,
                    retainedState.groupLag,
                    retainedState.streamFinishTag,
                    retainedState.streamLastServed,
                    retainedState.streamLag,
                    retainedState.streamClass,
                    retainedState.streamLastSeenBatch,
                    retainedState.smallBurstDisarmed,
                    runState.batchSeq(),
                    runState.interactiveStreak(),
                    runState.classSelectionsSinceBulk()
            );
        }

        return workspace.toOrderView(size);
    }
}
