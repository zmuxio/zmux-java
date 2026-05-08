package io.zmux;


import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class OrdinaryBatchBuildPlanner {
    private OrdinaryBatchBuildPlanner() {
    }

    static OrdinaryBatchOrderer.BatchBuild build(List<OrdinaryBatchOrderer.BatchFrame> batch,
                                                 SchedulerHint hint,
                                                 OrdinaryBatchOrderer.Workspace workspace) {
        workspace.prepareBuildScratch(batch == null ? 0 : batch.size());
        if (batch == null) {
            batch = Collections.emptyList();
        }
        int batchSize = batch.size();
        LinkedHashMap<OrdinaryBatchOrderer.GroupKey, OrdinaryBatchOrderer.BatchGroup> groups = workspace.groups();
        groups.clear();
        List<OrdinaryBatchOrderer.BatchGroup> groupsInOrder = workspace.groupsInOrder();
        groupsInOrder.clear();
        Map<Long, OrdinaryBatchOrderer.GroupKey> explicitGroups = workspace.explicitGroups();
        explicitGroups.clear();
        boolean hasRealStreamScoped = false;
        boolean hasPriorityUpdate = false;
        long syntheticStreamKey = Long.MIN_VALUE;
        for (int index = 0; index < batchSize; ++index) {
            OrdinaryBatchOrderer.BatchFrame frame = batch.get(index);
            boolean streamScoped = frame.streamScoped() && frame.streamId() != 0L;
            if (streamScoped) {
                hasRealStreamScoped = true;
            }
            if (frame.priorityUpdate()) {
                hasPriorityUpdate = true;
            }

            long streamKey = streamScoped ? frame.streamId() : syntheticStreamKey++;
            OrdinaryBatchOrderer.GroupKey groupKey = streamScoped
                    ? OrdinaryBatchSchedulingPolicy.streamGroupKey(frame, hint, explicitGroups, streamKey)
                    : new OrdinaryBatchOrderer.GroupKey(2, streamKey);

            OrdinaryBatchOrderer.BatchGroup group = groups.get(groupKey);
            if (group == null) {
                group = workspace.nextBatchGroup(groupKey, groups.size());
                groups.put(groupKey, group);
                groupsInOrder.add(group);
            }

            OrdinaryBatchOrderer.BatchStreamState stream = group.streams().get(streamKey);
            if (stream == null) {
                stream = workspace.nextBatchStream(
                        streamKey,
                        streamScoped,
                        frame.priority(),
                        group.streamsInOrder().size()
                );
                group.addStream(streamKey, stream);
            }

            long cost = OrdinaryBatchSchedulingPolicy.normalizeCost(frame.cost());
            stream.entries().add(workspace.nextBatchEntry(index, frame, cost));
            stream.addRemainingCost(cost);
        }
        return workspace.batchBuild().reset(hasRealStreamScoped, hasPriorityUpdate);
    }
}
