package io.zmux.runtime;

import io.zmux.SchedulerHint;

import java.util.List;
import java.util.Map;
import java.util.Set;

final class OrdinaryBatchClassStateTracker {
    private OrdinaryBatchClassStateTracker() {
    }

    static void applyClassState(List<OrdinaryBatchOrderer.BatchGroup> groups,
                                SchedulerHint hint,
                                long interactiveQuantum,
                                long batchSeq,
                                Map<Long, OrdinaryBatchOrderer.TrafficClass> streamClass,
                                Map<Long, Long> streamLastSeenBatch,
                                Set<Long> smallBurstDisarmed) {
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
                if (!stream.streamScoped()) {
                    stream.setTrafficClass(OrdinaryBatchOrderer.TrafficClass.INTERACTIVE);
                    stream.setSmallBurstBonusArmed(false);
                    continue;
                }
                OrdinaryBatchOrderer.TrafficClass previous = streamClass.get(stream.streamKey());
                stream.setTrafficClass(
                        OrdinaryBatchSchedulingPolicy.classifyStream(stream, hint, interactiveQuantum, previous)
                );
                Long lastSeenBatch = streamLastSeenBatch.get(stream.streamKey());
                if (lastSeenBatch == null || batchSeq - lastSeenBatch >= 2L) {
                    smallBurstDisarmed.remove(stream.streamKey());
                }
                stream.setSmallBurstBonusArmed(!smallBurstDisarmed.contains(stream.streamKey()));
            }
        }
    }

    static void retainClassState(List<OrdinaryBatchOrderer.BatchGroup> groups,
                                 Map<Long, OrdinaryBatchOrderer.TrafficClass> streamClass,
                                 Map<Long, Long> streamLastSeenBatch,
                                 long batchSeq) {
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
                if (!stream.streamScoped()) {
                    continue;
                }
                streamClass.put(stream.streamKey(), stream.trafficClass());
                streamLastSeenBatch.put(stream.streamKey(), batchSeq);
            }
        }
    }

    static void updateBypassSelections(List<OrdinaryBatchOrderer.BatchGroup> groups,
                                       OrdinaryBatchOrderer.TrafficClass targetClass,
                                       long selectedStream,
                                       LongIntCounterMap bypassSelections) {
        if (groups == null || bypassSelections == null || targetClass == null) {
            return;
        }
        for (OrdinaryBatchOrderer.BatchGroup group : groups) {
            if (group == null) {
                continue;
            }
            List<OrdinaryBatchOrderer.BatchStreamState> streams = group.streamsInOrder();
            for (int i = 0; i < streams.size(); ++i) {
                OrdinaryBatchOrderer.BatchStreamState stream = streams.get(i);
                if (stream == null
                        || !stream.streamScoped()
                        || !stream.hasSelection()
                        || stream.trafficClass() != targetClass) {
                    continue;
                }
                long streamKey = stream.streamKey();
                if (streamKey == selectedStream) {
                    bypassSelections.put(streamKey, 0);
                    continue;
                }
                bypassSelections.incrementSaturating(streamKey);
            }
        }
    }
}
