package io.zmux.internal;

import io.zmux.SchedulerHint;

import java.util.HashMap;
import java.util.Map;

final class SessionExplicitGroupTracker {
    private Map<Long, Integer> activeExplicitGroupRefs = new HashMap<>();
    private Map<Long, Integer> ordinaryBatchExplicitGroupRefs = new HashMap<>();
    private final OrdinaryBatchOrderer.RetainedBias ordinaryBatchBias;
    private final int maxExplicitGroups;
    private final long fallbackGroupBucket;

    SessionExplicitGroupTracker(OrdinaryBatchOrderer.RetainedBias ordinaryBatchBias,
                                int maxExplicitGroups,
                                long fallbackGroupBucket) {
        this.ordinaryBatchBias = ordinaryBatchBias;
        this.maxExplicitGroups = maxExplicitGroups;
        this.fallbackGroupBucket = fallbackGroupBucket;
    }

    private static boolean schedulerTracksExplicitGroups(SchedulerHint schedulerHint) {
        return schedulerHint == SchedulerHint.GROUP_FAIR;
    }

    private static int saturatingIncrement(int value) {
        return value == Integer.MAX_VALUE ? Integer.MAX_VALUE : value + 1;
    }

    private static void incrementRefLocked(Map<Long, Integer> refs, long group) {
        refs.merge(group, 1, (current, ignored) -> saturatingIncrement(current));
    }

    private static boolean countsOrdinaryBatchExplicitGroupLocked(StreamRuntime streamRuntime,
                                                                  SchedulerHint schedulerHint,
                                                                  boolean streamRegistered) {
        return streamRuntime != null
                && schedulerTracksExplicitGroups(schedulerHint)
                && streamRuntime.idAssigned()
                && streamRegistered;
    }

    private static boolean tracksExplicitGroupLocked(StreamRuntime streamRuntime, SchedulerHint schedulerHint) {
        return streamRuntime != null
                && schedulerTracksExplicitGroups(schedulerHint)
                && streamRuntime.idAssigned()
                && streamRuntime.localSend()
                && !streamRuntime.sendTerminalLocked()
                && streamRuntime.groupLocked() != null
                && streamRuntime.groupLocked() != 0L;
    }

    void trackNewStreamLocked(StreamRuntime streamRuntime, SchedulerHint schedulerHint, boolean streamRegistered) {
        trackOrdinaryBatchExplicitGroupLocked(streamRuntime, schedulerHint, streamRegistered);
        maybeTrackStreamGroupLocked(streamRuntime, schedulerHint);
    }

    void onOutboundSchedulingGroupChangedLocked(StreamRuntime streamRuntime,
                                                Long previousGroup,
                                                SchedulerHint schedulerHint,
                                                boolean streamRegistered) {
        if (streamRuntime == null) {
            return;
        }
        untrackOrdinaryBatchExplicitGroupLocked(previousGroup, schedulerHint);
        trackOrdinaryBatchExplicitGroupLocked(streamRuntime, schedulerHint, streamRegistered);
        untrackStreamGroupLocked(streamRuntime);
        maybeTrackStreamGroupLocked(streamRuntime, schedulerHint);
    }

    void onStreamSendTerminalLocked(StreamRuntime streamRuntime) {
        untrackStreamGroupLocked(streamRuntime);
    }

    void dropOrdinaryBatchStateLocked(StreamRuntime streamRuntime, SchedulerHint schedulerHint) {
        if (streamRuntime == null) {
            return;
        }
        untrackOrdinaryBatchExplicitGroupLocked(streamRuntime.groupLocked(), schedulerHint);
        untrackStreamGroupLocked(streamRuntime);
        ordinaryBatchBias.dropStream(streamRuntime.streamIdInternal());
    }

    Long outboundSchedulingGroupLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null) {
            return null;
        }
        long trackedBucket = trackedGroupBucketLocked(streamRuntime);
        if (trackedBucket != 0L) {
            return trackedBucket;
        }
        return streamRuntime.groupLocked();
    }

    Map<Long, Integer> activeExplicitGroupRefsView() {
        return activeExplicitGroupRefs;
    }

    void clear() {
        activeExplicitGroupRefs = new HashMap<>();
        ordinaryBatchExplicitGroupRefs = new HashMap<>();
    }

    private void trackOrdinaryBatchExplicitGroupLocked(StreamRuntime streamRuntime,
                                                       SchedulerHint schedulerHint,
                                                       boolean streamRegistered) {
        if (!countsOrdinaryBatchExplicitGroupLocked(streamRuntime, schedulerHint, streamRegistered)) {
            return;
        }
        Long group = streamRuntime.groupLocked();
        if (group == null || group == 0L) {
            return;
        }
        incrementRefLocked(ordinaryBatchExplicitGroupRefs, group);
    }

    private void untrackOrdinaryBatchExplicitGroupLocked(Long group, SchedulerHint schedulerHint) {
        if (!schedulerTracksExplicitGroups(schedulerHint) || group == null || group == 0L) {
            return;
        }
        decrementExplicitGroupRefLocked(ordinaryBatchExplicitGroupRefs, group);
    }

    private int trackedExplicitGroupCountLocked() {
        if (activeExplicitGroupRefs.isEmpty()) {
            return 0;
        }
        int count = activeExplicitGroupRefs.size();
        if (activeExplicitGroupRefs.containsKey(0L)) {
            --count;
        }
        if (activeExplicitGroupRefs.containsKey(fallbackGroupBucket)) {
            --count;
        }
        return Math.max(0, count);
    }

    private long trackedGroupBucketLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null) {
            return 0L;
        }
        long tracked = streamRuntime.trackedSchedulingGroupLocked();
        if (tracked != 0L) {
            return tracked;
        }
        Long group = streamRuntime.groupLocked();
        if (streamRuntime.schedulingGroupTrackedLocked() && group != null && group != 0L) {
            return group;
        }
        return 0L;
    }

    private long selectTrackedGroupBucketLocked(StreamRuntime streamRuntime, SchedulerHint schedulerHint) {
        if (!tracksExplicitGroupLocked(streamRuntime, schedulerHint)) {
            return 0L;
        }
        long tracked = trackedGroupBucketLocked(streamRuntime);
        if (tracked != 0L) {
            return tracked;
        }
        Long group = streamRuntime.groupLocked();
        if (group == null || group == 0L) {
            return 0L;
        }
        if (activeExplicitGroupRefs.getOrDefault(group, 0) > 0) {
            return group;
        }
        if (trackedExplicitGroupCountLocked() < maxExplicitGroups) {
            return group;
        }
        return fallbackGroupBucket;
    }

    private void maybeTrackStreamGroupLocked(StreamRuntime streamRuntime, SchedulerHint schedulerHint) {
        if (streamRuntime == null || streamRuntime.schedulingGroupTrackedLocked()) {
            return;
        }
        long groupBucket = selectTrackedGroupBucketLocked(streamRuntime, schedulerHint);
        if (groupBucket == 0L) {
            return;
        }
        trackExplicitGroupLocked(groupBucket);
        streamRuntime.markSchedulingGroupTrackedLocked(groupBucket);
    }

    private void untrackStreamGroupLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null || !streamRuntime.schedulingGroupTrackedLocked()) {
            return;
        }
        long groupBucket = trackedGroupBucketLocked(streamRuntime);
        streamRuntime.clearSchedulingGroupTrackedLocked();
        if (groupBucket != 0L) {
            untrackExplicitGroupLocked(groupBucket);
        }
    }

    private void trackExplicitGroupLocked(long groupBucket) {
        if (groupBucket == 0L) {
            return;
        }
        incrementRefLocked(activeExplicitGroupRefs, groupBucket);
    }

    private void untrackExplicitGroupLocked(long groupBucket) {
        if (groupBucket == 0L) {
            return;
        }
        decrementExplicitGroupRefLocked(activeExplicitGroupRefs, groupBucket);
    }

    private void decrementExplicitGroupRefLocked(Map<Long, Integer> refs, long group) {
        Integer current = refs.get(group);
        if (current == null) {
            return;
        }
        if (current > 1) {
            refs.put(group, current - 1);
            return;
        }
        refs.remove(group);
        ordinaryBatchBias.dropExplicitGroup(group);
    }
}
