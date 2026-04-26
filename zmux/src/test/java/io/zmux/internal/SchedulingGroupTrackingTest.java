package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class SchedulingGroupTrackingTest {
    private static void makePeerVisible(SessionRuntime runtime, StreamRuntime stream) throws Exception {
        SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "beginLocalOpenLocked",
                new Class<?>[]{StreamRuntime.class},
                stream
        );
        runtime.markLocalStreamOpeningCommittedLocked(stream);
        SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "markPeerVisibleLocked",
                new Class<?>[]{StreamRuntime.class},
                stream
        );
    }

    @SuppressWarnings("unchecked")
    private static Map<Long, Long> activeExplicitGroupRefs(SessionRuntime runtime) throws Exception {
        Field trackerField = SessionRuntime.class.getDeclaredField("explicitGroupTracker");
        trackerField.setAccessible(true);
        Object tracker = trackerField.get(runtime);
        Field refsField = tracker.getClass().getDeclaredField("activeExplicitGroupRefs");
        refsField.setAccessible(true);
        return (Map<Long, Long>) refsField.get(tracker);
    }

    private static OrdinaryBatchOrderer.RetainedBias retainedBias(SessionRuntime runtime) throws Exception {
        Field biasField = SessionRuntime.class.getDeclaredField("ordinaryBatchBias");
        biasField.setAccessible(true);
        return (OrdinaryBatchOrderer.RetainedBias) biasField.get(runtime);
    }

    private static OrdinaryBatchOrderer.GroupKey groupKey(int kind, long value) {
        return new OrdinaryBatchOrderer.GroupKey(kind, value);
    }

    @Test
    void overflowExplicitGroupUsesFallbackBucket() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                Protocol.CAPABILITY_STREAM_GROUPS,
                Settings.builder()
                        .schedulerHints(SchedulerHint.GROUP_FAIR)
                        .maxFramePayload(16_384L)
                        .build()
        );

        for (long group = 1L; group <= 16L; ++group) {
            StreamRuntime stream = (StreamRuntime) runtime.openStream(new OpenOptions(0L, group, new byte[0]));
            synchronized (runtime.lock()) {
                makePeerVisible(runtime, stream);
            }
        }

        StreamRuntime overflow = (StreamRuntime) runtime.openStream(new OpenOptions(0L, 99L, new byte[0]));
        synchronized (runtime.lock()) {
            makePeerVisible(runtime, overflow);

            Map<Long, Long> refs = activeExplicitGroupRefs(runtime);
            assertTrue(overflow.schedulingGroupTrackedLocked(), "overflow explicit group should still be tracked");
            assertEquals(Long.MAX_VALUE, overflow.trackedSchedulingGroupLocked(), "overflow explicit group should be assigned to the fallback bucket");
            assertFalse(refs.containsKey(99L), "raw overflow group id should not be tracked directly once explicit group capacity is full");
            assertEquals(Long.valueOf(1L), refs.get(Long.MAX_VALUE), "fallback bucket refcount should be incremented");
        }
    }

    @Test
    void cancelWriteReleasesTrackedExplicitGroup() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                Protocol.CAPABILITY_STREAM_GROUPS,
                Settings.builder()
                        .schedulerHints(SchedulerHint.GROUP_FAIR)
                        .maxFramePayload(16_384L)
                        .build()
        );
        StreamRuntime stream = (StreamRuntime) runtime.openStream(new OpenOptions(0L, 7L, new byte[0]));

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
            assertEquals(Long.valueOf(1L), activeExplicitGroupRefs(runtime).get(7L), "explicit group ref should be tracked once the stream is assigned");
        }

        stream.cancelWrite(41L);

        synchronized (runtime.lock()) {
            assertFalse(stream.schedulingGroupTrackedLocked(), "send-terminal stream should drop explicit group tracking");
            assertFalse(activeExplicitGroupRefs(runtime).containsKey(7L), "last explicit group ref should be released on send terminal");
        }
    }

    @Test
    void groupChangePreservesSharedOldExplicitGroupRef() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                Protocol.CAPABILITY_STREAM_GROUPS | Protocol.CAPABILITY_PRIORITY_UPDATE,
                Settings.builder()
                        .schedulerHints(SchedulerHint.GROUP_FAIR)
                        .maxFramePayload(16_384L)
                        .build()
        );
        StreamRuntime first = (StreamRuntime) runtime.openStream(new OpenOptions(0L, 7L, new byte[0]));
        StreamRuntime second = (StreamRuntime) runtime.openStream(new OpenOptions(0L, 7L, new byte[0]));

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, first);
            makePeerVisible(runtime, second);
            assertEquals(Long.valueOf(2L), activeExplicitGroupRefs(runtime).get(7L), "two streams in the same explicit group should share the tracked refcount");
        }

        first.updateMetadata(new MetadataUpdate(null, 9L));

        synchronized (runtime.lock()) {
            Map<Long, Long> refs = activeExplicitGroupRefs(runtime);
            assertEquals(Long.valueOf(1L), refs.get(7L), "shared old explicit group should keep one remaining ref");
            assertEquals(Long.valueOf(1L), refs.get(9L), "new explicit group should be tracked immediately");
            assertEquals(9L, first.trackedSchedulingGroupLocked(), "rebucketed stream should track its new explicit group");
            assertEquals(7L, second.trackedSchedulingGroupLocked(), "sibling stream should retain the old explicit group bucket");
        }
    }

    @Test
    void explicitGroupRefcountDoesNotSaturateAtIntMax() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                Protocol.CAPABILITY_STREAM_GROUPS,
                Settings.builder()
                        .schedulerHints(SchedulerHint.GROUP_FAIR)
                        .maxFramePayload(16_384L)
                        .build()
        );
        Map<Long, Long> refs = activeExplicitGroupRefs(runtime);
        refs.put(7L, (long) Integer.MAX_VALUE);
        StreamRuntime stream = (StreamRuntime) runtime.openStream(new OpenOptions(0L, 7L, new byte[0]));

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);

            assertEquals(Integer.MAX_VALUE + 1L, refs.get(7L),
                    "explicit group refs must keep counting beyond the int boundary");
        }
    }

    @Test
    void lastExplicitGroupDepartureDropsRetainedOrdinaryBatchState() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                Protocol.CAPABILITY_STREAM_GROUPS | Protocol.CAPABILITY_PRIORITY_UPDATE,
                Settings.builder()
                        .schedulerHints(SchedulerHint.GROUP_FAIR)
                        .maxFramePayload(16_384L)
                        .build()
        );
        StreamRuntime stream = (StreamRuntime) runtime.openStream(new OpenOptions(0L, 7L, new byte[0]));

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
            retainedBias(runtime).state().groupVirtualTime.put(groupKey(1, 7L), 11L);
            retainedBias(runtime).state().groupVirtualTime.put(groupKey(1, 9L), 13L);
        }

        stream.updateMetadata(new MetadataUpdate(null, 9L));

        synchronized (runtime.lock()) {
            Map<OrdinaryBatchOrderer.GroupKey, Long> groupVirtualTime = retainedBias(runtime).state().groupVirtualTime;
            assertFalse(groupVirtualTime.containsKey(groupKey(1, 7L)), "last departing explicit group should drop retained ordinary-batch state immediately");
            assertTrue(groupVirtualTime.containsKey(groupKey(1, 9L)), "unrelated retained explicit-group state should remain");
        }
    }
}
