package io.zmux.internal;

import io.zmux.SchedulerHint;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.zmux.TestLists.listOf;
import static org.junit.jupiter.api.Assertions.*;

final class OrdinaryBatchOrdererTest {
    private static OrdinaryBatchOrderer.BatchFrame dataFrame(long streamId, long cost, long priority) {
        return new OrdinaryBatchOrderer.BatchFrame(streamId, true, false, false, cost, priority, null);
    }

    private static OrdinaryBatchOrderer.BatchFrame sessionScopedFrame() {
        return new OrdinaryBatchOrderer.BatchFrame(0L, false, false, false, 1L, 0L, null);
    }

    private static List<Long> streamOrder(List<OrdinaryBatchOrderer.BatchFrame> batch, int[] order) {
        ArrayList<Long> streamIds = new ArrayList<>(order.length);
        for (int index : order) {
            streamIds.add(batch.get(index).streamId());
        }
        return streamIds;
    }

    @Test
    void rotatesFlatBatchHeadAcrossBatches() {
        OrdinaryBatchOrderer.RetainedBias retainedBias = new OrdinaryBatchOrderer.RetainedBias();

        int[] first = OrdinaryBatchOrderer.order(
                listOf(dataFrame(4L, 1L, 0L), dataFrame(8L, 1L, 0L)),
                SchedulerHint.UNSPECIFIED_OR_BALANCED,
                16_384L,
                retainedBias
        );
        int[] second = OrdinaryBatchOrderer.order(
                listOf(dataFrame(4L, 1L, 0L), dataFrame(8L, 1L, 0L)),
                SchedulerHint.UNSPECIFIED_OR_BALANCED,
                16_384L,
                retainedBias
        );

        assertArrayEquals(new int[]{0, 1}, first, "first flat batch should preserve input head");
        assertArrayEquals(new int[]{1, 0}, second, "retained batch bias should rotate the flat head across batches");
    }

    @Test
    void reusableWorkspacePreservesRetainedBiasAcrossBatches() {
        OrdinaryBatchOrderer.RetainedBias retainedBias = new OrdinaryBatchOrderer.RetainedBias();
        OrdinaryBatchOrderer.Workspace workspace = new OrdinaryBatchOrderer.Workspace();

        int[] first = OrdinaryBatchOrderer.order(
                listOf(dataFrame(4L, 1L, 0L), dataFrame(8L, 1L, 0L)),
                SchedulerHint.UNSPECIFIED_OR_BALANCED,
                16_384L,
                retainedBias,
                workspace
        );
        int[] second = OrdinaryBatchOrderer.order(
                listOf(dataFrame(4L, 1L, 0L), dataFrame(8L, 1L, 0L)),
                SchedulerHint.UNSPECIFIED_OR_BALANCED,
                16_384L,
                retainedBias,
                workspace
        );

        assertArrayEquals(new int[]{0, 1}, first, "first batch should still preserve the input head");
        assertArrayEquals(new int[]{1, 0}, second, "reused workspace should not lose retained rotation state");
    }

    @Test
    void retainedStateSnapshotCopiesPreferredStreamHeads() {
        OrdinaryBatchOrderer.GroupKey groupKey = new OrdinaryBatchOrderer.GroupKey(1, 7L);
        OrdinaryBatchRetainedState source = new OrdinaryBatchRetainedState();
        OrdinaryBatchRetainedState snapshot = new OrdinaryBatchRetainedState();

        source.preferredGroupHead = groupKey;
        source.preferredStreamHeads.put(groupKey, 4L);

        snapshot.loadFrom(source);
        source.preferredStreamHeads.put(groupKey, 8L);

        assertEquals(groupKey, snapshot.preferredGroupHead, "snapshot should preserve the preferred group head");
        assertEquals(4L, snapshot.preferredStreamHeads.get(groupKey), "snapshot should copy preferred stream heads instead of aliasing source state");
    }

    @Test
    void retainedBiasReleasesMapStorageAfterLastStreamDrop() {
        OrdinaryBatchOrderer.RetainedBias retainedBias = new OrdinaryBatchOrderer.RetainedBias();
        OrdinaryBatchRetainedState state = retainedBias.state();
        OrdinaryBatchOrderer.GroupKey groupKey = new OrdinaryBatchOrderer.GroupKey(0, 4L);

        state.preferredGroupHead = groupKey;
        state.preferredStreamHeads.put(groupKey, 4L);
        state.groupVirtualTime.put(groupKey, 1L);
        state.groupFinishTag.put(groupKey, 2L);
        state.groupLastServed.put(groupKey, 3L);
        state.groupLag.put(groupKey, 4L);
        state.streamFinishTag.put(4L, 5L);
        state.streamLastServed.put(4L, 6L);
        state.streamLag.put(4L, 7L);
        state.streamClass.put(4L, OrdinaryBatchOrderer.TrafficClass.INTERACTIVE);
        state.streamLastSeenBatch.put(4L, 8L);
        state.smallBurstDisarmed.add(4L);
        state.rootVirtualTime = 9L;
        state.serviceSeq = 10L;
        state.batchSeq = 11L;
        state.interactiveStreak = 12;
        state.classSelectionsSinceBulk = 13;

        Map<?, ?> previousGroupVirtualTime = state.groupVirtualTime;
        Map<?, ?> previousStreamFinishTag = state.streamFinishTag;
        Set<?> previousSmallBurstDisarmed = state.smallBurstDisarmed;

        retainedBias.dropStream(4L);

        assertNotSame(previousGroupVirtualTime, state.groupVirtualTime,
                "idle retained group state should release old HashMap backing");
        assertNotSame(previousStreamFinishTag, state.streamFinishTag,
                "idle retained stream state should release old HashMap backing");
        assertNotSame(previousSmallBurstDisarmed, state.smallBurstDisarmed,
                "idle retained small-burst state should release old HashSet backing");
        assertTrue(state.groupVirtualTime.isEmpty(), "released retained group state should be empty");
        assertTrue(state.streamFinishTag.isEmpty(), "released retained stream state should be empty");
        assertNull(state.preferredGroupHead, "released retained state should clear preferred group head");
        assertEquals(0L, state.rootVirtualTime, "released retained state should reset root virtual time");
        assertEquals(0L, state.batchSeq, "released retained state should reset batch sequence");
    }

    @Test
    void orderViewReusesWorkspaceOrderedScratch() throws Exception {
        OrdinaryBatchOrderer.Workspace workspace = new OrdinaryBatchOrderer.Workspace();

        OrdinaryBatchOrderer.OrderView view = OrdinaryBatchOrderer.orderView(
                listOf(dataFrame(4L, 1L, 0L), dataFrame(8L, 1L, 20L)),
                SchedulerHint.UNSPECIFIED_OR_BALANCED,
                16_384L,
                null,
                workspace
        );

        Field orderedField = OrdinaryBatchOrderer.Workspace.class.getDeclaredField("ordered");
        orderedField.setAccessible(true);
        int[] workspaceOrdered = (int[]) orderedField.get(workspace);

        Field orderField = OrdinaryBatchOrderer.OrderView.class.getDeclaredField("order");
        orderField.setAccessible(true);

        assertFalse(view.isIdentity(), "priority-reordered batch should produce a workspace-backed non-identity view");
        assertSame(workspaceOrdered, orderField.get(view), "order view should reuse workspace ordered scratch instead of copying");
        assertEquals(1, view.indexAt(0), "higher-priority stream should lead the order view");
        assertEquals(0, view.indexAt(1), "lower-priority stream should trail the order view");
    }

    @Test
    void oversizedWorkspaceScratchShrinksAfterSmallerBatch() throws Exception {
        OrdinaryBatchOrderer.Workspace workspace = new OrdinaryBatchOrderer.Workspace();
        ArrayList<OrdinaryBatchOrderer.BatchFrame> largeBatch = new ArrayList<>();
        for (int i = 0; i < 1_025; ++i) {
            largeBatch.add(dataFrame(4L + (long) i * 4L, 64L, 0L));
        }

        OrdinaryBatchOrderer.order(
                largeBatch,
                SchedulerHint.UNSPECIFIED_OR_BALANCED,
                16_384L,
                null,
                workspace
        );

        Field orderedField = OrdinaryBatchOrderer.Workspace.class.getDeclaredField("ordered");
        orderedField.setAccessible(true);
        int firstOrderedLength = ((int[]) orderedField.get(workspace)).length;

        Field batchEntryPoolField = OrdinaryBatchOrderer.Workspace.class.getDeclaredField("batchEntryPool");
        batchEntryPoolField.setAccessible(true);
        int firstEntryPoolSize = ((ArrayList<?>) batchEntryPoolField.get(workspace)).size();

        OrdinaryBatchOrderer.order(
                listOf(dataFrame(4L, 1L, 0L), dataFrame(8L, 1L, 20L)),
                SchedulerHint.UNSPECIFIED_OR_BALANCED,
                16_384L,
                null,
                workspace
        );

        int secondOrderedLength = ((int[]) orderedField.get(workspace)).length;
        int secondEntryPoolSize = ((ArrayList<?>) batchEntryPoolField.get(workspace)).size();

        assertTrue(secondOrderedLength < firstOrderedLength, "smaller rebuild should drop oversized ordered scratch");
        assertTrue(secondEntryPoolSize < firstEntryPoolSize, "smaller rebuild should trim oversized batch-entry pool");
    }

    @Test
    void oversizedWorkspaceHashScratchDropsAfterSmallerBatch() throws Exception {
        OrdinaryBatchOrderer.Workspace workspace = new OrdinaryBatchOrderer.Workspace();
        OrdinaryBatchOrderer.RetainedBias retainedBias = new OrdinaryBatchOrderer.RetainedBias();
        int oversizedHint = 1_025;

        Field groupsField = OrdinaryBatchOrderer.Workspace.class.getDeclaredField("groups");
        groupsField.setAccessible(true);
        Map<?, ?> firstGroups = (Map<?, ?>) groupsField.get(workspace);

        Field explicitGroupsField = OrdinaryBatchOrderer.Workspace.class.getDeclaredField("explicitGroups");
        explicitGroupsField.setAccessible(true);
        Map<?, ?> firstExplicitGroups = (Map<?, ?>) explicitGroupsField.get(workspace);

        Field bypassSelectionsField = OrdinaryBatchOrderer.Workspace.class.getDeclaredField("bypassSelections");
        bypassSelectionsField.setAccessible(true);
        Object firstBypassSelections = bypassSelectionsField.get(workspace);

        Field counterSizeField = LongIntCounterMap.class.getDeclaredField("size");
        counterSizeField.setAccessible(true);

        Field nextPreferredStreamHeadsField = OrdinaryBatchOrderer.Workspace.class.getDeclaredField("nextPreferredStreamHeads");
        nextPreferredStreamHeadsField.setAccessible(true);
        Map<?, ?> firstNextPreferredStreamHeads = (Map<?, ?>) nextPreferredStreamHeadsField.get(workspace);

        Field recordedGroupHeadsField = OrdinaryBatchOrderer.Workspace.class.getDeclaredField("recordedGroupHeads");
        recordedGroupHeadsField.setAccessible(true);
        java.util.Set<?> firstRecordedGroupHeads = (java.util.Set<?>) recordedGroupHeadsField.get(workspace);

        @SuppressWarnings("unchecked")
        Map<OrdinaryBatchOrderer.GroupKey, Object> oversizedGroups = (Map<OrdinaryBatchOrderer.GroupKey, Object>) firstGroups;
        @SuppressWarnings("unchecked")
        Map<Long, OrdinaryBatchOrderer.GroupKey> oversizedExplicitGroups =
                (Map<Long, OrdinaryBatchOrderer.GroupKey>) firstExplicitGroups;
        @SuppressWarnings("unchecked")
        Map<OrdinaryBatchOrderer.GroupKey, Long> oversizedNextPreferredStreamHeads =
                (Map<OrdinaryBatchOrderer.GroupKey, Long>) firstNextPreferredStreamHeads;
        @SuppressWarnings("unchecked")
        java.util.Set<OrdinaryBatchOrderer.GroupKey> oversizedRecordedGroupHeads =
                (java.util.Set<OrdinaryBatchOrderer.GroupKey>) firstRecordedGroupHeads;
        LongIntCounterMap oversizedBypassSelections = (LongIntCounterMap) firstBypassSelections;

        for (int i = 0; i < oversizedHint; ++i) {
            OrdinaryBatchOrderer.GroupKey key = new OrdinaryBatchOrderer.GroupKey(0, 4L + (long) i * 4L);
            oversizedGroups.put(key, null);
            oversizedExplicitGroups.put((long) i + 1L, key);
            oversizedNextPreferredStreamHeads.put(key, 4L + (long) i * 4L);
            oversizedRecordedGroupHeads.add(key);
            oversizedBypassSelections.incrementSaturating(4L + (long) i * 4L);
        }

        Field lastBuildCapHintField = OrdinaryBatchOrderer.Workspace.class.getDeclaredField("lastBuildCapHint");
        lastBuildCapHintField.setAccessible(true);
        lastBuildCapHintField.setInt(workspace, oversizedHint);

        OrdinaryBatchOrderer.order(
                listOf(dataFrame(4L, 1L, 0L), dataFrame(8L, 1L, 20L)),
                SchedulerHint.UNSPECIFIED_OR_BALANCED,
                16_384L,
                retainedBias,
                workspace
        );

        Map<?, ?> secondGroups = (Map<?, ?>) groupsField.get(workspace);
        Map<?, ?> secondExplicitGroups = (Map<?, ?>) explicitGroupsField.get(workspace);
        Object secondBypassSelections = bypassSelectionsField.get(workspace);
        Map<?, ?> secondNextPreferredStreamHeads = (Map<?, ?>) nextPreferredStreamHeadsField.get(workspace);
        java.util.Set<?> secondRecordedGroupHeads = (java.util.Set<?>) recordedGroupHeadsField.get(workspace);

        assertNotSame(firstGroups, secondGroups, "smaller rebuild should replace oversized group scratch maps");
        assertNotSame(firstExplicitGroups, secondExplicitGroups, "smaller rebuild should replace oversized explicit-group scratch maps");
        assertNotSame(firstBypassSelections, secondBypassSelections, "smaller rebuild should replace oversized bypass-selection scratch");
        assertNotSame(firstNextPreferredStreamHeads, secondNextPreferredStreamHeads, "smaller rebuild should replace oversized next-head scratch");
        assertNotSame(firstRecordedGroupHeads, secondRecordedGroupHeads, "smaller rebuild should replace oversized recorded-head scratch");
    }

    @Test
    void reusableWorkspaceDoesNotLeakBatchTopologyAcrossBuilds() {
        OrdinaryBatchOrderer.Workspace workspace = new OrdinaryBatchOrderer.Workspace();

        OrdinaryBatchOrderer.order(
                listOf(
                        new OrdinaryBatchOrderer.BatchFrame(4L, true, false, false, 64L, 0L, 11L),
                        new OrdinaryBatchOrderer.BatchFrame(8L, true, false, false, 256L, 0L, 22L),
                        new OrdinaryBatchOrderer.BatchFrame(8L, true, true, false, 1L, 0L, 22L),
                        sessionScopedFrame()
                ),
                SchedulerHint.GROUP_FAIR,
                1_024L,
                null,
                workspace
        );

        int[] second = OrdinaryBatchOrderer.order(
                listOf(dataFrame(16L, 1L, 0L), dataFrame(32L, 1L, 0L)),
                SchedulerHint.UNSPECIFIED_OR_BALANCED,
                16_384L,
                null,
                workspace
        );

        assertArrayEquals(new int[]{0, 1}, second, "reused workspace should not leak old groups, streams, or entries into the next batch");
    }

    @Test
    void sessionScopedOrdinaryDoesNotConsumeRetainedFlatHead() {
        OrdinaryBatchOrderer.RetainedBias retainedBias = new OrdinaryBatchOrderer.RetainedBias();

        int[] first = OrdinaryBatchOrderer.order(
                listOf(dataFrame(4L, 1L, 0L), dataFrame(8L, 1L, 0L)),
                SchedulerHint.UNSPECIFIED_OR_BALANCED,
                16_384L,
                retainedBias
        );
        int[] second = OrdinaryBatchOrderer.order(
                listOf(sessionScopedFrame(), dataFrame(4L, 1L, 0L), dataFrame(8L, 1L, 0L)),
                SchedulerHint.UNSPECIFIED_OR_BALANCED,
                16_384L,
                retainedBias
        );

        assertArrayEquals(new int[]{0, 1}, first, "seed batch should establish the retained flat head");
        assertArrayEquals(new int[]{0, 2, 1}, second, "session-scoped ordinary work should not consume the retained real-stream head");
    }

    @Test
    void streamWeightLatencyHintStronglyBoostsShortQueues() {
        long maxPayload = 16_384L;

        long shortQueue = OrdinaryBatchOrderer.streamWeight(4L, 512L, SchedulerHint.LATENCY, maxPayload);
        long longQueue = OrdinaryBatchOrderer.streamWeight(4L, maxPayload * 8L, SchedulerHint.LATENCY, maxPayload);

        assertTrue(shortQueue > longQueue, "latency hint should give short queues a stronger boost than long queues");
    }

    @Test
    void streamWeightBulkHintFlattensPrioritySpread() {
        long maxPayload = 16_384L;

        long balancedLow = OrdinaryBatchOrderer.streamWeight(0L, maxPayload, SchedulerHint.BALANCED_FAIR, maxPayload);
        long balancedHigh = OrdinaryBatchOrderer.streamWeight(20L, maxPayload, SchedulerHint.BALANCED_FAIR, maxPayload);
        long bulkLow = OrdinaryBatchOrderer.streamWeight(0L, maxPayload, SchedulerHint.BULK_THROUGHPUT, maxPayload);
        long bulkHigh = OrdinaryBatchOrderer.streamWeight(20L, maxPayload, SchedulerHint.BULK_THROUGHPUT, maxPayload);

        assertTrue(
                balancedHigh - balancedLow > bulkHigh - bulkLow,
                "bulk throughput hint should flatten priority spread relative to balanced scheduling"
        );
    }

    @Test
    void adjustWeightForLagBoostsFreshAndUnderServedFlows() {
        long base = 24L;
        long window = OrdinaryBatchOrderer.feedbackWindow(SchedulerHint.BALANCED_FAIR, 16_384L);

        long boosted = OrdinaryBatchOrderer.adjustWeightForLag(base, window, window, false);
        long fresh = OrdinaryBatchOrderer.adjustWeightForLag(base, 0L, window, true);

        assertTrue(boosted > base, "positive lag should boost the effective scheduling weight");
        assertTrue(fresh > base, "fresh streams should get a one-shot weight boost");
    }

    @Test
    void adjustWeightForLagUsesWideMultiplyDivideNearLongLimit() {
        long base = Long.MAX_VALUE / 2L + 1L;

        long boosted = OrdinaryBatchOrderer.adjustWeightForLag(base, Long.MAX_VALUE, Long.MAX_VALUE, false);

        assertEquals(Long.MAX_VALUE, boosted, "lag scaling should not under-boost after intermediate multiplication overflow");
    }

    @Test
    void serviceTagUsesWideCeilMultiplyDivide() {
        long cost = Long.MAX_VALUE / 2L;

        long tag = OrdinaryBatchCandidateSelector.serviceTag(cost, 256L);

        assertEquals(cost, tag, "WFQ service tag should not shrink after intermediate multiplication overflow");
    }

    @Test
    void adjustWeightForLagPenalizesOverServedFlows() {
        long base = 24L;
        long window = OrdinaryBatchOrderer.feedbackWindow(SchedulerHint.BALANCED_FAIR, 16_384L);

        long penalized = OrdinaryBatchOrderer.adjustWeightForLag(base, -window, window, false);

        assertTrue(penalized < base, "negative lag should penalize over-served flows");
        assertTrue(penalized >= 1L, "effective scheduling weight should never drop below one");
    }

    @Test
    void freshPeerCanBeatStalePreferredHead() throws Exception {
        OrdinaryBatchOrderer.RetainedBias retainedBias = new OrdinaryBatchOrderer.RetainedBias();
        OrdinaryBatchOrderer.GroupKey staleGroup = new OrdinaryBatchOrderer.GroupKey(0, 4L);

        retainedBias.state().preferredGroupHead = staleGroup;
        retainedBias.state().groupVirtualTime.put(staleGroup, 0L);
        retainedBias.state().groupFinishTag.put(staleGroup, 0L);
        retainedBias.state().groupLastServed.put(staleGroup, 1L);
        retainedBias.state().streamFinishTag.put(4L, 0L);
        retainedBias.state().streamLastServed.put(4L, 1L);

        int[] order = OrdinaryBatchOrderer.order(
                listOf(dataFrame(4L, 1L, 0L), dataFrame(8L, 1L, 0L)),
                SchedulerHint.UNSPECIFIED_OR_BALANCED,
                16_384L,
                retainedBias
        );

        assertArrayEquals(new int[]{1, 0}, order, "fresh peer should beat a stale preferred head once lag/freshness shaping is applied");
    }

    @Test
    void mixedBatchRetainsOnlyRealStreamState() throws Exception {
        OrdinaryBatchOrderer.RetainedBias retainedBias = new OrdinaryBatchOrderer.RetainedBias();

        OrdinaryBatchOrderer.order(
                listOf(sessionScopedFrame(), dataFrame(4L, 1L, 0L), sessionScopedFrame()),
                SchedulerHint.UNSPECIFIED_OR_BALANCED,
                16_384L,
                retainedBias
        );

        Map<Long, Long> streamFinishTag = retainedBias.state().streamFinishTag;
        Map<Long, Long> streamLag = retainedBias.state().streamLag;

        assertEquals(1, streamFinishTag.size(), "retained finish tags should only keep one real stream entry");
        assertTrue(streamFinishTag.containsKey(4L), "retained finish tags should keep the real stream id");
        assertTrue(streamLag.keySet().stream().allMatch(id -> id > 0L), "retained lag should not keep synthetic stream keys");
        assertTrue(retainedBias.state().groupVirtualTime.keySet().stream().noneMatch(key -> key.kind() == 2), "retained group state should not keep transient session-scoped groups");
        assertTrue(retainedBias.state().groupLag.keySet().stream().noneMatch(key -> key.kind() == 2), "retained group lag should not keep transient session-scoped groups");
    }

    @Test
    void balancedSchedulerReservesBulkOpportunityWithinFourSelections() {
        List<OrdinaryBatchOrderer.BatchFrame> batch = listOf(
                dataFrame(4L, 64L, 0L),
                dataFrame(4L, 64L, 0L),
                dataFrame(4L, 64L, 0L),
                dataFrame(4L, 64L, 0L),
                dataFrame(8L, 900L, 0L),
                dataFrame(8L, 900L, 0L),
                dataFrame(8L, 900L, 0L),
                dataFrame(8L, 900L, 0L)
        );

        int[] order = OrdinaryBatchOrderer.order(
                batch,
                SchedulerHint.LATENCY,
                1_024L,
                new OrdinaryBatchOrderer.RetainedBias()
        );

        List<Long> streamOrder = streamOrder(batch, order);
        long bulkSelections = streamOrder.subList(0, 4).stream().filter(id -> id == 8L).count();
        assertTrue(bulkSelections >= 1L, "when interactive and bulk work coexist, the first four selections should include one bulk opportunity");
    }

    @Test
    void retainedClassHysteresisKeepsMidQueueBulkAcrossBatches() throws Exception {
        OrdinaryBatchOrderer.RetainedBias retainedBias = new OrdinaryBatchOrderer.RetainedBias();

        OrdinaryBatchOrderer.order(
                listOf(
                        dataFrame(8L, 64L, 0L),
                        dataFrame(4L, 900L, 0L),
                        dataFrame(4L, 900L, 0L),
                        dataFrame(4L, 900L, 0L)
                ),
                SchedulerHint.BULK_THROUGHPUT,
                1_024L,
                retainedBias
        );

        Map<Long, OrdinaryBatchOrderer.TrafficClass> classMap = retainedBias.state().streamClass;
        assertEquals(OrdinaryBatchOrderer.TrafficClass.BULK, classMap.get(4L), "queue above two interactive quanta should enter bulk");

        OrdinaryBatchOrderer.order(
                listOf(
                        dataFrame(4L, 768L, 0L),
                        dataFrame(4L, 768L, 0L),
                        dataFrame(8L, 64L, 0L)
                ),
                SchedulerHint.BULK_THROUGHPUT,
                1_024L,
                retainedBias
        );

        classMap = retainedBias.state().streamClass;
        assertEquals(OrdinaryBatchOrderer.TrafficClass.BULK, classMap.get(4L), "queue inside the hysteresis band should retain the previous bulk class");
        assertEquals(OrdinaryBatchOrderer.TrafficClass.INTERACTIVE, classMap.get(8L), "short peer burst should stay interactive");
    }
}
