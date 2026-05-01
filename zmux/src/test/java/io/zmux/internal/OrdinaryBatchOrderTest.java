package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.zmux.TestLists.listOf;
import static org.junit.jupiter.api.Assertions.*;

final class OrdinaryBatchOrderTest {
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
    private static List<Object> collectReadyBatch(SessionRuntime runtime) throws Exception {
        return (List<Object>) SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "collectReadyBatchLocked",
                new Class<?>[0]
        );
    }

    private static List<Long> batchStreamIds(List<Object> batch) throws Exception {
        List<Long> streamIds = new ArrayList<>(batch.size());
        for (Object outbound : batch) {
            FrameCodec.Frame frame = SessionRuntimeTestSupport.outboundFrame(outbound);
            if (frame.type() == FrameType.DATA || frame.type() == FrameType.EXT) {
                streamIds.add(frame.streamId());
            }
        }
        return streamIds;
    }

    private static List<String> batchPayloads(List<Object> batch) throws Exception {
        List<String> payloads = new ArrayList<>(batch.size());
        for (Object outbound : batch) {
            FrameCodec.Frame frame = SessionRuntimeTestSupport.outboundFrame(outbound);
            if (frame.type() != FrameType.DATA) {
                continue;
            }
            payloads.add(new String(SessionRuntimeTestSupport.outboundPayload(outbound), StandardCharsets.UTF_8));
        }
        return payloads;
    }

    private static OrdinaryBatchOrderer.BatchFrame streamDataFrame(long streamId,
                                                                   long priority,
                                                                   Long group,
                                                                   long cost) {
        return new OrdinaryBatchOrderer.BatchFrame(streamId, true, false, false, cost, priority, group);
    }

    private static OrdinaryBatchOrderer.BatchFrame ordinarySessionExtFrame(long cost) {
        return new OrdinaryBatchOrderer.BatchFrame(0L, false, false, false, cost, 0L, null);
    }

    private static SessionRuntime.OutboundFrame streamExtFrame(long streamId, long subtype) throws Exception {
        return new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.EXT, 0, streamId, Varint62.encode(subtype)),
                null,
                0,
                false,
                false
        );
    }

    private static boolean priorityUpdateFrame(SessionRuntime.OutboundFrame outboundFrame) throws Exception {
        Method method = SessionWriterBatchOrderer.class.getDeclaredMethod(
                "priorityUpdateFrame",
                SessionRuntime.OutboundFrame.class
        );
        method.setAccessible(true);
        return (Boolean) method.invoke(null, outboundFrame);
    }

    private static List<Integer> toList(int[] order) {
        List<Integer> values = new ArrayList<>(order.length);
        for (int index : order) {
            values.add(index);
        }
        return values;
    }

    @Test
    void streamScopedExtPriorityClassificationUsesSubtype() throws Exception {
        assertTrue(
                priorityUpdateFrame(streamExtFrame(4L, Protocol.EXT_PRIORITY_UPDATE)),
                "PRIORITY_UPDATE ext frames should keep priority-update ordering"
        );
        assertFalse(
                priorityUpdateFrame(streamExtFrame(4L, 99L)),
                "unknown stream-scoped EXT frames must not be ordered as PRIORITY_UPDATE"
        );
        assertFalse(
                priorityUpdateFrame(streamExtFrame(0L, Protocol.EXT_PRIORITY_UPDATE)),
                "session-scoped EXT frames are not stream priority updates"
        );
        assertFalse(
                priorityUpdateFrame(new SessionRuntime.OutboundFrame(
                        new FrameCodec.Frame(FrameType.EXT, 0, 4L, new byte[]{0x40}),
                        null,
                        0,
                        false,
                        false
                )),
                "malformed EXT payloads must not be promoted to priority-update ordering"
        );
    }

    @Test
    void higherPriorityStreamLeadsOrdinaryBatch() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                Protocol.CAPABILITY_PRIORITY_HINTS,
                Settings.builder().maxFramePayload(16_384L).build()
        );
        StreamRuntime low = (StreamRuntime) runtime.openStream(new OpenOptions(0L, null, new byte[0]));
        StreamRuntime high = (StreamRuntime) runtime.openStream(new OpenOptions(20L, null, new byte[0]));

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, low);
            makePeerVisible(runtime, high);
            SessionRuntimeTestSupport.queueWrite(low, "low".getBytes(StandardCharsets.UTF_8));
            SessionRuntimeTestSupport.queueWrite(high, "high".getBytes(StandardCharsets.UTF_8));

            List<Object> batch = collectReadyBatch(runtime);
            assertEquals(listOf(high.streamIdInternal(), low.streamIdInternal()), batchStreamIds(batch), "higher-priority stream should lead the ordinary batch even when queued later");
            assertEquals(listOf("high", "low"), batchPayloads(batch), "reordered batch should preserve the chosen stream payloads");
        }
    }

    @Test
    void groupFairRotatesDistinctExplicitGroupsBeforeSecondMember() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                Protocol.CAPABILITY_STREAM_GROUPS,
                Settings.builder()
                        .maxFramePayload(16_384L)
                        .schedulerHints(SchedulerHint.GROUP_FAIR)
                        .build()
        );
        StreamRuntime firstGroupMember = (StreamRuntime) runtime.openStream(new OpenOptions(0L, 7L, new byte[0]));
        StreamRuntime secondGroupMember = (StreamRuntime) runtime.openStream(new OpenOptions(0L, 7L, new byte[0]));
        StreamRuntime otherGroup = (StreamRuntime) runtime.openStream(new OpenOptions(0L, 9L, new byte[0]));

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, firstGroupMember);
            makePeerVisible(runtime, secondGroupMember);
            makePeerVisible(runtime, otherGroup);
            SessionRuntimeTestSupport.queueWrite(firstGroupMember, "a".getBytes(StandardCharsets.UTF_8));
            SessionRuntimeTestSupport.queueWrite(secondGroupMember, "b".getBytes(StandardCharsets.UTF_8));
            SessionRuntimeTestSupport.queueWrite(otherGroup, "c".getBytes(StandardCharsets.UTF_8));

            List<Object> batch = collectReadyBatch(runtime);
            assertEquals(
                    listOf(firstGroupMember.streamIdInternal(), otherGroup.streamIdInternal(), secondGroupMember.streamIdInternal()),
                    batchStreamIds(batch),
                    "group_fair batches should rotate a different explicit group before serving a second member of the same group"
            );
            assertEquals(listOf("a", "c", "b"), batchPayloads(batch), "group_fair reordering should preserve per-stream FIFO payload order");
        }
    }

    @Test
    void flatBatchHeadRotatesAcrossBatches() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.builder().maxFramePayload(16_384L).build());
        StreamRuntime first = (StreamRuntime) runtime.openStream();
        StreamRuntime second = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, first);
            makePeerVisible(runtime, second);

            SessionRuntimeTestSupport.queueWrite(first, "a".getBytes(StandardCharsets.UTF_8));
            SessionRuntimeTestSupport.queueWrite(second, "b".getBytes(StandardCharsets.UTF_8));
            List<Object> firstBatch = collectReadyBatch(runtime);
            assertEquals(
                    listOf(first.streamIdInternal(), second.streamIdInternal()),
                    batchStreamIds(firstBatch),
                    "first flat batch should keep the initial head"
            );

            SessionRuntimeTestSupport.queueWrite(first, "c".getBytes(StandardCharsets.UTF_8));
            SessionRuntimeTestSupport.queueWrite(second, "d".getBytes(StandardCharsets.UTF_8));
            List<Object> secondBatch = collectReadyBatch(runtime);
            assertEquals(
                    listOf(second.streamIdInternal(), first.streamIdInternal()),
                    batchStreamIds(secondBatch),
                    "second flat batch should rotate the retained batch head"
            );
        }
    }

    @Test
    void ordinaryBatchInputScratchReusesBatchFrameObjects() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.builder().maxFramePayload(16_384L).build());
        StreamRuntime first = (StreamRuntime) runtime.openStream();
        StreamRuntime second = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, first);
            makePeerVisible(runtime, second);
            SessionRuntimeTestSupport.queueWrite(first, "a".getBytes(StandardCharsets.UTF_8));
            SessionRuntimeTestSupport.queueWrite(second, "b".getBytes(StandardCharsets.UTF_8));

            Object readyBatch = SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "collectReadyBatchStateLocked",
                    new Class<?>[]{boolean.class, boolean.class},
                    false,
                    false
            );
            @SuppressWarnings("unchecked")
            List<Object> frames = (List<Object>) SessionRuntimeTestSupport.invokePrivate(
                    readyBatch,
                    "frames",
                    new Class<?>[0]
            );

            java.lang.reflect.Field writerRuntimeField = SessionRuntime.class.getDeclaredField("writerRuntime");
            writerRuntimeField.setAccessible(true);
            Object writerRuntime = writerRuntimeField.get(runtime);

            SessionRuntimeTestSupport.invokePrivate(
                    writerRuntime,
                    "orderOrdinaryBatchLocked",
                    new Class<?>[]{List.class},
                    frames
            );

            java.lang.reflect.Field batchOrdererField = writerRuntime.getClass().getDeclaredField("batchOrderer");
            batchOrdererField.setAccessible(true);
            Object batchOrderer = batchOrdererField.get(writerRuntime);

            java.lang.reflect.Field ordinaryOrderItemsField = batchOrderer.getClass().getDeclaredField("ordinaryOrderItems");
            ordinaryOrderItemsField.setAccessible(true);
            @SuppressWarnings("unchecked")
            List<Object> firstItems = (List<Object>) ordinaryOrderItemsField.get(batchOrderer);
            Object firstBatchFrame0 = firstItems.get(0);
            Object firstBatchFrame1 = firstItems.get(1);
            java.lang.reflect.Field ordinaryOrderWindowField =
                    batchOrderer.getClass().getDeclaredField("ordinaryOrderWindow");
            ordinaryOrderWindowField.setAccessible(true);
            Object firstOrderWindow = ordinaryOrderWindowField.get(batchOrderer);
            java.lang.reflect.Field ordinaryOrderWorkspaceField =
                    batchOrderer.getClass().getDeclaredField("ordinaryOrderWorkspace");
            ordinaryOrderWorkspaceField.setAccessible(true);
            Object ordinaryOrderWorkspace = ordinaryOrderWorkspaceField.get(batchOrderer);
            java.lang.reflect.Field candidatePairField =
                    ordinaryOrderWorkspace.getClass().getDeclaredField("candidatePair");
            candidatePairField.setAccessible(true);
            Object firstCandidatePair = candidatePairField.get(ordinaryOrderWorkspace);
            java.lang.reflect.Field topCandidateScratchField =
                    ordinaryOrderWorkspace.getClass().getDeclaredField("topCandidateScratch");
            topCandidateScratchField.setAccessible(true);
            Object firstTopCandidateScratch = topCandidateScratchField.get(ordinaryOrderWorkspace);
            java.lang.reflect.Field roundSelectionField =
                    ordinaryOrderWorkspace.getClass().getDeclaredField("roundSelection");
            roundSelectionField.setAccessible(true);
            Object firstRoundSelection = roundSelectionField.get(ordinaryOrderWorkspace);

            SessionRuntimeTestSupport.invokePrivate(
                    writerRuntime,
                    "orderOrdinaryBatchLocked",
                    new Class<?>[]{List.class},
                    frames
            );

            @SuppressWarnings("unchecked")
            List<Object> secondItems = (List<Object>) ordinaryOrderItemsField.get(batchOrderer);
            assertSame(firstBatchFrame0, secondItems.get(0), "ordinary batch scratch should reuse the first BatchFrame object");
            assertSame(firstBatchFrame1, secondItems.get(1), "ordinary batch scratch should reuse the second BatchFrame object");
            assertSame(firstOrderWindow, ordinaryOrderWindowField.get(batchOrderer),
                    "ordinary batch scratch should reuse its bounded order input view");
            assertSame(firstCandidatePair, candidatePairField.get(ordinaryOrderWorkspace),
                    "ordinary batch scratch should reuse its candidate pair object");
            assertSame(firstTopCandidateScratch, topCandidateScratchField.get(ordinaryOrderWorkspace),
                    "ordinary batch scratch should reuse its top-candidate scratch object");
            assertSame(firstRoundSelection, roundSelectionField.get(ordinaryOrderWorkspace),
                    "ordinary batch scratch should reuse its round selection object");
        }
    }

    @Test
    void writerScratchClearsRetainedOutboundFrameRefsAfterBatch() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                Protocol.CAPABILITY_PRIORITY_HINTS,
                Settings.builder().maxFramePayload(16_384L).build()
        );
        StreamRuntime low = (StreamRuntime) runtime.openStream(new OpenOptions(0L, null, new byte[0]));
        StreamRuntime high = (StreamRuntime) runtime.openStream(new OpenOptions(20L, null, new byte[0]));

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, low);
            makePeerVisible(runtime, high);
            SessionRuntimeTestSupport.queueWrite(low, "low".getBytes(StandardCharsets.UTF_8));
            SessionRuntimeTestSupport.queueWrite(high, "high".getBytes(StandardCharsets.UTF_8));

            Object readyBatch = SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "collectReadyBatchStateLocked",
                    new Class<?>[]{boolean.class, boolean.class},
                    false,
                    false
            );
            @SuppressWarnings("unchecked")
            List<Object> frames = (List<Object>) SessionRuntimeTestSupport.invokePrivate(
                    readyBatch,
                    "frames",
                    new Class<?>[0]
            );

            java.lang.reflect.Field writerRuntimeField = SessionRuntime.class.getDeclaredField("writerRuntime");
            writerRuntimeField.setAccessible(true);
            Object writerRuntime = writerRuntimeField.get(runtime);

            SessionRuntimeTestSupport.invokePrivate(
                    writerRuntime,
                    "orderOrdinaryBatchLocked",
                    new Class<?>[]{List.class},
                    frames
            );

            java.lang.reflect.Field batchCollectorField = writerRuntime.getClass().getDeclaredField("batchCollector");
            batchCollectorField.setAccessible(true);
            Object batchCollector = batchCollectorField.get(writerRuntime);
            java.lang.reflect.Field collectBatchField = batchCollector.getClass().getDeclaredField("collectBatch");
            collectBatchField.setAccessible(true);
            @SuppressWarnings("unchecked")
            List<Object> collectBatch = (List<Object>) collectBatchField.get(batchCollector);

            java.lang.reflect.Field batchOrdererField = writerRuntime.getClass().getDeclaredField("batchOrderer");
            batchOrdererField.setAccessible(true);
            Object batchOrderer = batchOrdererField.get(writerRuntime);
            java.lang.reflect.Field orderedBatchField = batchOrderer.getClass().getDeclaredField("orderedBatch");
            orderedBatchField.setAccessible(true);
            @SuppressWarnings("unchecked")
            List<Object> orderedBatch = (List<Object>) orderedBatchField.get(batchOrderer);

            assertEquals(2, collectBatch.size(), "writer collect scratch should hold the staged batch before cleanup");
            assertEquals(2, orderedBatch.size(), "writer order scratch should hold the reordered batch before cleanup");

            SessionRuntimeTestSupport.invokePrivate(writerRuntime, "clearRetainedBatchRefs", new Class<?>[0]);

            assertTrue(collectBatch.isEmpty(), "writer collect scratch must not retain sent frame references");
            assertTrue(orderedBatch.isEmpty(), "writer order scratch must not retain sent frame references");
        }
    }

    @Test
    void groupFairRetainsPerGroupHeadAsNextBatchBias() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                Protocol.CAPABILITY_STREAM_GROUPS,
                Settings.builder()
                        .maxFramePayload(16_384L)
                        .schedulerHints(SchedulerHint.GROUP_FAIR)
                        .build()
        );
        StreamRuntime a = (StreamRuntime) runtime.openStream(new OpenOptions(0L, 7L, new byte[0]));
        StreamRuntime b = (StreamRuntime) runtime.openStream(new OpenOptions(0L, 7L, new byte[0]));
        StreamRuntime c = (StreamRuntime) runtime.openStream(new OpenOptions(0L, 9L, new byte[0]));
        StreamRuntime d = (StreamRuntime) runtime.openStream(new OpenOptions(0L, 9L, new byte[0]));

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, a);
            makePeerVisible(runtime, b);
            makePeerVisible(runtime, c);
            makePeerVisible(runtime, d);

            SessionRuntimeTestSupport.queueWrite(a, "a".getBytes(StandardCharsets.UTF_8));
            SessionRuntimeTestSupport.queueWrite(b, "b".getBytes(StandardCharsets.UTF_8));
            SessionRuntimeTestSupport.queueWrite(c, "c".getBytes(StandardCharsets.UTF_8));
            SessionRuntimeTestSupport.queueWrite(d, "d".getBytes(StandardCharsets.UTF_8));
            List<Object> firstBatch = collectReadyBatch(runtime);
            assertEquals(
                    listOf(a.streamIdInternal(), c.streamIdInternal(), b.streamIdInternal(), d.streamIdInternal()),
                    batchStreamIds(firstBatch),
                    "first group_fair batch should interleave the two groups"
            );

            SessionRuntimeTestSupport.queueWrite(a, "e".getBytes(StandardCharsets.UTF_8));
            SessionRuntimeTestSupport.queueWrite(b, "f".getBytes(StandardCharsets.UTF_8));
            SessionRuntimeTestSupport.queueWrite(c, "g".getBytes(StandardCharsets.UTF_8));
            SessionRuntimeTestSupport.queueWrite(d, "h".getBytes(StandardCharsets.UTF_8));
            List<Object> secondBatch = collectReadyBatch(runtime);
            assertEquals(
                    listOf(d.streamIdInternal(), b.streamIdInternal(), c.streamIdInternal(), a.streamIdInternal()),
                    batchStreamIds(secondBatch),
                    "next batch should reuse bounded per-group retained heads instead of restarting from the same peers"
            );
        }
    }

    @Test
    void transientSessionScopedOrdinaryGetsSingleHeadOpportunityBeforeRealStreams() {
        List<OrdinaryBatchOrderer.BatchFrame> batch = listOf(
                ordinarySessionExtFrame(5),
                streamDataFrame(4L, 0L, null, 5),
                streamDataFrame(8L, 0L, null, 5)
        );

        int[] order = OrdinaryBatchOrderer.order(
                batch,
                SchedulerHint.UNSPECIFIED_OR_BALANCED,
                16_384L,
                new OrdinaryBatchOrderer.RetainedBias()
        );

        assertEquals(listOf(0, 1, 2), toList(order), "session-scoped ordinary should get one transient batch-head opportunity before real-stream scheduling starts");
    }

    @Test
    void sessionScopedOrdinaryBatchDoesNotEraseRetainedRealBias() {
        OrdinaryBatchOrderer.RetainedBias retainedBias = new OrdinaryBatchOrderer.RetainedBias();

        List<OrdinaryBatchOrderer.BatchFrame> firstRealBatch = listOf(
                streamDataFrame(4L, 0L, null, 5),
                streamDataFrame(8L, 0L, null, 5)
        );
        assertEquals(
                listOf(0, 1),
                toList(OrdinaryBatchOrderer.order(firstRealBatch, SchedulerHint.UNSPECIFIED_OR_BALANCED, 16_384L, retainedBias)),
                "initial real batch should keep the original head"
        );

        List<OrdinaryBatchOrderer.BatchFrame> sessionOnlyBatch = listOf(
                ordinarySessionExtFrame(3),
                ordinarySessionExtFrame(4)
        );
        assertEquals(
                listOf(0, 1),
                toList(OrdinaryBatchOrderer.order(sessionOnlyBatch, SchedulerHint.UNSPECIFIED_OR_BALANCED, 16_384L, retainedBias)),
                "session-scoped ordinary batch should remain in identity order"
        );

        List<OrdinaryBatchOrderer.BatchFrame> nextRealBatch = listOf(
                streamDataFrame(4L, 0L, null, 5),
                streamDataFrame(8L, 0L, null, 5)
        );
        assertEquals(
                listOf(1, 0),
                toList(OrdinaryBatchOrderer.order(nextRealBatch, SchedulerHint.UNSPECIFIED_OR_BALANCED, 16_384L, retainedBias)),
                "session-scoped ordinary traffic must not erase retained real-stream next-head bias"
        );
    }

    @Test
    void retainedLagStateSkipsSyntheticStreamsAndTransientGroups() {
        OrdinaryBatchOrderer.RetainedBias retainedBias = new OrdinaryBatchOrderer.RetainedBias();

        List<OrdinaryBatchOrderer.BatchFrame> batch = listOf(
                streamDataFrame(4L, 0L, null, 1L),
                ordinarySessionExtFrame(1L),
                ordinarySessionExtFrame(1L)
        );

        int[] order = OrdinaryBatchOrderer.order(batch, SchedulerHint.GROUP_FAIR, 16_384L, retainedBias);
        assertEquals(batch.size(), order.length, "mixed transient batch should still produce a full ordering");

        OrdinaryBatchRetainedState retainedState = retainedBias.state();
        for (Long streamId : retainedState.streamLag.keySet()) {
            assertTrue(streamId != null && streamId > 0L, "retained lag state must skip synthetic stream ids");
        }
        for (OrdinaryBatchOrderer.GroupKey groupKey : retainedState.groupLag.keySet()) {
            assertNotNull(groupKey, "retained lag state should not contain null group keys");
            assertNotEquals(2, groupKey.kind(), "retained lag state must skip transient group keys");
        }
    }

    @Test
    void mixedBatchRetainsOnlyRealStreamFinishTags() {
        OrdinaryBatchOrderer.RetainedBias retainedBias = new OrdinaryBatchOrderer.RetainedBias();

        List<OrdinaryBatchOrderer.BatchFrame> batch = listOf(
                ordinarySessionExtFrame(1L),
                streamDataFrame(4L, 0L, null, 1L),
                ordinarySessionExtFrame(1L)
        );

        int[] order = OrdinaryBatchOrderer.order(batch, SchedulerHint.UNSPECIFIED_OR_BALANCED, 16_384L, retainedBias);
        assertEquals(batch.size(), order.length, "mixed batch should still produce a full ordering");

        OrdinaryBatchRetainedState retainedState = retainedBias.state();
        assertEquals(1, retainedState.streamFinishTag.size(), "retained finish tags should keep only the real stream");
        assertTrue(retainedState.streamFinishTag.containsKey(4L), "real stream finish tag should be retained");
        for (Long streamId : retainedState.streamFinishTag.keySet()) {
            assertTrue(streamId != null && streamId > 0L, "retained finish tags must skip synthetic stream ids");
        }
    }

    @Test
    void latencySchedulerReservesBulkOpportunityWithinFourSelections() {
        List<OrdinaryBatchOrderer.BatchFrame> batch = listOf(
                streamDataFrame(4L, 0L, null, 64L),
                streamDataFrame(4L, 0L, null, 64L),
                streamDataFrame(4L, 0L, null, 64L),
                streamDataFrame(4L, 0L, null, 64L),
                streamDataFrame(8L, 0L, null, 900L),
                streamDataFrame(8L, 0L, null, 900L),
                streamDataFrame(8L, 0L, null, 900L),
                streamDataFrame(8L, 0L, null, 900L)
        );

        int[] order = OrdinaryBatchOrderer.order(
                batch,
                SchedulerHint.LATENCY,
                1_024L,
                new OrdinaryBatchOrderer.RetainedBias()
        );

        int bulkSelections = 0;
        for (int i = 0; i < 4; ++i) {
            if (batch.get(order[i]).streamId() == 8L) {
                ++bulkSelections;
            }
        }
        assertTrue(bulkSelections >= 1, "latency scheduler should still reserve a bulk opportunity within the first four picks");
    }

    @Test
    void retainedClassHysteresisKeepsMidQueueBulkAcrossBatches() {
        OrdinaryBatchOrderer.RetainedBias retainedBias = new OrdinaryBatchOrderer.RetainedBias();

        OrdinaryBatchOrderer.order(
                listOf(
                        streamDataFrame(8L, 0L, null, 64L),
                        streamDataFrame(4L, 0L, null, 900L),
                        streamDataFrame(4L, 0L, null, 900L),
                        streamDataFrame(4L, 0L, null, 900L)
                ),
                SchedulerHint.BULK_THROUGHPUT,
                1_024L,
                retainedBias
        );

        OrdinaryBatchRetainedState retainedState = retainedBias.state();
        assertEquals(
                OrdinaryBatchOrderer.TrafficClass.BULK,
                retainedState.streamClass.get(4L),
                "large queued stream should be retained as bulk after the first batch"
        );

        OrdinaryBatchOrderer.order(
                listOf(
                        streamDataFrame(4L, 0L, null, 768L),
                        streamDataFrame(4L, 0L, null, 768L),
                        streamDataFrame(8L, 0L, null, 64L)
                ),
                SchedulerHint.BULK_THROUGHPUT,
                1_024L,
                retainedBias
        );

        assertEquals(
                OrdinaryBatchOrderer.TrafficClass.BULK,
                retainedState.streamClass.get(4L),
                "mid-queue stream should keep its retained bulk classification across the hysteresis window"
        );
        assertEquals(
                OrdinaryBatchOrderer.TrafficClass.INTERACTIVE,
                retainedState.streamClass.get(8L),
                "small queued stream should remain retained as interactive"
        );
    }
}
