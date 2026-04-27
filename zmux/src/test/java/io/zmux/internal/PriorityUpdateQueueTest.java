package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static io.zmux.TestLists.listOf;
import static org.junit.jupiter.api.Assertions.*;

final class PriorityUpdateQueueTest {
    private static long priorityCapabilities() {
        return Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS;
    }

    private static void makePeerVisible(SessionRuntime runtime, StreamRuntime stream) throws Exception {
        synchronized (runtime.lock()) {
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
            assertTrue(stream.peerVisible(), "stream should be peer-visible for queue-shape tests");
            assertFalse(stream.openingFramePendingLocked(), "queue-shape tests require no pending opener");
        }
    }

    private static void assertQueueTypes(Deque<Object> queue, FrameType... expected) throws Exception {
        List<FrameType> actual = new ArrayList<>();
        for (Object outbound : queue) {
            actual.add(SessionRuntimeTestSupport.outboundFrame(outbound).type());
        }
        assertEquals(listOf(expected), actual, "queue shape mismatch");
    }

    @SuppressWarnings("unchecked")
    private static List<FrameType> collectReadyBatchTypes(SessionRuntime runtime) throws Exception {
        return batchTypes(collectReadyBatch(runtime));
    }

    @SuppressWarnings("unchecked")
    private static List<Object> collectReadyBatch(SessionRuntime runtime) throws Exception {
        return (List<Object>) SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "collectReadyBatchLocked",
                new Class<?>[0]
        );
    }

    private static List<FrameType> batchTypes(List<Object> batch) throws Exception {
        List<FrameType> types = new ArrayList<>(batch.size());
        for (Object outbound : batch) {
            types.add(SessionRuntimeTestSupport.outboundFrame(outbound).type());
        }
        return types;
    }

    private static void assertPriorityUpdatePrecedesData(List<Object> batch, long streamId) throws Exception {
        int extPos = -1;
        int dataPos = -1;
        for (int i = 0; i < batch.size(); ++i) {
            FrameCodec.Frame frame = SessionRuntimeTestSupport.outboundFrame(batch.get(i));
            if (frame.streamId() != streamId) {
                continue;
            }
            if (frame.type() == FrameType.EXT && extPos < 0) {
                extPos = i;
            }
            if (frame.type() == FrameType.DATA && dataPos < 0) {
                dataPos = i;
            }
        }
        assertTrue(extPos >= 0, "missing priority update for stream " + streamId);
        assertTrue(dataPos >= 0, "missing DATA for stream " + streamId);
        assertTrue(extPos < dataPos, "priority update should precede future DATA for stream " + streamId);
    }

    @Test
    void priorityUpdateUsesAdvisoryQueueWhenNoCommittedStreamDataExists() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(priorityCapabilities(), Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
            stream.updateMetadata(new MetadataUpdate(7L, null));
            assertQueueTypes(SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue"));
            assertQueueTypes(SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue"));
            assertEquals(listOf(stream.streamIdInternal()), SessionRuntimeTestSupport.advisoryQueueStreamIds(runtime));
        }
    }

    @Test
    void priorityUpdateRetainsSingleAdvisorySlotForSupersededUpdates() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(priorityCapabilities(), Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
            stream.updateMetadata(new MetadataUpdate(7L, null));
            stream.updateMetadata(new MetadataUpdate(11L, null));
            assertEquals(listOf(stream.streamIdInternal()), SessionRuntimeTestSupport.advisoryQueueStreamIds(runtime));
            assertTrue(stream.hasPendingPriorityUpdateLocked(), "latest advisory update should stay staged until dequeue");
            assertTrue(stream.priorityUpdateQueuedLocked(), "latest advisory update should occupy the advisory slot");
            assertEquals(11L, stream.pendingPriorityUpdatePriorityLocked());
            List<Object> batch = collectReadyBatch(runtime);
            assertEquals(listOf(FrameType.EXT), batchTypes(batch));
            FrameCodec.ParsedPriorityUpdate queuedUpdate = FrameCodec.parsePriorityUpdatePayload(
                    SessionRuntimeTestSupport.outboundFrame(batch.get(0)).payload()
            );
            assertFalse(stream.hasPendingPriorityUpdateLocked(), "advisory dequeue should clear the staged update");
            assertFalse(stream.priorityUpdateQueuedLocked(), "advisory dequeue should clear the queued marker");
            assertTrue(queuedUpdate.valid(), "queued advisory frame should remain well-formed");
            assertTrue(queuedUpdate.hasPriority(), "queued advisory frame should carry priority");
            assertEquals(11L, queuedUpdate.priority());
        }
    }

    @Test
    void priorityUpdateWithCommittedSameStreamDataStaysInDataQueueButLeadsFutureBatchData() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(priorityCapabilities(), Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
            stream.write("payload".getBytes());
            stream.updateMetadata(new MetadataUpdate(11L, null));
            assertQueueTypes(SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue"));
            assertEquals(listOf(), SessionRuntimeTestSupport.advisoryQueueStreamIds(runtime));
            assertQueueTypes(SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue"), FrameType.DATA, FrameType.EXT);
            assertEquals(listOf(FrameType.EXT, FrameType.DATA), collectReadyBatchTypes(runtime));
        }
    }

    @Test
    void ordinaryBatchKeepsPriorityUpdatesAheadOfFuturePerStreamData() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(priorityCapabilities(), Settings.defaults());
        StreamRuntime first = (StreamRuntime) runtime.openStream();
        StreamRuntime second = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, first);
            makePeerVisible(runtime, second);
            first.updateMetadata(new MetadataUpdate(5L, null));
            first.write("one".getBytes());
            second.updateMetadata(new MetadataUpdate(7L, null));
            second.write("two".getBytes());
            List<Object> batch = collectReadyBatch(runtime);
            assertEquals(FrameType.EXT, SessionRuntimeTestSupport.outboundFrame(batch.get(0)).type());
            assertPriorityUpdatePrecedesData(batch, first.streamIdInternal());
            assertPriorityUpdatePrecedesData(batch, second.streamIdInternal());
        }
    }

    @Test
    void priorityUpdateBudgetDropLeavesNoQueuedAdvisoryWork() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(io.zmux.Role.RESPONDER)
                .capabilities(priorityCapabilities())
                .pendingPriorityBytesBudget(1L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, priorityCapabilities(), Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
            ZmuxException error = assertInstanceOf(
                    ZmuxException.class,
                    assertThrows(IOException.class, () -> stream.updateMetadata(new MetadataUpdate(7L, null))),
                    "budget-exhausted priority update should fail instead of silently mutating local state"
            );
            assertEquals(ErrorCode.INTERNAL.code(), error.code(), "priority-update budget failure code mismatch");
            assertEquals("write", error.operation(), "priority-update budget failure operation mismatch");
            assertEquals(ZmuxErrorScope.STREAM, error.scope(), "priority-update budget failure scope mismatch");
            assertEquals(ZmuxErrorSource.LOCAL, error.source(), "priority-update budget failure source mismatch");
            assertEquals(ZmuxErrorDirection.WRITE, error.direction(), "priority-update budget failure direction mismatch");
            assertEquals("pending priority update budget exceeded", error.getMessage(), "priority-update budget failure message mismatch");
            assertEquals(0L, stream.metadata().priority(), "rejected priority update must not mutate local metadata");
            assertFalse(stream.hasPendingPriorityUpdateLocked(), "rejected advisory update should not stay staged");
            assertEquals(listOf(), SessionRuntimeTestSupport.advisoryQueueStreamIds(runtime), "dropped advisory update must not occupy the advisory lane");
            assertQueueTypes(SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue"));
            assertQueueTypes(SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue"));
        }
        SessionStats stats = runtime.stats();
        assertEquals(1L, stats.diagnostics().droppedLocalPriorityUpdates(), "budget-dropped local PRIORITY_UPDATE should surface in diagnostics");
        assertEquals(1L, stats.diagnostics().protocolBacklogBlocked(), "priority backlog rejection should surface in diagnostics");
    }

    @Test
    void priorityUpdateAdvisoryHandoffDropsWhenFrameCostWouldExceedMemoryCap() throws Exception {
        byte[] payload = FrameCodec.buildPriorityUpdatePayload(
                priorityCapabilities(),
                7L,
                null,
                Settings.defaults().maxExtensionPayloadBytes()
        );
        ZmuxConfig config = ZmuxConfig.builder()
                .role(io.zmux.Role.RESPONDER)
                .capabilities(priorityCapabilities())
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, priorityCapabilities(), Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
            SessionRuntimeTestSupport.setField(
                    runtime,
                    "config",
                    config.toBuilder().sessionMemoryCap(payload.length).build()
            );
            stream.updateMetadata(new MetadataUpdate(7L, null));

            assertTrue(stream.hasPendingPriorityUpdateLocked(), "priority update should first fit the pending advisory budget");
            assertEquals(payload.length, runtime.outboundQueueBookkeepingInternal().pendingPriorityBytesLocked());
            assertEquals(listOf(stream.streamIdInternal()), SessionRuntimeTestSupport.advisoryQueueStreamIds(runtime));

            List<Object> batch = collectReadyBatch(runtime);

            assertEquals(listOf(), batchTypes(batch), "advisory update should be dropped instead of entering writer-held memory");
            assertFalse(stream.hasPendingPriorityUpdateLocked(), "dropped advisory handoff must release pending state");
            assertFalse(stream.priorityUpdateQueuedLocked(), "dropped advisory handoff must clear the advisory queue marker");
            assertEquals(0L, runtime.outboundQueueBookkeepingInternal().pendingPriorityBytesLocked());
            assertEquals(listOf(), SessionRuntimeTestSupport.advisoryQueueStreamIds(runtime));
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "writerHeldRetainedBytes"));
        }
        assertEquals(
                1L,
                runtime.stats().diagnostics().droppedLocalPriorityUpdates(),
                "advisory handoff memory drop should surface in diagnostics"
        );
    }

    @Test
    void priorityUpdatePayloadOverflowFailsBeforeShadowStateMutation() throws Exception {
        Settings peerSettings = Settings.defaults().toBuilder()
                .maxExtensionPayloadBytes(1L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(priorityCapabilities(), peerSettings);
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
            PriorityUpdateTooLargeException error = assertInstanceOf(
                    PriorityUpdateTooLargeException.class,
                    assertThrows(IOException.class, () -> stream.updateMetadata(new MetadataUpdate(7L, null))),
                    "oversized priority-update payload should fail before local metadata is mutated"
            );
            assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "priority-update overflow code mismatch");
            assertEquals("write", error.operation(), "priority-update overflow operation mismatch");
            assertEquals(ZmuxErrorScope.STREAM, error.scope(), "priority-update overflow scope mismatch");
            assertEquals(ZmuxErrorSource.LOCAL, error.source(), "priority-update overflow source mismatch");
            assertEquals(ZmuxErrorDirection.WRITE, error.direction(), "priority-update overflow direction mismatch");
            assertEquals(PriorityUpdateTooLargeException.MESSAGE, error.getMessage(), "priority-update overflow message mismatch");
            assertEquals(0L, stream.metadata().priority(), "overflowing priority update must not mutate local metadata");
            assertFalse(stream.hasPendingPriorityUpdateLocked(), "overflowing priority update must not stay staged");
            assertFalse(stream.priorityUpdateQueuedLocked(), "overflowing priority update must not occupy the advisory lane");
            assertQueueTypes(SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue"));
            assertQueueTypes(SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue"));
        }
    }
}
