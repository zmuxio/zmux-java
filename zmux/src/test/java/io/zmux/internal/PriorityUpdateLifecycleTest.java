package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static io.zmux.TestLists.listOf;
import static org.junit.jupiter.api.Assertions.*;

final class PriorityUpdateLifecycleTest {
    private static boolean hasQueuedFrame(SessionRuntime runtime, long streamId, FrameType type) throws Exception {
        return queueContains(runtime, "urgentQueue", streamId, type)
                || queueContains(runtime, "advisoryQueue", streamId, type)
                || queueContains(runtime, "dataQueue", streamId, type);
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

    private static boolean queueContains(SessionRuntime runtime, String fieldName, long streamId, FrameType type) throws Exception {
        if ("advisoryQueue".equals(fieldName)) {
            return type == FrameType.EXT && SessionRuntimeTestSupport.advisoryQueueStreamIds(runtime).contains(streamId);
        }
        Deque<Object> queue = SessionRuntimeTestSupport.outboundQueue(runtime, fieldName);
        for (Object outbound : queue) {
            FrameCodec.Frame frame = SessionRuntimeTestSupport.outboundFrame(outbound);
            if (frame.streamId() == streamId && frame.type() == type) {
                return true;
            }
        }
        return false;
    }

    @Test
    void openingWriteKeepsPendingPriorityUpdateQueuedUntilLaterBatch() throws Exception {
        long capabilities = Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS;
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(capabilities, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        stream.write("x".getBytes(StandardCharsets.UTF_8));
        stream.updateMetadata(new MetadataUpdate(9L, null));

        synchronized (runtime.lock()) {
            assertTrue(stream.hasPendingPriorityUpdateLocked(), "committed invisible stream should keep its priority update staged");
            List<Object> firstBatch = collectReadyBatch(runtime);
            assertEquals(listOf(FrameType.DATA), batchTypes(firstBatch), "opening batch should initially contain only the opening DATA");
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "afterWriteBatchLocked",
                    new Class<?>[]{List.class, long.class, long.class, long.class},
                    firstBatch,
                    1L,
                    1L,
                    2L
            );
            assertTrue(stream.peerVisible(), "opening DATA should make the stream peer-visible only after write completion");
            assertTrue(stream.hasPendingPriorityUpdateLocked(), "opening DATA write should keep the pending PRIORITY_UPDATE staged for a later batch");
            assertTrue(stream.priorityUpdateQueuedLocked(), "peer-visible transition should queue the staged PRIORITY_UPDATE for later emission");
        }

        stream.write("y".getBytes(StandardCharsets.UTF_8));

        synchronized (runtime.lock()) {
            List<Object> secondBatch = collectReadyBatch(runtime);
            assertEquals(listOf(FrameType.EXT, FrameType.DATA), batchTypes(secondBatch), "later batches should flush the queued PRIORITY_UPDATE ahead of future DATA");
            assertFalse(stream.hasPendingPriorityUpdateLocked(), "dequeueing the follow-up batch should consume the staged PRIORITY_UPDATE");
            assertFalse(stream.priorityUpdateQueuedLocked(), "dequeueing the follow-up batch should clear the queued PRIORITY_UPDATE marker");
        }
    }

    @Test
    void cancelWriteDropsQueuedPriorityUpdate() throws Exception {
        long capabilities = Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS;
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(capabilities, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        stream.write("body".getBytes(StandardCharsets.UTF_8));

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(runtime, "markPeerVisibleLocked", new Class<?>[]{StreamRuntime.class}, stream);
        }

        stream.updateMetadata(new MetadataUpdate(7L, null));

        synchronized (runtime.lock()) {
            assertTrue(hasQueuedFrame(runtime, stream.streamIdInternal(), FrameType.EXT), "visible metadata update should queue PRIORITY_UPDATE");
        }

        stream.cancelWrite(ErrorCode.CANCELLED.code());

        synchronized (runtime.lock()) {
            assertFalse(stream.hasPendingPriorityUpdateLocked(), "cancelWrite should clear any staged advisory update");
            assertFalse(hasQueuedFrame(runtime, stream.streamIdInternal(), FrameType.EXT), "cancelWrite should drop queued PRIORITY_UPDATE");
            assertTrue(hasQueuedFrame(runtime, stream.streamIdInternal(), FrameType.RESET), "cancelWrite should still queue RESET");
        }
    }

    @Test
    void drainedAdvisoryQueueReleasesRetainedBacking() throws Exception {
        long capabilities = Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS;
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(capabilities, Settings.defaults());

        for (int i = 0; i < 96; ++i) {
            StreamRuntime stream = (StreamRuntime) runtime.openStream();
            synchronized (runtime.lock()) {
                makePeerVisible(runtime, stream);
            }
            stream.updateMetadata(new MetadataUpdate((long) i + 1L, null));
        }

        Deque<Object> retainedQueue;
        synchronized (runtime.lock()) {
            retainedQueue = SessionRuntimeTestSupport.advisoryQueue(runtime);
            assertEquals(96, retainedQueue.size(), "test must build a large advisory queue");
            while (!SessionRuntimeTestSupport.advisoryQueue(runtime).isEmpty()) {
                List<Object> batch = collectReadyBatch(runtime);
                assertFalse(batch.isEmpty(), "advisory drain should make progress");
            }
            assertNotSame(
                    retainedQueue,
                    SessionRuntimeTestSupport.advisoryQueue(runtime),
                    "empty advisory queue should release its retained backing after drain"
            );
        }
    }
}
