package io.zmux.runtime;

import io.zmux.*;
import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;
import io.zmux.protocol.Protocol;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class SessionCloseCleanupRuntimeTest {
    private static final int MAX_PENDING_READ_LOOP_PROTOCOL_TASKS = 256;

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
    private static Deque<Object> readLoopProtocolTasks(SessionRuntime runtime) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField("readLoopProtocolTasks");
        field.setAccessible(true);
        return (Deque<Object>) field.get(runtime);
    }

    private static Object newReadLoopProtocolTask() throws Exception {
        Class<?> outboundFrameType = Class.forName("io.zmux.runtime.SessionRuntime$OutboundFrame");
        Class<?> taskType = Class.forName("io.zmux.runtime.SessionRuntime$ReadLoopProtocolTask");
        Constructor<?> constructor = taskType.getDeclaredConstructor(outboundFrameType);
        constructor.setAccessible(true);
        return constructor.newInstance(new Object[]{null});
    }

    private static void handleDataFrame(SessionRuntime runtime, FrameCodec.Frame frame) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField("readerRuntime");
        field.setAccessible(true);
        Object readerRuntime = field.get(runtime);
        try {
            SessionRuntimeTestSupport.invokePrivate(
                    readerRuntime,
                    "handleDataFrame",
                    new Class<?>[]{FrameCodec.Frame.class},
                    frame
            );
        } catch (InvocationTargetException error) {
            Throwable cause = error.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw error;
        }
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Object target, String name, Class<T> type) throws Exception {
        return (T) getField(target, name);
    }

    @Test
    void finishSessionClearsProtocolBacklogAndPendingWriterQueues() throws Exception {
        long capabilities = Protocol.CAPABILITY_PRIORITY_HINTS | Protocol.CAPABILITY_PRIORITY_UPDATE;
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(capabilities, Settings.defaults());
        StreamRuntime ordinary = (StreamRuntime) runtime.openStream();
        StreamRuntime advisory = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, ordinary);
            makePeerVisible(runtime, advisory);

            SessionRuntimeTestSupport.queueWrite(ordinary, "ordinary".getBytes(StandardCharsets.UTF_8));
            advisory.updateMetadata(MetadataUpdate.priority(7L));
            runtime.enqueueControlLocked(new FrameCodec.Frame(FrameType.PING, 0, 0L, new byte[]{1}));

            SessionRuntimeTestSupport.setField(runtime, "readLoopProtocolWorkerStarted", true);
            SessionRuntime.OutboundFrame protocolFrame = new SessionRuntime.OutboundFrame(
                    new FrameCodec.Frame(FrameType.PONG, 0, 0L, new byte[]{2}),
                    null,
                    0,
                    false,
                    false
            );
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "enqueueReadLoopProtocolFrameLocked",
                    new Class<?>[]{SessionRuntime.OutboundFrame.class, boolean.class},
                    protocolFrame,
                    false
            );

            assertFalse(SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue").isEmpty(),
                    "ordinary lane should hold queued stream data before close cleanup");
            assertFalse(SessionRuntimeTestSupport.advisoryQueue(runtime).isEmpty(),
                    "advisory lane should hold queued priority update before close cleanup");
            assertFalse(SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue").isEmpty(),
                    "urgent lane should hold queued control before close cleanup");
            assertFalse(readLoopProtocolTasks(runtime).isEmpty(),
                    "protocol backlog should hold queued read-loop work before close cleanup");
            assertTrue(runtime.outboundQueueBookkeepingInternal().pendingPriorityBytesLocked() > 0L,
                    "priority backlog should retain pending bytes before close cleanup");
            assertTrue(SessionRuntimeTestSupport.getLongField(runtime, "sessionQueuedDataBytes") > 0L,
                    "queued ordinary data should contribute to tracked session queue bytes before close cleanup");

            Deque<?> firstUrgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
            Deque<?> firstAdvisoryQueue = SessionRuntimeTestSupport.advisoryQueue(runtime);
            Deque<?> firstDataQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue");
            Map<?, ?> firstStreams = getField(runtime, "streams", Map.class);

            runtime.finishSessionLocked(new SessionClosedException(ZmuxErrorSource.LOCAL), SessionState.CLOSED);

            assertTrue(SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue").isEmpty(),
                    "ordinary lane must be drained on session close");
            assertTrue(SessionRuntimeTestSupport.advisoryQueue(runtime).isEmpty(),
                    "advisory lane must be drained on session close");
            assertTrue(SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue").isEmpty(),
                    "urgent lane must be drained on session close");
            assertTrue(readLoopProtocolTasks(runtime).isEmpty(),
                    "protocol backlog must be cleared on session close");
            assertEquals(0L, runtime.outboundQueueBookkeepingInternal().urgentQueuedControlBytesLocked(),
                    "urgent queued control bytes must reset after session close cleanup");
            assertEquals(0L, runtime.outboundQueueBookkeepingInternal().pendingPriorityBytesLocked(),
                    "pending priority bytes must reset after session close cleanup");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "sessionQueuedDataBytes"),
                    "queued data accounting must reset after session close cleanup");
            assertNotSame(firstUrgentQueue, SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue"),
                    "session close cleanup should drop retained urgent queue backing");
            assertNotSame(firstAdvisoryQueue, SessionRuntimeTestSupport.advisoryQueue(runtime),
                    "session close cleanup should drop retained advisory queue backing");
            assertNotSame(firstDataQueue, SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue"),
                    "session close cleanup should drop retained ordinary queue backing");
            assertNotSame(firstStreams, getField(runtime, "streams", Map.class),
                    "session close cleanup should drop retained live-stream map backing");
        }
    }

    @Test
    void readLoopProtocolDrainReleasesFullBacklogDeque() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        ArrayDeque<Object> oversizedBacklog = new ArrayDeque<>(MAX_PENDING_READ_LOOP_PROTOCOL_TASKS * 2);
        for (int i = 0; i < MAX_PENDING_READ_LOOP_PROTOCOL_TASKS; i++) {
            oversizedBacklog.addLast(newReadLoopProtocolTask());
        }

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setField(runtime, "readLoopProtocolTasks", oversizedBacklog);
        }

        CountDownLatch drained = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                SessionRuntimeTestSupport.invokePrivate(runtime, "readLoopProtocolLoop", new Class<?>[0]);
            } catch (Throwable error) {
                workerFailure.set(error);
            } finally {
                finished.countDown();
            }
        }, "test-protocol-drain");
        worker.start();

        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
        while (System.nanoTime() < deadlineNanos) {
            synchronized (runtime.lock()) {
                if (readLoopProtocolTasks(runtime).isEmpty()
                        && readLoopProtocolTasks(runtime) != oversizedBacklog) {
                    drained.countDown();
                    SessionRuntimeTestSupport.setField(runtime, "state", SessionState.CLOSED);
                    runtime.lock().notifyAll();
                    break;
                }
            }
            Thread.sleep(10L);
        }

        assertTrue(drained.await(1L, TimeUnit.SECONDS),
                "protocol worker should drain a full backlog and replace the retained deque backing");
        assertTrue(finished.await(1L, TimeUnit.SECONDS), "protocol worker should stop once the session is closed");
        assertNull(workerFailure.get(), "protocol worker should drain synthetic close-write tasks without failing");
    }

    @Test
    void finalApplicationReadCompactsFullyTerminalAcceptedStream() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        long streamId = SessionRuntime.firstPeerStreamId(Role.RESPONDER, true);
        byte[] payload = new byte[]{1, 2, 3};

        handleDataFrame(runtime, new FrameCodec.Frame(
                FrameType.DATA,
                Protocol.FRAME_FLAG_FIN,
                streamId,
                payload
        ));
        StreamRuntime stream = (StreamRuntime) runtime.acceptStream();
        stream.cancelWrite(ErrorCode.CANCELLED.code());

        synchronized (runtime.lock()) {
            assertSame(stream, runtime.liveStreamLocked(streamId),
                    "terminal stream must stay live while final payload is unread");
            assertTrue(stream.fullyTerminalLocked(), "test requires both stream halves to be terminal");
            assertEquals(0L, runtime.stats().activeStreams().peerBidi(),
                    "fully terminal peer-opened stream should release its incoming slot before unread payload drains");
        }

        byte[] dst = new byte[payload.length];
        assertEquals(payload.length, stream.read(dst));
        assertArrayEquals(payload, dst);

        synchronized (runtime.lock()) {
            assertNull(runtime.liveStreamLocked(streamId),
                    "draining the final payload should compact the terminal accepted stream");
            assertTrue(runtime.hasTerminalMarkerLocked(streamId),
                    "compaction should retain a terminal marker for future duplicate frames");
        }
        assertEquals(-1, stream.read(new byte[1]), "compacted stream object should still expose EOF");
    }

    @Test
    void terminalPeerUniReleasesIncomingSlotBeforeAcceptQueueDrains() throws Exception {
        Settings localSettings = Settings.defaults()
                .toBuilder()
                .maxIncomingStreamsUni(1L)
                .build();
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .settings(localSettings)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());
        long firstId = SessionRuntime.firstPeerStreamId(Role.RESPONDER, false);
        long secondId = firstId + 4L;

        handleDataFrame(runtime, new FrameCodec.Frame(
                FrameType.DATA,
                Protocol.FRAME_FLAG_FIN,
                firstId,
                new byte[]{1, 2, 3}
        ));

        synchronized (runtime.lock()) {
            StreamRuntime first = runtime.liveStreamLocked(firstId);
            assertNotNull(first, "terminal peer uni stream should stay live while still accept-queued");
            assertTrue(first.fullyTerminalLocked(), "peer uni DATA|FIN should make the stream fully terminal");
            assertTrue(first.acceptQueued(), "first peer uni stream should still be pending application accept");
            assertEquals(0L, runtime.stats().activeStreams().peerUni(),
                    "incoming uni slot should be reusable as soon as the stream is fully terminal");
        }

        handleDataFrame(runtime, new FrameCodec.Frame(
                FrameType.DATA,
                Protocol.FRAME_FLAG_FIN,
                secondId,
                new byte[]{4}
        ));

        synchronized (runtime.lock()) {
            assertNotNull(runtime.liveStreamLocked(secondId),
                    "second peer uni stream should not be refused while the first terminal stream is still queued");
            assertEquals(0L, runtime.stats().activeStreams().peerUni(),
                    "second terminal peer uni stream should also release its incoming slot immediately");
        }
    }

    @Test
    void localCancelWriteCompactsTerminalAcceptedStreamWithoutBufferedPayload() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        long streamId = SessionRuntime.firstPeerStreamId(Role.RESPONDER, true);

        handleDataFrame(runtime, new FrameCodec.Frame(
                FrameType.DATA,
                Protocol.FRAME_FLAG_FIN,
                streamId,
                StreamRuntime.EMPTY_BYTES
        ));
        StreamRuntime stream = (StreamRuntime) runtime.acceptStream();
        stream.cancelWrite(ErrorCode.CANCELLED.code());

        synchronized (runtime.lock()) {
            assertNull(runtime.liveStreamLocked(streamId),
                    "local RESET should compact a terminal accepted stream once no payload is buffered");
            assertTrue(runtime.hasTerminalMarkerLocked(streamId),
                    "RESET compaction should retain a terminal marker for future duplicate frames");
            Deque<Object> urgent = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
            assertEquals(1, urgent.size(), "compaction must not drop the queued RESET signal");
            assertEquals(FrameType.RESET, SessionRuntimeTestSupport.outboundFrame(urgent.peekFirst()).type(),
                    "local cancel should still leave RESET queued for the peer");
        }
        assertEquals(-1, stream.read(new byte[1]), "compacted stream object should still expose EOF");
    }

    @Test
    void localAbortCompactsAfterDiscardingBufferedPayload() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        long streamId = SessionRuntime.firstPeerStreamId(Role.RESPONDER, true);

        handleDataFrame(runtime, new FrameCodec.Frame(
                FrameType.DATA,
                0,
                streamId,
                new byte[]{1, 2, 3}
        ));
        StreamRuntime stream = (StreamRuntime) runtime.acceptStream();
        stream.closeWithError(ErrorCode.CANCELLED.code(), "abort");

        synchronized (runtime.lock()) {
            assertNull(runtime.liveStreamLocked(streamId),
                    "local ABORT should compact after discarding unread buffered payload");
            assertTrue(runtime.hasTerminalMarkerLocked(streamId),
                    "ABORT compaction should retain a terminal marker for future duplicate frames");
            Deque<Object> urgent = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
            assertEquals(1, urgent.size(), "compaction must not drop the queued ABORT signal");
            assertEquals(FrameType.ABORT, SessionRuntimeTestSupport.outboundFrame(urgent.peekFirst()).type(),
                    "local closeWithError should still leave ABORT queued for the peer");
        }
    }

    @Test
    void clearSessionCloseStateDropsRetainedCleanupBackings() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        OrdinaryBatchOrderer.RetainedBias ordinaryBatchBias =
                getField(runtime, "ordinaryBatchBias", OrdinaryBatchOrderer.RetainedBias.class);
        OrdinaryBatchRetainedState retainedState =
                getField(ordinaryBatchBias, "state", OrdinaryBatchRetainedState.class);
        @SuppressWarnings("unchecked")
        Map<OrdinaryBatchOrderer.GroupKey, Long> firstPreferredStreamHeads =
                getField(retainedState, "preferredStreamHeads", Map.class);
        @SuppressWarnings("unchecked")
        Map<Long, Long> firstStreamFinishTag = getField(retainedState, "streamFinishTag", Map.class);
        @SuppressWarnings("unchecked")
        java.util.Set<Long> firstSmallBurstDisarmed = getField(retainedState, "smallBurstDisarmed", java.util.Set.class);
        for (int i = 0; i < 1_025; ++i) {
            OrdinaryBatchOrderer.GroupKey key = new OrdinaryBatchOrderer.GroupKey(1, 4L + (long) i * 4L);
            firstPreferredStreamHeads.put(key, (long) i);
            firstStreamFinishTag.put(4L + (long) i * 4L, (long) i);
            firstSmallBurstDisarmed.add(4L + (long) i * 4L);
        }

        SessionExplicitGroupTracker explicitGroupTracker =
                getField(runtime, "explicitGroupTracker", SessionExplicitGroupTracker.class);
        @SuppressWarnings("unchecked")
        Map<Long, Long> firstActiveExplicitGroups = getField(explicitGroupTracker, "activeExplicitGroupRefs", Map.class);
        @SuppressWarnings("unchecked")
        Map<Long, Long> firstOrdinaryExplicitGroups =
                getField(explicitGroupTracker, "ordinaryBatchExplicitGroupRefs", Map.class);
        for (int i = 0; i < 1_025; ++i) {
            firstActiveExplicitGroups.put((long) i + 1L, 1L);
            firstOrdinaryExplicitGroups.put((long) i + 10_000L, 1L);
        }

        SessionLocalOpenTracker localOpenTracker = getField(runtime, "localOpenTracker", SessionLocalOpenTracker.class);
        @SuppressWarnings("unchecked")
        Deque<Object> firstProvisionalBidi = getField(localOpenTracker, "provisionalBidi", Deque.class);
        @SuppressWarnings("unchecked")
        Deque<Object> firstUnseenLocalUni = getField(localOpenTracker, "unseenLocalUni", Deque.class);
        for (int i = 0; i < 1_025; ++i) {
            firstProvisionalBidi.addLast(new Object());
            firstUnseenLocalUni.addLast(new Object());
        }

        SessionAcceptRegistry acceptRegistry = getField(runtime, "acceptRegistry", SessionAcceptRegistry.class);
        @SuppressWarnings("unchecked")
        Deque<StreamRuntime> firstAcceptBidi = getField(acceptRegistry, "acceptBidi", Deque.class);
        @SuppressWarnings("unchecked")
        Deque<StreamRuntime> firstAcceptUni = getField(acceptRegistry, "acceptUni", Deque.class);
        StreamRuntime acceptBidiSentinel = new StreamRuntime(runtime, false, true, null);
        StreamRuntime acceptUniSentinel = new StreamRuntime(runtime, false, false, null);
        for (int i = 0; i < 1_025; ++i) {
            firstAcceptBidi.addLast(acceptBidiSentinel);
            firstAcceptUni.addLast(acceptUniSentinel);
        }

        SessionStopSendingGracefulCoordinator gracefulCoordinator =
                getField(runtime, "stopSendingGracefulCoordinator", SessionStopSendingGracefulCoordinator.class);
        @SuppressWarnings("unchecked")
        Map<Object, Long> firstDeadlines = getField(gracefulCoordinator, "deadlines", Map.class);
        @SuppressWarnings("unchecked")
        PriorityQueue<Object> firstDeadlineHeap = getField(gracefulCoordinator, "deadlineHeap", PriorityQueue.class);
        Class<?> gracefulDeadlineType =
                Class.forName("io.zmux.runtime.SessionStopSendingGracefulCoordinator$GracefulDrainDeadline");
        Constructor<?> gracefulDeadlineConstructor =
                gracefulDeadlineType.getDeclaredConstructor(StreamRuntime.class, long.class);
        gracefulDeadlineConstructor.setAccessible(true);
        for (int i = 0; i < 1_025; ++i) {
            firstDeadlines.put(new Object(), (long) i + 1L);
            firstDeadlineHeap.add(gracefulDeadlineConstructor.newInstance((StreamRuntime) null, (long) i + 1L));
        }

        SessionTerminalBookkeeping terminalBookkeeping =
                getField(runtime, "terminalBookkeeping", SessionTerminalBookkeeping.class);
        @SuppressWarnings("unchecked")
        Map<Long, Object> firstTombstones = getField(terminalBookkeeping, "tombstones", Map.class);
        @SuppressWarnings("unchecked")
        Deque<Long> firstTombstoneOrder = getField(terminalBookkeeping, "tombstoneOrder", Deque.class);
        @SuppressWarnings("unchecked")
        Map<Long, Object> firstMarkerOnlyUsedStreams = getField(terminalBookkeeping, "markerOnlyUsedStreams", Map.class);
        @SuppressWarnings("unchecked")
        ArrayList<Object> firstMarkerOnlyRanges = getField(terminalBookkeeping, "markerOnlyRanges", ArrayList.class);
        @SuppressWarnings("unchecked")
        Deque<Long> firstHiddenTombstones = getField(terminalBookkeeping, "hiddenTombstones", Deque.class);
        for (int i = 0; i < 1_025; ++i) {
            long streamId = 4L + (long) i * 4L;
            firstTombstones.put(streamId, null);
            firstTombstoneOrder.addLast(streamId);
            firstMarkerOnlyUsedStreams.put(streamId, null);
            firstMarkerOnlyRanges.add(new Object());
            firstHiddenTombstones.addLast(streamId);
        }

        synchronized (runtime.lock()) {
            runtime.clearSessionCloseStateLocked();
        }

        assertNotSame(firstPreferredStreamHeads, getField(retainedState, "preferredStreamHeads"),
                "session close cleanup should drop retained ordinary-batch preferred-stream backing");
        assertNotSame(firstStreamFinishTag, getField(retainedState, "streamFinishTag"),
                "session close cleanup should drop retained ordinary-batch stream-finish backing");
        assertNotSame(firstSmallBurstDisarmed, getField(retainedState, "smallBurstDisarmed"),
                "session close cleanup should drop retained ordinary-batch disarmed-set backing");
        assertNotSame(firstActiveExplicitGroups, getField(explicitGroupTracker, "activeExplicitGroupRefs"),
                "session close cleanup should drop retained explicit-group refs backing");
        assertNotSame(firstOrdinaryExplicitGroups, getField(explicitGroupTracker, "ordinaryBatchExplicitGroupRefs"),
                "session close cleanup should drop retained ordinary-batch explicit-group refs backing");
        assertNotSame(firstProvisionalBidi, getField(localOpenTracker, "provisionalBidi"),
                "session close cleanup should drop retained provisional queue backing");
        assertNotSame(firstUnseenLocalUni, getField(localOpenTracker, "unseenLocalUni"),
                "session close cleanup should drop retained unseen-local queue backing");
        assertNotSame(firstAcceptBidi, getField(acceptRegistry, "acceptBidi"),
                "session close cleanup should drop retained accept-bidi queue backing");
        assertNotSame(firstAcceptUni, getField(acceptRegistry, "acceptUni"),
                "session close cleanup should drop retained accept-uni queue backing");
        assertNotSame(firstDeadlines, getField(gracefulCoordinator, "deadlines"),
                "session close cleanup should drop retained stop-sending deadline backing");
        assertNotSame(firstDeadlineHeap, getField(gracefulCoordinator, "deadlineHeap"),
                "session close cleanup should drop retained stop-sending heap backing");
        assertNotSame(firstTombstones, getField(terminalBookkeeping, "tombstones"),
                "session close cleanup should drop retained tombstone map backing");
        assertNotSame(firstTombstoneOrder, getField(terminalBookkeeping, "tombstoneOrder"),
                "session close cleanup should drop retained tombstone-order backing");
        assertNotSame(firstMarkerOnlyUsedStreams, getField(terminalBookkeeping, "markerOnlyUsedStreams"),
                "session close cleanup should drop retained marker-only map backing");
        assertNotSame(firstMarkerOnlyRanges, getField(terminalBookkeeping, "markerOnlyRanges"),
                "session close cleanup should drop retained marker-only range backing");
        assertNotSame(firstHiddenTombstones, getField(terminalBookkeeping, "hiddenTombstones"),
                "session close cleanup should drop retained hidden-tombstone backing");
    }
}
