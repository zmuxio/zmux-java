package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static io.zmux.TestLists.listOf;
import static org.junit.jupiter.api.Assertions.*;

final class WriterQueuePolicyTest {
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

    private static List<FrameType> batchTypes(List<Object> batch) throws Exception {
        List<FrameType> types = new ArrayList<>(batch.size());
        for (Object outbound : batch) {
            types.add(SessionRuntimeTestSupport.outboundFrame(outbound).type());
        }
        return types;
    }

    private static boolean hasQueuedDataFin(SessionRuntime runtime, long streamId) throws Exception {
        for (Object outbound : SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue")) {
            FrameCodec.Frame frame = SessionRuntimeTestSupport.outboundFrame(outbound);
            if (frame.streamId() == streamId
                    && frame.type() == FrameType.DATA
                    && (frame.flags() & Protocol.FRAME_FLAG_FIN) != 0) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasQueuedFrame(SessionRuntime runtime, long streamId, FrameType type) throws Exception {
        return queueContains(runtime, "urgentQueue", streamId, type)
                || queueContains(runtime, "dataQueue", streamId, type);
    }

    @SuppressWarnings("unchecked")
    private static List<Object> awaitNextWriterBatch(SessionRuntime runtime) throws Exception {
        java.lang.reflect.Field writerRuntimeField = SessionRuntime.class.getDeclaredField("writerRuntime");
        writerRuntimeField.setAccessible(true);
        Object writerRuntime = writerRuntimeField.get(runtime);
        try {
            Object pollResult = SessionRuntimeTestSupport.invokePrivate(
                    writerRuntime,
                    "awaitNextWriterBatchLocked",
                    new Class<?>[0]
            );
            return (List<Object>) SessionRuntimeTestSupport.invokePrivate(
                    pollResult,
                    "batch",
                    new Class<?>[0]
            );
        } catch (java.lang.reflect.InvocationTargetException invocationTargetException) {
            Throwable cause = invocationTargetException.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw invocationTargetException;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void completeWriterBatch(SessionRuntime runtime, List<Object> batch) throws IOException {
        long nowNanos = System.nanoTime();
        runtime.afterWriteBatchLocked((List) batch, 0L, nowNanos, nowNanos);
    }

    private static boolean queueContains(SessionRuntime runtime, String fieldName, long streamId, FrameType type) throws Exception {
        for (Object outbound : SessionRuntimeTestSupport.outboundQueue(runtime, fieldName)) {
            FrameCodec.Frame frame = SessionRuntimeTestSupport.outboundFrame(outbound);
            if (frame.streamId() == streamId && frame.type() == type) {
                return true;
            }
        }
        return false;
    }

    @Test
    void queuedDataWatermarkUsesQueuedBytesInsteadOfInflightReservedBytes() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .perStreamQueuedDataHwm(8L)
                .sessionQueuedDataHwm(8L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setLongField(stream, "reservedSendBytes", 8L);
            SessionRuntimeTestSupport.setLongField(runtime, "sessionReservedSendBytes", 8L);
            boolean allowed = (Boolean) SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "withinQueuedDataWatermarkLocked",
                    new Class<?>[]{StreamRuntime.class, int.class},
                    stream,
                    1
            );
            assertTrue(allowed, "queued-data HWM should ignore inflight reserved bytes once the queue itself is empty");
        }
    }

    @Test
    void urgentBatchOrdersByPriorityThenStreamScopeThenStreamId() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime lowStreamId = (StreamRuntime) runtime.openStream();
        StreamRuntime highStreamId = (StreamRuntime) runtime.openStream();
        StreamRuntime stopStream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, lowStreamId);
            makePeerVisible(runtime, highStreamId);
            makePeerVisible(runtime, stopStream);

            runtime.enqueueControlLocked(new FrameCodec.Frame(FrameType.PING, 0, 0L, new byte[8]));
            runtime.enqueueControlLocked(new FrameCodec.Frame(FrameType.PONG, 0, 0L, new byte[8]));
            runtime.enqueueControlLocked(new FrameCodec.Frame(FrameType.GOAWAY, 0, 0L,
                    FrameCodec.buildGoAwayPayload(0L, 0L, ErrorCode.NO_ERROR.code(), "")));
            runtime.enqueueAbortLocked(highStreamId, ErrorCode.CANCELLED.code(), "");
            runtime.enqueueStopSendingLocked(stopStream, ErrorCode.CANCELLED.code(), "");
            runtime.enqueueResetLocked(lowStreamId, ErrorCode.CANCELLED.code(), "");
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "queueSessionMaxDataLocked",
                    new Class<?>[]{long.class},
                    17L
            );
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "queueStreamMaxDataLocked",
                    new Class<?>[]{long.class, long.class},
                    highStreamId.streamIdInternal(),
                    19L
            );
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "flushPendingWindowUpdatesLocked",
                    new Class<?>[]{Long.class},
                    (Object) null
            );
            List<Object> batch = collectReadyBatch(runtime);
            assertEquals(
                    listOf(
                            FrameType.GOAWAY,
                            FrameType.ABORT,
                            FrameType.RESET,
                            FrameType.STOP_SENDING,
                            FrameType.MAX_DATA,
                            FrameType.MAX_DATA,
                            FrameType.PONG,
                            FrameType.PING
                    ),
                    batchTypes(batch),
                    "urgent batch ordering should follow repository-default control priority"
            );
            assertEquals(highStreamId.streamIdInternal(), SessionRuntimeTestSupport.outboundFrame(batch.get(1)).streamId(), "ABORT should outrank half-close terminal controls");
            assertEquals(lowStreamId.streamIdInternal(), SessionRuntimeTestSupport.outboundFrame(batch.get(2)).streamId(), "RESET should follow full-stream abortive terminal controls");
            assertEquals(stopStream.streamIdInternal(), SessionRuntimeTestSupport.outboundFrame(batch.get(3)).streamId(), "STOP_SENDING should follow RESET because it has lower priority");
            assertEquals(highStreamId.streamIdInternal(), SessionRuntimeTestSupport.outboundFrame(batch.get(4)).streamId(), "stream-scoped MAX_DATA should sort ahead of session-scoped MAX_DATA at the same rank");
            assertEquals(0L, SessionRuntimeTestSupport.outboundFrame(batch.get(5)).streamId(), "session-scoped MAX_DATA should follow the stream-scoped MAX_DATA at the same rank");

            runtime.enqueueControlLocked(new FrameCodec.Frame(FrameType.CLOSE, 0, 0L, FrameCodec.buildErrorPayload(ErrorCode.NO_ERROR.code(), "", 4096L)));
            List<Object> closeBatch = collectReadyBatch(runtime);
            assertEquals(listOf(FrameType.CLOSE), batchTypes(closeBatch), "CLOSE should short-circuit the urgent batch");
        }
    }

    @Test
    void urgentBatchKeepsOpeningDataBeforeSameStreamStopSending() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            stream.closeWrite();
            stream.cancelRead(ErrorCode.CANCELLED.code());

            List<Object> batch = collectReadyBatch(runtime);
            assertTrue(batch.size() >= 2, "opening FIN and STOP_SENDING should be collected together");
            FrameCodec.Frame opening = SessionRuntimeTestSupport.outboundFrame(batch.get(0));
            FrameCodec.Frame stopSending = SessionRuntimeTestSupport.outboundFrame(batch.get(1));
            assertEquals(FrameType.DATA, opening.type(), "opening DATA must remain before same-stream STOP_SENDING");
            assertEquals(stream.streamIdInternal(), opening.streamId(), "opening DATA stream id mismatch");
            assertTrue((opening.flags() & Protocol.FRAME_FLAG_FIN) != 0, "opening DATA should carry FIN");
            assertEquals(FrameType.STOP_SENDING, stopSending.type(), "STOP_SENDING should follow the opening prelude");
            assertEquals(stream.streamIdInternal(), stopSending.streamId(), "STOP_SENDING stream id mismatch");
        }
    }

    @Test
    void collectReadyBatchCapsUrgentFramesAtThirtyTwo() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        synchronized (runtime.lock()) {
            for (int i = 0; i < 40; ++i) {
                runtime.enqueueControlLocked(new FrameCodec.Frame(FrameType.PING, 0, 0L, new byte[0]));
            }
            List<Object> batch = collectReadyBatch(runtime);
            assertEquals(32, batch.size(), "writer batches should cap at repository-default 32 frames");
            for (Object outbound : batch) {
                assertEquals(FrameType.PING, SessionRuntimeTestSupport.outboundFrame(outbound).type(), "queued urgent frames should be retained in the batch");
            }
            Deque<Object> remaining = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
            assertEquals(8, remaining.size(), "frames beyond the 32-frame batch cap should remain queued");
        }
    }

    @Test
    void emptyControlFrameStillRetainsQueueCost() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        synchronized (runtime.lock()) {
            runtime.enqueueControlLocked(new FrameCodec.Frame(FrameType.PING, 0, 0L, new byte[0]));

            assertEquals(
                    1L,
                    runtime.outboundQueueBookkeepingInternal().urgentQueuedControlBytesLocked(),
                    "empty control frames should still account for their frame type byte"
            );
            assertEquals(
                    1L,
                    SessionRuntimeTestSupport.invokePrivate(runtime, "trackedSessionMemoryLocked", new Class<?>[0]),
                    "tracked session memory should include zero-payload control frames"
            );

            List<Object> batch = collectReadyBatch(runtime);
            assertEquals(1, batch.size(), "queued empty PING should still be writable");
            assertEquals(
                    0L,
                    runtime.outboundQueueBookkeepingInternal().urgentQueuedControlBytesLocked(),
                    "dequeueing the frame should release its retained queue cost"
            );
        }
    }

    @Test
    void urgentQueueCapRejectsControlAndFailsSession() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .urgentQueuedBytesCap(1L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());

        synchronized (runtime.lock()) {
            runtime.enqueueControlLocked(new FrameCodec.Frame(FrameType.PING, 0, 0L, new byte[0]));

            IOException thrown = assertThrows(
                    IOException.class,
                    () -> runtime.enqueueControlLocked(new FrameCodec.Frame(FrameType.PONG, 0, 0L, new byte[0])),
                    "urgent control should be rejected instead of growing beyond its hard cap"
            );
            ZmuxException error = assertInstanceOf(
                    ZmuxException.class,
                    thrown,
                    "urgent queue cap rejection should use structured INTERNAL session error"
            );
            assertEquals(ErrorCode.INTERNAL.code(), error.code(), "urgent queue cap failure code mismatch");
            assertEquals("queue urgent control", error.operation(), "urgent queue cap failure operation mismatch");
            assertEquals(
                    ZmuxTerminationKind.SESSION_TERMINATION,
                    error.terminationKind(),
                    "urgent queue cap failure termination mismatch"
            );
            assertTrue(runtime.stateInternal().terminal(), "resource-bound urgent control rejection should fail the session");
            assertTrue(
                    runtime.stats().diagnostics().protocolBacklogBlocked() >= 1L,
                    "urgent cap rejection should increment backlog diagnostics"
            );
            assertTrue(
                    runtime.outboundQueueBookkeepingInternal().urgentQueuedControlBytesLocked() <= 1L,
                    "urgent queue accounting must not grow beyond the configured cap"
            );
        }
    }

    @Test
    void pendingWindowUpdatesRespectUrgentCapAndRemainPending() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .urgentQueuedBytesCap(3L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());

        synchronized (runtime.lock()) {
            assertTrue(runtime.queueSessionMaxDataLocked(17L), "session MAX_DATA should be pending");
            assertTrue(
                    runtime.flowControlUpdateRegistryInternal().queueBlockedFrameLocked(0L, 23L),
                    "session BLOCKED should be pending"
            );

            List<Object> first = awaitNextWriterBatch(runtime);
            assertEquals(listOf(FrameType.MAX_DATA), batchTypes(first), "pending urgent control should drain in cap-sized chunks");
            assertTrue(
                    runtime.outboundQueueBookkeepingInternal().pendingControlBytesLocked() > 0L,
                    "overflow pending control should remain dirty for the next writer turn"
            );
            assertEquals(2L, SessionRuntimeTestSupport.getLongField(runtime, "writerHeldRetainedBytes"));
            completeWriterBatch(runtime, first);

            List<Object> second = awaitNextWriterBatch(runtime);
            assertEquals(listOf(FrameType.BLOCKED), batchTypes(second), "remaining pending control should flush after the first chunk");
            assertEquals(0L, runtime.outboundQueueBookkeepingInternal().pendingControlBytesLocked());
            completeWriterBatch(runtime, second);
        }
    }

    @Test
    void directPendingWindowFlushRespectsUrgentCapAndLeavesOverflowPending() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .urgentQueuedBytesCap(3L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());

        synchronized (runtime.lock()) {
            assertTrue(runtime.queueSessionMaxDataLocked(17L), "session MAX_DATA should be pending");
            assertTrue(
                    runtime.flowControlUpdateRegistryInternal().queueBlockedFrameLocked(0L, 23L),
                    "session BLOCKED should be pending"
            );

            runtime.flushPendingWindowUpdatesLocked(null);
            List<Object> first = awaitNextWriterBatch(runtime);
            assertEquals(
                    listOf(FrameType.MAX_DATA),
                    batchTypes(first),
                    "direct pending-control flush should only enqueue the chunk that fits the urgent cap"
            );
            assertTrue(
                    runtime.outboundQueueBookkeepingInternal().pendingControlBytesLocked() > 0L,
                    "direct pending-control overflow should remain dirty for a later flush"
            );
            assertEquals(SessionState.READY, runtime.stateInternal(), "coalescible pending-control overflow must not fail the session");
            completeWriterBatch(runtime, first);

            runtime.flushPendingWindowUpdatesLocked(null);
            List<Object> second = awaitNextWriterBatch(runtime);
            assertEquals(listOf(FrameType.BLOCKED), batchTypes(second), "remaining direct pending control should flush after the first chunk");
            assertEquals(0L, runtime.outboundQueueBookkeepingInternal().pendingControlBytesLocked());
            completeWriterBatch(runtime, second);
        }
    }

    @Test
    void directPendingWindowFlushDefersSingleFrameThatCannotFitUrgentQueue() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .urgentQueuedBytesCap(1L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());

        synchronized (runtime.lock()) {
            assertTrue(runtime.queueSessionMaxDataLocked(17L), "session MAX_DATA should be pending");

            runtime.flushPendingWindowUpdatesLocked(null);
            assertTrue(
                    SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue").isEmpty(),
                    "direct flush must not force a coalescible frame into an over-cap urgent queue"
            );
            assertTrue(
                    runtime.outboundQueueBookkeepingInternal().pendingControlBytesLocked() > 0L,
                    "over-cap direct flush should leave the coalescible frame pending for the writer"
            );
            assertEquals(SessionState.READY, runtime.stateInternal(), "over-cap direct flush must not fail the session");

            List<Object> batch = awaitNextWriterBatch(runtime);
            assertEquals(
                    listOf(FrameType.MAX_DATA),
                    batchTypes(batch),
                    "writer batch may still make progress with one coalescible pending-control frame"
            );
            completeWriterBatch(runtime, batch);
        }
    }

    @Test
    void pendingWindowUpdateHandoffFailsSessionWhenMemoryCapExceeded() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .sessionMemoryCap(1L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());

        synchronized (runtime.lock()) {
            assertTrue(runtime.queueSessionMaxDataLocked(17L), "pending MAX_DATA should fit the pending-control budget");

            IOException thrown = assertThrows(
                    IOException.class,
                    () -> awaitNextWriterBatch(runtime),
                    "writer handoff should reject pending control that would exceed session memory cap"
            );
            ZmuxException error = assertInstanceOf(
                    ZmuxException.class,
                    thrown,
                    "pending-control handoff rejection should use structured INTERNAL session error"
            );
            assertEquals(ErrorCode.INTERNAL.code(), error.code(), "pending handoff failure code mismatch");
            assertEquals("queue urgent control", error.operation(), "pending handoff failure operation mismatch");
            assertEquals(
                    ZmuxTerminationKind.SESSION_TERMINATION,
                    error.terminationKind(),
                    "pending handoff failure termination mismatch"
            );
            assertTrue(runtime.stateInternal().terminal(), "pending-control memory failure should fail the session");
            assertTrue(
                    runtime.stats().diagnostics().protocolBacklogBlocked() >= 1L,
                    "pending-control memory failure should increment backlog diagnostics"
            );
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "writerHeldRetainedBytes"));
        }
    }

    @Test
    void urgentPriorityReordersOnlyWithinCollectedBatch() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
            for (int i = 0; i < 32; ++i) {
                runtime.enqueueControlLocked(new FrameCodec.Frame(FrameType.PING, 0, 0L, new byte[0]));
            }
            runtime.enqueueResetLocked(stream, ErrorCode.CANCELLED.code(), "");

            List<Object> firstBatch = collectReadyBatch(runtime);
            assertEquals(32, firstBatch.size(), "first urgent batch should still stop at the repository-default frame cap");
            for (Object outbound : firstBatch) {
                assertEquals(FrameType.PING, SessionRuntimeTestSupport.outboundFrame(outbound).type(), "later higher-priority urgent work should not leap into an already-collected batch window");
            }

            List<Object> secondBatch = collectReadyBatch(runtime);
            assertEquals(listOf(FrameType.RESET), batchTypes(secondBatch), "the deferred higher-priority urgent frame should lead the next batch once it enters the collected window");
        }
    }

    @Test
    void ordinaryBatchCapsLargeDataFramesByBatchCost() throws Exception {
        long maxPayload = Settings.defaults().maxFramePayload();
        Settings peerSettings = Settings.defaults().toBuilder()
                .initialMaxData(maxPayload * 8L)
                .initialMaxStreamDataBidiPeerOpened(maxPayload * 8L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, peerSettings);
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        byte[] payload = new byte[(int) maxPayload];

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
        }
        for (int i = 0; i < 5; ++i) {
            SessionRuntimeTestSupport.queueWrite(stream, payload);
        }

        synchronized (runtime.lock()) {
            List<Object> batch = collectReadyBatch(runtime);
            assertEquals(4, batch.size(), "ordinary batch should stop once the repository-default batch-cost cap is reached");
            for (Object outbound : batch) {
                assertEquals(FrameType.DATA, SessionRuntimeTestSupport.outboundFrame(outbound).type(), "batch-cost cap should apply to queued DATA frames");
            }
            Deque<Object> remaining = SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue");
            assertEquals(1, remaining.size(), "frames beyond the batch-cost cap should remain queued for the next flush");
        }
    }

    @Test
    void sendCreditClampsWhenCountersSaturate() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setLongField(runtime, "sessionSendLimit", Long.MAX_VALUE);
            SessionRuntimeTestSupport.setLongField(runtime, "sessionSentBytes", Long.MAX_VALUE - 1L);
            SessionRuntimeTestSupport.setLongField(runtime, "sessionReservedSendBytes", 8L);
            assertEquals(0L, runtime.sessionRemainingSendCreditLocked(), "session credit must not reopen after counter saturation");

            SessionRuntimeTestSupport.setLongField(stream, "peerSendLimit", Long.MAX_VALUE);
            SessionRuntimeTestSupport.setLongField(stream, "sentBytes", Long.MAX_VALUE - 1L);
            SessionRuntimeTestSupport.setLongField(stream, "reservedSendBytes", 8L);
            java.lang.reflect.Method method = SessionOutboundDataCoordinator.class.getDeclaredMethod(
                    "streamSendCreditLocked",
                    StreamRuntime.class
            );
            method.setAccessible(true);
            assertEquals(0L, method.invoke(null, stream), "stream credit must not reopen after counter saturation");
        }
    }

    @Test
    void longRunningStatsCountersSaturate() throws Exception {
        Settings peerSettings = Settings.defaults().toBuilder()
                .initialMaxData(64L)
                .initialMaxStreamDataBidiPeerOpened(64L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, peerSettings);
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
        }
        SessionRuntimeTestSupport.queueWrite(stream, "data".getBytes());

        synchronized (runtime.lock()) {
            List<Object> batch = collectReadyBatch(runtime);
            SessionRuntimeTestSupport.setLongField(runtime, "sentFrames", Long.MAX_VALUE);
            SessionRuntimeTestSupport.setLongField(runtime, "sentDataBytes", Long.MAX_VALUE - 2L);
            SessionRuntimeTestSupport.setLongField(runtime, "sessionSentBytes", Long.MAX_VALUE - 2L);
            completeWriterBatch(runtime, batch);

            SessionRuntimeTestSupport.setLongField(runtime, "receivedFrames", Long.MAX_VALUE);
            SessionRuntimeTestSupport.setLongField(runtime, "receivedDataBytes", Long.MAX_VALUE - 2L);
            runtime.incrementReceivedFramesLocked();
            runtime.addReceivedDataBytesInternal(4L);

            assertEquals(Long.MAX_VALUE, SessionRuntimeTestSupport.getLongField(runtime, "sentFrames"));
            assertEquals(Long.MAX_VALUE, SessionRuntimeTestSupport.getLongField(runtime, "sentDataBytes"));
            assertEquals(Long.MAX_VALUE, SessionRuntimeTestSupport.getLongField(runtime, "sessionSentBytes"));
            assertEquals(Long.MAX_VALUE, SessionRuntimeTestSupport.getLongField(runtime, "receivedFrames"));
            assertEquals(Long.MAX_VALUE, SessionRuntimeTestSupport.getLongField(runtime, "receivedDataBytes"));
        }
    }

    @Test
    void stagedWriterBatchRetainsControlBytesInTrackedSessionMemory() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        synchronized (runtime.lock()) {
            runtime.enqueueControlLocked(new FrameCodec.Frame(FrameType.PONG, 0, 0L, new byte[]{1, 2, 3}));
            long queuedTracked = (Long) SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "trackedSessionMemoryLocked",
                    new Class<?>[0]
            );

            Object readyBatch = SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "collectReadyBatchStateLocked",
                    new Class<?>[]{boolean.class, boolean.class},
                    false,
                    true
            );

            long writerHeld = SessionRuntimeTestSupport.getLongField(runtime, "writerHeldRetainedBytes");
            long stagedTracked = (Long) SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "trackedSessionMemoryLocked",
                    new Class<?>[0]
            );
            @SuppressWarnings("unchecked")
            List<Object> frames = (List<Object>) SessionRuntimeTestSupport.invokePrivate(
                    readyBatch,
                    "frames",
                    new Class<?>[0]
            );

            assertEquals(4L, writerHeld, "writer-held accounting should retain the dequeued control payload bytes until the batch completes");
            assertEquals(queuedTracked, stagedTracked, "moving queued control into the writer-held batch should not shrink tracked session memory");
            assertEquals(1, frames.size(), "test requires exactly one staged urgent control frame");
        }
    }

    @Test
    void closeQueuedDuringOrdinaryBatchDropsStagedTail() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
        }
        SessionRuntimeTestSupport.queueWrite(stream, "body".getBytes());

        synchronized (runtime.lock()) {
            Object readyBatch = SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "collectReadyBatchStateLocked",
                    new Class<?>[]{boolean.class, boolean.class},
                    false,
                    true
            );
            @SuppressWarnings("unchecked")
            List<Object> frames = (List<Object>) SessionRuntimeTestSupport.invokePrivate(
                    readyBatch,
                    "frames",
                    new Class<?>[0]
            );
            long batchCost = (Long) SessionRuntimeTestSupport.invokePrivate(
                    readyBatch,
                    "batchCost",
                    new Class<?>[0]
            );
            long costLimit = (Long) SessionRuntimeTestSupport.invokePrivate(
                    readyBatch,
                    "costLimit",
                    new Class<?>[0]
            );

            SessionRuntimeTestSupport.setField(runtime, "state", SessionState.CLOSING);
            SessionRuntimeTestSupport.setField(runtime, "closeFrameQueued", true);

            @SuppressWarnings("unchecked")
            List<Object> dropped = (List<Object>) SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "finishOrdinaryBatchLocked",
                    new Class<?>[]{List.class, long.class, long.class},
                    frames,
                    batchCost,
                    costLimit
            );

            assertEquals(listOf(), dropped, "staged ordinary tail should be discarded once CLOSE has been queued");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "sessionQueuedDataBytes"), "discarding the staged tail should release queued data accounting");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "writerHeldRetainedBytes"), "discarding the staged tail should release writer-held retained bytes");
        }
    }

    @Test
    void filterWritableBatchDropsCancelledDataInPlace() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
        }
        SessionRuntimeTestSupport.queueWrite(stream, "body".getBytes());

        List<Object> staged;
        synchronized (runtime.lock()) {
            Object readyBatch = SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "collectReadyBatchStateLocked",
                    new Class<?>[]{boolean.class, boolean.class},
                    false,
                    true
            );
            @SuppressWarnings("unchecked")
            List<Object> frames = (List<Object>) SessionRuntimeTestSupport.invokePrivate(
                    readyBatch,
                    "frames",
                    new Class<?>[0]
            );
            staged = frames;
            assertEquals(1, frames.size(), "test requires exactly one staged data frame before cancellation");
        }

        stream.closeWithError(ErrorCode.CANCELLED.code(), "drop staged");

        synchronized (runtime.lock()) {
            java.lang.reflect.Field writerRuntimeField = SessionRuntime.class.getDeclaredField("writerRuntime");
            writerRuntimeField.setAccessible(true);
            Object writerRuntime = writerRuntimeField.get(runtime);
            @SuppressWarnings("unchecked")
            List<Object> filtered = (List<Object>) SessionRuntimeTestSupport.invokePrivate(
                    writerRuntime,
                    "filterWritableBatchLocked",
                    new Class<?>[]{List.class},
                    staged
            );

            assertSame(staged, filtered, "writer filtering should compact the reusable batch list in place");
            assertEquals(listOf(), filtered, "cancelled staged data should be removed from the write batch");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "sessionQueuedDataBytes"), "filtered staged data should release queued data accounting");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "writerHeldRetainedBytes"), "filtered staged data should release writer-held accounting");
        }
    }

    @Test
    void cancelWriteAfterQueuedCloseWriteStaysGracefullyClosed() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
        }
        SessionRuntimeTestSupport.queueWrite(stream, "body".getBytes());
        stream.closeWrite();
        WriteClosedException closed = assertInstanceOf(
                WriteClosedException.class,
                assertThrows(IOException.class, () -> stream.cancelWrite(ErrorCode.CANCELLED.code())),
                "cancelWrite must not replace an already-queued graceful close with RESET"
        );
        assertEquals(ZmuxErrorSource.LOCAL, closed.source(), "post-closeWrite cancelWrite source mismatch");
        assertEquals(ZmuxTerminationKind.GRACEFUL, closed.terminationKind(), "post-closeWrite cancelWrite termination mismatch");

        synchronized (runtime.lock()) {
            Deque<Object> dataQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue");
            Deque<Object> urgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
            assertEquals(2, dataQueue.size(), "rejected cancelWrite should preserve the queued DATA payload plus the queued DATA|FIN tail");
            assertTrue(hasQueuedDataFin(runtime, stream.streamIdInternal()), "queued graceful DATA|FIN should remain intact after rejected cancelWrite");
            assertEquals(0, urgentQueue.size(), "rejected cancelWrite must not enqueue RESET");
            assertEquals(4L, SessionRuntimeTestSupport.getLongField(stream, "queuedDataBytes"), "rejected cancelWrite must preserve per-stream queued-data accounting");
            assertEquals(4L, SessionRuntimeTestSupport.getLongField(stream, "reservedSendBytes"), "rejected cancelWrite must preserve per-stream reserved send credit");
            assertEquals(4L, SessionRuntimeTestSupport.getLongField(runtime, "sessionQueuedDataBytes"), "rejected cancelWrite must preserve session queued-data accounting");
            assertEquals(4L, SessionRuntimeTestSupport.getLongField(runtime, "sessionReservedSendBytes"), "rejected cancelWrite must preserve session reserved send credit");
        }
    }

    @Test
    void cancelWriteBeforeOpeningFrameCommitConsumesAssignedLocalIdWithAbort() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            runtime.beginLocalOpenForWriteLocked(stream);
            assertEquals(0L, stream.streamId(), "assigned local stream must not expose its wire id before opening-frame commit");
            assertFalse(stream.openedOnWire(), "test requires a local stream whose id is assigned but not yet opening-frame-committed");
            assertTrue(stream.awaitingPeerVisibilityLocked(), "assigned local stream should still await peer visibility before its first opening-eligible frame");
        }

        stream.cancelWrite(ErrorCode.CANCELLED.code());

        synchronized (runtime.lock()) {
            Deque<Object> urgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
            Deque<Object> dataQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue");
            assertEquals(1, urgentQueue.size(), "pre-commit cancelWrite should queue exactly one urgent ABORT");
            assertEquals(0, dataQueue.size(), "pre-commit cancelWrite must not leave opening DATA queued");

            Object abort = urgentQueue.peekFirst();
            FrameCodec.Frame frame = SessionRuntimeTestSupport.outboundFrame(abort);
            assertEquals(FrameType.ABORT, frame.type(), "pre-commit cancelWrite must consume the assigned local id with ABORT");
            assertTrue(SessionRuntimeTestSupport.outboundOpeningFrame(abort), "the first ABORT for an unseen local stream must still count as an opening-eligible frame");
            assertFalse(hasQueuedFrame(runtime, stream.streamIdInternal(), FrameType.RESET), "pre-commit cancelWrite must not degrade to RESET");
            assertEquals(stream.streamIdInternal(), stream.streamId(), "ABORT commit should make the consumed local stream id observable");

            ApplicationError aborted = assertInstanceOf(
                    ApplicationError.class,
                    stream.operationErrorLocked(),
                    "pre-commit cancelWrite should surface a local abortive stream error"
            );
            assertEquals(ErrorCode.CANCELLED.code(), aborted.code(), "pre-commit cancelWrite error code mismatch");
            assertEquals(ZmuxErrorSource.LOCAL, aborted.source(), "pre-commit cancelWrite error source mismatch");
            assertEquals(ZmuxTerminationKind.ABORT, aborted.terminationKind(), "pre-commit cancelWrite termination mismatch");

            List<Object> batch = collectReadyBatch(runtime);
            assertEquals(1, batch.size(), "writer batch should carry only the queued opening ABORT");
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "afterWriteBatchLocked",
                    new Class<?>[]{List.class, long.class, long.class, long.class},
                    batch,
                    0L,
                    1L,
                    2L
            );
            assertTrue(stream.peerVisible(), "the opening ABORT should advance the local stream to peer-visible only after write completion");
        }
    }

    @Test
    void droppedStagedOpeningFrameRequeuesOpeningAbortWithoutLeavingStaleData() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        SessionRuntimeTestSupport.queueWrite(stream, "x".getBytes());

        synchronized (runtime.lock()) {
            Object readyBatch = SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "collectReadyBatchStateLocked",
                    new Class<?>[]{boolean.class, boolean.class},
                    false,
                    true
            );
            assertFalse(stream.peerVisible(), "opening frame should not be peer-visible before writer staging");

            stream.closeWithError(ErrorCode.CANCELLED.code(), "drop staged opener");

            java.lang.reflect.Field writerRuntimeField = SessionRuntime.class.getDeclaredField("writerRuntime");
            writerRuntimeField.setAccessible(true);
            Object writerRuntime = writerRuntimeField.get(runtime);
            @SuppressWarnings("unchecked")
            List<Object> staged = (List<Object>) SessionRuntimeTestSupport.invokePrivate(
                    writerRuntime,
                    "stageReadyWriterBatchLocked",
                    new Class<?>[]{readyBatch.getClass()},
                    readyBatch
            );

            assertEquals(listOf(), staged, "writer staging should drop opening DATA that became unsendable before transport submission");
            assertFalse(hasQueuedFrame(runtime, stream.streamIdInternal(), FrameType.DATA), "dropped staged opener must not leave stale opening DATA queued");
            Deque<Object> urgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
            assertEquals(1, urgentQueue.size(), "dropped staged opener should leave exactly one replacement ABORT queued");
            Object abort = urgentQueue.peekFirst();
            assertEquals(FrameType.ABORT, SessionRuntimeTestSupport.outboundFrame(abort).type(), "replacement terminal control should be ABORT");
            assertTrue(SessionRuntimeTestSupport.outboundOpeningFrame(abort), "replacement ABORT should stay marked as the opening-eligible frame for the stream");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "writerHeldRetainedBytes"), "dropped staged opener should release writer-held accounting");
        }
    }

    @Test
    void closeWithErrorDropsQueuedDataImmediatelyInsteadOfWaitingForWriterFiltering() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
        }
        SessionRuntimeTestSupport.queueWrite(stream, "body".getBytes());
        stream.closeWithError(ErrorCode.INTERNAL.code(), "boom");

        synchronized (runtime.lock()) {
            Deque<Object> dataQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue");
            Deque<Object> urgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
            assertEquals(0, dataQueue.size(), "local abort should discard queued DATA immediately");
            assertEquals(1, urgentQueue.size(), "local abort should leave only ABORT queued for the stream");
            assertEquals(FrameType.ABORT, SessionRuntimeTestSupport.outboundFrame(urgentQueue.peekFirst()).type(), "local abort should queue ABORT after discarding queued DATA");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(stream, "queuedDataBytes"), "local abort should clear per-stream queued-data accounting immediately");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(stream, "reservedSendBytes"), "local abort should release per-stream reserved send credit immediately");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "sessionQueuedDataBytes"), "local abort should clear session queued-data accounting immediately");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "sessionReservedSendBytes"), "local abort should release session reserved send credit immediately");
        }
    }

    @Test
    void peerAbortDropsQueuedLocalSendDataImmediately() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
        }
        SessionRuntimeTestSupport.queueWrite(stream, "body".getBytes());

        synchronized (runtime.lock()) {
            stream.abortFromPeerLocked(ErrorCode.CANCELLED.code(), "", 0L);
            Deque<Object> dataQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue");
            assertEquals(0, dataQueue.size(), "peer abort should detach queued local DATA immediately");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(stream, "queuedDataBytes"), "peer abort should clear per-stream queued-data accounting immediately");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(stream, "reservedSendBytes"), "peer abort should release per-stream reserved send credit immediately");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "sessionQueuedDataBytes"), "peer abort should clear session queued-data accounting immediately");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "sessionReservedSendBytes"), "peer abort should release session reserved send credit immediately");
        }
    }

    @Test
    void runtimeWriterUsesConnectionOutputDirectly() throws Exception {
        RecordingOutputStream output = new RecordingOutputStream();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                new BasicDuplexConnection(SessionRuntimeTestSupport.emptyInput(), output),
                null,
                0L,
                Settings.defaults()
        );

        assertSame(output, runtime.outputInternal(), "writer path should not add a second buffering/copy layer");
    }

    @Test
    void mergedWriterBatchWritesWholeBatchInSingleOutputCall() throws Exception {
        RecordingOutputStream output = new RecordingOutputStream();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                new BasicDuplexConnection(SessionRuntimeTestSupport.emptyInput(), output),
                null,
                0L,
                Settings.defaults()
        );
        StreamRuntime first = (StreamRuntime) runtime.openStream();
        StreamRuntime second = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, first);
            makePeerVisible(runtime, second);
            SessionRuntimeTestSupport.queueWrite(first, "a".getBytes());
            SessionRuntimeTestSupport.queueWrite(second, "b".getBytes());

            @SuppressWarnings("unchecked")
            List<Object> batch = (List<Object>) SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "collectReadyBatchLocked",
                    new Class<?>[0]
            );
            assertEquals(2, batch.size(), "test requires a two-frame batch");

            java.lang.reflect.Field writerRuntimeField = SessionRuntime.class.getDeclaredField("writerRuntime");
            writerRuntimeField.setAccessible(true);
            Object writerRuntime = writerRuntimeField.get(runtime);

            long batchBytes = (Long) SessionRuntimeTestSupport.invokePrivate(
                    writerRuntime,
                    "writeBatch",
                    new Class<?>[]{List.class},
                    batch
            );

            assertTrue(batchBytes > 0L, "writeBatch should report encoded bytes");
            assertEquals(1, output.arrayWriteCalls(), "writer should emit the whole batch through one merged write");
            assertEquals(0, output.singleWriteCalls(), "merged writer path should not fall back to byte-at-a-time writes");

            ByteArrayInputStream input = new ByteArrayInputStream(output.bytes());
            FrameCodec.Frame firstFrame = FrameCodec.readFrame(input, Settings.defaults().limits());
            FrameCodec.Frame secondFrame = FrameCodec.readFrame(input, Settings.defaults().limits());
            assertEquals(first.streamIdInternal(), firstFrame.streamId(), "first merged frame stream id mismatch");
            assertArrayEquals("a".getBytes(), firstFrame.payload(), "first merged frame payload mismatch");
            assertEquals(second.streamIdInternal(), secondFrame.streamId(), "second merged frame stream id mismatch");
            assertArrayEquals("b".getBytes(), secondFrame.payload(), "second merged frame payload mismatch");
            assertEquals(0, input.available(), "merged batch should leave no trailing encoded bytes");
        }
    }

    @Test
    void mergedWriterBatchPreservesMultipartPayloadEncoding() throws Exception {
        RecordingOutputStream output = new RecordingOutputStream();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                new BasicDuplexConnection(SessionRuntimeTestSupport.emptyInput(), output),
                null,
                0L,
                Settings.defaults()
        );
        List<SessionRuntime.OutboundFrame> batch = new ArrayList<>();
        batch.add(new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.DATA, 0, 4L, StreamRuntime.EMPTY_BYTES),
                null,
                5,
                false,
                false,
                null,
                StreamRuntime.EMPTY_BYTES,
                0,
                5,
                new byte[][]{"hel".getBytes(), "lo".getBytes()},
                0,
                0
        ));
        batch.add(new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.DATA, 0, 8L, StreamRuntime.EMPTY_BYTES),
                null,
                5,
                false,
                false,
                null,
                StreamRuntime.EMPTY_BYTES,
                0,
                5,
                new byte[][]{"wor".getBytes(), "ld".getBytes()},
                0,
                0
        ));

        java.lang.reflect.Field writerRuntimeField = SessionRuntime.class.getDeclaredField("writerRuntime");
        writerRuntimeField.setAccessible(true);
        Object writerRuntime = writerRuntimeField.get(runtime);

        long batchBytes = (Long) SessionRuntimeTestSupport.invokePrivate(
                writerRuntime,
                "writeBatch",
                new Class<?>[]{List.class},
                batch
        );

        assertTrue(batchBytes > 0L, "writeBatch should report encoded bytes");
        assertEquals(1, output.arrayWriteCalls(), "multipart payloads should still be merged into one batch write");
        assertEquals(batchBytes, output.bytes().length, "merged writer should emit the full encoded batch");

        ByteArrayInputStream input = new ByteArrayInputStream(output.bytes());
        FrameCodec.Frame firstFrame = FrameCodec.readFrame(input, Settings.defaults().limits());
        FrameCodec.Frame secondFrame = FrameCodec.readFrame(input, Settings.defaults().limits());
        assertEquals(4L, firstFrame.streamId(), "first merged frame stream id mismatch");
        assertArrayEquals("hello".getBytes(), firstFrame.payload(), "first multipart payload mismatch");
        assertEquals(8L, secondFrame.streamId(), "second merged frame stream id mismatch");
        assertArrayEquals("world".getBytes(), secondFrame.payload(), "second multipart payload mismatch");
        assertEquals(0, input.available(), "merged multipart batch should leave no trailing encoded bytes");
    }

    @Test
    void mergedWriterBatchPreservesMixedEncodedFrameBytes() throws Exception {
        RecordingOutputStream output = new RecordingOutputStream();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                new BasicDuplexConnection(SessionRuntimeTestSupport.emptyInput(), output),
                null,
                0L,
                Settings.defaults()
        );

        List<SessionRuntime.OutboundFrame> batch = new ArrayList<>();
        batch.add(new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.PING, 0, 0L, new byte[]{0, 1, 2, 3, 4, 5, 6, 7}),
                null,
                0,
                false,
                false
        ));
        batch.add(new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.DATA, 0, 4L, "hello".getBytes()),
                null,
                0,
                false,
                false
        ));
        batch.add(new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.PONG, 0, 0L, new byte[]{7, 6, 5, 4, 3, 2, 1, 0}),
                null,
                0,
                false,
                false
        ));

        ByteArrayOutputStream want = new ByteArrayOutputStream();
        for (SessionRuntime.OutboundFrame outboundFrame : batch) {
            FrameEnvelopeCodec.writeFrame(want, outboundFrame.frame(), Settings.defaults().limits());
        }

        java.lang.reflect.Field writerRuntimeField = SessionRuntime.class.getDeclaredField("writerRuntime");
        writerRuntimeField.setAccessible(true);
        Object writerRuntime = writerRuntimeField.get(runtime);

        long batchBytes = (Long) SessionRuntimeTestSupport.invokePrivate(
                writerRuntime,
                "writeBatch",
                new Class<?>[]{List.class},
                batch
        );

        assertEquals(want.size(), batchBytes, "writeBatch should report the full mixed-frame encoded size");
        assertArrayEquals(want.toByteArray(), output.bytes(),
                "merged writer path should preserve the exact mixed-frame wire encoding");
    }

    @Test
    void mergedWriterBatchDropsOversizedEncodedScratchAfterWriteCompletion() throws Exception {
        RecordingOutputStream output = new RecordingOutputStream();
        Settings settings = Settings.defaults().toBuilder().maxFramePayload((1 << 20) + 4096L).build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                new BasicDuplexConnection(SessionRuntimeTestSupport.emptyInput(), output),
                null,
                0L,
                settings
        );
        byte[] payload = new byte[(1 << 20) + 1];
        List<SessionRuntime.OutboundFrame> batch = new ArrayList<>();
        batch.add(new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.DATA, 0, 4L, payload),
                null,
                payload.length,
                false,
                false
        ));

        java.lang.reflect.Field writerRuntimeField = SessionRuntime.class.getDeclaredField("writerRuntime");
        writerRuntimeField.setAccessible(true);
        Object writerRuntime = writerRuntimeField.get(runtime);

        long batchBytes = (Long) SessionRuntimeTestSupport.invokePrivate(
                writerRuntime,
                "writeBatch",
                new Class<?>[]{List.class},
                batch
        );

        java.lang.reflect.Field writerTransportField = writerRuntime.getClass().getDeclaredField("writerTransport");
        writerTransportField.setAccessible(true);
        Object writerTransport = writerTransportField.get(writerRuntime);

        java.lang.reflect.Field encodedScratchField = writerTransport.getClass().getDeclaredField("encodedBatchScratch");
        encodedScratchField.setAccessible(true);
        byte[] encodedScratch = (byte[]) encodedScratchField.get(writerTransport);

        assertEquals(batchBytes, output.bytes().length, "large merged batch should be written completely");
        assertEquals(0, encodedScratch.length, "oversized encoded batch scratch should be dropped after write completion");
    }

    @Test
    void mergedWriterBatchDoesNotEmitPartialBatchAfterAppendFailure() throws Exception {
        RecordingOutputStream output = new RecordingOutputStream();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                new BasicDuplexConnection(SessionRuntimeTestSupport.emptyInput(), output),
                null,
                0L,
                Settings.defaults()
        );
        List<SessionRuntime.OutboundFrame> batch = new ArrayList<>();
        batch.add(new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.DATA, 0, 4L, new byte[]{1, 2, 3, 4}),
                null,
                0,
                false,
                false
        ));
        batch.add(new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.PING, 0, 0L, new byte[]{1}),
                null,
                0,
                false,
                false
        ));

        java.lang.reflect.Field writerRuntimeField = SessionRuntime.class.getDeclaredField("writerRuntime");
        writerRuntimeField.setAccessible(true);
        Object writerRuntime = writerRuntimeField.get(runtime);

        java.lang.reflect.InvocationTargetException error = assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> SessionRuntimeTestSupport.invokePrivate(
                        writerRuntime,
                        "writeBatch",
                        new Class<?>[]{List.class},
                        batch
                )
        );
        ZmuxException frameSize = assertInstanceOf(ZmuxException.class, error.getCause());
        assertEquals(ErrorCode.FRAME_SIZE.code(), frameSize.code(), "invalid trailing frame should fail with FRAME_SIZE");
        assertEquals(0, output.bytes().length, "writer should not emit a partial batch after append failure");
    }

    private static final class RecordingOutputStream extends OutputStream {
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private int arrayWriteCalls;
        private int singleWriteCalls;

        @Override
        public void write(byte[] bytes, int offset, int length) {
            arrayWriteCalls++;
            output.write(bytes, offset, length);
        }

        @Override
        public void write(int value) {
            singleWriteCalls++;
            output.write(value);
        }

        int arrayWriteCalls() {
            return arrayWriteCalls;
        }

        int singleWriteCalls() {
            return singleWriteCalls;
        }

        byte[] bytes() {
            return output.toByteArray();
        }
    }
}
