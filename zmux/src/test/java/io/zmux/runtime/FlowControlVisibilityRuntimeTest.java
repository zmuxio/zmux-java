package io.zmux.runtime;

import io.zmux.*;
import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;
import io.zmux.protocol.Protocol;
import io.zmux.protocol.Varint62;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class FlowControlVisibilityRuntimeTest {
    private static boolean hasPendingStreamBlocked(SessionRuntime runtime, long streamId) throws Exception {
        Field field = SessionFlowControlUpdateRegistry.class.getDeclaredField("streamBlockedOffsets");
        field.setAccessible(true);
        LongLongSortedMap pending = (LongLongSortedMap) field.get(runtime.flowControlUpdateRegistryInternal());
        return pending.indexOf(streamId) >= 0;
    }

    private static SessionRuntime newReceiveReplenishRuntime() throws Exception {
        Settings localSettings = Settings.defaults().toBuilder()
                .initialMaxData(262_144L)
                .initialMaxStreamDataBidiPeerOpened(65_536L)
                .maxFramePayload(16_384L)
                .build();
        Settings peerSettings = Settings.defaults().toBuilder()
                .maxFramePayload(16_384L)
                .build();
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .settings(localSettings)
                .pendingControlBytesBudget(64L)
                .build();
        return SessionRuntimeTestSupport.newReadyRuntime(config, 0L, peerSettings);
    }

    private static StreamRuntime createPeerOpenedBidi(SessionRuntime runtime) {
        long streamId = SessionRuntime.firstPeerStreamId(Role.RESPONDER, true);
        return runtime.createPeerOpenedStreamLocked(streamId);
    }

    @SuppressWarnings("unchecked")
    private static SessionTerminalBookkeeping.Tombstone terminalTombstone(SessionRuntime runtime, long streamId)
            throws Exception {
        Field bookkeepingField = SessionRuntime.class.getDeclaredField("terminalBookkeeping");
        bookkeepingField.setAccessible(true);
        Object bookkeeping = bookkeepingField.get(runtime);
        Field tombstonesField = bookkeeping.getClass().getDeclaredField("tombstones");
        tombstonesField.setAccessible(true);
        Map<Long, SessionTerminalBookkeeping.Tombstone> tombstones =
                (Map<Long, SessionTerminalBookkeeping.Tombstone>) tombstonesField.get(bookkeeping);
        return tombstones.get(streamId);
    }

    private static ReplenishExpectations seedReceiveReplenishPending(SessionRuntime runtime, StreamRuntime stream)
            throws Exception {
        long sessionTarget = runtime.sessionWindowTargetLocked();
        long sessionReceived = sessionTarget - quarterThreshold(sessionTarget);
        runtime.setRecvSessionAdvertisedInternal(sessionTarget);
        runtime.setRecvSessionReceivedBytesInternal(sessionReceived);
        runtime.setRecvSessionPendingInternal(10L);

        long streamTarget = runtime.streamWindowTargetLocked(stream);
        long streamReceived = streamTarget - quarterThreshold(streamTarget);
        stream.raiseRecvAdvertisedLimitLocked(streamTarget);
        setStreamLongField(stream, "receiveWindowState", "recvReceivedBytes", streamReceived);
        stream.addRecvPendingLocked(10L);

        return new ReplenishExpectations(
                SessionRuntime.saturatingAdd(sessionReceived, sessionTarget),
                SessionRuntime.saturatingAdd(streamReceived, streamTarget)
        );
    }

    private static long quarterThreshold(long value) {
        return value <= 4L ? 1L : value / 4L;
    }

    private static void setStreamLongField(StreamRuntime stream, String ownerField, String fieldName, long value)
            throws Exception {
        Field owner = StreamRuntime.class.getDeclaredField(ownerField);
        owner.setAccessible(true);
        Object nestedOwner = owner.get(stream);
        Field field = nestedOwner.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setLong(nestedOwner, value);
    }

    private static long pendingSessionBlockedOffset(SessionRuntime runtime) throws Exception {
        Field field = SessionFlowControlUpdateRegistry.class.getDeclaredField("sessionBlockedOffset");
        field.setAccessible(true);
        return field.getLong(runtime.flowControlUpdateRegistryInternal());
    }

    private static long pendingStreamMaxData(SessionRuntime runtime, long streamId) throws Exception {
        Field field = SessionFlowControlUpdateRegistry.class.getDeclaredField("pendingStreamMaxData");
        field.setAccessible(true);
        LongLongSortedMap pending = (LongLongSortedMap) field.get(runtime.flowControlUpdateRegistryInternal());
        int index = pending.indexOf(streamId);
        if (index < 0) {
            throw new AssertionError("missing pending MAX_DATA for stream " + streamId);
        }
        return pending.valueAt(index);
    }

    private static void awaitWriteStreamWaiter(SessionRuntime runtime) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField("lockWaitersByKind");
        field.setAccessible(true);
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
        while (System.nanoTime() < deadlineNanos) {
            synchronized (runtime.lock()) {
                int[] waiters = (int[]) field.get(runtime);
                if (waiters[SessionRuntime.LockWaitKind.WRITE_STREAM.ordinal()] > 0) {
                    return;
                }
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("timed out waiting for blocked stream writer");
    }

    private static SessionRuntime newRuntimeWithLocalSettings(Settings localSettings) throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .settings(localSettings)
                .build();
        return SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());
    }

    private static StreamRuntime newCommittedLocalOpener(SessionRuntime runtime) throws Exception {
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        synchronized (runtime.lock()) {
            runtime.beginLocalOpenForWriteLocked(stream);
            runtime.markLocalStreamOpeningCommittedLocked(stream);
            assertFalse(stream.peerVisibleLocked(), "test requires a committed local opener that is still awaiting peer visibility");
        }
        return stream;
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

    @Test
    void zeroTargetReplenishmentThresholdsMatchRepositoryPolicy() throws Exception {
        SessionRuntime runtime = newReceiveReplenishRuntime();

        synchronized (runtime.lock()) {
            assertEquals(1L, SessionRuntime.replenishMinPending(0L, 16_384L));
            assertEquals(1L, runtime.streamEmergencyThresholdLocked(0L));
        }
    }

    @Test
    void sessionFlowControlViolationDoesNotMarkLocalOpenerPeerVisible() throws Exception {
        SessionRuntime runtime = newRuntimeWithLocalSettings(
                Settings.defaults().toBuilder()
                        .initialMaxData(0L)
                        .initialMaxStreamDataBidiLocallyOpened(16L)
                        .build()
        );
        StreamRuntime stream = newCommittedLocalOpener(runtime);

        IOException error = assertThrows(
                IOException.class,
                () -> handleDataFrame(runtime, new FrameCodec.Frame(
                        FrameType.DATA,
                        0,
                        stream.streamIdInternal(),
                        new byte[]{1}
                )),
                "session max_data violation should stay a session error"
        );

        synchronized (runtime.lock()) {
            assertFalse(stream.peerVisibleLocked(), "invalid DATA must not make a local opener peer-visible");
            assertEquals(ErrorCode.FLOW_CONTROL.code(), ZmuxErrors.code(error, -1L), "session flow-control error code mismatch");
            assertTrue(runtime.liveStreamLocked(stream.streamIdInternal()) == stream, "session-level violation should not compact the stream in the direct handler");
        }
    }

    @Test
    void streamFlowControlViolationDoesNotMarkLocalOpenerPeerVisible() throws Exception {
        SessionRuntime runtime = newRuntimeWithLocalSettings(
                Settings.defaults().toBuilder()
                        .initialMaxData(16L)
                        .initialMaxStreamDataBidiLocallyOpened(0L)
                        .build()
        );
        StreamRuntime stream = newCommittedLocalOpener(runtime);

        handleDataFrame(runtime, new FrameCodec.Frame(
                FrameType.DATA,
                0,
                stream.streamIdInternal(),
                new byte[]{1}
        ));

        synchronized (runtime.lock()) {
            Deque<Object> urgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
            assertFalse(stream.peerVisibleLocked(), "stream max_data violation must not make a local opener peer-visible");
            assertEquals(1, urgentQueue.size(), "stream max_data violation should queue one ABORT");
            FrameCodec.Frame abort = SessionRuntimeTestSupport.outboundFrame(urgentQueue.peekFirst());
            assertEquals(FrameType.ABORT, abort.type(), "stream max_data violation frame type mismatch");
            assertEquals(stream.streamIdInternal(), abort.streamId(), "stream max_data violation stream id mismatch");
        }
    }

    @Test
    void rapidLocalFlowControlAbortChurnTriggersProtocolClose() throws Exception {
        Settings localSettings = Settings.defaults().toBuilder()
                .initialMaxData(2L)
                .initialMaxStreamDataBidiPeerOpened(0L)
                .build();
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .settings(localSettings)
                .visibleTerminalChurnWindow(Duration.ofHours(1L))
                .visibleTerminalChurnThreshold(1)
                .acceptBacklogLimit(1024)
                .acceptBacklogBytesLimit(1L << 20)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());
        long firstPeerBidi = SessionRuntime.firstPeerStreamId(Role.RESPONDER, true);

        handleDataFrame(runtime, new FrameCodec.Frame(FrameType.DATA, 0, firstPeerBidi, new byte[]{1}));

        IOException error = assertThrows(
                IOException.class,
                () -> handleDataFrame(runtime, new FrameCodec.Frame(
                        FrameType.DATA,
                        0,
                        firstPeerBidi + 4L,
                        new byte[]{1}
                ))
        );

        assertEquals(ErrorCode.PROTOCOL.code(), ZmuxErrors.code(error, -1L),
                "rapid local flow-control abort churn should fail as protocol abuse");
        assertEquals(2L, runtime.stats().diagnostics().visibleTerminalChurnEvents(),
                "local flow-control aborts should contribute to visible terminal churn diagnostics");
    }

    @Test
    void dataFrameExceedingSessionAndStreamWindowPrefersSessionFlowControl() throws Exception {
        SessionRuntime runtime = newRuntimeWithLocalSettings(
                Settings.defaults().toBuilder()
                        .initialMaxData(0L)
                        .initialMaxStreamDataBidiLocallyOpened(0L)
                        .build()
        );
        StreamRuntime stream = newCommittedLocalOpener(runtime);

        IOException error = assertThrows(
                IOException.class,
                () -> handleDataFrame(runtime, new FrameCodec.Frame(
                        FrameType.DATA,
                        0,
                        stream.streamIdInternal(),
                        new byte[]{1}
                )),
                "simultaneous session+stream max_data violation should stay a session error"
        );

        synchronized (runtime.lock()) {
            assertEquals(ErrorCode.FLOW_CONTROL.code(), ZmuxErrors.code(error, -1L), "flow-control error code mismatch");
            assertTrue(SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue").isEmpty(), "session-level violation must not queue a stream ABORT");
        }
    }

    @Test
    void finQueuedStreamDropsPendingBlockedInsteadOfFlushingIt() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "beginLocalOpenLocked",
                    new Class<?>[]{StreamRuntime.class},
                    stream
            );
            runtime.markLocalStreamOpeningCommittedLocked(stream);
            runtime.markPeerVisibleLocked(stream);

            stream.markFinQueuedLocked();
            assertTrue(runtime.flowControlUpdateRegistryInternal().queueBlockedFrameLocked(stream.streamIdInternal(), 0L));
            assertTrue(stream.blockedQueuedLocked(), "queued BLOCKED should mark the stream as blocked");

            List<SessionFlowControlUpdateRegistry.PendingFrame> batch = new ArrayList<>();
            runtime.flowControlUpdateRegistryInternal().takePendingWindowUpdatesLocked(batch, 8, stream.streamIdInternal());

            assertTrue(batch.isEmpty(), "FIN_QUEUED stream must not flush a stale BLOCKED frame");
            assertFalse(stream.blockedQueuedLocked(), "dropped BLOCKED should clear blocked bookkeeping");
        }
    }

    @Test
    void stopSeenStreamDropsPendingBlockedImmediately() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "beginLocalOpenLocked",
                    new Class<?>[]{StreamRuntime.class},
                    stream
            );
            runtime.markLocalStreamOpeningCommittedLocked(stream);
            runtime.markPeerVisibleLocked(stream);

            assertTrue(runtime.flowControlUpdateRegistryInternal().queueBlockedFrameLocked(stream.streamIdInternal(), 7L));
            assertTrue(stream.blockedQueuedLocked(), "queued BLOCKED should mark the stream as blocked");
            assertTrue(hasPendingStreamBlocked(runtime, stream.streamIdInternal()), "pending BLOCKED should be registered before peer stop");

            assertTrue(
                    stream.stopSendingFromPeerLocked(
                            ErrorCode.CANCELLED.code(),
                            "peer stop",
                            "peer stop".getBytes(StandardCharsets.UTF_8).length
                    ),
                    "first peer STOP_SENDING on an open send half should remain actionable"
            );
            assertFalse(stream.blockedQueuedLocked(), "peer STOP_SENDING should clear blocked bookkeeping immediately");
            assertFalse(hasPendingStreamBlocked(runtime, stream.streamIdInternal()), "peer STOP_SENDING should drop pending BLOCKED immediately");
            assertFalse(stream.shouldFlushPendingStreamBlockedLocked(), "stop-seen send half must not flush BLOCKED");
            assertFalse(stream.shouldRetainPendingStreamBlockedLocked(), "stop-seen send half must not retain BLOCKED");

            List<SessionFlowControlUpdateRegistry.PendingFrame> batch = new ArrayList<>();
            runtime.flowControlUpdateRegistryInternal().takePendingWindowUpdatesLocked(batch, 8, stream.streamIdInternal());

            assertTrue(batch.isEmpty(), "STOP_SEEN stream must not flush a stale BLOCKED frame");
        }
    }

    @Test
    void closeStartSuppressesAndClearsPendingNonCloseFlowControl() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "beginLocalOpenLocked",
                    new Class<?>[]{StreamRuntime.class},
                    stream
            );
            runtime.markLocalStreamOpeningCommittedLocked(stream);
            runtime.markPeerVisibleLocked(stream);

            assertTrue(runtime.queueSessionMaxDataLocked(17L), "session MAX_DATA should queue before close-start");
            assertTrue(
                    runtime.flowControlUpdateRegistryInternal().queueBlockedFrameLocked(stream.streamIdInternal(), 23L),
                    "stream BLOCKED should queue before close-start"
            );
            assertTrue(stream.blockedQueuedLocked(), "queued BLOCKED should mark the stream");
            assertTrue(
                    runtime.outboundQueueBookkeepingInternal().pendingControlBytesLocked() > 0L,
                    "pending flow-control frames should reserve control budget"
            );

            runtime.setCloseFrameQueuedInternal(true);

            assertFalse(runtime.queueSessionMaxDataLocked(29L), "close-start must reject new session MAX_DATA");
            assertFalse(
                    runtime.flowControlUpdateRegistryInternal().queueBlockedFrameLocked(stream.streamIdInternal(), 31L),
                    "close-start must reject new BLOCKED"
            );

            List<SessionFlowControlUpdateRegistry.PendingFrame> batch = new ArrayList<>();
            runtime.flowControlUpdateRegistryInternal().takePendingWindowUpdatesLocked(batch, 8, stream.streamIdInternal());

            assertTrue(batch.isEmpty(), "close-start should not leave non-close flow-control frames to flush");
            assertFalse(stream.blockedQueuedLocked(), "clearing pending BLOCKED should reset stream bookkeeping");
            assertEquals(
                    0L,
                    runtime.outboundQueueBookkeepingInternal().pendingControlBytesLocked(),
                    "clearing pending non-close flow-control should release control budget"
            );
        }
    }

    @Test
    void streamScopedPendingControlAccountsStreamIdVarintAndReleasesExactly() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        synchronized (runtime.lock()) {
            assertTrue(runtime.queueStreamMaxDataLocked(64L, 63L), "stream MAX_DATA should queue");
            assertEquals(
                    Varint62.length(64L) + Varint62.length(63L),
                    runtime.outboundQueueBookkeepingInternal().pendingControlBytesLocked(),
                    "stream-scoped pending MAX_DATA should account stream_id plus value"
            );

            assertTrue(runtime.queueStreamMaxDataLocked(64L, 64L), "larger stream MAX_DATA should replace pending value");
            assertEquals(
                    Varint62.length(64L) + Varint62.length(64L),
                    runtime.outboundQueueBookkeepingInternal().pendingControlBytesLocked(),
                    "pending replacement should release the previous value-sized reservation"
            );

            runtime.flowControlUpdateRegistryInternal().clearStreamStateLocked(64L);
            assertEquals(0L, runtime.outboundQueueBookkeepingInternal().pendingControlBytesLocked());

            assertTrue(
                    runtime.flowControlUpdateRegistryInternal().queueBlockedFrameLocked(64L, 63L),
                    "stream BLOCKED should queue"
            );
            assertEquals(
                    Varint62.length(64L) + Varint62.length(63L),
                    runtime.outboundQueueBookkeepingInternal().pendingControlBytesLocked(),
                    "stream-scoped pending BLOCKED should account stream_id plus offset"
            );

            runtime.flowControlUpdateRegistryInternal().clearBlockedFrameLocked(64L);
            assertEquals(0L, runtime.outboundQueueBookkeepingInternal().pendingControlBytesLocked());
        }
    }

    @Test
    void pendingMaxDataClampsValuesAboveVarint62AtRegistryBoundary() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        synchronized (runtime.lock()) {
            assertTrue(runtime.queueSessionMaxDataLocked(Long.MAX_VALUE), "session MAX_DATA should clamp and queue");
            assertEquals(
                    Varint62.length(Protocol.MAX_VARINT62),
                    runtime.outboundQueueBookkeepingInternal().pendingControlBytesLocked(),
                    "session MAX_DATA should account the clamped varint62 value"
            );

            List<SessionFlowControlUpdateRegistry.PendingFrame> batch = new ArrayList<>();
            runtime.flowControlUpdateRegistryInternal().takePendingWindowUpdatesLocked(batch, 8, null);
            assertEquals(1, batch.size(), "clamped session MAX_DATA should drain");
            assertEquals(Protocol.MAX_VARINT62, batch.get(0).value(), "session MAX_DATA should clamp to varint62 max");
            assertEquals(0L, runtime.outboundQueueBookkeepingInternal().pendingControlBytesLocked());

            assertTrue(runtime.queueStreamMaxDataLocked(64L, Long.MAX_VALUE), "stream MAX_DATA should clamp and queue");
            assertEquals(
                    Varint62.length(64L) + Varint62.length(Protocol.MAX_VARINT62),
                    runtime.outboundQueueBookkeepingInternal().pendingControlBytesLocked(),
                    "stream MAX_DATA should account stream_id plus clamped value"
            );
            assertEquals(
                    Protocol.MAX_VARINT62,
                    pendingStreamMaxData(runtime, 64L),
                    "stream MAX_DATA should clamp to varint62 max"
            );
        }
    }

    @Test
    void replenishReceivePreservesCreditWhenMaxDataCannotQueue() throws Exception {
        SessionRuntime runtime = newReceiveReplenishRuntime();
        synchronized (runtime.lock()) {
            StreamRuntime stream = createPeerOpenedBidi(runtime);
            seedReceiveReplenishPending(runtime, stream);
            SessionRuntimeTestSupport.setLongField(runtime, "pendingControlBytes", 64L);

            runtime.onReadDiscardLocked(stream, true);

            assertEquals(10L, runtime.recvSessionPendingInternal(), "session pending credit must survive queue rejection");
            assertEquals(10L, stream.recvPendingLocked(), "stream pending credit must survive queue rejection");
            assertTrue(runtime.receiveReplenishRetryLocked(), "queue rejection should arm receive replenish retry");
            assertFalse(runtime.hasPendingMaxDataLocked(), "rejected replenish must not publish partial MAX_DATA state");
        }
    }

    @Test
    void receiveReplenishRetryQueuesPendingCreditAfterControlBudgetFrees() throws Exception {
        SessionRuntime runtime = newReceiveReplenishRuntime();
        synchronized (runtime.lock()) {
            StreamRuntime stream = createPeerOpenedBidi(runtime);
            ReplenishExpectations expectations = seedReceiveReplenishPending(runtime, stream);
            SessionRuntimeTestSupport.setLongField(runtime, "pendingControlBytes", 64L);

            runtime.onReadDiscardLocked(stream, true);
            SessionRuntimeTestSupport.setLongField(runtime, "pendingControlBytes", 0L);
            runtime.retryReceiveReplenishLocked();

            assertEquals(
                    expectations.sessionAdvertised(),
                    runtime.recvSessionAdvertisedInternal(),
                    "session advertised credit should advance on retry"
            );
            assertEquals(0L, runtime.recvSessionPendingInternal(), "session pending credit should clear after retry");
            assertEquals(
                    expectations.streamAdvertised(),
                    stream.recvAdvertisedLimit(),
                    "stream advertised credit should advance on retry"
            );
            assertEquals(0L, stream.recvPendingLocked(), "stream pending credit should clear after retry");
            assertFalse(runtime.receiveReplenishRetryLocked(), "successful retry should clear retry marker");

            List<SessionFlowControlUpdateRegistry.PendingFrame> batch = new ArrayList<>();
            runtime.flowControlUpdateRegistryInternal().takePendingWindowUpdatesLocked(batch, 8, stream.streamIdInternal());
            assertEquals(2, batch.size(), "retry should queue session and stream MAX_DATA");
            assertEquals(FrameType.MAX_DATA, batch.get(0).type());
            assertEquals(0L, batch.get(0).streamId());
            assertEquals(expectations.sessionAdvertised(), batch.get(0).value());
            assertEquals(FrameType.MAX_DATA, batch.get(1).type());
            assertEquals(stream.streamIdInternal(), batch.get(1).streamId());
            assertEquals(expectations.streamAdvertised(), batch.get(1).value());
        }
    }

    @Test
    void lateDataDiscardMarksReceiveReplenishRetryWhenMaxDataCannotQueue() throws Exception {
        SessionRuntime runtime = newReceiveReplenishRuntime();
        long streamId;
        long sessionTarget;
        long sessionReceived;
        synchronized (runtime.lock()) {
            StreamRuntime stream = createPeerOpenedBidi(runtime);
            streamId = stream.streamIdInternal();
            runtime.markPeerVisibleLocked(stream);
            sessionTarget = runtime.sessionWindowTargetLocked();
            sessionReceived = sessionTarget - quarterThreshold(sessionTarget);
            runtime.setRecvSessionAdvertisedInternal(sessionTarget);
            runtime.setRecvSessionReceivedBytesInternal(sessionReceived);
            stream.resetFromPeerLocked(ErrorCode.CANCELLED.code(), "", 0L);
            SessionRuntimeTestSupport.setLongField(runtime, "pendingControlBytes", 64L);
        }

        handleDataFrame(runtime, new FrameCodec.Frame(FrameType.DATA, 0, streamId, new byte[10]));

        synchronized (runtime.lock()) {
            assertEquals(sessionTarget, runtime.recvSessionAdvertisedInternal(), "late-discard MAX_DATA queue rejection must not advance advertised credit");
            assertEquals(sessionReceived + 10L, runtime.recvSessionReceivedBytesInternal(), "late data should still consume session receive credit");
            assertEquals(10L, runtime.recvSessionPendingInternal(), "late-discard session credit must remain pending after queue rejection");
            assertTrue(runtime.receiveReplenishRetryLocked(), "late-discard queue rejection should arm receive replenish retry");
            assertFalse(runtime.hasPendingMaxDataLocked(), "rejected late-discard replenish must not publish partial MAX_DATA state");
        }
    }

    @Test
    void lateDataDiscardQueuesSessionMaxDataImmediatelyWhenQueueFits() throws Exception {
        SessionRuntime runtime = newReceiveReplenishRuntime();
        long streamId;
        long sessionTarget;
        synchronized (runtime.lock()) {
            StreamRuntime stream = createPeerOpenedBidi(runtime);
            streamId = stream.streamIdInternal();
            sessionTarget = runtime.sessionWindowTargetLocked();
            runtime.setRecvSessionAdvertisedInternal(sessionTarget);
            runtime.setRecvSessionReceivedBytesInternal(0L);
            stream.resetFromPeerLocked(ErrorCode.CANCELLED.code(), "", 0L);
        }

        handleDataFrame(runtime, new FrameCodec.Frame(FrameType.DATA, 0, streamId, new byte[10]));

        synchronized (runtime.lock()) {
            long expectedAdvertised = sessionTarget + 10L;
            assertEquals(expectedAdvertised, runtime.recvSessionAdvertisedInternal(),
                    "late-discard should immediately return session receive credit");
            assertEquals(10L, runtime.recvSessionReceivedBytesInternal(),
                    "late data should still consume session receive credit");
            assertEquals(0L, runtime.recvSessionPendingInternal(),
                    "queued late-discard MAX_DATA should not leave pending receive credit");
            assertFalse(runtime.receiveReplenishRetryLocked(),
                    "successful late-discard MAX_DATA queueing should not arm retry");

            List<SessionFlowControlUpdateRegistry.PendingFrame> batch = new ArrayList<>();
            runtime.flowControlUpdateRegistryInternal().takePendingWindowUpdatesLocked(batch, 8, streamId);
            assertEquals(1, batch.size(), "late-discard should publish one session MAX_DATA");
            assertEquals(FrameType.MAX_DATA, batch.get(0).type());
            assertEquals(0L, batch.get(0).streamId());
            assertEquals(expectedAdvertised, batch.get(0).value());
        }
    }

    @Test
    void hiddenLateDataDiscardCountsHiddenUnreadBytes() throws Exception {
        SessionRuntime runtime = newReceiveReplenishRuntime();
        long streamId;
        synchronized (runtime.lock()) {
            StreamRuntime stream = createPeerOpenedBidi(runtime);
            assertFalse(stream.applicationVisible(), "test requires a hidden peer-opened stream");
            streamId = stream.streamIdInternal();
            runtime.setRecvSessionAdvertisedInternal(runtime.sessionWindowTargetLocked());
            stream.resetFromPeerLocked(ErrorCode.CANCELLED.code(), "", 0L);
        }

        handleDataFrame(runtime, new FrameCodec.Frame(FrameType.DATA, 0, streamId, new byte[4]));

        assertEquals(4L, runtime.stats().hiddenState().unreadBytesDiscarded(),
                "hidden late-discard bytes should be reported as hidden unread discards");
    }

    @Test
    void lateDataOnFullyTerminalLiveStreamSkipsPerStreamLateCounter() throws Exception {
        SessionRuntime runtime = newReceiveReplenishRuntime();
        StreamRuntime stream;
        long streamId;
        long beforeLateData;
        synchronized (runtime.lock()) {
            stream = createPeerOpenedBidi(runtime);
            streamId = stream.streamIdInternal();
            runtime.markPeerVisibleLocked(stream);
            runtime.setRecvSessionAdvertisedInternal(1024L);
            runtime.setRecvSessionReceivedBytesInternal(0L);
            stream.terminalStateInternal().recordLocalWriteReset(ErrorCode.CANCELLED.code());
            stream.halfStateInternal().markSendReset();
            stream.finishReceiveLocked();
            assertTrue(stream.fullyTerminalLocked(), "test requires a fully terminal live stream");
            beforeLateData = stream.lateDataReceivedLocked();
        }

        handleDataFrame(runtime, new FrameCodec.Frame(FrameType.DATA, 0, streamId, new byte[]{1, 2, 3, 4}));

        synchronized (runtime.lock()) {
            assertEquals(beforeLateData, stream.lateDataReceivedLocked(),
                    "fully terminal live stream should not consume the per-stream late-data budget");
            assertEquals(4L, runtime.aggregateLateDataReceivedInternal(),
                    "fully terminal late DATA should still count against the aggregate cap");
            assertEquals(4L, runtime.recvSessionReceivedBytesInternal(),
                    "fully terminal late DATA should still consume session receive credit");
            assertNull(runtime.liveStreamLocked(streamId), "fully terminal stream should compact after late DATA");
        }
    }

    @Test
    void lateDataOnTerminalTombstoneCountsSessionFlowControl() throws Exception {
        SessionRuntime runtime = newReceiveReplenishRuntime();
        long streamId = SessionRuntime.firstPeerStreamId(Role.RESPONDER, true);
        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "putTombstoneLocked",
                    new Class<?>[]{long.class, SessionTerminalBookkeeping.Tombstone.class},
                    streamId,
                    new SessionTerminalBookkeeping.Tombstone(
                            true,
                            true,
                            0L,
                            "",
                            LateDataCause.NONE
                    )
            );
            runtime.setRecvSessionAdvertisedInternal(10L);
            runtime.setRecvSessionReceivedBytesInternal(10L);
        }

        IOException error = assertThrows(
                IOException.class,
                () -> handleDataFrame(runtime, new FrameCodec.Frame(FrameType.DATA, 0, streamId, new byte[]{1}))
        );

        assertEquals(ErrorCode.FLOW_CONTROL.code(), ZmuxErrors.code(error, -1L),
                "late DATA on terminal tombstone should still consume session flow-control credit");
        synchronized (runtime.lock()) {
            assertEquals(10L, runtime.recvSessionReceivedBytesInternal(),
                    "flow-control rejected late tombstone DATA must not advance received bytes");
        }
    }

    @Test
    void lateDataOnTerminalTombstoneCountsPerStreamCap() throws Exception {
        SessionRuntime runtime = newReceiveReplenishRuntime();
        long streamId = SessionRuntime.firstPeerStreamId(Role.RESPONDER, true);
        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "putTombstoneLocked",
                    new Class<?>[]{long.class, SessionTerminalBookkeeping.Tombstone.class},
                    streamId,
                    new SessionTerminalBookkeeping.Tombstone(
                            true,
                            true,
                            0L,
                            "",
                            LateDataCause.NONE,
                            false,
                            0L,
                            0L,
                            1L,
                            true
                    )
            );
            runtime.setRecvSessionAdvertisedInternal(10L);
            runtime.setRecvSessionReceivedBytesInternal(0L);
        }

        handleDataFrame(runtime, new FrameCodec.Frame(FrameType.DATA, 0, streamId, new byte[]{1}));
        IOException error = assertThrows(
                IOException.class,
                () -> handleDataFrame(runtime, new FrameCodec.Frame(FrameType.DATA, 0, streamId, new byte[]{2}))
        );

        assertEquals(ErrorCode.PROTOCOL.code(), ZmuxErrors.code(error, -1L),
                "late DATA beyond terminal tombstone per-stream cap should fail with PROTOCOL");
        synchronized (runtime.lock()) {
            assertEquals(2L, terminalTombstone(runtime, streamId).lateDataReceived(),
                    "terminal tombstone should retain its own late-data counter");
            assertEquals(2L, runtime.aggregateLateDataReceivedInternal(),
                    "terminal tombstone late DATA should also count against the aggregate cap");
        }
    }

    @Test
    void releasingQueuedDataWakesWriterBlockedOnReturnedSendCredit() throws Exception {
        Settings peerSettings = Settings.defaults().toBuilder()
                .initialMaxData(1L)
                .initialMaxStreamDataBidiPeerOpened(2L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, peerSettings);
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        synchronized (runtime.lock()) {
            runtime.beginLocalOpenForWriteLocked(stream);
            runtime.markLocalStreamOpeningCommittedLocked(stream);
            runtime.markPeerVisibleLocked(stream);
        }
        SessionRuntimeTestSupport.queueWrite(stream, new byte[]{1});

        CountDownLatch writerDone = new CountDownLatch(1);
        AtomicReference<Throwable> writerError = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            try {
                SessionRuntimeTestSupport.queueWrite(stream, new byte[]{2});
            } catch (Throwable error) {
                writerError.set(error);
            } finally {
                writerDone.countDown();
            }
        }, "zmux-test-credit-release-writer");
        writer.start();
        try {
            awaitWriteStreamWaiter(runtime);
            synchronized (runtime.lock()) {
                assertEquals(
                        runtime.sessionSendLimitLocked(),
                        pendingSessionBlockedOffset(runtime),
                        "send-credit blocked writer should queue session BLOCKED before credit returns"
                );
                Deque<Object> dataQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue");
                Object queued = dataQueue.pollFirst();
                assertTrue(queued instanceof SessionRuntime.OutboundFrame, "expected queued DATA frame");
                runtime.releaseQueuedDataLocked((SessionRuntime.OutboundFrame) queued);
                assertEquals(
                        -1L,
                        pendingSessionBlockedOffset(runtime),
                        "returned session send credit should clear stale pending session BLOCKED"
                );
            }

            assertTrue(
                    writerDone.await(1, TimeUnit.SECONDS),
                    "writer blocked on send credit should wake when queued DATA releases reserved credit"
            );
            assertNull(writerError.get(), "writer should complete after released send credit");
        } finally {
            if (writerDone.getCount() != 0L) {
                writer.interrupt();
            }
            writer.join(1000L);
        }
    }

    @Test
    void releasingQueuedDataClearsStreamBlockedWhenStreamCreditReturns() throws Exception {
        Settings peerSettings = Settings.defaults().toBuilder()
                .initialMaxData(2L)
                .initialMaxStreamDataBidiPeerOpened(1L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, peerSettings);
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        synchronized (runtime.lock()) {
            runtime.beginLocalOpenForWriteLocked(stream);
            runtime.markLocalStreamOpeningCommittedLocked(stream);
            runtime.markPeerVisibleLocked(stream);
        }
        SessionRuntimeTestSupport.queueWrite(stream, new byte[]{1});

        CountDownLatch writerDone = new CountDownLatch(1);
        AtomicReference<Throwable> writerError = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            try {
                SessionRuntimeTestSupport.queueWrite(stream, new byte[]{2});
            } catch (Throwable error) {
                writerError.set(error);
            } finally {
                writerDone.countDown();
            }
        }, "zmux-test-stream-credit-release-writer");
        writer.start();
        try {
            awaitWriteStreamWaiter(runtime);
            synchronized (runtime.lock()) {
                assertTrue(
                        hasPendingStreamBlocked(runtime, stream.streamIdInternal()),
                        "stream-credit blocked writer should queue stream BLOCKED before credit returns"
                );
                Deque<Object> dataQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue");
                Object queued = dataQueue.pollFirst();
                assertTrue(queued instanceof SessionRuntime.OutboundFrame, "expected queued DATA frame");
                runtime.releaseQueuedDataLocked((SessionRuntime.OutboundFrame) queued);
                assertFalse(
                        hasPendingStreamBlocked(runtime, stream.streamIdInternal()),
                        "returned stream send credit should clear stale pending stream BLOCKED"
                );
            }

            assertTrue(
                    writerDone.await(1, TimeUnit.SECONDS),
                    "writer blocked on stream send credit should wake when queued DATA releases reserved credit"
            );
            assertNull(writerError.get(), "writer should complete after released stream send credit");
        } finally {
            if (writerDone.getCount() != 0L) {
                writer.interrupt();
            }
            writer.join(1000L);
        }
    }

    @Test
    void releasingQueuedDataWakesWriterBlockedOnlyByMemoryHeadroom() throws Exception {
        Settings peerSettings = Settings.defaults().toBuilder()
                .initialMaxData(65_536L)
                .initialMaxStreamDataBidiPeerOpened(65_536L)
                .build();
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .sessionMemoryCap(32_768L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, peerSettings);
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        int firstWriteBytes;
        synchronized (runtime.lock()) {
            runtime.beginLocalOpenForWriteLocked(stream);
            runtime.markLocalStreamOpeningCommittedLocked(stream);
            runtime.markPeerVisibleLocked(stream);
            long headroomToThreshold = runtime.sessionMemoryHighThresholdLocked()
                    - runtime.trackedSessionMemoryLocked()
                    - 1L;
            assertTrue(headroomToThreshold > 0L, "test requires initial tracked memory below high threshold");
            firstWriteBytes = (int) (headroomToThreshold - 16L);
        }
        SessionRuntimeTestSupport.queueWrite(stream, new byte[firstWriteBytes]);
        synchronized (runtime.lock()) {
            assertTrue(
                    runtime.trackedSessionMemoryLocked() < runtime.sessionMemoryHighThresholdLocked(),
                    "first write should leave current memory below the high threshold"
            );
            assertTrue(
                    runtime.sessionWriteMemoryBlockedLocked(32L),
                    "second write should be blocked only by projected memory headroom"
            );
        }

        CountDownLatch writerDone = new CountDownLatch(1);
        AtomicReference<Throwable> writerError = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            try {
                SessionRuntimeTestSupport.queueWrite(stream, new byte[32]);
            } catch (Throwable error) {
                writerError.set(error);
            } finally {
                writerDone.countDown();
            }
        }, "zmux-test-memory-release-writer");
        writer.start();
        try {
            awaitWriteStreamWaiter(runtime);
            synchronized (runtime.lock()) {
                Deque<Object> dataQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue");
                Object queued = dataQueue.pollFirst();
                assertTrue(queued instanceof SessionRuntime.OutboundFrame, "expected queued DATA frame");
                runtime.releaseQueuedDataLocked((SessionRuntime.OutboundFrame) queued);
            }

            assertTrue(
                    writerDone.await(1, TimeUnit.SECONDS),
                    "writer blocked by projected memory headroom should wake on any release below the threshold"
            );
            assertNull(writerError.get(), "writer should complete after memory headroom is released");
        } finally {
            if (writerDone.getCount() != 0L) {
                writer.interrupt();
            }
            writer.join(1000L);
        }
    }

    private static final class ReplenishExpectations {
        private final long sessionAdvertised;
        private final long streamAdvertised;

        private ReplenishExpectations(long sessionAdvertised, long streamAdvertised) {
            this.sessionAdvertised = sessionAdvertised;
            this.streamAdvertised = streamAdvertised;
        }

        private long sessionAdvertised() {
            return this.sessionAdvertised;
        }

        private long streamAdvertised() {
            return this.streamAdvertised;
        }
    }
}
