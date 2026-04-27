package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class SessionDiagnosticsRuntimeTest {
    private static StreamRuntime createPeerOpenedBidi(SessionRuntime runtime) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField("readerRuntime");
        field.setAccessible(true);
        Object readerRuntime = field.get(runtime);
        synchronized (runtime.lock()) {
            return (StreamRuntime) SessionRuntimeTestSupport.invokePrivate(
                    readerRuntime,
                    "createPeerOpenedStreamLocked",
                    new Class<?>[]{long.class},
                    SessionRuntime.firstPeerStreamId(Role.RESPONDER, true)
            );
        }
    }

    private static void handleReaderFrame(SessionRuntime runtime, String methodName, FrameCodec.Frame frame) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField("readerRuntime");
        field.setAccessible(true);
        Object readerRuntime = field.get(runtime);
        SessionRuntimeTestSupport.invokePrivate(
                readerRuntime,
                methodName,
                new Class<?>[]{FrameCodec.Frame.class},
                frame
        );
    }

    private static int getIntField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getInt(target);
    }

    private static FrameCodec.Frame dataFrame(long streamId, String payload) {
        return new FrameCodec.Frame(
                FrameType.DATA,
                0,
                streamId,
                payload.getBytes(StandardCharsets.UTF_8)
        );
    }

    private static FrameCodec.Frame controlFrame(FrameType type, long streamId, long code) throws Exception {
        return new FrameCodec.Frame(
                type,
                0,
                streamId,
                FrameCodec.buildErrorPayload(code, "", Settings.defaults().maxControlPayloadBytes())
        );
    }

    private static void appendTlv(ByteArrayOutputStream output, long type, byte[] value) throws Exception {
        Varint62.write(output, type);
        Varint62.write(output, value.length);
        output.write(value);
    }

    private static byte[] duplicateStandardDiagPayload(byte[] basePayload, String reason) throws Exception {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        payload.write(basePayload);
        appendTlv(payload, Protocol.DIAG_RETRY_AFTER_MILLIS, Varint62.encode(1L));
        appendTlv(payload, Protocol.DIAG_RETRY_AFTER_MILLIS, Varint62.encode(2L));
        if (!reason.isEmpty()) {
            appendTlv(payload, Protocol.DIAG_DEBUG_TEXT, reason.getBytes(StandardCharsets.UTF_8));
        }
        return payload.toByteArray();
    }

    private static byte[] invalidUtf8DiagPayload(byte[] basePayload) throws Exception {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        payload.write(basePayload);
        appendTlv(payload, Protocol.DIAG_DEBUG_TEXT, new byte[]{(byte) 0xe2, (byte) 0x82});
        return payload.toByteArray();
    }

    private static FrameCodec.Frame controlFrameWithDuplicateStandardDiag(FrameType type, long streamId, long code, String reason)
            throws Exception {
        return new FrameCodec.Frame(
                type,
                0,
                streamId,
                duplicateStandardDiagPayload(
                        FrameCodec.buildErrorPayload(code, "", Settings.defaults().maxControlPayloadBytes()),
                        reason
                )
        );
    }

    private static FrameCodec.Frame controlFrameWithInvalidUtf8Diag(FrameType type, long streamId, long code)
            throws Exception {
        return new FrameCodec.Frame(
                type,
                0,
                streamId,
                invalidUtf8DiagPayload(FrameCodec.buildErrorPayload(code, "", Settings.defaults().maxControlPayloadBytes()))
        );
    }

    private static byte[] encodeFrame(FrameType type, int flags, long streamId, byte[] payload) throws IOException {
        java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
        long frameLength = 1L + Varint62.length(streamId) + payload.length;
        Varint62.write(output, frameLength);
        output.write(type.code() | flags);
        Varint62.write(output, streamId);
        output.write(payload);
        return output.toByteArray();
    }

    private static BasicDuplexConnection failingOutputConnection(String message) {
        return new BasicDuplexConnection(
                SessionRuntimeTestSupport.emptyInput(),
                new OutputStream() {
                    @Override
                    public void write(int b) throws IOException {
                        throw new IOException(message);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        throw new IOException(message);
                    }
                }
        );
    }

    private static BasicDuplexConnection blockingOutputConnection(CountDownLatch releaseWrites) {
        return new BasicDuplexConnection(
                SessionRuntimeTestSupport.emptyInput(),
                new OutputStream() {
                    @Override
                    public void write(int b) throws IOException {
                        awaitRelease();
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        awaitRelease();
                    }

                    private void awaitRelease() throws IOException {
                        try {
                            releaseWrites.await();
                        } catch (InterruptedException interruptedException) {
                            Thread.currentThread().interrupt();
                            throw new IOException("synthetic blocked close write interrupted", interruptedException);
                        }
                    }
                }
        );
    }

    private static Thread startWriterLoop(SessionRuntime runtime, AtomicReference<Throwable> failure, String name) {
        Thread writer = new Thread(() -> {
            try {
                SessionRuntimeTestSupport.invokePrivate(runtime, "writerLoop", new Class<?>[0]);
            } catch (Throwable throwable) {
                failure.set(throwable);
            }
        }, name);
        writer.start();
        return writer;
    }

    private static Thread startReaderLoop(SessionRuntime runtime, AtomicReference<Throwable> failure, String name) {
        Thread reader = new Thread(() -> {
            try {
                SessionRuntimeTestSupport.invokePrivate(runtime, "readerLoop", new Class<?>[0]);
            } catch (Throwable throwable) {
                failure.set(throwable);
            }
        }, name);
        reader.start();
        return reader;
    }

    @Test
    void peerResetAndLateDataPopulateReasonAndDiagnosticStats() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime);

        handleReaderFrame(runtime, "handleResetFrame", controlFrame(
                FrameType.RESET,
                stream.streamIdInternal(),
                7L
        ));
        handleReaderFrame(runtime, "handleDataFrame", dataFrame(stream.streamIdInternal(), "later"));

        SessionStats stats = runtime.stats();
        assertEquals(Long.valueOf(1L), stats.reasons().reset().get(7L), "peer RESET reason count mismatch");
        assertEquals(5L, stats.diagnostics().lateDataAfterReset(), "late data after RESET bytes mismatch");
    }

    @Test
    void peerResetDuplicateStandardDiagDropsReasonButKeepsResetSemantics() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime);

        handleReaderFrame(runtime, "handleDataFrame", dataFrame(stream.streamIdInternal(), "abc"));
        handleReaderFrame(runtime, "handleResetFrame", controlFrameWithDuplicateStandardDiag(
                FrameType.RESET,
                stream.streamIdInternal(),
                ErrorCode.CANCELLED.code(),
                "peer reset"
        ));

        synchronized (runtime.lock()) {
            ApplicationError error = assertInstanceOf(
                    ApplicationError.class,
                    stream.operationErrorLocked(),
                    "peer RESET should still surface the structured reset error"
            );
            assertEquals(ErrorCode.CANCELLED.code(), error.code(), "peer RESET code mismatch");
            assertEquals("", error.reason(), "duplicate singleton DIAG should clear the retained RESET reason");
            assertEquals(ZmuxTerminationKind.RESET, error.terminationKind(), "peer RESET termination mismatch");
            assertTrue(stream.readBufferEmptyLocked(), "peer RESET should still discard buffered receive data");
            assertEquals(0L, runtime.bufferedReceiveBytesInternal(), "peer RESET should still release session buffered bytes");
        }
    }

    @Test
    void closeReadLateDataTracksCloseReadBytes() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime);

        stream.closeRead();
        handleReaderFrame(runtime, "handleDataFrame", dataFrame(stream.streamIdInternal(), "later"));

        SessionStats stats = runtime.stats();
        assertEquals(5L, stats.diagnostics().lateDataAfterCloseRead(), "late data after CloseRead bytes mismatch");
        assertTrue(stats.reasons().reset().isEmpty(), "CloseRead late data should not invent RESET reasons");
    }

    @Test
    void duplicatePendingAbortCoalescesTerminalSignal() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime);

        stream.closeWithError(7L, "abort");
        stream.closeWithError(7L, "abort");

        Deque<Object> urgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
        assertEquals(1, urgentQueue.size(), "duplicate pending ABORT should not grow the urgent queue");
        assertEquals(FrameType.ABORT, SessionRuntimeTestSupport.outboundFrame(urgentQueue.peekFirst()).type());
        assertEquals(1L, runtime.stats().diagnostics().coalescedTerminalSignals(), "duplicate ABORT should be counted as coalesced");
        assertEquals(0L, runtime.stats().diagnostics().supersededTerminalSignals(), "duplicate ABORT should not be counted as superseded");
    }

    @Test
    void pendingAbortSupersedesQueuedResetForSameStream() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime);

        stream.cancelWrite(11L);
        stream.closeWithError(12L, "abort");

        Deque<Object> urgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
        assertEquals(1, urgentQueue.size(), "ABORT should replace an unsent same-stream RESET");
        FrameCodec.Frame queued = SessionRuntimeTestSupport.outboundFrame(urgentQueue.peekFirst());
        assertEquals(FrameType.ABORT, queued.type(), "queued terminal control should be the stronger ABORT");
        assertEquals(12L, FrameCodec.parseErrorPayload(queued.payload()).code(), "queued ABORT should carry the replacement code");
        assertEquals(0L, runtime.stats().diagnostics().coalescedTerminalSignals(), "RESET -> ABORT should not be counted as coalesced");
        assertEquals(1L, runtime.stats().diagnostics().supersededTerminalSignals(), "RESET -> ABORT should be counted as superseded");
    }

    @Test
    void pendingAbortSupersedesQueuedStopSendingForSameStream() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime);

        stream.cancelRead(21L);
        stream.closeWithError(22L, "abort");

        Deque<Object> urgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
        assertEquals(1, urgentQueue.size(), "ABORT should replace an unsent same-stream STOP_SENDING");
        FrameCodec.Frame queued = SessionRuntimeTestSupport.outboundFrame(urgentQueue.peekFirst());
        assertEquals(FrameType.ABORT, queued.type(), "queued terminal control should be the stronger ABORT");
        assertEquals(22L, FrameCodec.parseErrorPayload(queued.payload()).code(), "queued ABORT should carry the replacement code");
        assertEquals(0L, runtime.stats().diagnostics().coalescedTerminalSignals(), "STOP_SENDING -> ABORT should not be counted as coalesced");
        assertEquals(1L, runtime.stats().diagnostics().supersededTerminalSignals(), "STOP_SENDING -> ABORT should be counted as superseded");
    }

    @Test
    void peerAbortDuplicateStandardDiagDropsReasonButKeepsAbortSemantics() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime);

        handleReaderFrame(runtime, "handleDataFrame", dataFrame(stream.streamIdInternal(), "ab"));
        handleReaderFrame(runtime, "handleAbortFrame", controlFrameWithDuplicateStandardDiag(
                FrameType.ABORT,
                stream.streamIdInternal(),
                ErrorCode.REFUSED_STREAM.code(),
                "peer abort"
        ));

        synchronized (runtime.lock()) {
            ApplicationError error = assertInstanceOf(
                    ApplicationError.class,
                    stream.operationErrorLocked(),
                    "peer ABORT should still surface the structured abort error"
            );
            assertEquals(ErrorCode.REFUSED_STREAM.code(), error.code(), "peer ABORT code mismatch");
            assertEquals("", error.reason(), "duplicate singleton DIAG should clear the retained ABORT reason");
            assertEquals(ZmuxTerminationKind.ABORT, error.terminationKind(), "peer ABORT termination mismatch");
            assertTrue(stream.readBufferEmptyLocked(), "peer ABORT should still discard buffered receive data");
            assertEquals(0L, runtime.bufferedReceiveBytesInternal(), "peer ABORT should still release session buffered bytes");
        }
    }

    @Test
    void peerResetInvalidUtf8DiagDropsReasonButKeepsResetSemantics() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime);

        handleReaderFrame(runtime, "handleDataFrame", dataFrame(stream.streamIdInternal(), "abc"));
        handleReaderFrame(runtime, "handleResetFrame", controlFrameWithInvalidUtf8Diag(
                FrameType.RESET,
                stream.streamIdInternal(),
                ErrorCode.CANCELLED.code()
        ));

        synchronized (runtime.lock()) {
            ApplicationError error = assertInstanceOf(ApplicationError.class, stream.operationErrorLocked());
            assertEquals(ErrorCode.CANCELLED.code(), error.code(), "peer RESET code mismatch");
            assertEquals("", error.reason(), "invalid UTF-8 DIAG should clear the retained RESET reason");
            assertEquals(ZmuxTerminationKind.RESET, error.terminationKind(), "peer RESET termination mismatch");
            assertTrue(stream.readBufferEmptyLocked(), "peer RESET should still discard buffered receive data");
            assertEquals(0L, runtime.bufferedReceiveBytesInternal(), "peer RESET should still release session buffered bytes");
        }
    }

    @Test
    void peerAbortInvalidUtf8DiagDropsReasonButKeepsAbortSemantics() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime);

        handleReaderFrame(runtime, "handleDataFrame", dataFrame(stream.streamIdInternal(), "ab"));
        handleReaderFrame(runtime, "handleAbortFrame", controlFrameWithInvalidUtf8Diag(
                FrameType.ABORT,
                stream.streamIdInternal(),
                ErrorCode.REFUSED_STREAM.code()
        ));

        synchronized (runtime.lock()) {
            ApplicationError error = assertInstanceOf(ApplicationError.class, stream.operationErrorLocked());
            assertEquals(ErrorCode.REFUSED_STREAM.code(), error.code(), "peer ABORT code mismatch");
            assertEquals("", error.reason(), "invalid UTF-8 DIAG should clear the retained ABORT reason");
            assertEquals(ZmuxTerminationKind.ABORT, error.terminationKind(), "peer ABORT termination mismatch");
            assertTrue(stream.readBufferEmptyLocked(), "peer ABORT should still discard buffered receive data");
            assertEquals(0L, runtime.bufferedReceiveBytesInternal(), "peer ABORT should still release session buffered bytes");
        }
    }

    @Test
    void ignoredPeerFramesDoNotConsumeInboundBudget() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .inboundControlFrameBudget(1)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());

        runtime.closeWithError(ErrorCode.NO_ERROR.code(), "");

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "recordInboundBudgetsLocked",
                    new Class<?>[]{FrameType.class, int.class},
                    FrameType.PING,
                    0
            );
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "recordInboundBudgetsLocked",
                    new Class<?>[]{FrameType.class, int.class},
                    FrameType.CLOSE,
                    0
            );
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "recordInboundBudgetsLocked",
                    new Class<?>[]{FrameType.class, int.class},
                    FrameType.PING,
                    0
            );
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "recordInboundBudgetsLocked",
                    new Class<?>[]{FrameType.class, int.class},
                    FrameType.CLOSE,
                    0
            );
        }
    }

    @Test
    void trackedSessionMemoryCountsRetainedOpenMetadataBackingUntilConsumed() throws Exception {
        long capabilities = Protocol.CAPABILITY_OPEN_METADATA;
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(capabilities, Settings.defaults());
        long streamId = SessionRuntime.firstPeerStreamId(Role.RESPONDER, true);
        byte[] openInfo = new byte[17];
        Arrays.fill(openInfo, (byte) 7);
        byte[] metadataPrefix = FrameCodec.buildOpenMetadataPrefix(
                capabilities,
                null,
                null,
                openInfo,
                Settings.defaults().maxFramePayload()
        );
        byte[] payload = Arrays.copyOf(metadataPrefix, metadataPrefix.length + 1);
        payload[payload.length - 1] = 'x';

        handleReaderFrame(runtime, "handleDataFrame", new FrameCodec.Frame(
                FrameType.DATA,
                Protocol.FRAME_FLAG_OPEN_METADATA,
                streamId,
                payload
        ));

        StreamRuntime accepted = (StreamRuntime) runtime.acceptStream();
        SessionStats queuedStats = runtime.stats();
        assertEquals(1L, queuedStats.pressure().bufferedReceiveBytes(), "logical buffered receive bytes should only include app data");
        assertEquals(payload.length, queuedStats.pressure().bufferedReceiveStorageBytes(), "buffered receive storage bytes should retain the full DATA frame backing");
        assertEquals(
                payload.length + openInfo.length,
                queuedStats.pressure().trackedSessionMemoryBytes(),
                "tracked session memory should retain the full frame backing plus retained open_info"
        );

        byte[] dst = new byte[1];
        assertEquals(1, accepted.read(dst, 0, dst.length), "accepted stream should still expose the app-data byte");
        assertEquals('x', dst[0], "accepted stream app-data mismatch");

        SessionStats drainedStats = runtime.stats();
        assertEquals(0L, drainedStats.pressure().bufferedReceiveBytes(), "buffered receive bytes should drop after draining the read buffer");
        assertEquals(0L, drainedStats.pressure().bufferedReceiveStorageBytes(), "buffered receive storage bytes should drop after the retained backing is released");
        assertEquals(
                openInfo.length,
                drainedStats.pressure().trackedSessionMemoryBytes(),
                "tracked session memory should release the retained frame backing after the final byte is consumed"
        );
    }

    @Test
    void hiddenAbortCompactionTracksHiddenAbortStats() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime);

        synchronized (runtime.lock()) {
            assertFalse(stream.applicationVisible(), "test requires a hidden peer-opened stream");
            stream.receiveDataLocked("hidden".getBytes(StandardCharsets.UTF_8));
        }

        handleReaderFrame(runtime, "handleAbortFrame", controlFrame(
                FrameType.ABORT,
                stream.streamIdInternal(),
                9L
        ));
        handleReaderFrame(runtime, "handleDataFrame", dataFrame(stream.streamIdInternal(), "later"));

        SessionStats stats = runtime.stats();
        assertEquals(Long.valueOf(1L), stats.reasons().abort().get(9L), "peer ABORT reason count mismatch");
        assertEquals(1L, stats.hiddenState().reaped(), "hidden stream reaped count mismatch");
        assertEquals(6L, stats.hiddenState().unreadBytesDiscarded(), "hidden unread discard bytes mismatch");
        assertEquals(
                1L,
                stats.pressure().retainedStateBreakdown().hiddenControl().count(),
                "hidden terminal compaction should retain hidden-control bookkeeping"
        );
        assertEquals(
                0L,
                stats.pressure().retainedStateBreakdown().visibleTombstones().count(),
                "hidden terminal compaction must not be counted as a visible tombstone"
        );
        assertEquals(5L, stats.diagnostics().lateDataAfterAbort(), "late data after ABORT bytes mismatch");
    }

    @Test
    void hiddenRefusedOpenTracksRefusedAndAbortReasonStats() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        long streamId = SessionRuntime.firstPeerStreamId(Role.RESPONDER, true);

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "refusePeerOpeningStreamLocked",
                    new Class<?>[]{long.class, boolean.class, boolean.class},
                    streamId,
                    true,
                    true
            );
        }

        SessionStats stats = runtime.stats();
        assertEquals(1L, stats.hiddenState().refused(), "hidden refused stream count mismatch");
        assertEquals(
                Long.valueOf(1L),
                stats.reasons().abort().get(ErrorCode.REFUSED_STREAM.code()),
                "REFUSED_STREAM should contribute to abort reason stats"
        );
    }

    @Test
    void terminalCompactionSkipsUnacceptedQueuedStreamWithOpenInfo() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(Protocol.CAPABILITY_OPEN_METADATA, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime);
        byte[] openInfo = "ssh".getBytes(StandardCharsets.UTF_8);

        synchronized (runtime.lock()) {
            stream.applyOpenMetadataLocked(0L, null, openInfo);
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "enqueueAcceptedLocked",
                    new Class<?>[]{StreamRuntime.class},
                    stream
            );
        }

        stream.closeWithError(9L, "");

        synchronized (runtime.lock()) {
            assertTrue(stream.fullyTerminalLocked(), "test requires a fully terminal queued stream");
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "maybeCompactStreamLocked",
                    new Class<?>[]{StreamRuntime.class},
                    stream
            );

            assertSame(stream, runtime.liveStreamLocked(stream.streamIdInternal()), "unaccepted queued stream must stay live");
            assertArrayEquals(openInfo, stream.openInfo(), "queued stream open_info must be preserved until accept");
            assertEquals(openInfo.length, runtime.retainedOpenInfoBytesLocked(), "retained open_info bytes must stay charged");
            assertNull(runtime.terminalDataDispositionForLocked(stream.streamIdInternal()), "queued stream must not be tombstoned before accept");
            Object accepted = SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "pollAcceptedHeadLocked",
                    new Class<?>[]{boolean.class},
                    true
            );
            assertSame(stream, accepted, "accept queue should retain the terminal stream until application accepts it");
        }
    }

    @Test
    void visiblePeerAbortTracksTerminalChurnDiagnostic() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime);

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "enqueueAcceptedLocked",
                    new Class<?>[]{StreamRuntime.class},
                    stream
            );
        }
        handleReaderFrame(runtime, "handleAbortFrame", controlFrame(
                FrameType.ABORT,
                stream.streamIdInternal(),
                13L
        ));

        SessionStats stats = runtime.stats();
        assertEquals(1L, stats.diagnostics().visibleTerminalChurnEvents(), "visible peer terminal churn should surface in diagnostics");
        assertEquals(Long.valueOf(1L), stats.reasons().abort().get(13L), "visible peer abort should still contribute to abort reasons");
    }

    @Test
    void priorityUpdateGroupRebucketTracksDiagnostic() throws Exception {
        long capabilities = Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_STREAM_GROUPS;
        Settings peerSettings = Settings.defaults().toBuilder()
                .schedulerHints(SchedulerHint.GROUP_FAIR)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(capabilities, peerSettings);
        StreamRuntime stream = createPeerOpenedBidi(runtime);

        handleReaderFrame(runtime, "handleExtFrame", new FrameCodec.Frame(
                FrameType.EXT,
                0,
                stream.streamIdInternal(),
                FrameCodec.buildPriorityUpdatePayload(
                        capabilities,
                        null,
                        7L,
                        Settings.defaults().maxExtensionPayloadBytes()
                )
        ));

        assertEquals(1L, runtime.stats().diagnostics().groupRebucketEvents(), "effective group rebucket should surface in diagnostics");
    }

    @Test
    void priorityUpdateGroupChangeOutsideGroupFairDoesNotTrackDiagnostic() throws Exception {
        long capabilities = Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_STREAM_GROUPS;
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(capabilities, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime);

        handleReaderFrame(runtime, "handleExtFrame", new FrameCodec.Frame(
                FrameType.EXT,
                0,
                stream.streamIdInternal(),
                FrameCodec.buildPriorityUpdatePayload(
                        capabilities,
                        null,
                        7L,
                        Settings.defaults().maxExtensionPayloadBytes()
                )
        ));

        assertEquals(Long.valueOf(7L), stream.metadata().group(), "group update should still apply outside group-fair scheduling");
        assertEquals(0L, runtime.stats().diagnostics().groupRebucketEvents(), "non-group-fair updates must not count as rebucket churn");
    }

    @Test
    void closeFrameWriteFailureTracksCloseFlushDiagnostic() throws Exception {
        BasicDuplexConnection connection = failingOutputConnection("synthetic close flush failure");
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                connection,
                null,
                0L,
                Settings.defaults()
        );
        AtomicReference<Throwable> failure = new AtomicReference<>();

        runtime.closeWithError(ErrorCode.INTERNAL.code(), "close flush");

        Thread writer = new Thread(() -> {
            try {
                SessionRuntimeTestSupport.invokePrivate(runtime, "writerLoop", new Class<?>[0]);
            } catch (Throwable throwable) {
                failure.set(throwable);
            }
        }, "session-close-flush-diagnostic");
        writer.start();
        writer.join(1_000L);

        assertFalse(writer.isAlive(), "writer loop should terminate after the synthetic close flush failure");
        assertNull(failure.get(), "writer loop should surface the close flush failure through runtime state, not as an uncaught test failure");
        assertTrue(runtime.awaitTermination(Duration.ofSeconds(1)), "close flush failure should still finish the session");
        assertEquals(0L, runtime.stats().diagnostics().skippedCloseOnDeadIO(), "close flush failure should not be counted as a skipped close on dead IO");
        assertEquals(1L, runtime.stats().diagnostics().closeFrameFlushErrors(), "close frame write failure should increment diagnostics");
    }

    @Test
    void invalidLocalCloseCodeDoesNotTerminateSession() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> runtime.closeWithError(-1L, "invalid"),
                "invalid local close code should fail before committing terminal session state"
        );

        assertTrue(error.getMessage().contains("varint62 value out of range"), "invalid close code should fail through the varint encoder");
        assertEquals(SessionState.READY, runtime.state(), "invalid close code must not fail or close the session");
        assertFalse(runtime.awaitTermination(Duration.ofMillis(20)), "invalid close code must not signal termination");

        runtime.closeWithError(ErrorCode.NO_ERROR.code(), "");
        assertEquals(SessionState.CLOSING, runtime.state(), "valid retry should start terminal close");
        assertEquals(1, SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue").size(), "valid retry should queue one CLOSE frame");
        assertEquals(
                FrameType.CLOSE,
                SessionRuntimeTestSupport.outboundFrame(SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue").peekFirst()).type(),
                "valid retry should queue CLOSE"
        );
    }

    @Test
    void closeQueueFailureOverridesRequestedSessionCloseError() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .urgentQueuedBytesCap(1L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());

        ZmuxException error = assertInstanceOf(
                ZmuxException.class,
                assertThrows(IOException.class, () -> runtime.closeWithError(ErrorCode.PROTOCOL.code(), "requested-close")),
                "closeWithError should surface the urgent queue admission failure"
        );

        assertEquals("queue urgent control", error.operation(), "close queue failure operation mismatch");
        assertTrue(error.getMessage().contains("urgent control queue cap exceeded"), "close queue failure message mismatch");
        assertEquals(SessionState.FAILED, runtime.state(), "queue failure should fail the session");
        assertTrue(runtime.awaitTermination(Duration.ofSeconds(1)), "queue failure should terminate the session");

        IOException terminationCause = runtime.terminationCause().orElseThrow(() ->
                new AssertionError("queue failure should remain as the runtime termination cause")
        );
        ZmuxException terminal = assertInstanceOf(
                ZmuxException.class,
                terminationCause,
                "termination cause should be the queue admission failure rather than the requested close error"
        );
        assertEquals("queue urgent control", terminal.operation(), "termination cause operation mismatch");
        assertTrue(terminal.getMessage().contains("urgent control queue cap exceeded"), "termination cause message mismatch");
        assertFalse(terminationCause instanceof ApplicationError, "termination cause should not retain the requested close ApplicationError");

        IOException openError = assertThrows(IOException.class, runtime::openStream, "follow-up open should surface the queue failure");
        assertEquals("open", ZmuxErrors.operation(openError), "follow-up open operation mismatch");
        assertNotNull(openError.getCause(), "follow-up open should retain the queue failure as its cause");
        assertEquals("queue urgent control", ZmuxErrors.operation(openError.getCause()), "follow-up open cause operation mismatch");
        assertTrue(
                openError.getCause().getMessage().contains("urgent control queue cap exceeded"),
                "follow-up open should retain the queue failure message"
        );
    }

    @Test
    void repeatedCloseWithErrorDoesNotQueueDuplicateCloseFrames() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        runtime.closeWithError(ErrorCode.INTERNAL.code(), "fatal");
        runtime.closeWithError(ErrorCode.PROTOCOL.code(), "duplicate");

        Deque<Object> urgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
        assertEquals(1, urgentQueue.size(), "repeated closeWithError should not queue a duplicate CLOSE frame once one is pending");

        FrameCodec.ErrorPayload payload = FrameCodec.parseErrorPayload(
                SessionRuntimeTestSupport.outboundFrame(urgentQueue.peekFirst()).payload()
        );
        assertEquals(ErrorCode.INTERNAL.code(), payload.code(), "first queued CLOSE code should win");
        assertEquals("fatal", payload.reason(), "first queued CLOSE reason should remain unchanged");
    }

    @Test
    void writerDeadIoWithoutCloseTracksSkippedCloseDiagnostic() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                failingOutputConnection("synthetic data write failure"),
                null,
                0L,
                Settings.defaults()
        );
        AtomicReference<Throwable> failure = new AtomicReference<>();

        ((StreamRuntime) runtime.openStream()).write("payload".getBytes(StandardCharsets.UTF_8));

        Thread writer = new Thread(() -> {
            try {
                SessionRuntimeTestSupport.invokePrivate(runtime, "writerLoop", new Class<?>[0]);
            } catch (Throwable throwable) {
                failure.set(throwable);
            }
        }, "session-skipped-close-diagnostic");
        writer.start();
        writer.join(1_000L);

        assertFalse(writer.isAlive(), "writer loop should terminate after the synthetic data write failure");
        assertNull(failure.get(), "writer loop should surface the dead-IO failure through runtime state, not as an uncaught test failure");
        assertTrue(runtime.awaitTermination(Duration.ofSeconds(1)), "writer dead IO should finish the session immediately");
        assertEquals(1L, runtime.stats().diagnostics().skippedCloseOnDeadIO(), "dead writer IO should increment skipped-close diagnostics");
        assertEquals(0L, runtime.stats().diagnostics().closeFrameFlushErrors(), "data write failure without an inflight close must not count as a close flush error");
    }

    @Test
    void writerDeadIoWithoutCloseSurfacesStructuredTransportError() throws Exception {
        String message = "synthetic structured data write failure";
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                failingOutputConnection(message),
                null,
                0L,
                Settings.defaults()
        );
        AtomicReference<Throwable> failure = new AtomicReference<>();

        ((StreamRuntime) runtime.openStream()).write("payload".getBytes(StandardCharsets.UTF_8));

        Thread writer = startWriterLoop(runtime, failure, "session-structured-writer-io");
        writer.join(1_000L);

        assertFalse(writer.isAlive(), "writer loop should terminate after the synthetic data write failure");
        assertNull(failure.get(), "writer loop should store the transport failure in runtime state");
        assertTrue(runtime.awaitTermination(Duration.ofSeconds(1)), "writer dead IO should finish the session immediately");

        IOException error = assertThrows(IOException.class, runtime::openStream);
        assertEquals(ErrorCode.INTERNAL.code(), ZmuxErrors.code(error, -1L), "transport write code mismatch");
        assertEquals("open", ZmuxErrors.operation(error), "transport write operation mismatch");
        assertEquals(ZmuxErrorScope.SESSION, ZmuxErrors.scope(error), "transport write scope mismatch");
        assertEquals(ZmuxErrorSource.TRANSPORT, ZmuxErrors.source(error), "transport write source mismatch");
        assertEquals(ZmuxErrorDirection.BOTH, ZmuxErrors.direction(error), "transport write direction mismatch");
        assertEquals(
                ZmuxTerminationKind.SESSION_TERMINATION,
                ZmuxErrors.terminationKind(error),
                "transport write termination mismatch"
        );
        assertNotNull(error.getCause(), "transport write cause should be retained");
        assertEquals("write", ZmuxErrors.operation(error.getCause()), "transport write cause operation mismatch");
        assertEquals(message, error.getCause().getCause().getMessage(), "transport write root cause should be retained");
    }

    @Test
    void readerDeadIoWithoutCloseSurfacesStructuredTransportError() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                new BasicDuplexConnection(
                        SessionRuntimeTestSupport.emptyInput(),
                        SessionRuntimeTestSupport.discardingOutput()
                ),
                null,
                0L,
                Settings.defaults()
        );
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread reader = startReaderLoop(runtime, failure, "session-structured-reader-io");
        reader.join(1_000L);

        assertFalse(reader.isAlive(), "reader loop should terminate after transport EOF");
        assertNull(failure.get(), "reader loop should store the transport failure in runtime state");

        IOException error = assertThrows(IOException.class, runtime::openStream);
        assertEquals(ErrorCode.INTERNAL.code(), ZmuxErrors.code(error, -1L), "transport read code mismatch");
        assertEquals("open", ZmuxErrors.operation(error), "transport read operation mismatch");
        assertEquals(ZmuxErrorScope.SESSION, ZmuxErrors.scope(error), "transport read scope mismatch");
        assertEquals(ZmuxErrorSource.TRANSPORT, ZmuxErrors.source(error), "transport read source mismatch");
        assertEquals(ZmuxErrorDirection.BOTH, ZmuxErrors.direction(error), "transport read direction mismatch");
        assertEquals(
                ZmuxTerminationKind.SESSION_TERMINATION,
                ZmuxErrors.terminationKind(error),
                "transport read termination mismatch"
        );
        assertNotNull(error.getCause(), "transport read cause should be retained");
        assertEquals("read", ZmuxErrors.operation(error.getCause()), "transport read cause operation mismatch");
    }

    @Test
    void readerLoopStillParsesPeerCloseDiagnosticsAfterTransportFailure() throws Exception {
        byte[] closePayload = FrameCodec.buildErrorPayload(
                ErrorCode.FLOW_CONTROL.code(),
                "peer-diagnostics",
                Settings.defaults().maxControlPayloadBytes()
        );
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                new BasicDuplexConnection(
                        new java.io.ByteArrayInputStream(encodeFrame(FrameType.CLOSE, 0, 0L, closePayload)),
                        SessionRuntimeTestSupport.discardingOutput()
                ),
                null,
                0L,
                Settings.defaults()
        );
        AtomicReference<Throwable> failure = new AtomicReference<>();

        runtime.failSession(new ZmuxException(
                ErrorCode.INTERNAL.code(),
                "read",
                "zmux: transport read failed",
                new IOException("synthetic transport failure"),
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.TRANSPORT,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        ));
        synchronized (runtime.lock()) {
            long nowNanos = System.nanoTime();
            SessionRuntimeTestSupport.setIntField(runtime, "inboundControlFrameCount", 4);
            SessionRuntimeTestSupport.setIntField(runtime, "inboundMixedFrameCount", 6);
            SessionRuntimeTestSupport.setIntField(runtime, "noOpControlCount", 8);
            SessionRuntimeTestSupport.setLongField(runtime, "inboundControlBudgetWindowStartedAtNanos", nowNanos);
            SessionRuntimeTestSupport.setLongField(runtime, "inboundMixedBudgetWindowStartedAtNanos", nowNanos);
        }

        Thread reader = startReaderLoop(runtime, failure, "session-transport-failure-peer-close");
        reader.join(1_000L);

        assertFalse(reader.isAlive(), "reader loop should still terminate after parsing the buffered peer CLOSE");
        assertNull(failure.get(), "reader loop should retain peer CLOSE diagnostics in runtime state, not fail the test thread");
        assertTrue(runtime.awaitTermination(Duration.ofSeconds(1)), "reader loop should finish the failed session after transport-failure peer CLOSE");
        assertNotNull(runtime.peerCloseError(), "peer CLOSE diagnostics should still be retained after transport failure");
        assertEquals(ErrorCode.FLOW_CONTROL.code(), runtime.peerCloseError().code(), "peer CLOSE code mismatch after transport failure");
        assertEquals("peer-diagnostics", runtime.peerCloseError().reason(), "peer CLOSE reason mismatch after transport failure");
        synchronized (runtime.lock()) {
            assertEquals(5, getIntField(runtime, "inboundControlFrameCount"), "transport-failure peer CLOSE should still count toward control budget");
            assertEquals(7, getIntField(runtime, "inboundMixedFrameCount"), "transport-failure peer CLOSE should still count toward mixed budget");
            assertEquals(8, getIntField(runtime, "noOpControlCount"), "parsed peer CLOSE should not perturb the no-op control budget");
        }
    }

    @Test
    void readerLoopExitsOnceAllInboundFramesAreIgnored() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                new BasicDuplexConnection(
                        SessionRuntimeTestSupport.emptyInput(),
                        SessionRuntimeTestSupport.discardingOutput()
                ),
                null,
                0L,
                Settings.defaults()
        );
        AtomicReference<Throwable> failure = new AtomicReference<>();

        runtime.closeWithError(ErrorCode.NO_ERROR.code(), "");

        Thread reader = startReaderLoop(runtime, failure, "session-reader-close-start-noop-exit");
        reader.join(1_000L);

        assertFalse(reader.isAlive(), "reader loop should exit once local close makes every inbound frame ignorable");
        assertNull(failure.get(), "reader loop should not treat ignored inbound state as a transport failure");
        assertFalse(runtime.awaitTermination(Duration.ofMillis(20)), "reader loop exit should not finalize the session before the queued CLOSE flushes");
        assertEquals(SessionState.CLOSING, runtime.state(), "local close start should remain in CLOSING until the close frame flushes");
    }

    @Test
    void readerProtocolFailurePromotesSessionTerminationKind() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                new BasicDuplexConnection(
                        new java.io.ByteArrayInputStream(encodeFrame(FrameType.PING, 0, 4L, new byte[8])),
                        SessionRuntimeTestSupport.discardingOutput()
                ),
                null,
                0L,
                Settings.defaults()
        );
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread reader = startReaderLoop(runtime, failure, "session-structured-reader-protocol");
        reader.join(1_000L);

        assertFalse(reader.isAlive(), "reader loop should terminate after protocol failure");
        assertNull(failure.get(), "reader loop should store the protocol failure in runtime state");

        IOException error = assertThrows(IOException.class, runtime::openStream);
        assertEquals(ErrorCode.PROTOCOL.code(), ZmuxErrors.code(error, -1L), "protocol failure code mismatch");
        assertEquals("open", ZmuxErrors.operation(error), "protocol failure operation mismatch");
        assertEquals(ZmuxErrorScope.SESSION, ZmuxErrors.scope(error), "protocol failure scope mismatch");
        assertEquals(ZmuxErrorSource.REMOTE, ZmuxErrors.source(error), "protocol failure source mismatch");
        assertEquals(ZmuxErrorDirection.BOTH, ZmuxErrors.direction(error), "protocol failure direction mismatch");
        assertEquals(
                ZmuxTerminationKind.SESSION_TERMINATION,
                ZmuxErrors.terminationKind(error),
                "protocol failure termination mismatch"
        );
        assertNotNull(error.getCause(), "protocol failure cause should be retained");
        assertEquals("validate frame scope", ZmuxErrors.operation(error.getCause()), "protocol failure cause operation mismatch");
        assertEquals(ZmuxErrorSource.REMOTE, ZmuxErrors.source(error.getCause()), "protocol failure cause source mismatch");
        assertEquals(ZmuxErrorDirection.READ, ZmuxErrors.direction(error.getCause()), "protocol failure cause direction mismatch");

        IOException terminationCause = runtime.terminationCause().orElseThrow(() ->
                new AssertionError("protocol failure should remain as the runtime termination cause")
        );
        assertEquals("validate frame scope", ZmuxErrors.operation(terminationCause), "termination cause operation mismatch");
        assertEquals(ZmuxErrorSource.REMOTE, ZmuxErrors.source(terminationCause), "termination cause source mismatch");
        assertEquals(ZmuxErrorDirection.READ, ZmuxErrors.direction(terminationCause), "termination cause direction mismatch");
        assertEquals(
                ZmuxTerminationKind.SESSION_TERMINATION,
                ZmuxErrors.terminationKind(terminationCause),
                "termination cause termination mismatch"
        );
    }

    @Test
    void readerProtocolFailureReturnsPooledInboundPayloadOnHandlerError() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                new BasicDuplexConnection(
                        new java.io.ByteArrayInputStream(encodeFrame(FrameType.PING, 0, 0L, new byte[]{1})),
                        SessionRuntimeTestSupport.discardingOutput()
                ),
                null,
                0L,
                Settings.defaults()
        );
        Field poolField = SessionRuntime.class.getDeclaredField("inboundPayloadPool");
        poolField.setAccessible(true);
        InboundPayloadPool pool = (InboundPayloadPool) poolField.get(runtime);
        InboundPayloadPool.Handle seeded = pool.acquire(1);
        assertNotNull(seeded, "test requires payload pooling for 1-byte frames");
        seeded.release();
        assertEquals(1L, pool.retainedBytes(), "seeded pool should retain one 1-byte payload buffer before the read loop runs");

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread reader = startReaderLoop(runtime, failure, "session-reader-protocol-pool-release");
        reader.join(1_000L);

        assertFalse(reader.isAlive(), "reader loop should terminate after protocol failure");
        assertNull(failure.get(), "reader loop should retain the protocol failure in runtime state");
        assertEquals(1L, pool.retainedBytes(), "handler failure should return the borrowed pooled payload buffer to the inbound pool");
    }

    @Test
    void gracefulCloseTimeoutTracksDiagnostic() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                io.zmux.ZmuxConfig.builder()
                        .role(Role.RESPONDER)
                        .gracefulCloseDrainTimeout(Duration.ofMillis(1))
                        .build(),
                0L,
                Settings.defaults()
        );
        AtomicReference<Throwable> failure = new AtomicReference<>();

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setLongField(runtime, "gracefulCloseBlockingStreams", 1L);
        }

        Thread writer = startWriterLoop(runtime, failure, "session-graceful-close-timeout");
        assertThrows(GracefulCloseTimeoutException.class, runtime::close, "graceful drain timeout should still surface through close()");
        writer.join(1_000L);

        assertFalse(writer.isAlive(), "writer loop should terminate after graceful close timeout");
        assertNull(failure.get(), "writer loop should not fail the test while draining the timed-out close");
        assertTrue(runtime.awaitTermination(Duration.ofSeconds(1)), "graceful close timeout should still finish the session");
        assertEquals(1L, runtime.stats().diagnostics().gracefulCloseTimeouts(), "graceful close timeout should increment diagnostics");
        assertEquals(0L, runtime.stats().diagnostics().closeCompletionTimeouts(), "graceful drain timeout should not increment close-completion diagnostics");
        assertEquals(0L, runtime.stats().diagnostics().keepaliveTimeouts(), "graceful close timeout should not increment keepalive diagnostics");
    }

    @Test
    void closeCompletionTimeoutTracksDiagnostic() throws Exception {
        CountDownLatch releaseWrites = new CountDownLatch(1);
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                blockingOutputConnection(releaseWrites),
                io.zmux.ZmuxConfig.builder()
                        .role(Role.RESPONDER)
                        .gracefulCloseDrainTimeout(Duration.ofMillis(1))
                        .build(),
                0L,
                Settings.defaults()
        );
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread writer = startWriterLoop(runtime, failure, "session-close-completion-timeout");
        assertThrows(GracefulCloseTimeoutException.class, runtime::close, "close() should surface a timeout when termination does not finish within the close wait budget");
        assertEquals(1L, runtime.stats().diagnostics().closeCompletionTimeouts(), "close completion timeout should increment diagnostics");
        assertEquals(0L, runtime.stats().diagnostics().gracefulCloseTimeouts(), "close completion timeout should not increment graceful-drain diagnostics");
        assertEquals(0L, runtime.stats().diagnostics().keepaliveTimeouts(), "close completion timeout should not increment keepalive diagnostics");

        releaseWrites.countDown();
        writer.join(1_000L);

        assertFalse(writer.isAlive(), "writer loop should terminate after the blocked close write is released");
        assertNull(failure.get(), "writer loop should not surface the synthetic close stall as an uncaught test failure");
        assertTrue(runtime.awaitTermination(Duration.ofSeconds(1)), "runtime should still terminate once the blocked close write is released");
    }

    @Test
    void keepaliveTimeoutTracksDiagnostic() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                io.zmux.ZmuxConfig.builder()
                        .role(Role.RESPONDER)
                        .keepaliveInterval(Duration.ofMillis(10))
                        .keepaliveTimeout(Duration.ofMillis(1))
                        .build(),
                0L,
                Settings.defaults()
        );
        AtomicReference<Throwable> failure = new AtomicReference<>();

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setField(
                    runtime,
                    "activePing",
                    SessionRuntimeTestSupport.newPendingPing(
                            System.nanoTime() - Duration.ofSeconds(1).toNanos(),
                            new byte[]{1}
                    )
            );
        }

        Thread writer = startWriterLoop(runtime, failure, "session-keepalive-timeout");
        SessionRuntimeTestSupport.invokePrivate(runtime, "closeForKeepaliveTimeout", new Class<?>[0]);
        writer.join(1_000L);

        assertFalse(writer.isAlive(), "writer loop should terminate after keepalive timeout");
        assertNull(failure.get(), "writer loop should not fail the test while flushing the keepalive close");
        assertTrue(runtime.awaitTermination(Duration.ofSeconds(1)), "keepalive timeout should finish the session");
        assertEquals(1L, runtime.stats().diagnostics().keepaliveTimeouts(), "keepalive timeout should increment diagnostics");
        assertEquals(0L, runtime.stats().diagnostics().gracefulCloseTimeouts(), "keepalive timeout should not increment graceful-close diagnostics");
        assertEquals(0L, runtime.stats().diagnostics().closeCompletionTimeouts(), "keepalive timeout should not increment close-completion diagnostics");
    }
}
