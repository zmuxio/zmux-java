package io.zmux.runtime;

import io.zmux.*;
import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;
import io.zmux.protocol.Protocol;
import io.zmux.protocol.Varint62;
import io.zmux.transport.BasicDuplexConnection;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

final class GoAwayRuntimeTest {
    private static SessionRuntime newRuntimeWithNoOpThreshold(int threshold) throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .noOpControlFloodThreshold(threshold)
                .build();
        return SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());
    }

    private static long maxLocalGoAwayWatermark(boolean bidirectional) {
        long first = SessionRuntime.firstLocalStreamId(Role.RESPONDER, bidirectional);
        return first + (Protocol.MAX_VARINT62 - first) / 4L * 4L;
    }

    private static long peerGoAwayWatermark(boolean bidirectional, int offset) {
        return SessionRuntime.firstPeerStreamId(Role.RESPONDER, bidirectional) + (long) offset * 4L;
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

    private static void handleGoAway(SessionRuntime runtime, long lastAcceptedBidi, long lastAcceptedUni)
            throws Exception {
        FrameCodec.Frame frame = new FrameCodec.Frame(
                FrameType.GOAWAY,
                0,
                0L,
                FrameCodec.buildGoAwayPayload(
                        lastAcceptedBidi,
                        lastAcceptedUni,
                        ErrorCode.NO_ERROR.code(),
                        "",
                        Settings.defaults().maxControlPayloadBytes()
                )
        );
        try {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "handleGoAwayFrame",
                    new Class<?>[]{FrameCodec.Frame.class},
                    frame
            );
        } catch (InvocationTargetException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw exception;
        }
    }

    private static void handleGoAway(SessionRuntime runtime, byte[] payload) throws Exception {
        FrameCodec.Frame frame = new FrameCodec.Frame(FrameType.GOAWAY, 0, 0L, payload);
        try {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "handleGoAwayFrame",
                    new Class<?>[]{FrameCodec.Frame.class},
                    frame
            );
        } catch (InvocationTargetException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw exception;
        }
    }

    private static void awaitCondition(BooleanSupplier condition, String message) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError(message);
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

    private static Thread startGoAway(SessionRuntime runtime,
                                      long lastAcceptedBidi,
                                      long lastAcceptedUni,
                                      long code,
                                      String reason,
                                      AtomicReference<Throwable> failure,
                                      String name) {
        Thread thread = new Thread(() -> {
            try {
                runtime.goAway(lastAcceptedBidi, lastAcceptedUni, code, reason);
            } catch (Throwable throwable) {
                failure.set(throwable);
            }
        }, name);
        thread.start();
        return thread;
    }

    private static void joinThread(Thread thread, String message) throws InterruptedException {
        thread.join(1_000L);
        assertFalse(thread.isAlive(), message);
    }

    private static void assertNoThreadFailure(AtomicReference<Throwable> failure, String message) {
        Throwable throwable = failure.get();
        if (throwable != null) {
            fail(message + ": " + throwable);
        }
    }

    @SuppressWarnings("unchecked")
    private static int localGoAwayWaiterCount(SessionRuntime runtime) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField("localGoAwayWaiters");
        field.setAccessible(true);
        synchronized (runtime.lock()) {
            return ((List<Object>) field.get(runtime)).size();
        }
    }

    private static boolean hasCause(Throwable error, Throwable expectedCause) {
        Throwable current = error;
        while (current != null) {
            if (current == expectedCause) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static Thread flushAndStop(SessionRuntime runtime,
                                       AtomicReference<Throwable> writerFailure,
                                       String name) throws Exception {
        runtime.closeWithError(ErrorCode.NO_ERROR.code(), "");
        return startWriterLoop(runtime, writerFailure, name);
    }

    @Test
    void duplicatePeerGoAwayCountsAsNoOpControl() throws Exception {
        SessionRuntime runtime = newRuntimeWithNoOpThreshold(1);
        long maxBidi = maxLocalGoAwayWatermark(true);
        long maxUni = maxLocalGoAwayWatermark(false);

        handleGoAway(runtime, maxBidi, maxUni);
        assertEquals(SessionState.DRAINING, runtime.state(), "first restrictive GOAWAY should move READY to DRAINING");

        handleGoAway(runtime, maxBidi, maxUni);
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> handleGoAway(runtime, maxBidi, maxUni),
                "repeated unchanged GOAWAY should consume the mixed no-op control budget"
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "duplicate GOAWAY flood should be a protocol error");
    }

    @Test
    void peerGoAwayChangeClearsMixedNoOpControlBudget() throws Exception {
        SessionRuntime runtime = newRuntimeWithNoOpThreshold(1);
        long maxBidi = maxLocalGoAwayWatermark(true);
        long maxUni = maxLocalGoAwayWatermark(false);
        long lowerBidi = maxBidi - 4L;

        handleGoAway(runtime, maxBidi, maxUni);
        handleGoAway(runtime, maxBidi, maxUni);
        handleGoAway(runtime, lowerBidi, maxUni);

        handleGoAway(runtime, lowerBidi, maxUni);
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> handleGoAway(runtime, lowerBidi, maxUni),
                "a changed GOAWAY should clear the prior no-op budget, not permanently poison later no-op accounting"
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "post-change duplicate GOAWAY flood should still be enforced");
    }

    @Test
    void peerGoAwayRejectsWrongCreatorWatermark() throws Exception {
        SessionRuntime runtime = newRuntimeWithNoOpThreshold(1);
        long peerCreatedBidi = peerGoAwayWatermark(true, 1);

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> handleGoAway(runtime, peerCreatedBidi, 0L),
                "peer GOAWAY watermark must refer to locally created streams"
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "wrong-creator peer GOAWAY should be a protocol error");
        assertEquals(SessionState.READY, runtime.state(), "invalid peer GOAWAY must not move the session to DRAINING");
    }

    @Test
    void invalidLocalGoAwayCodeDoesNotCommitDrainStateOrWatermarks() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        long initialBidi = runtime.localGoAwayBidiInternal();
        long initialUni = runtime.localGoAwayUniInternal();

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> runtime.goAway(0L, 0L, -1L, "invalid"),
                "invalid GOAWAY code should fail before committing local GOAWAY state"
        );

        assertTrue(error.getMessage().contains("varint62 value out of range"), "invalid GOAWAY code should fail through the varint encoder");
        assertEquals(SessionState.READY, runtime.state(), "invalid GOAWAY payload must not move the session to DRAINING");
        assertEquals(initialBidi, runtime.localGoAwayBidiInternal(), "invalid GOAWAY payload must not commit the bidi watermark");
        assertEquals(initialUni, runtime.localGoAwayUniInternal(), "invalid GOAWAY payload must not commit the uni watermark");
        assertEquals(0, SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue").size(), "invalid GOAWAY payload must not enqueue a control frame");
    }

    @Test
    void outOfRangeLocalGoAwayWatermarkDoesNotCommitDrainStateOrWatermarks() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        long initialBidi = runtime.localGoAwayBidiInternal();
        long initialUni = runtime.localGoAwayUniInternal();

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> runtime.goAway(Protocol.MAX_VARINT62 + 1L, 0L, ErrorCode.NO_ERROR.code(), "invalid"),
                "out-of-range GOAWAY watermark should fail before committing local GOAWAY state"
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "out-of-range GOAWAY watermark error code mismatch");
        assertEquals("GOAWAY watermark exceeds varint62 range", error.getMessage(), "out-of-range GOAWAY watermark message mismatch");
        assertEquals(SessionState.READY, runtime.state(), "out-of-range GOAWAY watermark must not move the session to DRAINING");
        assertEquals(initialBidi, runtime.localGoAwayBidiInternal(), "out-of-range GOAWAY watermark must not commit the bidi watermark");
        assertEquals(initialUni, runtime.localGoAwayUniInternal(), "out-of-range GOAWAY watermark must not commit the uni watermark");
        assertEquals(0, SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue").size(), "out-of-range GOAWAY watermark must not enqueue a control frame");
    }

    @Test
    void duplicateLocalGoAwayDoesNotQueueDuplicateControlFrames() throws Exception {
        SessionRuntime runtime = newRuntimeWithNoOpThreshold(1);

        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        AtomicReference<Throwable> secondFailure = new AtomicReference<>();
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        Thread first = startGoAway(runtime, 0L, 0L, ErrorCode.NO_ERROR.code(), "first", firstFailure, "goaway-first");
        awaitCondition(() -> {
            try {
                return SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue").size() == 1;
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        }, "first GOAWAY should be queued");
        Thread second = startGoAway(runtime, 0L, 0L, ErrorCode.INTERNAL.code(), "second", secondFailure, "goaway-second");
        awaitCondition(() -> {
            try {
                return localGoAwayWaiterCount(runtime) == 2;
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        }, "duplicate GOAWAY should wait for the already queued covering frame");

        Deque<Object> urgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
        assertEquals(1, urgentQueue.size(), "duplicate local GOAWAY should not grow the urgent queue");
        FrameCodec.GoAwayPayload payload = FrameCodec.parseGoAwayPayload(
                SessionRuntimeTestSupport.outboundFrame(urgentQueue.peekFirst()).payload()
        );
        assertEquals("first", payload.reason(), "duplicate local GOAWAY should keep the already queued payload");

        Thread writer = flushAndStop(runtime, writerFailure, "duplicate-goaway-writer");
        joinThread(first, "first GOAWAY should finish after writer sends the queued frame");
        joinThread(second, "duplicate GOAWAY should finish after writer sends the covering frame");
        joinThread(writer, "writer should stop after CLOSE");
        assertNoThreadFailure(firstFailure, "first GOAWAY failure mismatch");
        assertNoThreadFailure(secondFailure, "duplicate GOAWAY failure mismatch");
        assertNoThreadFailure(writerFailure, "writer failure mismatch");
    }

    @Test
    void stricterLocalGoAwayReplacesQueuedOlderGoAway() throws Exception {
        SessionRuntime runtime = newRuntimeWithNoOpThreshold(1);
        long highBidi = peerGoAwayWatermark(true, 2);
        long highUni = peerGoAwayWatermark(false, 2);
        long lowerBidi = highBidi - 4L;

        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        AtomicReference<Throwable> secondFailure = new AtomicReference<>();
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        Thread first = startGoAway(runtime, highBidi, highUni, ErrorCode.NO_ERROR.code(), "old", firstFailure, "goaway-old");
        awaitCondition(() -> {
            try {
                return SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue").size() == 1;
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        }, "initial GOAWAY should be queued");
        Thread second = startGoAway(runtime, lowerBidi, highUni, ErrorCode.INTERNAL.code(), "new", secondFailure, "goaway-new");
        awaitCondition(() -> {
            try {
                Deque<Object> queue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
                if (queue.size() != 1) {
                    return false;
                }
                FrameCodec.GoAwayPayload queued = FrameCodec.parseGoAwayPayload(
                        SessionRuntimeTestSupport.outboundFrame(queue.peekFirst()).payload()
                );
                return "new".equals(queued.reason());
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        }, "stricter GOAWAY should replace the queued older GOAWAY");

        Deque<Object> urgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
        assertEquals(1, urgentQueue.size(), "stricter local GOAWAY should replace an unsent older GOAWAY");
        FrameCodec.GoAwayPayload payload = FrameCodec.parseGoAwayPayload(
                SessionRuntimeTestSupport.outboundFrame(urgentQueue.peekFirst()).payload()
        );
        assertEquals(lowerBidi, payload.lastAcceptedBidi(), "queued GOAWAY should carry the stricter bidi watermark");
        assertEquals(highUni, payload.lastAcceptedUni(), "queued GOAWAY should preserve the unchanged uni watermark");
        assertEquals(ErrorCode.INTERNAL.code(), payload.code(), "queued GOAWAY should carry the replacement code");
        assertEquals("new", payload.reason(), "queued GOAWAY should carry the replacement reason");

        Thread writer = flushAndStop(runtime, writerFailure, "replace-goaway-writer");
        joinThread(first, "replaced GOAWAY caller should finish when stricter GOAWAY is sent");
        joinThread(second, "stricter GOAWAY caller should finish when its frame is sent");
        joinThread(writer, "writer should stop after CLOSE");
        assertNoThreadFailure(firstFailure, "replaced GOAWAY failure mismatch");
        assertNoThreadFailure(secondFailure, "stricter GOAWAY failure mismatch");
        assertNoThreadFailure(writerFailure, "writer failure mismatch");
    }

    @Test
    void weakerLocalGoAwayCoveredByExistingStrictWatermarkIsNoOp() throws Exception {
        SessionRuntime runtime = newRuntimeWithNoOpThreshold(1);
        long highBidi = peerGoAwayWatermark(true, 3);
        long highUni = peerGoAwayWatermark(false, 3);
        long lowerBidi = highBidi - 4L;
        long lowerUni = highUni - 4L;

        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        AtomicReference<Throwable> secondFailure = new AtomicReference<>();
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        Thread first = startGoAway(runtime, lowerBidi, lowerUni, ErrorCode.NO_ERROR.code(), "strict", firstFailure, "goaway-strict");
        awaitCondition(() -> {
            try {
                return SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue").size() == 1;
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        }, "strict GOAWAY should be queued");
        Thread second = startGoAway(runtime, highBidi, highUni, ErrorCode.INTERNAL.code(), "covered", secondFailure, "goaway-covered");
        awaitCondition(() -> {
            try {
                return localGoAwayWaiterCount(runtime) == 2;
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        }, "covered weaker GOAWAY should wait for the already queued stricter frame");

        Deque<Object> urgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
        assertEquals(1, urgentQueue.size(), "covered weaker GOAWAY should not enqueue a second control frame");
        FrameCodec.GoAwayPayload payload = FrameCodec.parseGoAwayPayload(
                SessionRuntimeTestSupport.outboundFrame(urgentQueue.peekFirst()).payload()
        );
        assertEquals(lowerBidi, payload.lastAcceptedBidi(), "covered weaker GOAWAY must keep the stricter bidi watermark");
        assertEquals(lowerUni, payload.lastAcceptedUni(), "covered weaker GOAWAY must keep the stricter uni watermark");
        assertEquals(ErrorCode.NO_ERROR.code(), payload.code(), "covered weaker GOAWAY must keep the original payload");
        assertEquals("strict", payload.reason(), "covered weaker GOAWAY must keep the original reason");

        Thread writer = flushAndStop(runtime, writerFailure, "covered-goaway-writer");
        joinThread(first, "strict GOAWAY caller should finish when its frame is sent");
        joinThread(second, "covered GOAWAY caller should finish when covering frame is sent");
        joinThread(writer, "writer should stop after CLOSE");
        assertNoThreadFailure(firstFailure, "strict GOAWAY failure mismatch");
        assertNoThreadFailure(secondFailure, "covered GOAWAY failure mismatch");
        assertNoThreadFailure(writerFailure, "writer failure mismatch");
    }

    @Test
    void localGoAwayReturnsAfterFrameIsWritten() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                new BasicDuplexConnection(SessionRuntimeTestSupport.emptyInput(), output),
                ZmuxConfig.builder().role(Role.RESPONDER).build(),
                0L,
                Settings.defaults()
        );
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        Thread writer = startWriterLoop(runtime, writerFailure, "goaway-written-writer");

        runtime.goAway(0L, 0L, ErrorCode.NO_ERROR.code(), "written");

        FrameCodec.Frame written = FrameCodec.readFrame(
                new ByteArrayInputStream(output.toByteArray()),
                Settings.defaults().limits()
        );
        assertEquals(FrameType.GOAWAY, written.type(), "GOAWAY should be written before goAway returns");
        assertEquals("written", FrameCodec.parseGoAwayPayload(written.payload()).reason(), "written GOAWAY reason mismatch");

        runtime.closeWithError(ErrorCode.NO_ERROR.code(), "");
        joinThread(writer, "writer should stop after CLOSE");
        assertNoThreadFailure(writerFailure, "writer failure mismatch");
    }

    @Test
    void localGoAwayReportsWriterFailure() throws Exception {
        IOException writeFailure = new IOException("forced GOAWAY write failure");
        OutputStream failingOutput = new OutputStream() {
            @Override
            public void write(int value) throws IOException {
                throw writeFailure;
            }

            @Override
            public void write(byte[] buffer, int offset, int length) throws IOException {
                throw writeFailure;
            }
        };
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                new BasicDuplexConnection(SessionRuntimeTestSupport.emptyInput(), failingOutput),
                ZmuxConfig.builder().role(Role.RESPONDER).build(),
                0L,
                Settings.defaults()
        );
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        Thread writer = startWriterLoop(runtime, writerFailure, "goaway-failure-writer");

        IOException error = assertThrows(
                IOException.class,
                () -> runtime.goAway(0L, 0L, ErrorCode.NO_ERROR.code(), "fail"),
                "GOAWAY should surface writer failure"
        );

        assertEquals("goAway", ZmuxErrors.operation(error), "GOAWAY failure should retain the caller operation");
        assertTrue(hasCause(error, writeFailure), "GOAWAY failure should retain the transport writer error");
        joinThread(writer, "writer should stop after write failure");
        assertEquals(SessionState.FAILED, runtime.state(), "writer failure should fail the session");
        assertNoThreadFailure(writerFailure, "writer loop should handle transport write failure internally");
    }

    @Test
    void goAwayDrainIntervalAdaptsToRecentRtt() throws Exception {
        SessionRuntime runtime = newRuntimeWithNoOpThreshold(1);
        SessionRuntimeTestSupport.setLongField(runtime, "lastPingRttNanos", TimeUnit.MILLISECONDS.toNanos(800L));

        synchronized (runtime.lock()) {
            assertEquals(
                    TimeUnit.MILLISECONDS.toNanos(200L),
                    runtime.goAwayDrainIntervalNanosLocked(),
                    "GOAWAY drain interval should follow Go's max(10ms, RTT/4) adaptive rule"
            );
        }
    }

    @Test
    void peerGoAwayDuplicateStandardDiagDropsReasonButKeepsPrimarySemantics() throws Exception {
        SessionRuntime runtime = newRuntimeWithNoOpThreshold(1);
        long acceptedBidi = maxLocalGoAwayWatermark(true) - 4L;
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        Varint62.write(payload, acceptedBidi);
        Varint62.write(payload, 0L);
        Varint62.write(payload, ErrorCode.PROTOCOL.code());

        handleGoAway(runtime, duplicateStandardDiagPayload(payload.toByteArray(), "maintenance"));

        assertEquals(SessionState.DRAINING, runtime.state(), "peer GOAWAY should still move the session to DRAINING");
        assertNotNull(runtime.peerGoAwayError(), "peer GOAWAY should still be recorded");
        assertEquals(ErrorCode.PROTOCOL.code(), runtime.peerGoAwayError().code(), "peer GOAWAY code mismatch");
        assertEquals("", runtime.peerGoAwayError().reason(), "duplicate singleton DIAG should clear the retained GOAWAY reason");
        assertEquals(acceptedBidi, runtime.peerGoAwayBidiInternal(), "peer GOAWAY bidi watermark mismatch");
        assertEquals(0L, runtime.peerGoAwayUniInternal(), "peer GOAWAY uni watermark mismatch");
    }

    @Test
    void peerGoAwayInvalidUtf8DiagDropsReasonButKeepsPrimarySemantics() throws Exception {
        SessionRuntime runtime = newRuntimeWithNoOpThreshold(1);
        long acceptedBidi = maxLocalGoAwayWatermark(true) - 8L;
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        Varint62.write(payload, acceptedBidi);
        Varint62.write(payload, 0L);
        Varint62.write(payload, ErrorCode.INTERNAL.code());

        handleGoAway(runtime, invalidUtf8DiagPayload(payload.toByteArray()));

        assertEquals(SessionState.DRAINING, runtime.state(), "peer GOAWAY should still move the session to DRAINING");
        assertNotNull(runtime.peerGoAwayError(), "peer GOAWAY should still be recorded");
        assertEquals(ErrorCode.INTERNAL.code(), runtime.peerGoAwayError().code(), "peer GOAWAY code mismatch");
        assertEquals("", runtime.peerGoAwayError().reason(), "invalid UTF-8 DIAG should clear the retained GOAWAY reason");
        assertEquals(acceptedBidi, runtime.peerGoAwayBidiInternal(), "peer GOAWAY bidi watermark mismatch");
        assertEquals(0L, runtime.peerGoAwayUniInternal(), "peer GOAWAY uni watermark mismatch");
    }

    @Test
    void closeAfterQueuedLocalGoAwayPreservesPriorGoAwayBeforeRefinedReplacement() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                ZmuxConfig.builder()
                        .role(Role.RESPONDER)
                        .gracefulCloseDrainTimeout(java.time.Duration.ofMillis(200L))
                        .build(),
                0L,
                Settings.defaults()
        );
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        long initialBidi = peerGoAwayWatermark(true, 2);
        long initialUni = peerGoAwayWatermark(false, 1);
        long refinedBidi = peerGoAwayWatermark(true, 1);
        long refinedUni = 0L;
        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "beginLocalOpenLocked",
                    new Class<?>[]{StreamRuntime.class},
                    stream
            );
            SessionRuntimeTestSupport.setLongField(runtime, "lastAcceptedPeerBidi", refinedBidi);
            SessionRuntimeTestSupport.setLongField(runtime, "lastAcceptedPeerUni", refinedUni);
        }
        AtomicReference<Throwable> goAwayFailure = new AtomicReference<>();
        Thread goAwayThread = startGoAway(
                runtime,
                initialBidi,
                initialUni,
                ErrorCode.NO_ERROR.code(),
                "prior",
                goAwayFailure,
                "prior-goaway"
        );
        awaitCondition(() -> {
            try {
                return SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue").size() == 1;
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        }, "prior GOAWAY should be queued");

        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        Thread closeThread = new Thread(() -> {
            try {
                runtime.close();
            } catch (Throwable throwable) {
                closeFailure.set(throwable);
            }
        }, "close-after-prior-goaway");
        closeThread.start();

        Thread writer;
        try {
            awaitCondition(() -> {
                synchronized (runtime.lock()) {
                    try {
                        Deque<Object> urgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
                        Object last = urgentQueue.peekLast();
                        return last != null
                                && urgentQueue.size() == 2
                                && SessionRuntimeTestSupport.outboundFrame(last).type() == FrameType.GOAWAY;
                    } catch (Exception exception) {
                        throw new RuntimeException(exception);
                    }
                }
            }, "close should queue the refined GOAWAY and wait for it to be sent");

            synchronized (runtime.lock()) {
                Deque<Object> urgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
                assertEquals(2, urgentQueue.size(), "close after a queued prior GOAWAY should retain both GOAWAY frames before flushing");
                Object[] frames = urgentQueue.toArray();
                assertEquals(FrameType.GOAWAY, SessionRuntimeTestSupport.outboundFrame(frames[0]).type(), "first queued control frame mismatch");
                assertEquals(FrameType.GOAWAY, SessionRuntimeTestSupport.outboundFrame(frames[1]).type(), "second queued control frame mismatch");

                FrameCodec.GoAwayPayload initial = FrameCodec.parseGoAwayPayload(
                        SessionRuntimeTestSupport.outboundFrame(frames[0]).payload()
                );
                FrameCodec.GoAwayPayload refined = FrameCodec.parseGoAwayPayload(
                        SessionRuntimeTestSupport.outboundFrame(frames[1]).payload()
                );
                assertEquals(initialBidi, initial.lastAcceptedBidi(), "prior GOAWAY bidi watermark mismatch");
                assertEquals(initialUni, initial.lastAcceptedUni(), "prior GOAWAY uni watermark mismatch");
                assertEquals(refinedBidi, refined.lastAcceptedBidi(), "refined GOAWAY bidi watermark mismatch");
                assertEquals(refinedUni, refined.lastAcceptedUni(), "refined GOAWAY uni watermark mismatch");
            }
        } finally {
            writer = startWriterLoop(runtime, writerFailure, "close-after-prior-goaway-writer");
            writer.join(1_000L);
            closeThread.join(1_000L);
            goAwayThread.join(1_000L);
        }

        assertFalse(writer.isAlive(), "writer loop should terminate after flushing queued close frames");
        assertFalse(closeThread.isAlive(), "close should finish once the writer flushes the queued frames");
        assertFalse(goAwayThread.isAlive(), "prior GOAWAY should finish once a covering GOAWAY is sent");
        assertNull(writerFailure.get(), "writer loop failure mismatch");
        assertNull(closeFailure.get(), "close failure mismatch");
        assertNull(goAwayFailure.get(), "prior GOAWAY failure mismatch");
    }
}
