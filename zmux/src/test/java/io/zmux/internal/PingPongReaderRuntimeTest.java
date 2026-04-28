package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class PingPongReaderRuntimeTest {
    private static void handlePingFrame(SessionRuntime runtime, FrameCodec.Frame frame) throws Exception {
        invokeReaderFrameHandler(runtime, "handlePingFrame", frame);
    }

    private static void handlePongFrame(SessionRuntime runtime, FrameCodec.Frame frame) throws Exception {
        invokeReaderFrameHandler(runtime, "handlePongFrame", frame);
    }

    private static void invokeReaderFrameHandler(SessionRuntime runtime, String name, FrameCodec.Frame frame) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField("readerRuntime");
        field.setAccessible(true);
        Object readerRuntime = field.get(runtime);
        try {
            SessionRuntimeTestSupport.invokePrivate(
                    readerRuntime,
                    name,
                    new Class<?>[]{FrameCodec.Frame.class},
                    frame
            );
        } catch (InvocationTargetException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw exception;
        }
    }

    private static int getIntField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getInt(target);
    }

    private static byte[] taggedPingPayload(long nonce, long key) {
        byte[] payload = new byte[Long.BYTES * 2];
        writeLongBigEndian(payload, 0, nonce);
        writeLongBigEndian(payload, Long.BYTES, pingPaddingTag(key, nonce));
        return payload;
    }

    private static long pingPaddingTag(long key, long nonce) {
        long value = key ^ nonce ^ 0x6d1d9f6d33f9772dL;
        value = (value ^ value >>> 30) * -4658895280553007687L;
        value = (value ^ value >>> 27) * -7723592293110705685L;
        return value ^ value >>> 31;
    }

    private static void writeLongBigEndian(byte[] output, int offset, long value) {
        for (int i = 0; i < Long.BYTES; i++) {
            output[offset + Long.BYTES - 1 - i] = (byte) (value >>> i * 8);
        }
    }

    @SuppressWarnings("unchecked")
    private static Deque<Object> readLoopProtocolTasks(SessionRuntime runtime) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField("readLoopProtocolTasks");
        field.setAccessible(true);
        return (Deque<Object>) field.get(runtime);
    }

    private static Object readLoopProtocolTaskOutboundFrame(Object task) throws Exception {
        Field field = task.getClass().getDeclaredField("outboundFrame");
        field.setAccessible(true);
        return field.get(task);
    }

    private static byte[] awaitQueuedPayload(SessionRuntime runtime, FrameType type) throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
        while (System.nanoTime() < deadlineNanos) {
            synchronized (runtime.lock()) {
                Deque<Object> queue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
                for (Object outbound : queue) {
                    if (SessionRuntimeTestSupport.outboundFrame(outbound).type() == type) {
                        return SessionRuntimeTestSupport.outboundPayload(outbound);
                    }
                }
            }
            TimeUnit.MILLISECONDS.sleep(1L);
        }
        fail("timed out waiting for queued " + type);
        return new byte[0];
    }

    @Test
    void directMalformedPongFailsBeforeCloseStart() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> handlePongFrame(runtime, new FrameCodec.Frame(FrameType.PONG, 0, 0L, new byte[]{1}))
        );
        assertEquals(ErrorCode.FRAME_SIZE.code(), error.code(), "short direct PONG should fail with FRAME_SIZE");
    }

    @Test
    void directMalformedPongIsIgnoredAfterCloseStart() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        synchronized (runtime.lock()) {
            runtime.setCloseFrameQueuedInternal(true);
        }

        handlePongFrame(runtime, new FrameCodec.Frame(FrameType.PONG, 0, 0L, new byte[]{1}));

        synchronized (runtime.lock()) {
            assertEquals(0, getIntField(runtime, "noOpControlCount"), "ignored closing PONG must not consume no-op budget");
        }
    }

    @Test
    void lateMatchingPongAfterPingTimeoutClearsNoOpBudget() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                ZmuxConfig.builder()
                        .role(Role.RESPONDER)
                        .noOpControlFloodThreshold(1)
                        .build(),
                0L,
                Settings.defaults()
        );
        AtomicReference<Throwable> pingFailure = new AtomicReference<>();
        Thread pingThread = new Thread(() -> {
            try {
                runtime.ping(new byte[]{9}, Duration.ofMillis(20L));
                pingFailure.set(new AssertionError("ping should time out without a PONG"));
            } catch (Throwable error) {
                pingFailure.set(error);
            }
        }, "late-pong-timeout");

        pingThread.start();
        byte[] timedOutPingPayload = awaitQueuedPayload(runtime, FrameType.PING);
        pingThread.join(1_000L);

        assertFalse(pingThread.isAlive(), "timed-out ping should return");
        assertInstanceOf(PingTimeoutException.class, pingFailure.get(), "ping should fail with a timeout");

        byte[] unexpected = new byte[]{0, 0, 0, 0, 0, 0, 0, 1};
        handlePongFrame(runtime, new FrameCodec.Frame(FrameType.PONG, 0, 0L, unexpected));
        handlePongFrame(runtime, new FrameCodec.Frame(FrameType.PONG, 0, 0L, timedOutPingPayload));

        synchronized (runtime.lock()) {
            assertEquals(0, getIntField(runtime, "noOpControlCount"), "late matching PONG should reset no-op budget");
        }
        handlePongFrame(runtime, new FrameCodec.Frame(FrameType.PONG, 0, 0L, unexpected));
    }

    @Test
    void lateMatchingPongAfterPingInterruptClearsNoOpBudget() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                ZmuxConfig.builder()
                        .role(Role.RESPONDER)
                        .noOpControlFloodThreshold(1)
                        .build(),
                0L,
                Settings.defaults()
        );
        AtomicReference<Throwable> pingFailure = new AtomicReference<>();
        Thread pingThread = new Thread(() -> {
            try {
                runtime.ping(new byte[]{7}, Duration.ofSeconds(5L));
                pingFailure.set(new AssertionError("ping should be interrupted before a PONG"));
            } catch (Throwable error) {
                pingFailure.set(error);
            }
        }, "late-pong-interrupt");

        pingThread.start();
        byte[] interruptedPingPayload = awaitQueuedPayload(runtime, FrameType.PING);
        pingThread.interrupt();
        pingThread.join(1_000L);

        assertFalse(pingThread.isAlive(), "interrupted ping should return");
        assertInstanceOf(ZmuxInterruptedException.class, pingFailure.get(), "ping should fail with an interrupted sentinel");

        byte[] unexpected = new byte[]{0, 0, 0, 0, 0, 0, 0, 2};
        handlePongFrame(runtime, new FrameCodec.Frame(FrameType.PONG, 0, 0L, unexpected));
        handlePongFrame(runtime, new FrameCodec.Frame(FrameType.PONG, 0, 0L, interruptedPingPayload));

        synchronized (runtime.lock()) {
            assertEquals(0, getIntField(runtime, "noOpControlCount"), "late matching PONG should reset no-op budget after interrupt");
        }
        handlePongFrame(runtime, new FrameCodec.Frame(FrameType.PONG, 0, 0L, unexpected));
    }

    @Test
    void inboundPingFloodUsesConfiguredThreshold() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                ZmuxConfig.builder()
                        .role(Role.RESPONDER)
                        .inboundPingFloodThreshold(1)
                        .build(),
                0L,
                Settings.defaults()
        );

        byte[] payload = new byte[]{0, 0, 0, 0, 0, 0, 0, 1};
        handlePingFrame(runtime, new FrameCodec.Frame(FrameType.PING, 0, 0L, payload));

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> handlePingFrame(runtime, new FrameCodec.Frame(FrameType.PING, 0, 0L, payload))
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "repeated inbound PING should honor the configured flood threshold");
    }

    @Test
    void inboundPingEchoRetainsStablePayloadSnapshot() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        byte[] payload = new byte[]{0, 1, 2, 3, 4, 5, 6, 7};
        byte[] expected = Arrays.copyOf(payload, payload.length);

        handlePingFrame(runtime, new FrameCodec.Frame(FrameType.PING, 0, 0L, payload));
        payload[0] = 99;

        Object outbound;
        synchronized (runtime.lock()) {
            outbound = SessionRuntimeTestSupport.pollLastOutboundQueue(runtime, "urgentQueue");
        }
        assertNotNull(outbound, "PING should enqueue a PONG response");
        assertEquals(FrameType.PONG, SessionRuntimeTestSupport.outboundFrame(outbound).type(), "PING response frame type mismatch");
        assertArrayEquals(expected, SessionRuntimeTestSupport.outboundPayload(outbound), "PONG should echo the inbound PING payload snapshot");
    }

    @Test
    void paddedOutgoingPingAcceptsPongWithMatchingPrefixAndExtraSuffix() throws Exception {
        Settings localSettings = Settings.defaults().toBuilder()
                .pingPaddingKey(321L)
                .build();
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .settings(localSettings)
                .pingPadding(true)
                .pingPaddingMinBytes(8L)
                .pingPaddingMaxBytes(8L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());
        AtomicReference<Throwable> pingFailure = new AtomicReference<>();
        Thread pingThread = new Thread(() -> {
            try {
                runtime.ping(new byte[]{7}, Duration.ofSeconds(1L));
            } catch (Throwable error) {
                pingFailure.set(error);
            }
        }, "padded-ping-prefix-pong");

        pingThread.start();
        byte[] pingPayload = awaitQueuedPayload(runtime, FrameType.PING);

        assertTrue(pingPayload.length > Long.BYTES + 1, "padded PING should carry tag bytes before echo");
        byte[] paddedPong = Arrays.copyOf(pingPayload, pingPayload.length + 3);
        handlePongFrame(runtime, new FrameCodec.Frame(FrameType.PONG, 0, 0L, paddedPong));
        pingThread.join(1_000L);

        assertFalse(pingThread.isAlive(), "matching padded PONG should complete ping");
        assertNull(pingFailure.get(), "matching padded PONG should not fail ping");
    }

    @Test
    void inboundTaggedPingReceivesPaddedPongReply() throws Exception {
        long peerPaddingKey = 654L;
        Settings peerSettings = Settings.defaults().toBuilder()
                .pingPaddingKey(peerPaddingKey)
                .build();
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .pingPadding(true)
                .pingPaddingMinBytes(8L)
                .pingPaddingMaxBytes(8L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, peerSettings);
        byte[] payload = taggedPingPayload(9L, peerPaddingKey);

        handlePingFrame(runtime, new FrameCodec.Frame(FrameType.PING, 0, 0L, payload));

        Object outbound;
        synchronized (runtime.lock()) {
            outbound = SessionRuntimeTestSupport.pollLastOutboundQueue(runtime, "urgentQueue");
        }
        assertNotNull(outbound, "tagged PING should enqueue a PONG response");
        byte[] reply = SessionRuntimeTestSupport.outboundPayload(outbound);
        assertEquals(FrameType.PONG, SessionRuntimeTestSupport.outboundFrame(outbound).type());
        assertTrue(reply.length > payload.length, "recognized tagged PING should receive PONG padding");
        assertArrayEquals(payload, Arrays.copyOf(reply, payload.length), "padded PONG must preserve original PING prefix");
    }

    @Test
    void deferredInboundPingEchoRetainsStablePayloadSnapshot() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                ZmuxConfig.builder()
                        .role(Role.RESPONDER)
                        .urgentQueuedBytesCap(1L)
                        .build(),
                0L,
                Settings.defaults()
        );
        byte[] payload = new byte[]{0, 1, 2, 3, 4, 5, 6, 7};
        byte[] expected = Arrays.copyOf(payload, payload.length);

        synchronized (runtime.lock()) {
            runtime.enqueueControlLocked(new FrameCodec.Frame(FrameType.PING, 0, 0L, new byte[0]));
            SessionRuntimeTestSupport.setField(runtime, "readLoopProtocolWorkerStarted", true);
        }

        handlePingFrame(runtime, new FrameCodec.Frame(FrameType.PING, 0, 0L, payload));
        payload[0] = 99;

        synchronized (runtime.lock()) {
            Deque<Object> tasks = readLoopProtocolTasks(runtime);
            assertEquals(1, tasks.size(), "backpressured PONG should be deferred into the read-loop protocol queue");
            Object outbound = readLoopProtocolTaskOutboundFrame(tasks.peekFirst());
            assertEquals(FrameType.PONG, SessionRuntimeTestSupport.outboundFrame(outbound).type(), "deferred response frame type mismatch");
            assertArrayEquals(expected, SessionRuntimeTestSupport.outboundPayload(outbound), "deferred PONG should retain the inbound PING snapshot");
        }
    }

    @Test
    void inboundPingBackpressureUsesBoundedProtocolQueue() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                ZmuxConfig.builder()
                        .role(Role.RESPONDER)
                        .urgentQueuedBytesCap(1L)
                        .inboundPingFloodThreshold(10_000)
                        .build(),
                0L,
                Settings.defaults()
        );

        synchronized (runtime.lock()) {
            runtime.enqueueControlLocked(new FrameCodec.Frame(FrameType.PING, 0, 0L, new byte[0]));
        }

        byte[] payload = new byte[]{0, 1, 2, 3, 4, 5, 6, 7};
        for (int i = 0; i < 512; i++) {
            handlePingFrame(runtime, new FrameCodec.Frame(FrameType.PING, 0, 0L, payload));
        }

        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2L);
        while (runtime.stats().diagnostics().protocolBacklogBlocked() == 0L
                && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }

        assertTrue(
                runtime.stats().diagnostics().protocolBacklogBlocked() > 0L,
                "read-loop PONG overflow should be dropped and surfaced in backlog diagnostics"
        );
        synchronized (runtime.lock()) {
            assertFalse(runtime.stateInternal().terminal(), "droppable PONG backlog must not fail the session");
            assertTrue(
                    runtime.outboundQueueBookkeepingInternal().urgentQueuedControlBytesLocked() <= 1L,
                    "urgent queue accounting must stay within the configured cap"
            );
        }
    }

    @Test
    void readLoopAbortBackpressureUsesBoundedProtocolQueue() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                ZmuxConfig.builder()
                        .role(Role.RESPONDER)
                        .urgentQueuedBytesCap(1L)
                        .build(),
                0L,
                Settings.defaults()
        );

        synchronized (runtime.lock()) {
            runtime.enqueueControlLocked(new FrameCodec.Frame(FrameType.PING, 0, 0L, new byte[0]));
            StreamRuntime streamRuntime = runtime.createPeerOpenedStreamLocked(
                    SessionRuntime.firstPeerStreamId(Role.RESPONDER, true)
            );
            streamRuntime.abortFromLocalLocked(ErrorCode.STREAM_STATE.code(), "");
            byte[] payload = runtime.buildControlErrorPayloadLocked(ErrorCode.STREAM_STATE.code(), "");
            for (int i = 0; i < 512; i++) {
                runtime.enqueueReadLoopAbortLocked(streamRuntime, ErrorCode.STREAM_STATE.code(), payload);
            }

            assertTrue(
                    runtime.stats().diagnostics().protocolBacklogBlocked() > 0L,
                    "read-loop ABORT overflow should be dropped and surfaced in backlog diagnostics"
            );
            assertFalse(runtime.stateInternal().terminal(), "droppable ABORT backlog must not fail the session");
            assertTrue(
                    runtime.outboundQueueBookkeepingInternal().urgentQueuedControlBytesLocked() <= 1L,
                    "urgent queue accounting must stay within the configured cap"
            );
        }
    }
}
