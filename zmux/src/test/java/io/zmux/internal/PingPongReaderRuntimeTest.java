package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

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
