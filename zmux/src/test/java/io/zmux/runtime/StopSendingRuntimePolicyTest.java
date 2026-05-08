package io.zmux.runtime;

import io.zmux.OpenMetadataTooLargeException;
import io.zmux.Settings;
import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;
import io.zmux.protocol.Protocol;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

final class StopSendingRuntimePolicyTest {
    private static void makePeerVisible(SessionRuntime runtime, StreamRuntime stream) throws Exception {
        runtime.beginLocalOpenLocked(stream);
        runtime.markLocalStreamOpeningCommittedLocked(stream);
        SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "markPeerVisibleLocked",
                new Class<?>[]{StreamRuntime.class},
                stream
        );
    }

    private static void seedQueuedOnlyTail(SessionRuntime runtime, StreamRuntime stream, int queuedBytes) throws Exception {
        stream.markLocalSendStartedLocked();
        stream.reserveQueuedDataBytesLocked(queuedBytes);
        stream.reserveSendBytesLocked(queuedBytes);
        SessionRuntimeTestSupport.setLongField(runtime, "sessionQueuedDataBytes", queuedBytes);
        SessionRuntimeTestSupport.setLongField(runtime, "sessionReservedSendBytes", queuedBytes);
    }

    @Test
    void fastLinkEstimateAllowsGracefulFinishForLargeQueuedOnlyTail() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
            seedQueuedOnlyTail(runtime, stream, 768);
            SessionRuntimeTestSupport.setLongField(runtime, "sendRateEstimateBytesPerSecond", 16L << 10);

            boolean graceful = (Boolean) SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "tryGracefulStopSendingLocked",
                    new Class<?>[]{StreamRuntime.class},
                    stream
            );

            assertTrue(graceful, "fast-link estimate should allow graceful STOP_SENDING completion for a larger queued-only tail");
            assertEquals(1, SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue").size(), "graceful finish should enqueue a terminal DATA frame");

            Object outbound = SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue").peekFirst();
            FrameCodec.Frame fin = SessionRuntimeTestSupport.outboundFrame(outbound);
            assertEquals(FrameType.DATA, fin.type(), "graceful finish should emit DATA");
            assertTrue((fin.flags() & Protocol.FRAME_FLAG_FIN) != 0, "graceful finish should carry FIN");
            assertEquals(0, SessionRuntimeTestSupport.outboundDataBytes(outbound), "graceful finish should not invent extra payload bytes");
        }
    }

    @Test
    void withoutSendRateEstimateLargeQueuedOnlyTailDoesNotGracefullyFinish() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
            seedQueuedOnlyTail(runtime, stream, 768);

            boolean graceful = (Boolean) SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "tryGracefulStopSendingLocked",
                    new Class<?>[]{StreamRuntime.class},
                    stream
            );

            assertFalse(graceful, "without a slow-link estimate the repository-default static tail cap should still reject a large queued-only tail");
            assertTrue(SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue").isEmpty(), "non-graceful path should not enqueue DATA|FIN here");
        }
    }

    @Test
    void gracefulStopSendingReemitsOpeningFrameWhenCommittedInvisibleOpenNeedsEmit() throws Exception {
        long capabilities = Protocol.CAPABILITY_OPEN_METADATA;
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(capabilities, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream(
                new io.zmux.OpenOptions(0L, null, "tag".getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );

        synchronized (runtime.lock()) {
            runtime.beginLocalOpenLocked(stream);
            runtime.markLocalStreamOpeningCommittedLocked(stream);
            stream.clearOpeningFramePendingLocked();
            assertEquals(LocalOpenPhase.NEEDS_EMIT, stream.localOpenPhaseLocked(), "test requires a committed local stream with no queued opener");

            boolean graceful = (Boolean) SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "tryGracefulStopSendingLocked",
                    new Class<?>[]{StreamRuntime.class},
                    stream
            );

            assertTrue(graceful, "committed invisible open should still be allowed to conclude gracefully");
            Object outbound = SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue").peekFirst();
            assertTrue(SessionRuntimeTestSupport.outboundOpeningFrame(outbound), "graceful STOP_SENDING finish should re-emit the opener while the stream still needs emission");
            FrameCodec.Frame fin = SessionRuntimeTestSupport.outboundFrame(outbound);
            assertEquals(FrameType.DATA, fin.type(), "graceful finish should still emit DATA");
            assertTrue((fin.flags() & Protocol.FRAME_FLAG_FIN) != 0, "graceful finish should carry FIN");
            assertTrue((fin.flags() & Protocol.FRAME_FLAG_OPEN_METADATA) != 0, "graceful finish should preserve opening metadata on the re-emitted opener");
        }
    }

    @Test
    void gracefulStopSendingQueueFailureDoesNotArmDrainDeadline() throws Exception {
        Settings peerSettings = Settings.defaults().toBuilder()
                .maxFramePayload(16L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(Protocol.CAPABILITY_OPEN_METADATA, peerSettings);
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            runtime.beginLocalOpenLocked(stream);
            runtime.markLocalStreamOpeningCommittedLocked(stream);
            stream.clearOpeningFramePendingLocked();
            stream.applyOpenMetadataLocked(0L, null, "oversized-open-info".getBytes(StandardCharsets.UTF_8));
            assertEquals(LocalOpenPhase.NEEDS_EMIT, stream.localOpenPhaseLocked(), "test requires a committed invisible opener");

            assertInstanceOf(
                    OpenMetadataTooLargeException.class,
                    assertThrows(IOException.class, () -> runtime.tryGracefulStopSendingLocked(stream)),
                    "oversized opening metadata should fail before graceful STOP_SENDING finish is queued"
            );
            assertEquals(0L, stream.stopSendingGracefulDeadlineNanosLocked(), "failed graceful finish queueing must not leave a stream deadline armed");
            assertEquals(0L, runtime.nextStopSendingGracefulDeadlineLocked(), "failed graceful finish queueing must not leave a session wake deadline armed");
            assertTrue(SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue").isEmpty(), "failed graceful finish queueing must not retain DATA frames");
        }
    }
}
