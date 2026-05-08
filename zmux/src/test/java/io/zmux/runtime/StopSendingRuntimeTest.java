package io.zmux.runtime;

import io.zmux.ErrorCode;
import io.zmux.Settings;
import io.zmux.WriteClosedException;
import io.zmux.ZmuxErrorSource;
import io.zmux.ZmuxTerminationKind;
import io.zmux.protocol.Frame;
import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;
import io.zmux.protocol.Protocol;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class StopSendingRuntimeTest {
    private static Settings constrainedPeerSettings() {
        return Settings.defaults().toBuilder()
                .initialMaxStreamDataBidiPeerOpened(4096L)
                .initialMaxData(4096L)
                .maxFramePayload(256L)
                .build();
    }

    private static FrameCodec.Frame stopSendingFrame(long streamId) throws IOException {
        return stopSendingFrame(streamId, ErrorCode.CANCELLED.code(), "");
    }

    private static FrameCodec.Frame stopSendingFrame(long streamId, long code, String reason) throws IOException {
        return new FrameCodec.Frame(
                FrameType.STOP_SENDING,
                0,
                streamId,
                FrameCodec.buildErrorPayload(code, reason, Settings.defaults().maxControlPayloadBytes())
        );
    }

    private static boolean hasQueuedReset(SessionRuntime runtime, long streamId) throws Exception {
        return hasQueuedFrame(runtime, streamId, FrameType.RESET, false);
    }

    private static boolean hasQueuedDataFin(SessionRuntime runtime, long streamId) throws Exception {
        return hasQueuedFrame(runtime, streamId, FrameType.DATA, true);
    }

    private static boolean hasQueuedFrame(SessionRuntime runtime, long streamId, FrameType type, boolean requireFin) throws Exception {
        return queueContains(runtime, "urgentQueue", streamId, type, requireFin)
                || queueContains(runtime, "dataQueue", streamId, type, requireFin);
    }

    private static boolean queueContains(SessionRuntime runtime, String fieldName, long streamId, FrameType type, boolean requireFin) throws Exception {
        for (Object outbound : SessionRuntimeTestSupport.outboundQueue(runtime, fieldName)) {
            FrameCodec.Frame frame = SessionRuntimeTestSupport.outboundFrame(outbound);
            if (frame.streamId() != streamId || frame.type() != type) {
                continue;
            }
            if (!requireFin || (frame.flags() & Protocol.FRAME_FLAG_FIN) != 0) {
                return true;
            }
        }
        return false;
    }

    @Test
    void smallInflightTailWithLargeSuppressibleQueueStillFinishesGracefully() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, constrainedPeerSettings());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        SessionRuntimeTestSupport.queueWrite(stream, new byte[576]);

        synchronized (runtime.lock()) {
            Object inflight = SessionRuntimeTestSupport.pollLastOutboundQueue(runtime, "dataQueue");
            assertNotNull(inflight, "test requires a queued tail frame to simulate writer-admitted bytes");
            assertEquals(64, SessionRuntimeTestSupport.outboundDataBytes(inflight), "expected a small tail slice to become in-flight");
            assertEquals(512L, stream.queuedDataBytesLocked(), "queued byte accounting should retain only the suppressible tail");
            assertEquals(64L, SessionRuntimeTestSupport.invokePrivate(runtime, "inflightQueuedBytesForStreamLocked", new Class<?>[]{StreamRuntime.class}, stream), "in-flight byte accounting should retain the admitted tail");
            assertEquals(576L, stream.reservedSendBytes(), "reserved send bytes should include both queued and in-flight committed tail");
        }

        SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "handleStopSendingFrame",
                new Class<?>[]{FrameCodec.Frame.class},
                stopSendingFrame(stream.streamIdInternal())
        );

        synchronized (runtime.lock()) {
            assertTrue(stream.shouldEmitQueuedDataLocked(), "graceful STOP_SENDING should keep the committed tail writable until FIN is sent");
            assertTrue(hasQueuedDataFin(runtime, stream.streamIdInternal()), "graceful STOP_SENDING should append an empty DATA|FIN");
            assertFalse(hasQueuedReset(runtime, stream.streamIdInternal()), "graceful STOP_SENDING must not queue RESET");
        }

        WriteClosedException stop = assertInstanceOf(
                WriteClosedException.class,
                assertThrows(IOException.class, () -> stream.write(new byte[]{1})),
                "once graceful STOP_SENDING completion has queued DATA|FIN, future writes should surface the local graceful close"
        );
        assertEquals(ZmuxErrorSource.LOCAL, stop.source(), "post-finish write failure source mismatch");
        assertEquals(ZmuxTerminationKind.GRACEFUL, stop.terminationKind(), "post-finish write failure termination mismatch");
    }

    @Test
    void cancelWriteAfterGracefulStopSendingFinishStaysGracefullyClosed() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, constrainedPeerSettings());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        SessionRuntimeTestSupport.queueWrite(stream, new byte[576]);

        synchronized (runtime.lock()) {
            Object inflight = SessionRuntimeTestSupport.pollLastOutboundQueue(runtime, "dataQueue");
            assertNotNull(inflight, "test requires a queued tail frame to simulate writer-admitted bytes");
        }

        SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "handleStopSendingFrame",
                new Class<?>[]{FrameCodec.Frame.class},
                stopSendingFrame(stream.streamIdInternal())
        );

        WriteClosedException closed = assertInstanceOf(
                WriteClosedException.class,
                assertThrows(IOException.class, () -> stream.cancelWrite(ErrorCode.CANCELLED.code())),
                "cancelWrite must not override an already-queued graceful STOP_SENDING finish with RESET"
        );
        assertEquals(ZmuxErrorSource.LOCAL, closed.source(), "post-finish cancelWrite source mismatch");
        assertEquals(ZmuxTerminationKind.GRACEFUL, closed.terminationKind(), "post-finish cancelWrite termination mismatch");

        synchronized (runtime.lock()) {
            assertFalse(hasQueuedReset(runtime, stream.streamIdInternal()), "cancelWrite after queued graceful finish must not enqueue RESET");
            assertTrue(hasQueuedDataFin(runtime, stream.streamIdInternal()), "queued graceful DATA|FIN must remain intact after rejected cancelWrite");
        }
    }

    @Test
    void largeQueuedOnlyTailFallsBackToReset() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, constrainedPeerSettings());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        SessionRuntimeTestSupport.queueWrite(stream, new byte[576]);

        SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "handleStopSendingFrame",
                new Class<?>[]{FrameCodec.Frame.class},
                stopSendingFrame(stream.streamIdInternal())
        );

        synchronized (runtime.lock()) {
            assertFalse(stream.shouldEmitQueuedDataLocked(), "reset conclusion should close the committed tail immediately");
            assertTrue(hasQueuedReset(runtime, stream.streamIdInternal()), "large queued-only tail should queue RESET");
            assertFalse(hasQueuedDataFin(runtime, stream.streamIdInternal()), "reset conclusion must not also append DATA|FIN");
            assertEquals(0L, stream.queuedDataBytesLocked(), "reset conclusion should release queued-data accounting immediately");
            assertEquals(0L, stream.reservedSendBytes(), "reset conclusion should release reserved send credit for withdrawable queued bytes immediately");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "sessionQueuedDataBytes"), "reset conclusion should clear session queued-data accounting immediately");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "sessionReservedSendBytes"), "reset conclusion should release session reserved send credit for withdrawable queued bytes immediately");
        }
    }

    @Test
    void closeWriteAfterStopDrivenResetBecomesBenignNoOp() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, constrainedPeerSettings());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        SessionRuntimeTestSupport.queueWrite(stream, new byte[576]);

        SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "handleStopSendingFrame",
                new Class<?>[]{FrameCodec.Frame.class},
                stopSendingFrame(stream.streamIdInternal())
        );

        stream.closeWrite();

        synchronized (runtime.lock()) {
            assertTrue(hasQueuedReset(runtime, stream.streamIdInternal()), "stop-driven reset should remain queued after benign closeWrite");
            assertFalse(hasQueuedDataFin(runtime, stream.streamIdInternal()), "closeWrite after stop-driven reset must not enqueue a second DATA|FIN");
        }
    }

    @Test
    void writeAfterStopDrivenResetSurfacesPeerStop() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, constrainedPeerSettings());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        SessionRuntimeTestSupport.queueWrite(stream, new byte[576]);

        SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "handleStopSendingFrame",
                new Class<?>[]{FrameCodec.Frame.class},
                stopSendingFrame(stream.streamIdInternal(), 77L, "peer stop")
        );

        WriteClosedException error = assertInstanceOf(
                WriteClosedException.class,
                assertThrows(IOException.class, () -> stream.write(new byte[]{1})),
                "writes after a STOP_SENDING-driven RESET should preserve the peer STOP_SENDING surface"
        );
        assertEquals(77L, error.code(), "peer STOP_SENDING code should win over the local CANCELLED reset");
        assertEquals("peer stop", error.reason(), "peer STOP_SENDING reason should be retained");
        assertEquals(ZmuxErrorSource.REMOTE, error.source(), "peer STOP_SENDING source mismatch");
        assertEquals(ZmuxTerminationKind.STOPPED, error.terminationKind(), "peer STOP_SENDING termination mismatch");
    }

    @Test
    void gracefulDrainDeadlineTrackingDropsClearedAndTerminalStreams() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, constrainedPeerSettings());
        StreamRuntime first = (StreamRuntime) runtime.openStream();
        StreamRuntime second = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            long now = System.nanoTime();
            first.armStopSendingGracefulDrainLocked(now + TimeUnit.MILLISECONDS.toNanos(1L));
            second.armStopSendingGracefulDrainLocked(now + TimeUnit.MILLISECONDS.toNanos(5L));

            first.abortFromLocalLocked(ErrorCode.CANCELLED.code(), "");
            assertEquals(0L, first.stopSendingGracefulDeadlineNanosLocked(), "send-terminal transition should clear the first stream's graceful drain deadline");

            long nextDeadline = (Long) SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "nextStopSendingGracefulDeadlineLocked",
                    new Class<?>[0]
            );
            assertEquals(second.stopSendingGracefulDeadlineNanosLocked(), nextDeadline, "writer wake deadline should skip cleared terminal streams");

            second.clearStopSendingGracefulDrainLocked();
            assertEquals(
                    0L,
                    (Long) SessionRuntimeTestSupport.invokePrivate(runtime, "nextStopSendingGracefulDeadlineLocked", new Class<?>[0]),
                    "clearing the last active graceful drain should drop the session-level wake deadline"
            );
        }
    }

    @Test
    void defaultGracefulDrainWindowAdaptsToRecentRtt() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, constrainedPeerSettings());
        SessionRuntimeTestSupport.setLongField(runtime, "lastPingRttNanos", TimeUnit.MILLISECONDS.toNanos(800L));

        synchronized (runtime.lock()) {
            assertEquals(
                    Duration.ofMillis(1600L),
                    runtime.stopSendingGracefulDrainWindowLocked(),
                    "default STOP_SENDING graceful drain window should follow Go's 2x RTT adaptive floor"
            );
        }
    }
}
