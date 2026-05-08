package io.zmux;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Deque;

import static org.junit.jupiter.api.Assertions.*;

final class CloseReadRuntimeTest {
    @Test
    void closeReadKeepsLocalReadStopWhenOpeningMetadataValidationFails() throws Exception {
        Settings peerSettings = Settings.defaults().toBuilder()
                .maxFramePayload(16L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(Protocol.CAPABILITY_OPEN_METADATA, peerSettings);
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "beginLocalOpenLocked",
                    new Class<?>[]{StreamRuntime.class},
                    stream
            );
            stream.applyOpenMetadataLocked(0L, null, "oversized-open-info".getBytes(StandardCharsets.UTF_8));
        }

        OpenMetadataTooLargeException error = assertInstanceOf(
                OpenMetadataTooLargeException.class,
                assertThrows(IOException.class, stream::closeRead, "oversized opener metadata should fail local CloseRead"),
                "CloseRead open-metadata validation failure should surface a protocol-coded error"
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "CloseRead open-metadata failure code mismatch");
        assertEquals(OpenMetadataTooLargeException.MESSAGE, error.getMessage(), "CloseRead open-metadata failure mismatch");

        synchronized (runtime.lock()) {
            assertTrue(stream.readClosed(), "CloseRead should still close the local read side after opener validation failure");
            assertTrue(stream.recvStoppedOrTerminal(), "CloseRead should retain the local STOP_SENT state");
            assertFalse(stream.openedOnWire(), "failed opener validation must not commit local stream visibility");
            assertFalse(stream.peerVisible(), "failed opener validation must not mark the stream peer-visible");
            assertFalse(stream.openingFramePendingLocked(), "failed opener validation must not leave an opening frame pending");
            assertTrue(SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue").isEmpty(), "CloseRead failure must not queue urgent frames");
            assertTrue(SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue").isEmpty(), "CloseRead failure must not queue DATA frames");
        }
    }

    @Test
    void closeReadRetryAfterOpeningMetadataCorrectionQueuesStopSending() throws Exception {
        Settings peerSettings = Settings.defaults().toBuilder()
                .maxFramePayload(16L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(Protocol.CAPABILITY_OPEN_METADATA, peerSettings);
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "beginLocalOpenLocked",
                    new Class<?>[]{StreamRuntime.class},
                    stream
            );
            stream.applyOpenMetadataLocked(0L, null, "oversized-open-info".getBytes(StandardCharsets.UTF_8));
        }

        assertThrows(IOException.class, stream::closeRead, "oversized opener metadata should fail before STOP_SENDING is queued");
        synchronized (runtime.lock()) {
            assertTrue(stream.halfStateInternal().localReadSignalPending(), "failed CloseRead should retain a pending STOP_SENDING signal");
            stream.applyOpenMetadataLocked(0L, null, new byte[0]);
        }

        stream.closeRead();

        synchronized (runtime.lock()) {
            assertFalse(stream.halfStateInternal().localReadSignalPending(), "successful retry should clear the pending read-stop signal");
            Deque<Object> urgent = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
            assertEquals(2, urgent.size(), "retry should queue opener and STOP_SENDING");
            Object[] queued = urgent.toArray();
            Object opener = queued[0];
            Object stopSending = queued[1];
            assertEquals(FrameType.DATA, SessionRuntimeTestSupport.outboundFrame(opener).type(), "retry should first queue the control opener");
            assertTrue(SessionRuntimeTestSupport.outboundOpeningFrame(opener), "control opener should retain opening-frame identity");
            assertEquals(FrameType.STOP_SENDING, SessionRuntimeTestSupport.outboundFrame(stopSending).type(), "retry should queue STOP_SENDING after opener");
            assertEquals(stream.streamIdInternal(), SessionRuntimeTestSupport.outboundFrame(stopSending).streamId(), "STOP_SENDING stream id mismatch");
        }
        assertInstanceOf(
                ReadClosedException.class,
                assertThrows(IOException.class, stream::closeRead),
                "CloseRead after successful STOP_SENDING queueing should return the stable read-closed surface"
        );
    }

    @Test
    void closeReadPreservesStoppedReadSemanticsAfterPeerFin() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        stream.closeRead();

        synchronized (runtime.lock()) {
            stream.finishReceiveLocked();

            ReadClosedException readClosed = assertInstanceOf(
                    ReadClosedException.class,
                    assertThrows(IOException.class, () -> stream.read(new byte[1]), "read after local CloseRead + peer FIN should remain read-closed"),
                    "local read-stop should keep the read side in stopped state after peer FIN"
            );
            assertEquals(ZmuxErrorSource.LOCAL, readClosed.source(), "read-after-FIN source should remain local");
            assertEquals(ZmuxTerminationKind.STOPPED, readClosed.terminationKind(), "read-after-FIN termination kind should remain stopped");
            assertFalse(stream.receiveGracefulLocked(), "local CloseRead should prevent recv tombstones from becoming graceful after peer FIN");
            assertEquals(StreamRuntime.PeerDataAction.IGNORE, stream.peerDataActionLocked(false), "late DATA after CloseRead + peer FIN should remain on the discard path");
            assertEquals(StreamRuntime.PeerDataAction.IGNORE_AND_FIN, stream.peerDataActionLocked(true), "late DATA|FIN after CloseRead + peer FIN should keep STOP_SENT semantics");

            ApplicationError operationError = assertInstanceOf(
                    ApplicationError.class,
                    stream.operationErrorLocked(),
                    "operation error after CloseRead + peer FIN should preserve STOPPED identity"
            );
            assertEquals(ZmuxErrorSource.LOCAL, operationError.source(), "operation error source should remain local after CloseRead + peer FIN");
            assertEquals(ZmuxTerminationKind.STOPPED, operationError.terminationKind(), "operation error kind should remain stopped after CloseRead + peer FIN");
            assertEquals(ErrorCode.CANCELLED.code(), operationError.code(), "operation error code should retain the local CloseRead code");
        }
    }

    @Test
    void closeReadKeepsReadClosedSurfaceAfterPeerReset() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        stream.closeRead();

        synchronized (runtime.lock()) {
            stream.resetFromPeerLocked(ErrorCode.CANCELLED.code(), "peer reset", 0L);

            ReadClosedException readClosed = assertInstanceOf(
                    ReadClosedException.class,
                    assertThrows(IOException.class, () -> stream.read(new byte[1]), "read after local CloseRead + peer RESET should still surface read-closed"),
                    "local read-stop should dominate later peer RESET on the read surface"
            );
            assertEquals(ZmuxErrorSource.LOCAL, readClosed.source(), "read-after-RESET source should remain local");
            assertEquals(ZmuxTerminationKind.STOPPED, readClosed.terminationKind(), "read-after-RESET termination kind should remain stopped");

            ApplicationError operationError = assertInstanceOf(
                    ApplicationError.class,
                    stream.operationErrorLocked(),
                    "operation error after CloseRead + peer RESET should preserve STOPPED identity"
            );
            assertEquals(ZmuxErrorSource.LOCAL, operationError.source(), "operation error source should remain local after CloseRead + peer RESET");
            assertEquals(ZmuxTerminationKind.STOPPED, operationError.terminationKind(), "operation error kind should remain stopped after CloseRead + peer RESET");
            assertEquals(ErrorCode.CANCELLED.code(), operationError.code(), "operation error code should retain the local CloseRead code after peer RESET");
        }
    }

    @Test
    void closeReadKeepsReadClosedSurfaceAfterLocalAbort() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        stream.closeRead();

        synchronized (runtime.lock()) {
            stream.abortFromLocalLocked(99L, "local abort");

            ReadClosedException readClosed = assertInstanceOf(
                    ReadClosedException.class,
                    assertThrows(IOException.class, () -> stream.read(new byte[1]), "read after local CloseRead + local ABORT should still surface read-closed"),
                    "local read-stop should dominate later local abort on the read surface"
            );
            assertEquals(ZmuxErrorSource.LOCAL, readClosed.source(), "read-after-local-ABORT source should remain local");
            assertEquals(ZmuxTerminationKind.STOPPED, readClosed.terminationKind(), "read-after-local-ABORT termination kind should remain stopped");

            ApplicationError operationError = assertInstanceOf(
                    ApplicationError.class,
                    stream.operationErrorLocked(),
                    "operation error should still expose the stream abort for non-read surfaces"
            );
            assertEquals(99L, operationError.code(), "operation error should retain the local abort code");
            assertEquals(ZmuxTerminationKind.ABORT, operationError.terminationKind(), "operation error should retain abort termination");
        }
    }
}
