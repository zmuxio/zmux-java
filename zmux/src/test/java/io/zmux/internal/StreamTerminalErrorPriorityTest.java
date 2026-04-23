package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

final class StreamTerminalErrorPriorityTest {
    @Test
    void peerResetPreservesStructuredRemoteResetErrorInOperationError() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            stream.resetFromPeerLocked(ErrorCode.CANCELLED.code(), "peer reset", 0L);

            ApplicationError error = assertInstanceOf(
                    ApplicationError.class,
                    stream.operationErrorLocked(),
                    "peer RESET should surface the structured remote reset error instead of a generic local fallback"
            );
            assertEquals(ErrorCode.CANCELLED.code(), error.code(), "peer RESET code mismatch");
            assertEquals("peer reset", error.reason(), "peer RESET reason mismatch");
            assertEquals(ZmuxErrorSource.REMOTE, error.source(), "peer RESET source mismatch");
            assertEquals(ZmuxErrorDirection.READ, error.direction(), "peer RESET direction mismatch");
            assertEquals(ZmuxTerminationKind.RESET, error.terminationKind(), "peer RESET termination mismatch");
        }
    }

    @Test
    void peerAbortPreservesStructuredRemoteAbortErrorInOperationError() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            stream.abortFromPeerLocked(ErrorCode.CANCELLED.code(), "peer abort", 0L);

            ApplicationError error = assertInstanceOf(
                    ApplicationError.class,
                    stream.operationErrorLocked(),
                    "peer ABORT should surface the structured remote abort error instead of a generic local fallback"
            );
            assertEquals(ErrorCode.CANCELLED.code(), error.code(), "peer ABORT code mismatch");
            assertEquals("peer abort", error.reason(), "peer ABORT reason mismatch");
            assertEquals(ZmuxErrorSource.REMOTE, error.source(), "peer ABORT source mismatch");
            assertEquals(ZmuxErrorDirection.BOTH, error.direction(), "peer ABORT direction mismatch");
            assertEquals(ZmuxTerminationKind.ABORT, error.terminationKind(), "peer ABORT termination mismatch");
        }
    }

    @Test
    void localSendResetWinsOverPeerResetAtSameSeverity() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            stream.resetFromPeerLocked(11L, "peer reset", 0L);
            stream.terminalStateInternal().recordLocalWriteReset(17L);
            stream.halfStateInternal().markSendReset();

            ApplicationError error = assertInstanceOf(
                    ApplicationError.class,
                    stream.operationErrorLocked(),
                    "same-severity terminal errors should prefer the local send reset over the peer receive reset"
            );
            assertEquals(17L, error.code(), "local send-reset code mismatch");
            assertEquals(ZmuxErrorSource.LOCAL, error.source(), "local send-reset source mismatch");
            assertEquals(ZmuxErrorDirection.WRITE, error.direction(), "local send-reset direction mismatch");
            assertEquals(ZmuxTerminationKind.RESET, error.terminationKind(), "local send-reset termination mismatch");
        }
    }

    @Test
    void peerStopSendingSurfacesRemoteWriteClosedInOperationError() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            stream.stopSendingFromPeerLocked(ErrorCode.CANCELLED.code(), "peer stop", 0L);

            WriteClosedException error = assertInstanceOf(
                    WriteClosedException.class,
                    stream.operationErrorLocked(),
                    "peer STOP_SENDING should surface the typed remote write-closed error in operationError"
            );
            assertEquals(ErrorCode.CANCELLED.code(), error.code(), "peer STOP_SENDING code mismatch");
            assertEquals("peer stop", error.reason(), "peer STOP_SENDING reason mismatch");
            assertEquals(ZmuxErrorSource.REMOTE, error.source(), "peer STOP_SENDING source mismatch");
            assertEquals(ZmuxErrorDirection.WRITE, error.direction(), "peer STOP_SENDING direction mismatch");
            assertEquals(ZmuxTerminationKind.STOPPED, error.terminationKind(), "peer STOP_SENDING termination mismatch");
        }
    }

    @Test
    void queuedGracefulSendFinBeatsEarlierPeerStopInOperationError() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            stream.stopSendingFromPeerLocked(ErrorCode.CANCELLED.code(), "peer stop", 0L);
            stream.markFinQueuedLocked();

            WriteClosedException error = assertInstanceOf(
                    WriteClosedException.class,
                    stream.operationErrorLocked(),
                    "once send FIN is queued, operationError should switch from remote STOPPED to local GRACEFUL"
            );
            assertEquals(ZmuxErrorSource.LOCAL, error.source(), "queued graceful finish source mismatch");
            assertEquals(ZmuxErrorDirection.WRITE, error.direction(), "queued graceful finish direction mismatch");
            assertEquals(ZmuxTerminationKind.GRACEFUL, error.terminationKind(), "queued graceful finish termination mismatch");
        }
    }

    @Test
    void peerFinSurfacesRemoteReadClosedInOperationError() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            stream.finishReceiveLocked();

            ReadClosedException error = assertInstanceOf(
                    ReadClosedException.class,
                    stream.operationErrorLocked(),
                    "peer FIN should surface a typed remote read-closed error in operationError"
            );
            assertEquals(ZmuxErrorSource.REMOTE, error.source(), "peer FIN source mismatch");
            assertEquals(ZmuxErrorDirection.READ, error.direction(), "peer FIN direction mismatch");
            assertEquals(ZmuxTerminationKind.GRACEFUL, error.terminationKind(), "peer FIN termination mismatch");
        }
    }
}
