package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

final class StreamTerminalErrorPriorityTest {
    private static void commitLocalStream(SessionRuntime runtime, StreamRuntime stream) throws Exception {
        synchronized (runtime.lock()) {
            runtime.beginLocalOpenLocked(stream);
            runtime.markLocalStreamOpeningCommittedLocked(stream);
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "markPeerVisibleLocked",
                    new Class<?>[]{StreamRuntime.class},
                    stream
            );
        }
    }

    private static void awaitWaiter(SessionRuntime runtime, SessionRuntime.LockWaitKind kind) throws Exception {
        Field waiterField = SessionRuntime.class.getDeclaredField("lockWaitersByKind");
        waiterField.setAccessible(true);
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
        while (System.nanoTime() < deadlineNanos) {
            synchronized (runtime.lock()) {
                int[] waiters = (int[]) waiterField.get(runtime);
                if (waiters[kind.ordinal()] > 0) {
                    return;
                }
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("timed out waiting for " + kind + " waiter");
    }

    private static void assertSessionCloseHalfError(
            ApplicationError error,
            long expectedCode,
            String expectedReason,
            ZmuxErrorDirection expectedDirection
    ) {
        assertEquals(expectedCode, error.code(), "session-close stream error code mismatch");
        assertEquals(expectedReason, error.reason(), "session-close stream error reason mismatch");
        assertEquals(ZmuxErrorScope.STREAM, error.scope(), "session-close stream error scope mismatch");
        assertEquals(ZmuxErrorSource.LOCAL, error.source(), "session-close stream error source mismatch");
        assertEquals(expectedDirection, error.direction(), "session-close stream error direction mismatch");
        assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, error.terminationKind(), "session-close stream termination mismatch");
    }

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
    void peerResetWakesBlockedReadWaiter() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        AtomicReference<Throwable> readFailure = new AtomicReference<>();
        Thread reader = new Thread(() -> {
            try {
                stream.read(new byte[1]);
            } catch (Throwable failure) {
                readFailure.set(failure);
            }
        }, "peer-reset-read-waiter");

        reader.start();
        try {
            awaitWaiter(runtime, SessionRuntime.LockWaitKind.READ_STREAM);
            synchronized (runtime.lock()) {
                stream.resetFromPeerLocked(ErrorCode.CANCELLED.code(), "peer reset", 0L);
                runtime.notifyLockWaitersLocked();
            }
            reader.join(TimeUnit.SECONDS.toMillis(1L));

            assertFalse(reader.isAlive(), "peer RESET should wake a blocked stream reader");
            ApplicationError error = assertInstanceOf(
                    ApplicationError.class,
                    readFailure.get(),
                    "blocked read should surface the peer RESET error"
            );
            assertEquals(ErrorCode.CANCELLED.code(), error.code(), "peer RESET code mismatch");
            assertEquals(ZmuxErrorSource.REMOTE, error.source(), "peer RESET source mismatch");
            assertEquals(ZmuxTerminationKind.RESET, error.terminationKind(), "peer RESET termination mismatch");
        } finally {
            if (reader.isAlive()) {
                reader.interrupt();
                reader.join(TimeUnit.SECONDS.toMillis(1L));
            }
        }
        assertFalse(reader.isAlive(), "peer RESET read waiter leaked");
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

    @Test
    void fatalSessionClosePreservesErrorCodeOnLiveAndProvisionalStreams() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime live = (StreamRuntime) runtime.openStream();
        commitLocalStream(runtime, live);
        StreamRuntime provisional = (StreamRuntime) runtime.openStream();

        runtime.closeWithError(ErrorCode.FRAME_SIZE.code(), "payload too large");

        synchronized (runtime.lock()) {
            assertSessionCloseHalfError(
                    assertInstanceOf(ApplicationError.class, live.terminalStateInternal().sendCloseError()),
                    ErrorCode.FRAME_SIZE.code(),
                    "payload too large",
                    ZmuxErrorDirection.WRITE
            );
            assertSessionCloseHalfError(
                    assertInstanceOf(ApplicationError.class, live.terminalStateInternal().recvCloseError()),
                    ErrorCode.FRAME_SIZE.code(),
                    "payload too large",
                    ZmuxErrorDirection.READ
            );
            assertSessionCloseHalfError(
                    assertInstanceOf(ApplicationError.class, provisional.terminalStateInternal().sendCloseError()),
                    ErrorCode.FRAME_SIZE.code(),
                    "payload too large",
                    ZmuxErrorDirection.WRITE
            );
            assertSessionCloseHalfError(
                    assertInstanceOf(ApplicationError.class, provisional.terminalStateInternal().recvCloseError()),
                    ErrorCode.FRAME_SIZE.code(),
                    "payload too large",
                    ZmuxErrorDirection.READ
            );
        }
    }

    @Test
    void keepaliveTimeoutPreservesErrorCodeAndReasonOnLiveAndProvisionalStreams() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .keepaliveInterval(java.time.Duration.ofMillis(10))
                .keepaliveTimeout(java.time.Duration.ofMillis(1))
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());
        StreamRuntime live = (StreamRuntime) runtime.openStream();
        commitLocalStream(runtime, live);
        StreamRuntime provisional = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setField(
                    runtime,
                    "activePing",
                    SessionRuntimeTestSupport.newPendingPing(
                            System.nanoTime() - java.time.Duration.ofSeconds(1).toNanos(),
                            new byte[]{1}
                    )
            );
        }

        runtime.closeForKeepaliveTimeout();

        synchronized (runtime.lock()) {
            assertSessionCloseHalfError(
                    assertInstanceOf(ApplicationError.class, live.terminalStateInternal().sendCloseError()),
                    SessionTelemetryState.keepaliveTimeoutCode(),
                    SessionTelemetryState.keepaliveTimeoutReason(),
                    ZmuxErrorDirection.WRITE
            );
            assertSessionCloseHalfError(
                    assertInstanceOf(ApplicationError.class, live.terminalStateInternal().recvCloseError()),
                    SessionTelemetryState.keepaliveTimeoutCode(),
                    SessionTelemetryState.keepaliveTimeoutReason(),
                    ZmuxErrorDirection.READ
            );
            assertSessionCloseHalfError(
                    assertInstanceOf(ApplicationError.class, provisional.terminalStateInternal().sendCloseError()),
                    SessionTelemetryState.keepaliveTimeoutCode(),
                    SessionTelemetryState.keepaliveTimeoutReason(),
                    ZmuxErrorDirection.WRITE
            );
            assertSessionCloseHalfError(
                    assertInstanceOf(ApplicationError.class, provisional.terminalStateInternal().recvCloseError()),
                    SessionTelemetryState.keepaliveTimeoutCode(),
                    SessionTelemetryState.keepaliveTimeoutReason(),
                    ZmuxErrorDirection.READ
            );
        }
    }
}
