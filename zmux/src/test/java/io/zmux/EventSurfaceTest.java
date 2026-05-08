package io.zmux;

import io.zmux.protocol.Protocol;
import io.zmux.transport.BasicDuplexConnection;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class EventSurfaceTest {
    private static ZmuxEvent awaitEventType(BlockingQueue<ZmuxEvent> events, ZmuxEventType type) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (true) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                throw new AssertionError("timed out waiting for event type " + type);
            }
            ZmuxEvent event = events.poll(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos)), TimeUnit.MILLISECONDS);
            if (event != null && event.type() == type) {
                return event;
            }
        }
    }

    private static ZmuxConfig defaultConfig() {
        return ZmuxConfig.builder().build();
    }

    private static void rethrow(Throwable error) {
        if (error == null) {
            return;
        }
        if (error instanceof RuntimeException) {
            throw (RuntimeException) error;
        }
        throw new RuntimeException(error);
    }

    @Test
    void streamOpenedAndAcceptedEventsExposeExpectedFields() throws Exception {
        BlockingQueue<ZmuxEvent> clientEvents = new LinkedBlockingQueue<>();
        BlockingQueue<ZmuxEvent> serverEvents = new LinkedBlockingQueue<>();
        AtomicReference<ZmuxNativeSession> clientRef = new AtomicReference<>();
        AtomicReference<ZmuxNativeSession> serverRef = new AtomicReference<>();

        ZmuxConfig clientConfig = ZmuxConfig.builder()
                .capabilities(Protocol.CAPABILITY_OPEN_METADATA)
                .eventHandler(event -> {
                    ZmuxNativeSession session = clientRef.get();
                    if (session != null) {
                        session.stats();
                    }
                    if (event.stream() != null) {
                        event.stream().metadata();
                        event.stream().streamId();
                    }
                    clientEvents.offer(event);
                })
                .build();
        ZmuxConfig serverConfig = ZmuxConfig.builder()
                .capabilities(Protocol.CAPABILITY_OPEN_METADATA)
                .eventHandler(event -> {
                    ZmuxNativeSession session = serverRef.get();
                    if (session != null) {
                        session.stats();
                    }
                    if (event.stream() != null) {
                        event.stream().metadata();
                        event.stream().streamId();
                    }
                    serverEvents.offer(event);
                })
                .build();

        try (SessionPair pair = SessionPair.open(clientConfig, serverConfig)) {
            clientRef.set(pair.client());
            serverRef.set(pair.server());

            byte[] openInfo = "tag".getBytes(StandardCharsets.UTF_8);
            ZmuxStream stream = pair.client().openStream(new OpenOptions(7L, 9L, openInfo));
            stream.write("x".getBytes(StandardCharsets.UTF_8));

            ZmuxEvent openedEvent = awaitEventType(clientEvents, ZmuxEventType.STREAM_OPENED);
            ZmuxStream accepted = pair.server().acceptStream(Duration.ofSeconds(1));
            ZmuxEvent acceptedEvent = awaitEventType(serverEvents, ZmuxEventType.STREAM_ACCEPTED);

            assertEquals(stream.streamId(), openedEvent.streamId(), "opened event stream ID mismatch");
            assertEquals(SessionState.READY, openedEvent.sessionState(), "opened event session state mismatch");
            assertTrue(openedEvent.local(), "opened event should describe a local stream");
            assertTrue(openedEvent.bidirectional(), "opened event should describe a bidi stream");
            assertFalse(openedEvent.applicationVisible(), "opened event should fire before application visibility");
            assertNull(openedEvent.error(), "opened event should not carry an error");
            assertNull(openedEvent.errorDetails(), "opened event should not expose error details");
            assertFalse(openedEvent.errorHasCode(), "opened event should not expose an error code");
            assertEquals(-1L, openedEvent.errorCode(-1L), "opened event error-code fallback mismatch");
            assertEquals("", openedEvent.errorReason(), "opened event error reason mismatch");
            assertEquals(ZmuxErrorScope.UNKNOWN, openedEvent.errorScope(), "opened event error scope mismatch");
            assertEquals(ZmuxErrorSource.UNKNOWN, openedEvent.errorSource(), "opened event error source mismatch");
            assertEquals(ZmuxErrorDirection.UNKNOWN, openedEvent.errorDirection(), "opened event error direction mismatch");
            assertEquals(ZmuxTerminationKind.UNKNOWN, openedEvent.errorTerminationKind(), "opened event termination mismatch");
            assertFalse(openedEvent.errorTimeout(), "opened event should not be a timeout");
            assertFalse(openedEvent.errorInterrupted(), "opened event should not be interrupted");
            assertNotNull(openedEvent.time(), "opened event should carry a timestamp");
            assertNotNull(openedEvent.stream(), "opened event should expose a stream view");
            assertEquals(stream.streamId(), openedEvent.stream().streamId(), "opened event stream view should match the opened stream");
            assertEquals(9L, openedEvent.stream().metadata().group(), "opened event metadata should be queryable");
            assertArrayEquals(openInfo, openedEvent.stream().openInfo(), "opened event open_info mismatch");

            assertEquals(accepted.streamId(), acceptedEvent.streamId(), "accepted event stream ID mismatch");
            assertEquals(SessionState.READY, acceptedEvent.sessionState(), "accepted event session state mismatch");
            assertFalse(acceptedEvent.local(), "accepted event should describe a peer-opened stream");
            assertTrue(acceptedEvent.bidirectional(), "accepted event should describe a bidi stream");
            assertTrue(acceptedEvent.applicationVisible(), "accepted event should fire after application visibility");
            assertNull(acceptedEvent.error(), "accepted event should not carry an error");
            assertNull(acceptedEvent.errorDetails(), "accepted event should not expose error details");
            assertFalse(acceptedEvent.errorHasCode(), "accepted event should not expose an error code");
            assertNotNull(acceptedEvent.time(), "accepted event should carry a timestamp");
            assertNotNull(acceptedEvent.stream(), "accepted event should expose a stream view");
            assertEquals(accepted.streamId(), acceptedEvent.stream().streamId(), "accepted event stream view should match the accepted stream");
            assertArrayEquals(openInfo, acceptedEvent.stream().openInfo(), "accepted event open_info mismatch");
        }
    }

    @Test
    void closeWriteEmitsStreamOpenedEvent() throws Exception {
        BlockingQueue<ZmuxEvent> clientEvents = new LinkedBlockingQueue<>();
        ZmuxConfig clientConfig = ZmuxConfig.builder()
                .eventHandler(clientEvents::offer)
                .build();

        try (SessionPair pair = SessionPair.open(clientConfig, defaultConfig())) {
            ZmuxStream stream = pair.client().openStream();
            stream.closeWrite();

            ZmuxEvent openedEvent = awaitEventType(clientEvents, ZmuxEventType.STREAM_OPENED);
            assertTrue(openedEvent.streamId() != 0L, "closeWrite should commit a non-zero stream ID");
            assertEquals(stream.streamId(), openedEvent.streamId(), "closeWrite opened event stream ID mismatch");
            assertTrue(openedEvent.local(), "closeWrite event should describe a local stream");

            ZmuxStream accepted = pair.server().acceptStream(Duration.ofSeconds(1));
            assertEquals(stream.streamId(), accepted.streamId(), "peer should accept the same committed stream ID");
        }
    }

    @Test
    void closeReadEmitsStreamOpenedEvent() throws Exception {
        BlockingQueue<ZmuxEvent> clientEvents = new LinkedBlockingQueue<>();
        ZmuxConfig clientConfig = ZmuxConfig.builder()
                .eventHandler(clientEvents::offer)
                .build();

        try (SessionPair pair = SessionPair.open(clientConfig, defaultConfig())) {
            ZmuxStream stream = pair.client().openStream();
            stream.closeRead();

            ZmuxEvent openedEvent = awaitEventType(clientEvents, ZmuxEventType.STREAM_OPENED);
            assertTrue(openedEvent.streamId() != 0L, "closeRead should commit a non-zero stream ID");
            assertEquals(stream.streamId(), openedEvent.streamId(), "closeRead opened event stream ID mismatch");
            assertTrue(openedEvent.local(), "closeRead event should describe a local stream");
            assertFalse(openedEvent.applicationVisible(), "closeRead opened event should still be pre-accept");
        }
    }

    @Test
    void sessionClosedEventCarriesTerminalErrorAndAllowsReentrantClose() throws Exception {
        BlockingQueue<ZmuxEvent> closeEvents = new LinkedBlockingQueue<>();
        CountDownLatch handlerDone = new CountDownLatch(1);
        AtomicReference<ZmuxNativeSession> clientRef = new AtomicReference<>();
        AtomicReference<Throwable> handlerError = new AtomicReference<>();

        ZmuxConfig clientConfig = ZmuxConfig.builder()
                .eventHandler(event -> {
                    if (event.type() != ZmuxEventType.SESSION_CLOSED) {
                        return;
                    }
                    try {
                        ZmuxNativeSession session = clientRef.get();
                        if (session != null) {
                            session.close();
                        }
                        closeEvents.offer(event);
                    } catch (Throwable error) {
                        handlerError.compareAndSet(null, error);
                    } finally {
                        handlerDone.countDown();
                    }
                })
                .build();

        try (SessionPair pair = SessionPair.open(clientConfig, defaultConfig())) {
            clientRef.set(pair.client());

            AtomicReference<Throwable> closeError = new AtomicReference<>();
            CountDownLatch closeDone = new CountDownLatch(1);
            Thread closeThread = new Thread(() -> {
                try {
                    pair.client().closeWithError(ErrorCode.INTERNAL.code(), "close test");
                } catch (Throwable error) {
                    closeError.set(error);
                } finally {
                    closeDone.countDown();
                }
            }, "event-surface-close");
            closeThread.start();

            ZmuxEvent closedEvent = awaitEventType(closeEvents, ZmuxEventType.SESSION_CLOSED);
            assertTrue(handlerDone.await(2, TimeUnit.SECONDS), "session_closed handler should finish");
            assertTrue(closeDone.await(2, TimeUnit.SECONDS), "closeWithError should not deadlock while handler reenters close");
            assertNull(handlerError.get(), "reentrant session_closed handler should not fail");
            assertNull(closeError.get(), "closeWithError should complete without surfacing an error");

            assertEquals(SessionState.FAILED, closedEvent.sessionState(), "session_closed state mismatch");
            ApplicationError error = assertInstanceOf(ApplicationError.class, closedEvent.error());
            assertEquals(ErrorCode.INTERNAL.code(), error.code(), "session_closed application error code mismatch");
            assertEquals("close test", error.reason(), "session_closed reason mismatch");
            assertEquals(error, closedEvent.errorDetails(), "session_closed structured details mismatch");
            assertTrue(closedEvent.errorHasCode(), "session_closed should expose an error code");
            assertEquals(ErrorCode.INTERNAL.code(), closedEvent.errorCode(-1L), "session_closed event error code mismatch");
            assertEquals("", closedEvent.errorOperation(), "session_closed operation mismatch");
            assertEquals("close test", closedEvent.errorReason(), "session_closed structured reason mismatch");
            assertEquals(ZmuxErrorScope.SESSION, closedEvent.errorScope(), "session_closed scope mismatch");
            assertEquals(ZmuxErrorSource.LOCAL, closedEvent.errorSource(), "session_closed source mismatch");
            assertEquals(ZmuxErrorDirection.BOTH, closedEvent.errorDirection(), "session_closed direction mismatch");
            assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, closedEvent.errorTerminationKind(), "session_closed termination mismatch");
            assertFalse(closedEvent.errorTimeout(), "session_closed should not be a timeout");
            assertFalse(closedEvent.errorInterrupted(), "session_closed should not be interrupted");
        }
    }

    @Test
    void eventHandlersMayReenterSessionAndStreamQueryApis() throws Exception {
        CountDownLatch streamOpenedDone = new CountDownLatch(1);
        CountDownLatch sessionClosedDone = new CountDownLatch(1);
        AtomicReference<ZmuxNativeSession> clientRef = new AtomicReference<>();
        AtomicReference<Throwable> handlerError = new AtomicReference<>();

        ZmuxConfig clientConfig = ZmuxConfig.builder()
                .eventHandler(event -> {
                    try {
                        ZmuxNativeSession session = clientRef.get();
                        switch (event.type()) {
                            case STREAM_OPENED:
                                if (session != null) {
                                    session.stats();
                                }
                                assertNotNull(event.stream(), "stream_opened should expose a stream");
                                event.stream().metadata();
                                event.stream().streamId();
                                streamOpenedDone.countDown();
                                break;
                            case SESSION_CLOSED:
                                if (session != null) {
                                    session.stats();
                                    assertNull(session.peerCloseError(), "local close should not expose a peer close error");
                                }
                                sessionClosedDone.countDown();
                                break;
                            default:
                                return;
                        }
                    } catch (Throwable error) {
                        handlerError.compareAndSet(null, error);
                    }
                })
                .build();

        try (SessionPair pair = SessionPair.open(clientConfig, defaultConfig())) {
            clientRef.set(pair.client());

            ZmuxStream stream = pair.client().openStream();
            stream.write("x".getBytes(StandardCharsets.UTF_8));

            assertTrue(streamOpenedDone.await(2, TimeUnit.SECONDS), "stream_opened handler should finish");
            assertNull(handlerError.get(), "reentrant event-surface queries should not fail");

            pair.client().closeWithError(ErrorCode.INTERNAL.code(), "reenter");

            assertTrue(sessionClosedDone.await(2, TimeUnit.SECONDS), "session_closed handler should finish after local closeWithError");
            assertNull(handlerError.get(), "reentrant session_closed queries should not fail");
        }
    }

    private static final class SessionPair implements AutoCloseable {
        private final ZmuxNativeSession client;
        private final ZmuxNativeSession server;

        private SessionPair(ZmuxNativeSession client, ZmuxNativeSession server) {
            this.client = client;
            this.server = server;
        }

        static SessionPair open(ZmuxConfig clientConfig, ZmuxConfig serverConfig) throws Exception {
            ServerSocket listener = new ServerSocket(0);
            Socket leftSocket = new Socket("127.0.0.1", listener.getLocalPort());
            Socket rightSocket = listener.accept();
            listener.close();

            BasicDuplexConnection leftConn = new BasicDuplexConnection(
                    leftSocket.getInputStream(),
                    leftSocket.getOutputStream(),
                    leftSocket,
                    leftSocket.getLocalSocketAddress(),
                    leftSocket.getRemoteSocketAddress()
            );
            BasicDuplexConnection rightConn = new BasicDuplexConnection(
                    rightSocket.getInputStream(),
                    rightSocket.getOutputStream(),
                    rightSocket,
                    rightSocket.getLocalSocketAddress(),
                    rightSocket.getRemoteSocketAddress()
            );

            AtomicReference<ZmuxNativeSession> clientRef = new AtomicReference<>();
            AtomicReference<ZmuxNativeSession> serverRef = new AtomicReference<>();
            AtomicReference<Throwable> errorRef = new AtomicReference<>();
            CountDownLatch established = new CountDownLatch(2);

            Thread clientThread = new Thread(() -> {
                try {
                    clientRef.set(Zmux.client(leftConn, clientConfig));
                } catch (Throwable error) {
                    errorRef.compareAndSet(null, error);
                } finally {
                    established.countDown();
                }
            }, "event-surface-client-open");
            Thread serverThread = new Thread(() -> {
                try {
                    serverRef.set(Zmux.server(rightConn, serverConfig));
                } catch (Throwable error) {
                    errorRef.compareAndSet(null, error);
                } finally {
                    established.countDown();
                }
            }, "event-surface-server-open");
            clientThread.start();
            serverThread.start();
            established.await();
            rethrow(errorRef.get());
            return new SessionPair(clientRef.get(), serverRef.get());
        }

        ZmuxNativeSession client() {
            return client;
        }

        ZmuxNativeSession server() {
            return server;
        }

        @Override
        public void close() throws Exception {
            IOException error = null;
            try {
                client.closeWithError(ErrorCode.NO_ERROR.code(), "");
            } catch (IOException e) {
                error = e;
            }
            try {
                server.closeWithError(ErrorCode.NO_ERROR.code(), "");
            } catch (IOException e) {
                if (error == null) {
                    error = e;
                }
            }
            if (!client.awaitTermination(Duration.ofSeconds(2))) {
                throw new AssertionError("timed out terminating client session");
            }
            if (!server.awaitTermination(Duration.ofSeconds(2))) {
                throw new AssertionError("timed out terminating server session");
            }
            if (error != null) {
                throw error;
            }
        }
    }
}
