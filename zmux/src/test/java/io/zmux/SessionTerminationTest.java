package io.zmux;

import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;
import io.zmux.protocol.Preface;
import io.zmux.protocol.Protocol;
import io.zmux.transport.BasicDuplexConnection;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class SessionTerminationTest {
    private static FrameCodec.Frame closeFrame(long code, String reason) throws IOException {
        return new FrameCodec.Frame(
                FrameType.CLOSE,
                0,
                0L,
                FrameCodec.buildErrorPayload(code, reason, Settings.defaults().maxControlPayloadBytes())
        );
    }

    private static ZmuxConfig defaultConfig() {
        return ZmuxConfig.builder().build();
    }

    private static void rethrow(Throwable error) throws Exception {
        if (error == null) {
            return;
        }
        if (error instanceof Exception) {
            throw (Exception) error;
        }
        throw new RuntimeException(error);
    }

    @Test
    void localCloseReadFailsSubsequentReadsWithTypedError() throws Exception {
        try (SessionPair pair = SessionPair.open(defaultConfig(), defaultConfig())) {
            ZmuxStream clientStream = pair.client().openStream();
            clientStream.write("x".getBytes(StandardCharsets.UTF_8));

            ZmuxStream accepted = pair.server().acceptStream(Duration.ofSeconds(1));
            assertEquals(1, accepted.read(new byte[1]), "accepted stream should consume the first byte before local read-stop");

            accepted.closeRead();

            ReadClosedException error = assertThrows(ReadClosedException.class, () -> accepted.read(new byte[1]));
            assertEquals(ReadClosedException.MESSAGE, error.getMessage(), "local CloseRead should surface the typed local read-closed error");

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void peerNoErrorCloseSurfacesTypedSessionClosedError() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(defaultConfig(), 0L)) {
            peer.send(closeFrame(ErrorCode.NO_ERROR.code(), ""));
            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "peer close should terminate the session");

            SessionClosedException error = assertThrows(SessionClosedException.class, () -> peer.session().openStream());
            assertEquals(SessionClosedException.MESSAGE, error.getMessage(), "no-error peer close should surface the typed session-closed error");
            assertEquals("open", error.operation(), "peer no-error close should preserve the failed open operation");
        }
    }

    @Test
    void peerCloseDiscardsBufferedDataOnAcceptedStream() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(defaultConfig(), 0L)) {
            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    0,
                    4L,
                    "hello".getBytes(StandardCharsets.UTF_8)
            ));

            ZmuxStream accepted = peer.session().acceptStream(Duration.ofSeconds(1));

            peer.send(closeFrame(ErrorCode.PROTOCOL.code(), "protocol"));
            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "peer close should terminate the session");

            IOException error = assertThrows(IOException.class, () -> accepted.read(new byte[8]));
            ApplicationError appError = assertInstanceOf(ApplicationError.class, error);
            assertEquals(ErrorCode.PROTOCOL.code(), appError.code(), "buffered read after peer close should surface the session error");
            assertEquals("protocol", appError.reason(), "peer close reason mismatch");
        }
    }

    @Test
    void peerNoErrorCloseFailsUnfinishedReceiveHalvesInsteadOfEof() throws Exception {
        // SPEC 6.10: CLOSE fails all remaining open streams whatever its code; only a real peer FIN reads as EOF.
        try (RawPeerSession peer = RawPeerSession.open(defaultConfig(), 0L)) {
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 4L, "partial".getBytes(StandardCharsets.UTF_8)));
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 8L, "unread".getBytes(StandardCharsets.UTF_8)));
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 12L, new byte[0]));
            ZmuxStream drained = peer.session().acceptStream(Duration.ofSeconds(1));
            ZmuxStream buffered = peer.session().acceptStream(Duration.ofSeconds(1));
            ZmuxStream blocked = peer.session().acceptStream(Duration.ofSeconds(1));
            byte[] body = new byte[16];
            assertEquals(7, drained.read(body), "first stream body should be readable before the close");

            AtomicReference<Throwable> blockedResult = new AtomicReference<>();
            CountDownLatch blockedDone = new CountDownLatch(1);
            Thread blockedReader = new Thread(() -> {
                try {
                    blockedResult.set(new AssertionError("blocked read returned " + blocked.read(new byte[8])));
                } catch (Throwable error) {
                    blockedResult.set(error);
                } finally {
                    blockedDone.countDown();
                }
            }, "session-termination-blocked-read");
            blockedReader.start();
            Thread.sleep(50L);

            peer.send(closeFrame(ErrorCode.NO_ERROR.code(), ""));
            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "peer close should terminate the session");

            assertTrue(blockedDone.await(1, java.util.concurrent.TimeUnit.SECONDS), "blocked read should be woken by the close");
            assertInstanceOf(SessionClosedException.class, blockedResult.get(), "a read blocked at CLOSE(NO_ERROR) must fail, not see EOF");
            SessionClosedException later = assertThrows(SessionClosedException.class, () -> drained.read(body));
            assertEquals("read", later.operation(), "later read should report the read operation");
            assertThrows(
                    SessionClosedException.class,
                    () -> buffered.read(body),
                    "discarded unread data must not be reported as a complete body"
            );
            assertThrows(SessionClosedException.class, () -> drained.write(new byte[]{1}), "writes after the close fail too");
        }
    }

    @Test
    void sessionCloseAfterPeerFinKeepsEofOnlyWhenNothingWasDiscarded() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(defaultConfig(), 0L)) {
            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 4L, "done".getBytes(StandardCharsets.UTF_8)));
            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 8L, "unread".getBytes(StandardCharsets.UTF_8)));
            ZmuxStream complete = peer.session().acceptStream(Duration.ofSeconds(1));
            ZmuxStream unread = peer.session().acceptStream(Duration.ofSeconds(1));
            byte[] body = new byte[16];
            assertEquals(4, complete.read(body), "complete body should be readable");

            peer.send(closeFrame(ErrorCode.NO_ERROR.code(), ""));
            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "peer close should terminate the session");

            assertEquals(-1, complete.read(body), "a peer FIN whose data was fully delivered still reads as EOF");
            assertThrows(
                    SessionClosedException.class,
                    () -> unread.read(body),
                    "SPEC 9.2: EOF only after all buffered data was delivered; discarded data must surface an error"
            );
        }
    }

    @Test
    void localGracefulCloseFailsUnfinishedReceiveHalves() throws Exception {
        try (SessionPair pair = SessionPair.open(defaultConfig(), defaultConfig())) {
            ZmuxStream clientStream = pair.client().openStream();
            clientStream.write("partial".getBytes(StandardCharsets.UTF_8));
            ZmuxStream accepted = pair.server().acceptStream(Duration.ofSeconds(1));
            byte[] body = new byte[16];
            assertEquals(7, accepted.read(body), "body prefix should be readable");

            pair.server().closeWithError(ErrorCode.NO_ERROR.code(), "");
            assertTrue(pair.server().awaitTermination(Duration.ofSeconds(2)), "local close should terminate the session");
            assertThrows(SessionClosedException.class, () -> accepted.read(body), "local close(0) must not turn an unfinished body into EOF");
            assertTrue(pair.client().awaitTermination(Duration.ofSeconds(2)), "peer should see the CLOSE");
            assertThrows(SessionClosedException.class, () -> clientStream.read(body), "peer CLOSE(NO_ERROR) must not read as EOF either");
        }
    }

    @Test
    void peerCloseClearsAcceptedBacklog() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(defaultConfig(), 0L)) {
            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    0,
                    4L,
                    "hello".getBytes(StandardCharsets.UTF_8)
            ));
            peer.send(closeFrame(ErrorCode.PROTOCOL.code(), "protocol"));
            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "peer close should terminate the session");

            IOException error = assertThrows(IOException.class, () -> peer.session().acceptStream(Duration.ofMillis(100)));
            ApplicationError appError = assertInstanceOf(ApplicationError.class, error);
            assertEquals(ErrorCode.PROTOCOL.code(), appError.code(), "accept after peer close should surface the session error");
            assertEquals("accept", appError.operation(), "accept after peer close should preserve the failed accept operation");
            assertEquals("protocol", appError.reason(), "peer close reason mismatch");
        }
    }

    @Test
    void localCloseWithErrorUnblocksBlockedWrite() throws Exception {
        Settings limitedServerSettings = Settings.builder()
                .initialMaxData(1L)
                .initialMaxStreamDataBidiPeerOpened(1L)
                .initialMaxStreamDataBidiLocallyOpened(1L)
                .build();
        // Small receive HWMs disable standing window growth, so the 1-byte windows stay a hard cap even when the
        // blocked writer's BLOCKED reaches the server (a forced replenish may only grow an exhausted window).
        ZmuxConfig serverConfig = ZmuxConfig.builder()
                .settings(limitedServerSettings)
                .perStreamQueuedDataHwm(1L)
                .sessionQueuedDataHwm(1L)
                .build();

        try (SessionPair pair = SessionPair.open(defaultConfig(), serverConfig)) {
            ZmuxStream stream = pair.client().openStream();
            stream.write("x".getBytes(StandardCharsets.UTF_8));

            CountDownLatch started = new CountDownLatch(1);
            AtomicReference<Throwable> writeError = new AtomicReference<>();
            AtomicReference<Boolean> writeReturned = new AtomicReference<>(false);
            Thread blockedWrite = new Thread(() -> {
                started.countDown();
                try {
                    stream.write("y".getBytes(StandardCharsets.UTF_8));
                    writeReturned.set(true);
                } catch (Throwable t) {
                    writeError.set(t);
                }
            }, "session-close-blocked-write");
            blockedWrite.start();

            assertTrue(started.await(Duration.ofSeconds(1).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS), "blocked write did not start");
            Thread.sleep(50L);
            assertFalse(writeReturned.get(), "second write should still be flow-control blocked before session close");

            pair.client().closeWithError(ErrorCode.PROTOCOL.code(), "bye");

            blockedWrite.join(Duration.ofSeconds(1).toMillis());
            assertFalse(blockedWrite.isAlive(), "blocked write should wake when the session closes");
            assertFalse(writeReturned.get(), "blocked write must not succeed after closeWithError");

            Throwable error = writeError.get();
            assertNotNull(error, "blocked write should fail after closeWithError");
            ApplicationError appError = assertInstanceOf(ApplicationError.class, error);
            assertEquals(ErrorCode.PROTOCOL.code(), appError.code(), "blocked write should inherit the local close code");
            assertEquals("bye", appError.reason(), "blocked write should inherit the local close reason");

            assertTrue(pair.client().awaitTermination(Duration.ofSeconds(1)), "client session should terminate after closeWithError");
            assertEquals(SessionState.FAILED, pair.client().state(), "non-zero CLOSE should leave the session in FAILED");
        }
    }

    @Test
    void closeWithNullThrowablePreservesGracefulClosePathWhileUnblockingBlockedWrite() throws Exception {
        Settings limitedServerSettings = Settings.builder()
                .initialMaxData(1L)
                .initialMaxStreamDataBidiPeerOpened(1L)
                .initialMaxStreamDataBidiLocallyOpened(1L)
                .build();
        // Small receive HWMs disable standing window growth, so the 1-byte windows stay a hard cap even when the
        // blocked writer's BLOCKED reaches the server (a forced replenish may only grow an exhausted window).
        ZmuxConfig serverConfig = ZmuxConfig.builder()
                .settings(limitedServerSettings)
                .perStreamQueuedDataHwm(1L)
                .sessionQueuedDataHwm(1L)
                .build();

        try (SessionPair pair = SessionPair.open(defaultConfig(), serverConfig)) {
            ZmuxStream stream = pair.client().openStream();
            stream.write("x".getBytes(StandardCharsets.UTF_8));

            CountDownLatch started = new CountDownLatch(1);
            AtomicReference<Throwable> writeError = new AtomicReference<>();
            AtomicReference<Boolean> writeReturned = new AtomicReference<>(false);
            Thread blockedWrite = new Thread(() -> {
                started.countDown();
                try {
                    stream.write("y".getBytes(StandardCharsets.UTF_8));
                    writeReturned.set(true);
                } catch (Throwable t) {
                    writeError.set(t);
                }
            }, "session-graceful-close-blocked-write");
            blockedWrite.start();

            assertTrue(started.await(Duration.ofSeconds(1).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS), "blocked write did not start");
            Thread.sleep(50L);
            assertFalse(writeReturned.get(), "second write should still be flow-control blocked before graceful close starts");

            GracefulCloseTimeoutException closeError = assertThrows(
                    GracefulCloseTimeoutException.class,
                    () -> pair.client().closeWithError((Throwable) null),
                    "graceful close helper should preserve the graceful-close timeout path when active local send work never drains"
            );
            assertEquals(
                    GracefulCloseTimeoutException.MESSAGE,
                    closeError.getMessage(),
                    "null closeWithError helper should surface the typed graceful-close timeout when draining stalls"
            );

            blockedWrite.join(Duration.ofSeconds(1).toMillis());
            assertFalse(blockedWrite.isAlive(), "blocked write should wake when graceful session close starts");
            assertFalse(writeReturned.get(), "blocked write must not succeed after graceful close starts");

            Throwable error = writeError.get();
            assertNotNull(error, "blocked write should fail after graceful close starts");
            SessionClosedException closed = assertInstanceOf(SessionClosedException.class, error);
            assertEquals(ZmuxErrorSource.LOCAL, closed.source(), "blocked write should surface a local session-closed error");
            assertEquals(SessionState.CLOSED, pair.client().state(), "null closeWithError helper should still finish via a no-error graceful close");
        }
    }

    private static final class SessionPair implements AutoCloseable {
        private final ZmuxSession client;
        private final ZmuxSession server;

        private SessionPair(ZmuxSession client, ZmuxSession server) {
            this.client = client;
            this.server = server;
        }

        static SessionPair open(ZmuxConfig clientConfig, ZmuxConfig serverConfig) throws Exception {
            ServerSocket listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
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

            AtomicReference<ZmuxSession> clientRef = new AtomicReference<>();
            AtomicReference<ZmuxSession> serverRef = new AtomicReference<>();
            AtomicReference<Throwable> errorRef = new AtomicReference<>();
            CountDownLatch established = new CountDownLatch(2);

            Thread clientThread = new Thread(() -> {
                try {
                    clientRef.set(Zmux.client(leftConn, clientConfig));
                } catch (Throwable t) {
                    errorRef.set(t);
                } finally {
                    established.countDown();
                }
            }, "session-termination-client-open");
            Thread serverThread = new Thread(() -> {
                try {
                    serverRef.set(Zmux.server(rightConn, serverConfig));
                } catch (Throwable t) {
                    errorRef.set(t);
                } finally {
                    established.countDown();
                }
            }, "session-termination-server-open");
            clientThread.start();
            serverThread.start();
            established.await();
            rethrow(errorRef.get());
            return new SessionPair(clientRef.get(), serverRef.get());
        }

        ZmuxSession client() {
            return client;
        }

        ZmuxSession server() {
            return server;
        }

        @Override
        public void close() throws Exception {
            IOException error = null;
            try {
                client.close();
            } catch (IOException e) {
                error = e;
            }
            try {
                server.close();
            } catch (IOException e) {
                if (error == null) {
                    error = e;
                }
            }
            if (error != null) {
                throw error;
            }
        }
    }

    private static final class RawPeerSession implements AutoCloseable {
        private final ZmuxSession session;
        private final Socket socket;
        private final BufferedInputStream input;
        private final BufferedOutputStream output;

        private RawPeerSession(ZmuxSession session, Socket socket, BufferedInputStream input, BufferedOutputStream output) {
            this.session = session;
            this.socket = socket;
            this.input = input;
            this.output = output;
        }

        static RawPeerSession open(ZmuxConfig sessionConfig, long rawCapabilities) throws Exception {
            ServerSocket listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Socket peerSocket = new Socket("127.0.0.1", listener.getLocalPort());
            Socket sessionSocket = listener.accept();
            listener.close();

            BasicDuplexConnection sessionConn = new BasicDuplexConnection(
                    sessionSocket.getInputStream(),
                    sessionSocket.getOutputStream(),
                    sessionSocket,
                    sessionSocket.getLocalSocketAddress(),
                    sessionSocket.getRemoteSocketAddress()
            );
            BufferedInputStream peerInput = new BufferedInputStream(peerSocket.getInputStream());
            BufferedOutputStream peerOutput = new BufferedOutputStream(peerSocket.getOutputStream());

            AtomicReference<ZmuxSession> sessionRef = new AtomicReference<>();
            AtomicReference<Throwable> errorRef = new AtomicReference<>();
            CountDownLatch established = new CountDownLatch(1);

            Thread sessionThread = new Thread(() -> {
                try {
                    sessionRef.set(Zmux.server(sessionConn, sessionConfig));
                } catch (Throwable t) {
                    errorRef.set(t);
                } finally {
                    established.countDown();
                }
            }, "session-termination-raw-open");
            sessionThread.start();

            FrameCodec.writePreface(peerOutput, new Preface(
                    Protocol.PREFACE_VERSION,
                    Role.INITIATOR,
                    0L,
                    Protocol.PROTO_VERSION,
                    Protocol.PROTO_VERSION,
                    rawCapabilities,
                    Settings.defaults()
            ));
            peerOutput.flush();
            FrameCodec.readPreface(peerInput);
            established.await();
            rethrow(errorRef.get());
            return new RawPeerSession(sessionRef.get(), peerSocket, peerInput, peerOutput);
        }

        ZmuxSession session() {
            return session;
        }

        void send(FrameCodec.Frame frame) throws IOException {
            FrameCodec.writeFrame(output, frame, Settings.defaults().limits());
            output.flush();
        }

        @Override
        public void close() throws Exception {
            IOException error = null;
            try {
                session.close();
            } catch (IOException e) {
                error = e;
            }
            try {
                input.close();
            } catch (IOException e) {
                if (error == null) {
                    error = e;
                }
            }
            try {
                output.close();
            } catch (IOException e) {
                if (error == null) {
                    error = e;
                }
            }
            try {
                socket.close();
            } catch (IOException e) {
                if (error == null) {
                    error = e;
                }
            }
            if (error != null) {
                throw error;
            }
        }
    }
}
