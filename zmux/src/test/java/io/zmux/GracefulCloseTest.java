package io.zmux;

import io.zmux.transport.BasicDuplexConnection;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class GracefulCloseTest {
    private static ZmuxConfig testConfig() {
        return ZmuxConfig.builder()
                .gracefulCloseDrainTimeout(Duration.ofSeconds(1))
                .build();
    }

    private static void awaitCondition(BooleanSupplier condition, Duration timeout, String message) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError(message);
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

    private static String readUtf8(ZmuxStream stream) throws IOException {
        byte[] buf = new byte[32];
        int n = stream.read(buf);
        if (n < 0) {
            return "";
        }
        return new String(buf, 0, n, StandardCharsets.UTF_8);
    }

    @Test
    void manualGoAwayStillAllowsLocalOpen() throws Exception {
        try (SessionPair pair = SessionPair.open(testConfig(), testConfig())) {
            pair.client().goAway(0L, 0L, ErrorCode.NO_ERROR.code(), "");

            ZmuxStream stream = pair.client().openStream();
            stream.writeFinal("ok".getBytes(StandardCharsets.UTF_8));

            ZmuxStream accepted = pair.server().acceptStream(Duration.ofSeconds(1));
            assertEquals("ok", readUtf8(accepted), "manual GOAWAY should not block locally opened streams");
            accepted.closeWrite();
        }
    }

    @Test
    void closeBlocksNewLocalOpensDuringGracefulDrain() throws Exception {
        try (SessionPair pair = SessionPair.open(testConfig(), testConfig())) {
            ZmuxSendStream stream = pair.client().openUniStream();
            stream.write("hello".getBytes(StandardCharsets.UTF_8));

            AtomicReference<Throwable> closeError = new AtomicReference<>();
            Thread closeThread = new Thread(() -> {
                try {
                    pair.client().close();
                } catch (Throwable t) {
                    closeError.set(t);
                }
            }, "graceful-close-blocks-open");
            closeThread.start();

            awaitCondition(
                    () -> pair.client().state() == SessionState.DRAINING && closeThread.isAlive(),
                    Duration.ofSeconds(1),
                    "session did not enter graceful draining"
            );

            IOException openError = assertThrows(IOException.class, pair.client()::openStream);
            SessionClosedException closeInProgress = assertInstanceOf(SessionClosedException.class, openError);
            assertEquals("open", closeInProgress.operation(), "open during graceful close should be tagged as the open operation");
            assertEquals(ZmuxErrorSource.LOCAL, closeInProgress.source(), "open during graceful close should surface a local session-closed error");

            stream.closeWrite();

            closeThread.join(Duration.ofSeconds(2).toMillis());
            assertFalse(closeThread.isAlive(), "close should finish once the last visible local stream drains");
            rethrow(closeError.get());
        }
    }

    @Test
    void closeCompletedSurfacesLocalSessionClosedOnSubsequentOpen() throws Exception {
        try (SessionPair pair = SessionPair.open(testConfig(), testConfig())) {
            pair.client().close();

            IOException openError = assertThrows(IOException.class, pair.client()::openStream);
            SessionClosedException closed = assertInstanceOf(SessionClosedException.class, openError);
            assertEquals("open", closed.operation(), "open after completed local close should be tagged as the open operation");
            assertEquals(ZmuxErrorSource.LOCAL, closed.source(), "open after completed local close should remain a local session-closed error");
        }
    }

    @Test
    void closeReclaimsProvisionalLocalStreams() throws Exception {
        try (SessionPair pair = SessionPair.open(testConfig(), testConfig())) {
            ZmuxStream bidi = pair.client().openStream();
            ZmuxSendStream uni = pair.client().openUniStream();

            AtomicReference<Throwable> closeError = new AtomicReference<>();
            Thread closeThread = new Thread(() -> {
                try {
                    pair.client().close();
                } catch (Throwable t) {
                    closeError.set(t);
                }
            }, "graceful-close-reclaims-provisionals");
            closeThread.start();

            closeThread.join(Duration.ofSeconds(2).toMillis());
            assertFalse(closeThread.isAlive(), "close should not hang on provisional local streams");
            rethrow(closeError.get());

            IOException bidiError = assertThrows(IOException.class, () -> bidi.write("x".getBytes(StandardCharsets.UTF_8)));
            ApplicationError bidiRefused = assertInstanceOf(ApplicationError.class, bidiError);
            assertEquals(ErrorCode.REFUSED_STREAM.code(), bidiRefused.code(), "reclaimed provisional bidi stream should surface REFUSED_STREAM");

            IOException uniError = assertThrows(IOException.class, () -> uni.write("y".getBytes(StandardCharsets.UTF_8)));
            ApplicationError uniRefused = assertInstanceOf(ApplicationError.class, uniError);
            assertEquals(ErrorCode.REFUSED_STREAM.code(), uniRefused.code(), "reclaimed provisional uni stream should surface REFUSED_STREAM");
        }
    }

    @Test
    void closeIgnoresUnreadPeerUniStream() throws Exception {
        try (SessionPair pair = SessionPair.open(testConfig(), testConfig())) {
            ZmuxSendStream peerStream = pair.server().openUniStream();
            peerStream.writeFinal("peer".getBytes(StandardCharsets.UTF_8));
            pair.client().acceptUniStream(Duration.ofSeconds(1));

            AtomicReference<Throwable> closeError = new AtomicReference<>();
            Thread closeThread = new Thread(() -> {
                try {
                    pair.client().close();
                } catch (Throwable t) {
                    closeError.set(t);
                }
            }, "graceful-close-unread-peer-uni");
            closeThread.start();

            closeThread.join(Duration.ofSeconds(2).toMillis());
            assertFalse(closeThread.isAlive(), "close should not wait for unread peer-opened uni tails");
            rethrow(closeError.get());
        }
    }

    @Test
    void closeIgnoresPeerOpenedBidiWithoutLocalSend() throws Exception {
        try (SessionPair pair = SessionPair.open(testConfig(), testConfig())) {
            ZmuxStream peerStream = pair.server().openStream();
            peerStream.writeFinal("peer".getBytes(StandardCharsets.UTF_8));
            pair.client().acceptStream(Duration.ofSeconds(1));

            AtomicReference<Throwable> closeError = new AtomicReference<>();
            Thread closeThread = new Thread(() -> {
                try {
                    pair.client().close();
                } catch (Throwable t) {
                    closeError.set(t);
                }
            }, "graceful-close-peer-bidi-no-local-send");
            closeThread.start();

            closeThread.join(Duration.ofSeconds(2).toMillis());
            assertFalse(closeThread.isAlive(), "close should not wait for peer-opened bidi streams without local send work");
            rethrow(closeError.get());
        }
    }

    @Test
    void closeWaitsForPeerOpenedBidiWithLocalSend() throws Exception {
        try (SessionPair pair = SessionPair.open(testConfig(), testConfig())) {
            ZmuxStream peerStream = pair.server().openStream();
            peerStream.writeFinal("peer".getBytes(StandardCharsets.UTF_8));
            ZmuxStream accepted = pair.client().acceptStream(Duration.ofSeconds(1));
            accepted.write("reply".getBytes(StandardCharsets.UTF_8));

            AtomicReference<Throwable> closeError = new AtomicReference<>();
            Thread closeThread = new Thread(() -> {
                try {
                    pair.client().close();
                } catch (Throwable t) {
                    closeError.set(t);
                }
            }, "graceful-close-peer-bidi-local-send");
            closeThread.start();

            awaitCondition(
                    () -> pair.client().state() == SessionState.DRAINING && closeThread.isAlive(),
                    Duration.ofSeconds(1),
                    "close should remain draining while a peer-opened bidi stream still has local send work"
            );

            accepted.closeWrite();

            closeThread.join(Duration.ofSeconds(2).toMillis());
            assertFalse(closeThread.isAlive(), "close should finish once the peer-opened bidi local send half drains");
            rethrow(closeError.get());
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
                } catch (Throwable t) {
                    errorRef.set(t);
                } finally {
                    established.countDown();
                }
            }, "graceful-close-client-open");
            Thread serverThread = new Thread(() -> {
                try {
                    serverRef.set(Zmux.server(rightConn, serverConfig));
                } catch (Throwable t) {
                    errorRef.set(t);
                } finally {
                    established.countDown();
                }
            }, "graceful-close-server-open");
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
}
