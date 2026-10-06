package io.zmux;

import io.zmux.transport.BasicDuplexConnection;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Running out of local stream IDs (SPEC 3.1): no wraparound or reuse, a local open-limited error instead of a
 * PROTOCOL-coded one, and one GOAWAY that starts graceful replacement without tightening the peer's watermarks.
 */
final class LocalStreamIdExhaustionTest {
    // The last valid initiator-owned bidirectional stream ID.
    private static final long LAST_CLIENT_BIDI = 0x3FFFFFFFFFFFFFFCL;

    private static void rethrow(Throwable error) throws Exception {
        if (error == null) {
            return;
        }
        if (error instanceof Exception) {
            throw (Exception) error;
        }
        throw new RuntimeException(error);
    }

    private static void setCursor(ZmuxSession session, String name, long value) throws ReflectiveOperationException {
        Field lockField = session.getClass().getDeclaredField("lock");
        lockField.setAccessible(true);
        Field field = session.getClass().getDeclaredField(name);
        field.setAccessible(true);
        synchronized (lockField.get(session)) {
            field.setLong(session, value);
        }
    }

    private static String readAll(ZmuxRecvStream stream) throws IOException {
        return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }

    private static void await(Duration timeout, java.util.function.BooleanSupplier condition, String description)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(5L);
        }
        throw new AssertionError("timed out waiting for " + description);
    }

    @Test
    void exhaustedLocalBidiIdsFailOpensLocallyAndStartGracefulReplacement() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            setCursor(pair.client(), "nextLocalBidi", LAST_CLIENT_BIDI);
            setCursor(pair.server(), "nextPeerBidi", LAST_CLIENT_BIDI);

            ZmuxStream last = pair.client().openStream();
            last.write("last".getBytes(StandardCharsets.UTF_8));
            last.closeWrite();
            ZmuxStream accepted = pair.server().acceptStream(Duration.ofSeconds(2));
            assertEquals(LAST_CLIENT_BIDI, accepted.streamId(), "the last valid ID is still usable");
            assertEquals("last", readAll(accepted));

            for (int attempt = 0; attempt < 2; attempt++) {
                OpenLimitedException limited = assertThrows(OpenLimitedException.class, () -> pair.client().openStream());
                assertNotEquals(ErrorCode.PROTOCOL.code(), ZmuxErrors.code(limited, -1L), "exhaustion is not a peer violation");
                assertTrue(
                        String.valueOf(limited.getCause()).contains("local stream ID space exhausted"),
                        "exhaustion should name its reason: " + limited.getCause()
                );
            }
            assertEquals(SessionState.DRAINING, pair.client().state(), "exhaustion starts graceful replacement");
            await(Duration.ofSeconds(2), () -> ((ZmuxNativeSession) pair.server()).peerGoAwayError() != null, "peer GOAWAY");
            assertEquals(ErrorCode.NO_ERROR.code(), ((ZmuxNativeSession) pair.server()).peerGoAwayError().code());

            // The GOAWAY keeps the current watermarks: the peer can still open streams, and the other local class works.
            ZmuxStream fromServer = pair.server().openStream();
            fromServer.write("srv".getBytes(StandardCharsets.UTF_8));
            fromServer.closeWrite();
            assertEquals("srv", readAll(pair.client().acceptStream(Duration.ofSeconds(2))));

            ZmuxSendStream uni = pair.client().openUniStream();
            uni.write("uni".getBytes(StandardCharsets.UTF_8));
            uni.closeWrite();
            assertEquals("uni", readAll(pair.server().acceptUniStream(Duration.ofSeconds(2))));

            assertFalse(pair.client().state().terminal());
            assertFalse(pair.server().state().terminal());
        }
    }

    private static final class SessionPair implements AutoCloseable {
        private final ZmuxSession client;
        private final ZmuxSession server;

        private SessionPair(ZmuxSession client, ZmuxSession server) {
            this.client = client;
            this.server = server;
        }

        static SessionPair open() throws Exception {
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
                    clientRef.set(Zmux.client(leftConn, ZmuxConfig.builder().build()));
                } catch (Throwable error) {
                    errorRef.compareAndSet(null, error);
                } finally {
                    established.countDown();
                }
            }, "id-exhaustion-client-open");
            Thread serverThread = new Thread(() -> {
                try {
                    serverRef.set(Zmux.server(rightConn, ZmuxConfig.builder().build()));
                } catch (Throwable error) {
                    errorRef.compareAndSet(null, error);
                } finally {
                    established.countDown();
                }
            }, "id-exhaustion-server-open");
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
            if (error != null) {
                throw error;
            }
        }
    }
}
