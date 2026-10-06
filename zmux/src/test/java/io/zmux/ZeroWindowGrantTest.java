package io.zmux;

import io.zmux.protocol.*;
import io.zmux.transport.BasicDuplexConnection;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A receiver with a zero initial window must still grant credit when the peer is blocked on it (SPEC 8 "avoid
 * avoidable sender stalls"; IMPLEMENTATION 2.1.1 zero-window opens).
 */
final class ZeroWindowGrantTest {
    private static final byte[] PAYLOAD = "0123456789".getBytes(StandardCharsets.UTF_8);

    private static void rethrow(Throwable error) throws Exception {
        if (error == null) {
            return;
        }
        if (error instanceof Exception) {
            throw (Exception) error;
        }
        throw new RuntimeException(error);
    }

    private static byte[] varintPayload(long value) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Varint62.write(output, value);
        return output.toByteArray();
    }

    private static ZmuxConfig serverConfig(Settings settings) {
        return ZmuxConfig.builder().settings(settings).build();
    }

    private static void readFully(ZmuxRecvStream stream, byte[] buffer) throws IOException {
        int read = 0;
        while (read < buffer.length) {
            int n = stream.read(buffer, read, buffer.length - read);
            assertTrue(n > 0, "stream ended before all bytes arrived");
            read += n;
        }
    }

    private static void assertJavaPairTransfers(Settings serverSettings, boolean bidirectional) throws Exception {
        try (SessionPair pair = SessionPair.open(ZmuxConfig.builder().build(), serverConfig(serverSettings))) {
            AtomicReference<Throwable> writeError = new AtomicReference<>();
            CountDownLatch writeDone = new CountDownLatch(1);
            Thread writer = new Thread(() -> {
                try {
                    if (bidirectional) {
                        pair.client().openStream().writeFinal(PAYLOAD);
                    } else {
                        pair.client().openUniStream().writeFinal(PAYLOAD);
                    }
                } catch (Throwable error) {
                    writeError.set(error);
                } finally {
                    writeDone.countDown();
                }
            }, "zero-window-writer");
            writer.start();

            ZmuxRecvStream inbound = bidirectional
                    ? pair.server().acceptStream(Duration.ofSeconds(2))
                    : pair.server().acceptUniStream(Duration.ofSeconds(2));
            inbound.setReadTimeout(Duration.ofSeconds(2));
            byte[] buffer = new byte[PAYLOAD.length];
            readFully(inbound, buffer);
            assertArrayEquals(PAYLOAD, buffer, "zero-window receiver should still receive the payload");
            assertEquals(-1, inbound.read(new byte[1]));
            assertTrue(writeDone.await(2, TimeUnit.SECONDS), "writer should finish once credit is granted");
            rethrow(writeError.get());
        }
    }

    @Test
    void zeroInitialBidiStreamWindowStillDeliversData() throws Exception {
        assertJavaPairTransfers(Settings.defaults().toBuilder().initialMaxStreamDataBidiPeerOpened(0L).build(), true);
    }

    @Test
    void zeroInitialUniStreamWindowStillDeliversData() throws Exception {
        assertJavaPairTransfers(Settings.defaults().toBuilder().initialMaxStreamDataUni(0L).build(), false);
    }

    @Test
    void zeroInitialSessionWindowStillDeliversData() throws Exception {
        assertJavaPairTransfers(Settings.defaults().toBuilder().initialMaxData(0L).build(), true);
    }

    @Test
    void peerStreamBlockedOnZeroWindowTriggersStreamGrant() throws Exception {
        Settings settings = Settings.defaults().toBuilder().initialMaxStreamDataBidiPeerOpened(0L).build();
        try (RawPeer peer = RawPeer.open(serverConfig(settings))) {
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 4L, new byte[0]));
            peer.send(new FrameCodec.Frame(FrameType.BLOCKED, 0, 4L, varintPayload(0L)));

            FrameCodec.Frame maxData = peer.awaitFrame(FrameType.MAX_DATA, 4L, Duration.ofSeconds(1));
            assertTrue(Varint62.decode(maxData.payload(), 0).value() > 0L, "BLOCKED on a zero stream window should grant credit");
            assertFalse(peer.session().state().terminal(), "a BLOCKED that causes a grant is not a no-op");
        }
    }

    @Test
    void peerSessionBlockedOnZeroWindowTriggersSessionGrant() throws Exception {
        Settings settings = Settings.defaults().toBuilder().initialMaxData(0L).build();
        try (RawPeer peer = RawPeer.open(serverConfig(settings))) {
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 4L, new byte[0]));
            peer.send(new FrameCodec.Frame(FrameType.BLOCKED, 0, 0L, varintPayload(0L)));

            FrameCodec.Frame maxData = peer.awaitFrame(FrameType.MAX_DATA, 0L, Duration.ofSeconds(1));
            assertTrue(Varint62.decode(maxData.payload(), 0).value() > 0L, "session BLOCKED on a zero window should grant credit");
        }
    }

    @Test
    void blockingReadOnZeroWindowGrantsWithoutPeerBlocked() throws Exception {
        Settings settings = Settings.defaults().toBuilder().initialMaxStreamDataBidiPeerOpened(0L).build();
        try (RawPeer peer = RawPeer.open(serverConfig(settings))) {
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 4L, new byte[0]));
            ZmuxStream inbound = peer.session().acceptStream(Duration.ofSeconds(1));

            AtomicReference<Throwable> readError = new AtomicReference<>();
            byte[] buffer = new byte[PAYLOAD.length];
            CountDownLatch readDone = new CountDownLatch(1);
            Thread reader = new Thread(() -> {
                try {
                    readFully(inbound, buffer);
                } catch (Throwable error) {
                    readError.set(error);
                } finally {
                    readDone.countDown();
                }
            }, "zero-window-reader");
            reader.start();

            FrameCodec.Frame maxData = peer.awaitFrame(FrameType.MAX_DATA, 4L, Duration.ofSeconds(1));
            assertTrue(Varint62.decode(maxData.payload(), 0).value() >= PAYLOAD.length,
                    "a reader blocked on an exhausted zero window should grant stream credit");
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 4L, PAYLOAD));
            assertTrue(readDone.await(2, TimeUnit.SECONDS), "reader should receive the granted data");
            rethrow(readError.get());
            assertArrayEquals(PAYLOAD, buffer);
        }
    }

    private static final class RawPeer implements AutoCloseable {
        private final ZmuxSession session;
        private final Socket socket;
        private final BufferedInputStream input;
        private final BufferedOutputStream output;

        private RawPeer(ZmuxSession session, Socket socket, BufferedInputStream input, BufferedOutputStream output) {
            this.session = session;
            this.socket = socket;
            this.input = input;
            this.output = output;
        }

        static RawPeer open(ZmuxConfig sessionConfig) throws Exception {
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
            }, "zero-window-raw-open");
            sessionThread.start();

            FrameCodec.writePreface(peerOutput, new Preface(
                    Protocol.PREFACE_VERSION,
                    Role.INITIATOR,
                    0L,
                    Protocol.PROTO_VERSION,
                    Protocol.PROTO_VERSION,
                    0L,
                    Settings.defaults()
            ));
            peerOutput.flush();
            FrameCodec.readPreface(peerInput);
            established.await();
            rethrow(errorRef.get());
            return new RawPeer(sessionRef.get(), peerSocket, peerInput, peerOutput);
        }

        ZmuxSession session() {
            return session;
        }

        void send(FrameCodec.Frame frame) throws IOException {
            FrameCodec.writeFrame(output, frame, Settings.defaults().limits());
            output.flush();
        }

        FrameCodec.Frame pollFrame(FrameType type, long streamId, Duration timeout) throws IOException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                FrameCodec.Frame frame = RawFrameReads.readFrameIfStarted(socket, input, 20);
                if (frame != null && frame.type() == type && frame.streamId() == streamId) {
                    return frame;
                }
            }
            return null;
        }

        FrameCodec.Frame awaitFrame(FrameType type, long streamId, Duration timeout) throws IOException {
            FrameCodec.Frame frame = pollFrame(type, streamId, timeout);
            if (frame == null) {
                throw new AssertionError("timed out waiting for " + type + " on stream " + streamId);
            }
            return frame;
        }

        @Override
        public void close() throws Exception {
            try {
                session.closeWithError(ErrorCode.NO_ERROR.code(), "");
            } catch (IOException ignored) {
                // the session may already have failed
            }
            socket.close();
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
                } catch (Throwable error) {
                    errorRef.compareAndSet(null, error);
                } finally {
                    established.countDown();
                }
            }, "zero-window-client-open");
            Thread serverThread = new Thread(() -> {
                try {
                    serverRef.set(Zmux.server(rightConn, serverConfig));
                } catch (Throwable error) {
                    errorRef.compareAndSet(null, error);
                } finally {
                    established.countDown();
                }
            }, "zero-window-server-open");
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
            try {
                client.closeWithError(ErrorCode.NO_ERROR.code(), "");
            } finally {
                server.closeWithError(ErrorCode.NO_ERROR.code(), "");
            }
        }
    }
}
