package io.zmux;

import io.zmux.protocol.*;
import io.zmux.transport.BasicDuplexConnection;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class BlockedEmissionTest {
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
    void smallReadDoesNotImmediatelyEmitMaxDataBeforeBlocked() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L, Settings.defaults())) {
            long streamId = 4L;
            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    0,
                    streamId,
                    "hello".getBytes(StandardCharsets.UTF_8)
            ));

            ZmuxStream stream = peer.session().acceptStream(Duration.ofSeconds(1));
            byte[] buffer = new byte[5];
            int read = stream.read(buffer);
            assertEquals(5, read, "read should consume the buffered payload");
            assertEquals("hello", new String(buffer, StandardCharsets.UTF_8), "payload mismatch before BLOCKED");

            assertNull(peer.pollFrame(Duration.ofMillis(150)), "small read should not immediately emit MAX_DATA before replenish thresholds or BLOCKED");

            peer.send(new FrameCodec.Frame(
                    FrameType.BLOCKED,
                    0,
                    streamId,
                    Varint62.encode(0L)
            ));

            FrameCodec.Frame first = peer.readFrame(Duration.ofSeconds(1));
            FrameCodec.Frame second = peer.readFrame(Duration.ofSeconds(1));
            boolean sawSession = first.type() == FrameType.MAX_DATA && first.streamId() == 0L
                    || second.type() == FrameType.MAX_DATA && second.streamId() == 0L;
            boolean sawStream = first.type() == FrameType.MAX_DATA && first.streamId() == streamId
                    || second.type() == FrameType.MAX_DATA && second.streamId() == streamId;
            assertEquals(true, sawSession, "BLOCKED should force a session MAX_DATA flush");
            assertEquals(true, sawStream, "BLOCKED should force a stream MAX_DATA flush");

            stream.close();
        }
    }

    @Test
    void zeroWindowWriteEmitsBlockedAfterOpeningData() throws Exception {
        Settings peerSettings = Settings.defaults().toBuilder()
                .initialMaxData(0L)
                .initialMaxStreamDataBidiPeerOpened(0L)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L, peerSettings)) {
            AtomicReference<Throwable> writeError = new AtomicReference<>();
            AtomicReference<ZmuxStream> streamRef = new AtomicReference<>();

            Thread writer = new Thread(() -> {
                try {
                    ZmuxStream stream = peer.session().openStream();
                    streamRef.set(stream);
                    stream.write("x".getBytes(StandardCharsets.UTF_8));
                } catch (Throwable t) {
                    writeError.set(t);
                }
            }, "blocked-emission-writer");
            writer.start();

            FrameCodec.Frame opener = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.DATA, opener.type(), "first frame should open the stream");
            assertEquals(1L, opener.streamId(), "server-local bidi stream id mismatch");
            assertEquals(0, opener.payload().length, "zero-window opener should carry no app data");

            FrameCodec.Frame sessionBlocked = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.BLOCKED, sessionBlocked.type(), "second frame should be session BLOCKED");
            assertEquals(0L, sessionBlocked.streamId(), "session BLOCKED stream id mismatch");
            assertEquals(0L, Varint62.decode(sessionBlocked.payload(), 0).value(), "session BLOCKED offset mismatch");

            FrameCodec.Frame streamBlocked = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.BLOCKED, streamBlocked.type(), "third frame should be stream BLOCKED");
            assertEquals(opener.streamId(), streamBlocked.streamId(), "stream BLOCKED stream id mismatch");
            assertEquals(0L, Varint62.decode(streamBlocked.payload(), 0).value(), "stream BLOCKED offset mismatch");

            assertNull(peer.pollFrame(Duration.ofMillis(100)), "same limiting offsets should not spam duplicate BLOCKED frames");

            peer.send(new FrameCodec.Frame(FrameType.MAX_DATA, 0, 0L, Varint62.encode(1L)));
            peer.send(new FrameCodec.Frame(FrameType.MAX_DATA, 0, opener.streamId(), Varint62.encode(1L)));

            FrameCodec.Frame payload = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.DATA, payload.type(), "credit grant should unblock payload DATA");
            assertEquals(opener.streamId(), payload.streamId(), "payload DATA stream id mismatch");
            assertEquals("x", new String(payload.payload(), StandardCharsets.UTF_8), "payload DATA mismatch");

            writer.join(1000L);
            assertFalse(writer.isAlive(), "writer should unblock after peer MAX_DATA");
            assertNull(writeError.get(), "writer should complete without error");
            assertNotNull(streamRef.get(), "writer should have opened a stream");
            streamRef.get().close();
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

        static RawPeerSession open(ZmuxConfig sessionConfig, long rawCapabilities, Settings rawSettings) throws Exception {
            ServerSocket listener = new ServerSocket(0);
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
            }, "blocked-emission-raw-open");
            sessionThread.start();

            FrameCodec.writePreface(peerOutput, new Preface(
                    Protocol.PREFACE_VERSION,
                    Role.INITIATOR,
                    0L,
                    Protocol.PROTO_VERSION,
                    Protocol.PROTO_VERSION,
                    rawCapabilities,
                    rawSettings
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

        FrameCodec.Frame readFrame(Duration timeout) throws Exception {
            socket.setSoTimeout((int) timeout.toMillis());
            try {
                return FrameCodec.readFrame(input, Settings.defaults().limits());
            } catch (SocketTimeoutException e) {
                throw new AssertionError("timed out waiting for frame", e);
            }
        }

        FrameCodec.Frame pollFrame(Duration timeout) throws IOException {
            socket.setSoTimeout((int) timeout.toMillis());
            try {
                return FrameCodec.readFrame(input, Settings.defaults().limits());
            } catch (SocketTimeoutException e) {
                return null;
            }
        }

        @Override
        public void close() throws Exception {
            IOException error = null;
            try {
                session.closeWithError(ErrorCode.NO_ERROR.code(), "");
            } catch (IOException e) {
                error = e;
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
