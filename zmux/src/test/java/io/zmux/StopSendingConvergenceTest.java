package io.zmux;

import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class StopSendingConvergenceTest {
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
    void locallyOpenedCommittedEmptyTailGracefullyFinishesAfterPeerStopSending() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L)) {
            ZmuxStream stream = peer.session().openStream();
            stream.write("x".getBytes(StandardCharsets.UTF_8));

            FrameCodec.Frame opener = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.DATA, opener.type(), "expected opening DATA frame");
            assertEquals("x", new String(opener.payload(), StandardCharsets.UTF_8), "opening payload mismatch");

            peer.send(new FrameCodec.Frame(
                    FrameType.STOP_SENDING,
                    0,
                    opener.streamId(),
                    FrameCodec.buildErrorPayload(ErrorCode.CANCELLED.code(), "", Settings.defaults().maxControlPayloadBytes())
            ));

            FrameCodec.Frame fin = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.DATA, fin.type(), "STOP_SENDING with empty committed tail should finish with DATA|FIN");
            assertEquals(opener.streamId(), fin.streamId(), "graceful finish stream id mismatch");
            assertTrue((fin.flags() & Protocol.FRAME_FLAG_FIN) != 0, "graceful finish must carry FIN");
            assertEquals(0, fin.payload().length, "empty committed tail should finish with an empty DATA|FIN");

            FrameCodec.Frame extra = peer.pollFrame(Duration.ofMillis(150));
            if (extra != null) {
                assertTrue(extra.type() != FrameType.RESET || extra.streamId() != opener.streamId(), "graceful finish must not also emit RESET");
            }

            WriteClosedException stop = assertInstanceOf(
                    WriteClosedException.class,
                    assertThrows(IOException.class, () -> stream.write("y".getBytes(StandardCharsets.UTF_8))),
                    "once graceful STOP_SENDING completion has emitted DATA|FIN, future writes should surface the local graceful close"
            );
            assertEquals(ZmuxErrorSource.LOCAL, stop.source(), "post-finish write failure source mismatch");
            assertEquals(ZmuxTerminationKind.GRACEFUL, stop.terminationKind(), "post-finish write failure termination mismatch");
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
            }, "stop-sending-raw-session-open");
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
            ZmuxSession session = sessionRef.get();
            assertNotNull(session, "session should establish");
            return new RawPeerSession(session, peerSocket, peerInput, peerOutput);
        }

        ZmuxSession session() {
            return session;
        }

        void send(FrameCodec.Frame frame) throws IOException {
            FrameCodec.writeFrame(output, frame, Settings.defaults().limits());
            output.flush();
        }

        FrameCodec.Frame readFrame(Duration timeout) throws IOException {
            socket.setSoTimeout((int) timeout.toMillis());
            return FrameCodec.readFrame(input, Settings.defaults().limits());
        }

        FrameCodec.Frame pollFrame(Duration timeout) throws IOException {
            socket.setSoTimeout((int) timeout.toMillis());
            try {
                return FrameCodec.readFrame(input, Settings.defaults().limits());
            } catch (java.net.SocketTimeoutException e) {
                return null;
            }
        }

        @Override
        public void close() throws Exception {
            IOException error = null;
            try {
                session.closeWithError(ErrorCode.NO_ERROR.code(), "");
                session.awaitTermination(Duration.ofSeconds(1));
            } catch (IOException e) {
                error = e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                error = new IOException("interrupted while closing raw peer session", e);
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
