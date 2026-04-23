package io.zmux;

import io.zmux.internal.FrameCodec;
import io.zmux.internal.Varint62;
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
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

final class AbuseSemanticsTest {
    private static FrameCodec.Frame awaitFrame(RawPeerSession peer, Predicate<FrameCodec.Frame> predicate, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            FrameCodec.Frame frame = peer.pollFrame(Duration.ofMillis(20));
            if (frame != null && predicate.test(frame)) {
                return frame;
            }
        }
        throw new AssertionError("timed out waiting for matching frame");
    }

    private static void abortAcceptedBidi(RawPeerSession peer, long streamId, String payload) throws Exception {
        peer.send(new FrameCodec.Frame(FrameType.DATA, 0, streamId, payload.getBytes(StandardCharsets.UTF_8)));
        ZmuxStream stream = peer.session().acceptStream(Duration.ofSeconds(1));
        byte[] buffer = new byte[payload.length()];
        assertEquals(payload.length(), stream.read(buffer), "payload mismatch before accepted ABORT");
        peer.send(new FrameCodec.Frame(FrameType.ABORT, 0, streamId, cancelledPayload()));
    }

    private static byte[] cancelledPayload() throws IOException {
        return FrameCodec.buildErrorPayload(
                ErrorCode.CANCELLED.code(),
                "",
                Settings.defaults().maxControlPayloadBytes()
        );
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
    void unexpectedPongFloodFailsSession() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .abuseWindow(Duration.ofSeconds(1))
                .noOpControlFloodThreshold(1)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            peer.send(new FrameCodec.Frame(FrameType.PONG, 0, 0L, "unexpected-a".getBytes(StandardCharsets.UTF_8)));
            assertFalse(peer.session().awaitTermination(Duration.ofMillis(150)), "single unexpected PONG should stay below the threshold");

            peer.send(new FrameCodec.Frame(FrameType.PONG, 0, 0L, "unexpected-b".getBytes(StandardCharsets.UTF_8)));
            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "unexpected PONG flood should terminate the session");
            assertEquals(SessionState.FAILED, peer.session().state(), "unexpected PONG flood should fail the session");
        }
    }

    @Test
    void matchingPongClearsNoOpControlBudget() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .abuseWindow(Duration.ofSeconds(1))
                .noOpControlFloodThreshold(1)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            peer.send(new FrameCodec.Frame(FrameType.PONG, 0, 0L, "unexpected-a".getBytes(StandardCharsets.UTF_8)));
            assertFalse(peer.session().awaitTermination(Duration.ofMillis(150)), "first unexpected PONG should not terminate the session");

            AtomicReference<Duration> pingRtt = new AtomicReference<>();
            AtomicReference<Throwable> pingError = new AtomicReference<>();
            Thread pingThread = new Thread(() -> {
                try {
                    pingRtt.set(peer.session().ping("budget".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(2)));
                } catch (Throwable t) {
                    pingError.set(t);
                }
            }, "abuse-ping");
            pingThread.start();

            FrameCodec.Frame ping = awaitFrame(peer, frame -> frame.type() == FrameType.PING, Duration.ofSeconds(1));
            peer.send(new FrameCodec.Frame(FrameType.PONG, 0, 0L, ping.payload()));

            pingThread.join(Duration.ofSeconds(2).toMillis());
            assertFalse(pingThread.isAlive(), "ping thread should finish after a matching PONG");
            rethrow(pingError.get());
            assertNotNull(pingRtt.get(), "matching PONG should complete the pending ping");

            peer.send(new FrameCodec.Frame(FrameType.PONG, 0, 0L, "unexpected-b".getBytes(StandardCharsets.UTF_8)));
            assertFalse(peer.session().awaitTermination(Duration.ofMillis(150)), "matching PONG should clear the mixed no-op control budget");
        }
    }

    @Test
    void effectiveResetClearsMixedNoOpControlBudget() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .abuseWindow(Duration.ofSeconds(1))
                .noOpControlFloodThreshold(1)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            peer.send(new FrameCodec.Frame(FrameType.PONG, 0, 0L, "unexpected-a".getBytes(StandardCharsets.UTF_8)));
            assertFalse(peer.session().awaitTermination(Duration.ofMillis(150)), "first unexpected PONG should stay below threshold");

            long streamId = 2L;
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, streamId, "x".getBytes(StandardCharsets.UTF_8)));
            ZmuxRecvStream stream = peer.session().acceptUniStream(Duration.ofSeconds(1));
            byte[] buffer = new byte[1];
            assertEquals(1, stream.read(buffer), "payload mismatch before effective RESET");

            peer.send(new FrameCodec.Frame(FrameType.RESET, 0, streamId, cancelledPayload()));
            assertFalse(peer.session().awaitTermination(Duration.ofMillis(150)), "effective RESET should clear the mixed no-op budget");

            peer.send(new FrameCodec.Frame(FrameType.PONG, 0, 0L, "unexpected-b".getBytes(StandardCharsets.UTF_8)));
            assertFalse(peer.session().awaitTermination(Duration.ofMillis(150)), "post-RESET no-op control should start a fresh budget window");
        }
    }

    @Test
    void ignoredResetAfterPeerFinCountsNoOpControlBudget() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .abuseWindow(Duration.ofSeconds(1))
                .noOpControlFloodThreshold(1)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            long streamId = 4L;
            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    Protocol.FRAME_FLAG_FIN,
                    streamId,
                    "x".getBytes(StandardCharsets.UTF_8)
            ));

            ZmuxStream stream = peer.session().acceptStream(Duration.ofSeconds(1));
            byte[] buffer = new byte[1];
            assertEquals(1, stream.read(buffer), "payload mismatch before peer FIN");
            assertEquals(-1, stream.read(new byte[1]), "peer FIN should close the receive side");

            peer.send(new FrameCodec.Frame(FrameType.RESET, 0, streamId, cancelledPayload()));
            assertFalse(peer.session().awaitTermination(Duration.ofMillis(150)), "first ignored RESET should stay below threshold");

            peer.send(new FrameCodec.Frame(FrameType.RESET, 0, streamId, cancelledPayload()));
            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "repeated ignored RESET should terminate the session");
            assertEquals(SessionState.FAILED, peer.session().state(), "ignored RESET flood should fail the session");
        }
    }

    @Test
    void acceptedVisibleAbortDoesNotCountTerminalChurn() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .visibleTerminalChurnWindow(Duration.ofSeconds(1))
                .visibleTerminalChurnThreshold(1)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            abortAcceptedBidi(peer, 4L, "a");
            abortAcceptedBidi(peer, 8L, "b");

            assertFalse(peer.session().awaitTermination(Duration.ofMillis(200)), "accepted streams must not count as visible terminal churn");
        }
    }

    @Test
    void terminalStreamMaxDataFloodFailsSession() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .abuseWindow(Duration.ofSeconds(1))
                .noOpControlFloodThreshold(100)
                .noOpMaxDataFloodThreshold(1)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            long streamId = 4L;
            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    Protocol.FRAME_FLAG_FIN,
                    streamId,
                    "hello".getBytes(StandardCharsets.UTF_8)
            ));

            ZmuxStream stream = peer.session().acceptStream(Duration.ofSeconds(1));
            stream.closeWrite();

            FrameCodec.Frame fin = awaitFrame(
                    peer,
                    frame -> frame.type() == FrameType.DATA
                            && frame.streamId() == streamId
                            && (frame.flags() & Protocol.FRAME_FLAG_FIN) != 0,
                    Duration.ofSeconds(1)
            );
            assertNotNull(fin, "closeWrite should emit FIN before late MAX_DATA arrives");

            long lateLimit = 1_000_000L;
            byte[] payload = Varint62.encode(lateLimit);
            peer.send(new FrameCodec.Frame(FrameType.MAX_DATA, 0, streamId, payload));
            assertFalse(peer.session().awaitTermination(Duration.ofMillis(150)), "first late MAX_DATA should only consume the no-op budget");

            peer.send(new FrameCodec.Frame(FrameType.MAX_DATA, 0, streamId, payload));
            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "repeated late MAX_DATA on a terminal stream should fail the session");
            assertEquals(SessionState.FAILED, peer.session().state(), "late MAX_DATA flood should transition the session to FAILED");
        }
    }

    @Test
    void lateMaxDataOnTerminalRecvOnlyStreamIsIgnored() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.defaults(), 0L)) {
            long streamId = 2L;
            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    Protocol.FRAME_FLAG_FIN,
                    streamId,
                    "hello".getBytes(StandardCharsets.UTF_8)
            ));

            ZmuxRecvStream stream = peer.session().acceptUniStream(Duration.ofSeconds(1));
            byte[] buffer = new byte[5];
            assertEquals(5, stream.read(buffer), "payload mismatch before late MAX_DATA");
            assertEquals("hello", new String(buffer, StandardCharsets.UTF_8), "payload mismatch before late MAX_DATA");
            assertEquals(-1, stream.read(new byte[1]), "peer FIN should close the receive side");

            peer.send(new FrameCodec.Frame(FrameType.MAX_DATA, 0, streamId, Varint62.encode(1024L)));

            assertNull(peer.pollFrame(Duration.ofMillis(150)), "late MAX_DATA on a terminal recv-only stream must not emit ABORT");
            assertFalse(peer.session().awaitTermination(Duration.ofMillis(150)), "late MAX_DATA no-op must not fail the session");
        }
    }

    @Test
    void lateBlockedOnTerminalSendOnlyStreamIsIgnored() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.defaults(), 0L)) {
            ZmuxSendStream stream = peer.session().openUniStream();
            stream.writeFinal("x".getBytes(StandardCharsets.UTF_8));

            FrameCodec.Frame opener = awaitFrame(
                    peer,
                    frame -> frame.type() == FrameType.DATA
                            && (frame.flags() & Protocol.FRAME_FLAG_FIN) != 0,
                    Duration.ofSeconds(1)
            );

            peer.send(new FrameCodec.Frame(FrameType.BLOCKED, 0, opener.streamId(), Varint62.encode(0L)));

            assertNull(peer.pollFrame(Duration.ofMillis(150)), "late BLOCKED on a terminal send-only stream must not emit ABORT");
            assertFalse(peer.session().awaitTermination(Duration.ofMillis(150)), "late BLOCKED no-op must not fail the session");
        }
    }

    @Test
    void repeatedNoOpSessionBlockedFloodFailsSession() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .abuseWindow(Duration.ofSeconds(1))
                .noOpControlFloodThreshold(100)
                .noOpBlockedFloodThreshold(1)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            FrameCodec.Frame blocked = new FrameCodec.Frame(FrameType.BLOCKED, 0, 0L, Varint62.encode(0L));
            peer.send(blocked);
            assertFalse(peer.session().awaitTermination(Duration.ofMillis(150)), "first no-op session BLOCKED should stay below the threshold");

            peer.send(blocked);
            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "repeated no-op session BLOCKED should terminate the session");
            assertEquals(SessionState.FAILED, peer.session().state(), "no-op session BLOCKED flood should fail the session");
        }
    }

    @Test
    void replenishingBlockedClearsPriorNoOpBlockedBudget() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .abuseWindow(Duration.ofSeconds(1))
                .noOpControlFloodThreshold(100)
                .noOpBlockedFloodThreshold(1)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            peer.send(new FrameCodec.Frame(FrameType.BLOCKED, 0, 0L, Varint62.encode(0L)));
            assertFalse(peer.session().awaitTermination(Duration.ofMillis(150)), "first no-op session BLOCKED should remain below the threshold");

            long streamId = 4L;
            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    0,
                    streamId,
                    "hello".getBytes(StandardCharsets.UTF_8)
            ));

            ZmuxStream stream = peer.session().acceptStream(Duration.ofSeconds(1));
            byte[] buffer = new byte[5];
            assertEquals(5, stream.read(buffer), "read should consume the buffered payload");
            assertEquals("hello", new String(buffer, StandardCharsets.UTF_8), "payload mismatch before replenishing BLOCKED");

            peer.send(new FrameCodec.Frame(FrameType.BLOCKED, 0, streamId, Varint62.encode(0L)));
            FrameCodec.Frame first = awaitFrame(peer, frame -> frame.type() == FrameType.MAX_DATA, Duration.ofSeconds(1));
            FrameCodec.Frame second = awaitFrame(peer, frame -> frame.type() == FrameType.MAX_DATA, Duration.ofSeconds(1));
            assertTrue(first.streamId() == 0L || second.streamId() == 0L, "stream BLOCKED should flush a session MAX_DATA");
            assertTrue(first.streamId() == streamId || second.streamId() == streamId, "stream BLOCKED should flush a stream MAX_DATA");

            peer.send(new FrameCodec.Frame(FrameType.BLOCKED, 0, 0L, Varint62.encode(0L)));
            assertFalse(peer.session().awaitTermination(Duration.ofMillis(150)), "replenishing BLOCKED should clear the prior no-op BLOCKED budget");

            stream.close();
        }
    }

    private static final class RawPeerSession implements AutoCloseable {
        private final ZmuxNativeSession session;
        private final Socket socket;
        private final BufferedInputStream input;
        private final BufferedOutputStream output;

        private RawPeerSession(ZmuxNativeSession session, Socket socket, BufferedInputStream input, BufferedOutputStream output) {
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

            AtomicReference<ZmuxNativeSession> sessionRef = new AtomicReference<>();
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
            }, "abuse-raw-session-open");
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

        ZmuxNativeSession session() {
            return session;
        }

        void send(FrameCodec.Frame frame) throws IOException {
            FrameCodec.writeFrame(output, frame, Settings.defaults().limits());
            output.flush();
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
                socket.close();
            } catch (IOException e) {
                error = e;
            }
            try {
                session.closeWithError(ErrorCode.NO_ERROR.code(), "");
            } catch (IOException e) {
                if (error == null) {
                    error = e;
                }
            }
            if (!session.awaitTermination(Duration.ofSeconds(2))) {
                throw new AssertionError("timed out terminating raw peer session");
            }
            if (error != null) {
                throw error;
            }
        }
    }
}
