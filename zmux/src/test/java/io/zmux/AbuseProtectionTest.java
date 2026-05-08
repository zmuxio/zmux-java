package io.zmux;

import io.zmux.protocol.Frame;
import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;
import io.zmux.protocol.Preface;
import io.zmux.protocol.Protocol;
import io.zmux.protocol.Varint62;
import io.zmux.transport.BasicDuplexConnection;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
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

final class AbuseProtectionTest {
    private static ZmuxConfig hiddenAbortChurnConfig(int threshold, Duration window) {
        return ZmuxConfig.builder()
                .hiddenAbortChurnThreshold(threshold)
                .hiddenAbortChurnWindow(window)
                .build();
    }

    private static void sendAbort(RawPeerSession peer, long streamId) throws IOException {
        peer.send(new FrameCodec.Frame(
                FrameType.ABORT,
                0,
                streamId,
                FrameCodec.buildErrorPayload(ErrorCode.CANCELLED.code(), "", Settings.defaults().maxControlPayloadBytes())
        ));
    }

    private static void await(Duration timeout, BooleanSupplier condition, String description) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10L);
        }
        assertTrue(condition.getAsBoolean(), description);
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
    void zeroLengthDataNoOpFloodFailsSession() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(
                ZmuxConfig.builder()
                        .abuseWindow(Duration.ofHours(1))
                        .noOpZeroDataFloodThreshold(1)
                        .build(),
                0L
        )) {
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 4L, new byte[0]));
            peer.session().acceptStream(Duration.ofSeconds(1));

            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 4L, new byte[0]));
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 4L, new byte[0]));

            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "session should fail after repeated no-op DATA");
            assertEquals(SessionState.FAILED, peer.session().state(), "no-op DATA flood should fail the session");
        }
    }

    @Test
    void zeroLengthDataCounterResetsAfterProgress() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(
                ZmuxConfig.builder()
                        .abuseWindow(Duration.ofHours(1))
                        .noOpZeroDataFloodThreshold(1)
                        .build(),
                0L
        )) {
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 4L, new byte[0]));
            peer.session().acceptStream(Duration.ofSeconds(1));

            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 4L, new byte[0]));
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 4L, "x".getBytes(StandardCharsets.UTF_8)));
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 4L, new byte[0]));

            assertFalse(peer.session().awaitTermination(Duration.ofMillis(200)), "progressing DATA should reset the no-op counter");
        }
    }

    @Test
    void inboundControlBudgetFailsSession() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(
                ZmuxConfig.builder()
                        .abuseWindow(Duration.ofHours(1))
                        .inboundControlFrameBudget(1)
                        .build(),
                0L
        )) {
            byte[] pong = "12345678".getBytes(StandardCharsets.UTF_8);
            peer.send(new FrameCodec.Frame(FrameType.PONG, 0, 0L, pong));
            peer.send(new FrameCodec.Frame(FrameType.PONG, 0, 0L, pong));

            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "control-frame budget should fail the session");
            assertEquals(SessionState.FAILED, peer.session().state(), "control-frame budget overflow should fail the session");
        }
    }

    @Test
    void mixedBudgetCountsControlAndExtTogether() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(
                ZmuxConfig.builder()
                        .abuseWindow(Duration.ofHours(1))
                        .inboundMixedFrameBudget(1)
                        .build(),
                0L
        )) {
            peer.send(new FrameCodec.Frame(FrameType.PONG, 0, 0L, "12345678".getBytes(StandardCharsets.UTF_8)));
            peer.send(new FrameCodec.Frame(FrameType.EXT, 0, 0L, Varint62.encode(99L)));

            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "mixed control/EXT budget should fail the session");
            assertEquals(SessionState.FAILED, peer.session().state(), "mixed budget overflow should fail the session");
        }
    }

    @Test
    void visibleTerminalChurnFailsSession() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(
                ZmuxConfig.builder()
                        .visibleTerminalChurnWindow(Duration.ofHours(1))
                        .visibleTerminalChurnThreshold(1)
                        .build(),
                0L
        )) {
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 4L, new byte[0]));
            peer.send(new FrameCodec.Frame(
                    FrameType.ABORT,
                    0,
                    4L,
                    FrameCodec.buildErrorPayload(ErrorCode.CANCELLED.code(), "", Settings.defaults().maxControlPayloadBytes())
            ));
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 8L, new byte[0]));
            peer.send(new FrameCodec.Frame(
                    FrameType.ABORT,
                    0,
                    8L,
                    FrameCodec.buildErrorPayload(ErrorCode.CANCELLED.code(), "", Settings.defaults().maxControlPayloadBytes())
            ));

            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "visible terminal churn should fail the session");
            assertEquals(SessionState.FAILED, peer.session().state(), "visible terminal churn overflow should fail the session");
        }
    }

    @Test
    void hiddenAbortChurnFailsSession() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(hiddenAbortChurnConfig(1, Duration.ofHours(1)), 0L)) {
            sendAbort(peer, 4L);
            sendAbort(peer, 8L);

            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "hidden abort churn should fail the session");
            assertEquals(SessionState.FAILED, peer.session().state(), "hidden abort churn overflow should fail the session");
        }
    }

    @Test
    void hiddenAbortChurnWindowExpiryResetsCounter() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(hiddenAbortChurnConfig(1, Duration.ofMillis(50)), 0L)) {
            sendAbort(peer, 4L);
            await(
                    Duration.ofSeconds(1),
                    () -> peer.session().stats().pressure().retainedStateBreakdown().hiddenControl().count() == 1L,
                    "first hidden abort was not retained"
            );
            Thread.sleep(80L);
            sendAbort(peer, 8L);

            assertFalse(peer.session().awaitTermination(Duration.ofMillis(200)), "hidden abort churn window expiry should reset the counter");
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
