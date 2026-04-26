package io.zmux;

import io.zmux.internal.FrameCodec;
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
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class PeerReasonBudgetTest {
    private static void await(Duration timeout, BooleanSupplier condition, String description) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("timed out waiting for " + description);
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
    void peerGoAwayReasonIsTrimmedAndReplacingItReleasesOldBytes() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .retainedPeerReasonBytesBudget(2L)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            peer.send(new FrameCodec.Frame(
                    FrameType.GOAWAY,
                    0,
                    0L,
                    FrameCodec.buildGoAwayPayload(0L, 0L, ErrorCode.NO_ERROR.code(), "first", Settings.defaults().maxControlPayloadBytes())
            ));
            await(Duration.ofSeconds(1), () -> {
                ApplicationError error = peer.session().peerGoAwayError();
                return error != null && "fi".equals(error.reason());
            }, "trimmed GOAWAY reason");

            assertEquals(2L, peer.session().stats().retainedPeerReasonBytes(), "GOAWAY reason bytes should count against the retained peer-reason budget");
            assertEquals(2L, peer.session().stats().retainedPeerReasonBudget(), "peer-reason budget mismatch");

            peer.send(new FrameCodec.Frame(
                    FrameType.GOAWAY,
                    0,
                    0L,
                    FrameCodec.buildGoAwayPayload(0L, 0L, ErrorCode.NO_ERROR.code(), "xy", Settings.defaults().maxControlPayloadBytes())
            ));
            await(Duration.ofSeconds(1), () -> {
                ApplicationError error = peer.session().peerGoAwayError();
                return error != null && "xy".equals(error.reason());
            }, "replacement GOAWAY reason");

            assertEquals("xy", peer.session().peerGoAwayError().reason(), "later GOAWAY should replace the retained reason");
            assertEquals(2L, peer.session().stats().retainedPeerReasonBytes(), "replacing GOAWAY reason should release the old retained bytes first");
        }
    }

    @Test
    void peerResetReasonIsTrimmedAndReleasedAfterTerminalCompaction() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .retainedPeerReasonBytesBudget(2L)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    0,
                    2L,
                    "payload".getBytes(StandardCharsets.UTF_8)
            ));

            ZmuxRecvStream accepted = peer.session().acceptUniStream(Duration.ofSeconds(1));

            peer.send(new FrameCodec.Frame(
                    FrameType.RESET,
                    0,
                    2L,
                    FrameCodec.buildErrorPayload(ErrorCode.CANCELLED.code(), "abcd", Settings.defaults().maxControlPayloadBytes())
            ));

            await(Duration.ofSeconds(1), () -> peer.session().stats().retainedPeerReasonBytes() == 0L, "RESET reason release after terminal compaction");

            ApplicationError error = readUntilApplicationError(accepted);
            assertEquals("ab", error.reason(), "RESET reason should be trimmed to the retained peer-reason budget");
            assertEquals(0L, peer.session().stats().retainedPeerReasonBytes(), "terminal compaction should release retained RESET reason bytes");
        }
    }

    private static ApplicationError readUntilApplicationError(ZmuxRecvStream stream) throws IOException {
        byte[] buffer = new byte[32];
        try {
            stream.read(buffer);
            return assertThrows(ApplicationError.class, () -> stream.read(buffer), "peer RESET should surface an ApplicationError");
        } catch (ApplicationError error) {
            return error;
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
            }, "peer-reason-budget-raw-open");
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
                session.close();
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
