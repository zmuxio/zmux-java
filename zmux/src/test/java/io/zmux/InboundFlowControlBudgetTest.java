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
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * State-advancing MAX_DATA (and BLOCKED that coincides with a grant) is mandatory flow-control progress and must
 * not consume the inbound control/mixed rate budgets; only non-advancing ones are charged (SPEC 11/13).
 */
final class InboundFlowControlBudgetTest {
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

    @Test
    void defaultConfigBulkTransferDoesNotTripControlBudget() throws Exception {
        long total = 128L << 20;
        try (SessionPair pair = SessionPair.open(ZmuxConfig.builder().build(), ZmuxConfig.builder().build())) {
            AtomicReference<Throwable> writeError = new AtomicReference<>();
            CountDownLatch writeDone = new CountDownLatch(1);
            Thread writer = new Thread(() -> {
                try {
                    ZmuxStream outbound = pair.client().openStream();
                    byte[] chunk = new byte[64 << 10];
                    for (long sent = 0L; sent < total; sent += chunk.length) {
                        outbound.write(chunk);
                    }
                    outbound.closeWrite();
                } catch (Throwable error) {
                    writeError.set(error);
                } finally {
                    writeDone.countDown();
                }
            }, "control-budget-bulk-writer");
            writer.start();

            ZmuxStream inbound = pair.server().acceptStream(Duration.ofSeconds(5));
            byte[] buffer = new byte[32 << 10];
            long received = 0L;
            int n;
            while ((n = inbound.read(buffer)) > 0) {
                received += n;
            }
            assertTrue(writeDone.await(10, TimeUnit.SECONDS), "bulk writer should finish");
            rethrow(writeError.get());
            assertEquals(total, received, "the whole transfer should arrive");
            assertFalse(pair.client().state().terminal(), "the data sender must not fail on the receiver's MAX_DATA cadence");
            assertFalse(pair.server().state().terminal(), "the receiver must not fail on the sender's BLOCKED cadence");
        }
    }

    @Test
    void stateAdvancingMaxDataIsNotChargedToControlBudget() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .inboundControlFrameBudget(16)
                .inboundMixedFrameBudget(16)
                .build();
        try (RawPeer peer = RawPeer.open(config)) {
            long limit = Settings.defaults().initialMaxData();
            for (int i = 1; i <= 4096; i++) {
                peer.send(new FrameCodec.Frame(FrameType.MAX_DATA, 0, 0L, varintPayload(limit + i)));
            }
            assertNull(peer.pollFrame(FrameType.CLOSE, 0L, Duration.ofMillis(200)),
                    "strictly increasing MAX_DATA must not trip the inbound control budget");
            assertFalse(peer.session().state().terminal());
        }
    }

    @Test
    void nonAdvancingMaxDataIsStillChargedToControlBudget() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .inboundControlFrameBudget(16)
                .noOpControlFloodThreshold(1000)
                .noOpMaxDataFloodThreshold(1000)
                .build();
        try (RawPeer peer = RawPeer.open(config)) {
            for (int i = 0; i < 17; i++) {
                peer.send(new FrameCodec.Frame(FrameType.MAX_DATA, 0, 0L, varintPayload(1L)));
            }
            FrameCodec.Frame close = peer.awaitFrame(FrameType.CLOSE, 0L, Duration.ofSeconds(1));
            FrameCodec.ErrorPayload payload = FrameCodec.parseErrorPayload(close.payload());
            assertEquals(ErrorCode.PROTOCOL.code(), payload.code());
            assertTrue(payload.reason().contains("inbound control-frame budget exceeded"), payload.reason());
        }
    }

    @Test
    void repeatedNoOpMaxDataStillTripsNoOpBudget() throws Exception {
        try (RawPeer peer = RawPeer.open(ZmuxConfig.builder().build())) {
            for (int i = 0; i < 200; i++) {
                peer.send(new FrameCodec.Frame(FrameType.MAX_DATA, 0, 0L, varintPayload(1L)));
            }
            FrameCodec.Frame close = peer.awaitFrame(FrameType.CLOSE, 0L, Duration.ofSeconds(1));
            assertEquals(ErrorCode.PROTOCOL.code(), FrameCodec.parseErrorPayload(close.payload()).code(),
                    "redundant MAX_DATA must keep the no-op flood protection");
        }
    }

    @Test
    void blockedRacingAGrantIsNotANoOpButRepeatsAre() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .noOpBlockedFloodThreshold(2)
                .noOpControlFloodThreshold(1000)
                .build();
        try (RawPeer peer = RawPeer.open(config)) {
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 4L, new byte[16_384]));
            ZmuxStream inbound = peer.session().acceptStream(Duration.ofSeconds(1));
            byte[] buffer = new byte[16_384];
            int read = 0;
            while (read < buffer.length) {
                read += inbound.read(buffer, read, buffer.length - read);
            }
            FrameCodec.Frame grant = peer.awaitFrame(FrameType.MAX_DATA, 4L, Duration.ofSeconds(1));
            long grantedLimit = Varint62.decode(grant.payload(), 0).value();

            // The sender blocked at the limit it knew before our MAX_DATA reached it: progress, not a no-op.
            peer.send(new FrameCodec.Frame(FrameType.BLOCKED, 0, 4L, varintPayload(65_536L)));
            // Repeats without any intervening grant are redundant and keep consuming the no-op budget.
            peer.send(new FrameCodec.Frame(FrameType.BLOCKED, 0, 4L, varintPayload(grantedLimit)));
            peer.send(new FrameCodec.Frame(FrameType.BLOCKED, 0, 4L, varintPayload(grantedLimit)));
            assertNull(peer.pollFrame(FrameType.CLOSE, 0L, Duration.ofMillis(200)),
                    "a BLOCKED that raced a grant must not be charged as a no-op");
            assertFalse(peer.session().state().terminal());

            peer.send(new FrameCodec.Frame(FrameType.BLOCKED, 0, 4L, varintPayload(grantedLimit)));
            FrameCodec.Frame close = peer.awaitFrame(FrameType.CLOSE, 0L, Duration.ofSeconds(1));
            assertEquals(ErrorCode.PROTOCOL.code(), FrameCodec.parseErrorPayload(close.payload()).code(),
                    "redundant BLOCKED must keep the no-op flood protection");
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
            }, "control-budget-raw-open");
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
            }, "control-budget-client-open");
            Thread serverThread = new Thread(() -> {
                try {
                    serverRef.set(Zmux.server(rightConn, serverConfig));
                } catch (Throwable error) {
                    errorRef.compareAndSet(null, error);
                } finally {
                    established.countDown();
                }
            }, "control-budget-server-open");
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
