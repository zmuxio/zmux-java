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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class KeepaliveTest {
    private static FrameCodec.Frame awaitFrameType(RawPeerSession peer, FrameType expected, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        ArrayList<FrameCodec.Frame> deferred = new ArrayList<>();
        while (System.nanoTime() < deadline) {
            long remainingNanos = deadline - System.nanoTime();
            long sliceMillis = Math.max(
                    1L,
                    Math.min(Duration.ofMillis(200).toNanos(), remainingNanos) / 1_000_000L
            );
            FrameCodec.Frame frame = peer.pollFrame(Duration.ofMillis(sliceMillis));
            if (frame == null) {
                continue;
            }
            if (frame.type() == expected) {
                peer.prependFrames(deferred);
                return frame;
            }
            deferred.add(frame);
        }
        peer.prependFrames(deferred);
        throw new AssertionError("timed out waiting for frame type " + expected);
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
    void outboundWriteDoesNotMaskReadIdleKeepaliveProbe() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .keepaliveInterval(Duration.ofMillis(80))
                .keepaliveTimeout(Duration.ofMillis(500))
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            Thread.sleep(40L);

            ZmuxSendStream stream = peer.session().openUniStream();
            stream.writeFinal("x".getBytes(StandardCharsets.UTF_8));

            FrameCodec.Frame data = awaitFrameType(peer, FrameType.DATA, Duration.ofSeconds(1));
            assertEquals(FrameType.DATA, data.type(), "first emitted frame should be DATA");

            FrameCodec.Frame ping = awaitFrameType(peer, FrameType.PING, Duration.ofSeconds(2));
            assertNotNull(ping, "read-idle keepalive should still fire even when outbound DATA recently reset write-idle");
            peer.send(new FrameCodec.Frame(FrameType.PONG, 0, 0L, ping.payload()));
        }
    }

    @Test
    void keepaliveTimeoutEmitsCloseWithIdleTimeout() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .keepaliveInterval(Duration.ofMillis(40))
                .keepaliveTimeout(Duration.ofMillis(70))
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            FrameCodec.Frame ping = awaitFrameType(peer, FrameType.PING, Duration.ofSeconds(2));
            assertNotNull(ping, "keepalive should emit PING when idle");

            FrameCodec.Frame close = awaitFrameType(peer, FrameType.CLOSE, Duration.ofSeconds(2));
            FrameCodec.ErrorPayload payload = FrameCodec.parseErrorPayload(close.payload());
            assertEquals(ErrorCode.IDLE_TIMEOUT.code(), payload.code(), "keepalive timeout should emit CLOSE(IDLE_TIMEOUT)");
            assertEquals("zmux: keepalive timeout", payload.reason(), "keepalive timeout reason mismatch");

            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "session should terminate after keepalive timeout");
            assertEquals(SessionState.FAILED, peer.session().state(), "keepalive timeout should transition to FAILED after local CLOSE is written");
        }
    }

    @Test
    void keepaliveMaxPingIntervalTriggersBeforeIdleDeadline() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .keepaliveInterval(Duration.ofSeconds(1))
                .keepaliveMaxPingInterval(Duration.ofMillis(80))
                .keepaliveTimeout(Duration.ofMillis(500))
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            FrameCodec.Frame ping = awaitFrameType(peer, FrameType.PING, Duration.ofSeconds(2));
            assertNotNull(ping, "max ping interval should trigger a keepalive ping before the idle interval expires");
            peer.send(new FrameCodec.Frame(FrameType.PONG, 0, 0L, ping.payload()));
        }
    }

    private static final class RawPeerSession implements AutoCloseable {
        private final ZmuxSession session;
        private final Socket socket;
        private final BufferedInputStream input;
        private final BufferedOutputStream output;
        private final ArrayDeque<FrameCodec.Frame> pendingFrames = new ArrayDeque<>();

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
            }, "keepalive-raw-session-open");
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

        FrameCodec.Frame readFrame(Duration timeout) throws Exception {
            socket.setSoTimeout((int) timeout.toMillis());
            try {
                return FrameCodec.readFrame(input, Settings.defaults().limits());
            } catch (SocketTimeoutException e) {
                throw new AssertionError("timed out waiting for frame", e);
            }
        }

        FrameCodec.Frame pollFrame(Duration timeout) throws IOException {
            FrameCodec.Frame pending = pendingFrames.pollFirst();
            if (pending != null) {
                return pending;
            }
            socket.setSoTimeout((int) timeout.toMillis());
            try {
                return FrameCodec.readFrame(input, Settings.defaults().limits());
            } catch (SocketTimeoutException e) {
                return null;
            }
        }

        void prependFrames(List<FrameCodec.Frame> frames) {
            for (int i = frames.size() - 1; i >= 0; --i) {
                pendingFrames.addFirst(frames.get(i));
            }
        }

        FrameCodec.Frame pollNonDataFrame(Duration timeout) throws IOException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                FrameCodec.Frame frame = pollFrame(Duration.ofMillis(10));
                if (frame == null) {
                    continue;
                }
                if (frame.type() != FrameType.DATA) {
                    return frame;
                }
            }
            return null;
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
