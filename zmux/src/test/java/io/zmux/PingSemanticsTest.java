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

import static org.junit.jupiter.api.Assertions.*;

final class PingSemanticsTest {
    private static ZmuxConfig defaultConfig() {
        return ZmuxConfig.builder().build();
    }

    private static ZmuxConfig configWithSettings(Settings settings) {
        return ZmuxConfig.builder()
                .settings(settings)
                .build();
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
    void pingPayloadIsBoundedByMinLocalAndPeerControlLimits() throws Exception {
        Settings localSettings = Settings.defaults().toBuilder()
                .maxControlPayloadBytes(5000L)
                .build();
        Settings peerSettings = Settings.defaults().toBuilder()
                .maxControlPayloadBytes(4096L)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(configWithSettings(localSettings), 0L, peerSettings)) {
            byte[] oversizedEcho = new byte[4089];
            IOException error = assertThrows(IOException.class, () ->
                    peer.session().ping(oversizedEcho, Duration.ofSeconds(1))
            );
            ZmuxException frameSize = assertInstanceOf(ZmuxException.class, error);
            assertEquals(ErrorCode.FRAME_SIZE.code(), frameSize.code(), "oversized PING should surface FRAME_SIZE");
        }
    }

    @Test
    void secondPingWaitsForOutstandingPing() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(defaultConfig(), 0L)) {
            AtomicReference<Duration> firstRtt = new AtomicReference<>();
            AtomicReference<Duration> secondRtt = new AtomicReference<>();
            AtomicReference<Throwable> firstError = new AtomicReference<>();
            AtomicReference<Throwable> secondError = new AtomicReference<>();

            Thread first = new Thread(() -> {
                try {
                    firstRtt.set(peer.session().ping("one".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(2)));
                } catch (Throwable t) {
                    firstError.set(t);
                }
            }, "ping-first");
            first.start();

            FrameCodec.Frame firstPing = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.PING, firstPing.type(), "first control frame should be PING");

            Thread second = new Thread(() -> {
                try {
                    secondRtt.set(peer.session().ping("two".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(2)));
                } catch (Throwable t) {
                    secondError.set(t);
                }
            }, "ping-second");
            second.start();

            assertNull(peer.pollFrame(Duration.ofMillis(150)), "second PING must not be emitted while the first is outstanding");

            peer.send(new FrameCodec.Frame(FrameType.PONG, 0, 0L, firstPing.payload()));

            FrameCodec.Frame secondPing = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.PING, secondPing.type(), "second PING should be emitted after the first completes");
            peer.send(new FrameCodec.Frame(FrameType.PONG, 0, 0L, secondPing.payload()));

            first.join(Duration.ofSeconds(2).toMillis());
            second.join(Duration.ofSeconds(2).toMillis());
            assertFalse(first.isAlive(), "first ping thread should finish");
            assertFalse(second.isAlive(), "second ping thread should finish");

            rethrow(firstError.get());
            rethrow(secondError.get());
            assertNotNull(firstRtt.get(), "first ping should complete with an RTT");
            assertNotNull(secondRtt.get(), "second ping should complete with an RTT");
        }
    }

    @Test
    void pingFailsWhenSessionCloses() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(defaultConfig(), 0L)) {
            AtomicReference<Duration> pingRtt = new AtomicReference<>();
            AtomicReference<Throwable> pingError = new AtomicReference<>();

            Thread ping = new Thread(() -> {
                try {
                    pingRtt.set(peer.session().ping("close".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(2)));
                } catch (Throwable t) {
                    pingError.set(t);
                }
            }, "ping-close");
            ping.start();

            FrameCodec.Frame pingFrame = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.PING, pingFrame.type(), "expected a PING before peer CLOSE");

            peer.send(new FrameCodec.Frame(
                    FrameType.CLOSE,
                    0,
                    0L,
                    FrameCodec.buildErrorPayload(ErrorCode.NO_ERROR.code(), "", Settings.defaults().maxControlPayloadBytes())
            ));

            ping.join(Duration.ofSeconds(2).toMillis());
            assertFalse(ping.isAlive(), "ping should finish when the session closes");
            assertNull(pingRtt.get(), "ping must not report success after session close");
            Throwable error = pingError.get();
            assertNotNull(error, "ping should fail when the session closes");
            assertInstanceOf(IOException.class, error, "ping close error should surface as IOException");
        }
    }

    @Test
    void pingCopiesCallerPayloadBeforeQueueingFrame() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(defaultConfig(), 0L)) {
            byte[] echo = "ping-echo".getBytes(StandardCharsets.UTF_8);
            byte[] expectedSuffix = echo.clone();
            AtomicReference<Duration> pingRtt = new AtomicReference<>();
            AtomicReference<Throwable> pingError = new AtomicReference<>();

            Thread ping = new Thread(() -> {
                try {
                    pingRtt.set(peer.session().ping(echo, Duration.ofSeconds(2)));
                } catch (Throwable t) {
                    pingError.set(t);
                }
            }, "ping-copy-payload");
            ping.start();

            FrameCodec.Frame pingFrame = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.PING, pingFrame.type(), "expected a PING frame");

            echo[0] = (byte) 'X';
            byte[] queuedPayload = pingFrame.payload();
            assertTrue(
                    queuedPayload.length >= 16 + expectedSuffix.length,
                    "default padded PING should include nonce, padding tag, caller echo, and optional random padding"
            );
            byte[] queuedEcho = java.util.Arrays.copyOfRange(queuedPayload, 16, 16 + expectedSuffix.length);
            assertArrayEquals(expectedSuffix, queuedEcho, "queued PING should preserve the caller payload snapshot");

            peer.send(new FrameCodec.Frame(FrameType.PONG, 0, 0L, pingFrame.payload()));
            ping.join(Duration.ofSeconds(2).toMillis());
            assertFalse(ping.isAlive(), "ping should finish after matching PONG");
            rethrow(pingError.get());
            assertNotNull(pingRtt.get(), "ping should still complete successfully");
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
            return open(sessionConfig, rawCapabilities, Settings.defaults());
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
            }, "ping-raw-session-open");
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

        ZmuxNativeSession session() {
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
