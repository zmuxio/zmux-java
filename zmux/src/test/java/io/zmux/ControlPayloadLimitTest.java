package io.zmux;

import io.zmux.internal.FrameCodec;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ControlPayloadLimitTest {
    private static void rethrow(Throwable error) throws Exception {
        if (error == null) {
            return;
        }
        if (error instanceof Exception) {
            throw (Exception) error;
        }
        throw new RuntimeException(error);
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }

    @Test
    void sessionControlReasonsUsePeerControlPayloadLimit() throws Exception {
        Settings peerSettings = Settings.builder()
                .maxControlPayloadBytes(4096L)
                .build();
        String reason = repeat('x', 5000);

        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L, peerSettings)) {
            peer.session().goAway(0L, 0L, ErrorCode.INTERNAL.code(), reason);

            FrameCodec.Frame goAway = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.GOAWAY, goAway.type(), "expected GOAWAY frame");
            assertTrue(goAway.payload().length <= 4096, "GOAWAY payload should respect the peer control-payload limit");
            FrameCodec.GoAwayPayload goAwayPayload = FrameCodec.parseGoAwayPayload(goAway.payload());
            assertEquals(reason.substring(0, 4090), goAwayPayload.reason(), "GOAWAY reason should be UTF-8-safe truncated to the peer limit");

            peer.session().closeWithError(ErrorCode.INTERNAL.code(), reason);

            FrameCodec.Frame close = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.CLOSE, close.type(), "expected CLOSE frame");
            assertTrue(close.payload().length <= 4096, "CLOSE payload should respect the peer control-payload limit");
            FrameCodec.ErrorPayload closePayload = FrameCodec.parseErrorPayload(close.payload());
            assertEquals(reason.substring(0, 4092), closePayload.reason(), "CLOSE reason should be UTF-8-safe truncated to the peer limit");
            assertEquals(ErrorCode.INTERNAL.code(), closePayload.code(), "CLOSE code mismatch");

            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "session should terminate after closeWithError");
            assertEquals(SessionState.FAILED, peer.session().state(), "non-zero CLOSE should leave the session in FAILED");
        }
    }

    @Test
    void outboundControlReasonsMayUsePeerLimitWhenPeerExceedsLocalLimit() throws Exception {
        Settings peerSettings = Settings.builder()
                .maxControlPayloadBytes(8192L)
                .build();
        String reason = repeat('x', 9000);

        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L, peerSettings)) {
            peer.session().goAway(0L, 0L, ErrorCode.INTERNAL.code(), reason);

            FrameCodec.Frame goAway = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.GOAWAY, goAway.type(), "expected GOAWAY frame");
            assertTrue(goAway.payload().length > 4096, "GOAWAY should not be capped by the sender's local control-payload limit");
            assertTrue(goAway.payload().length <= 8192, "GOAWAY payload should respect the peer control-payload limit");
            FrameCodec.GoAwayPayload goAwayPayload = FrameCodec.parseGoAwayPayload(goAway.payload());
            assertTrue(goAwayPayload.reason().length() > 4090, "GOAWAY reason should preserve bytes allowed by the peer limit");

            peer.session().closeWithError(ErrorCode.INTERNAL.code(), reason);

            FrameCodec.Frame close = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.CLOSE, close.type(), "expected CLOSE frame");
            assertTrue(close.payload().length > 4096, "CLOSE should not be capped by the sender's local control-payload limit");
            assertTrue(close.payload().length <= 8192, "CLOSE payload should respect the peer control-payload limit");
            FrameCodec.ErrorPayload closePayload = FrameCodec.parseErrorPayload(close.payload());
            assertTrue(closePayload.reason().length() > 4092, "CLOSE reason should preserve bytes allowed by the peer limit");
        }
    }

    private static final class RawPeerSession implements AutoCloseable {
        private final ZmuxNativeSession session;
        private final Socket socket;
        private final BufferedInputStream input;
        private final BufferedOutputStream output;
        private final Limits readLimits;

        private RawPeerSession(ZmuxNativeSession session,
                               Socket socket,
                               BufferedInputStream input,
                               BufferedOutputStream output,
                               Limits readLimits) {
            this.session = session;
            this.socket = socket;
            this.input = input;
            this.output = output;
            this.readLimits = readLimits;
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
            }, "control-payload-limit-open");
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
            return new RawPeerSession(sessionRef.get(), peerSocket, peerInput, peerOutput, rawSettings.limits());
        }

        ZmuxNativeSession session() {
            return session;
        }

        FrameCodec.Frame readFrame(Duration timeout) throws Exception {
            socket.setSoTimeout((int) timeout.toMillis());
            try {
                return FrameCodec.readFrame(input, readLimits);
            } catch (SocketTimeoutException e) {
                throw new AssertionError("timed out waiting for frame", e);
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
