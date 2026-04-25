package io.zmux;

import io.zmux.internal.FrameCodec;
import io.zmux.internal.Varint62;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProtocolCloseTest {
    private static void rethrow(Throwable error) throws Exception {
        if (error == null) {
            return;
        }
        if (error instanceof Exception) {
            throw (Exception) error;
        }
        throw new RuntimeException(error);
    }

    private static void appendTlv(ByteArrayOutputStream output, long type, byte[] value) throws IOException {
        Varint62.write(output, type);
        Varint62.write(output, value.length);
        output.write(value);
    }

    private static byte[] duplicateStandardDiagPayload(byte[] basePayload, String reason) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        payload.write(basePayload);
        appendTlv(payload, Protocol.DIAG_RETRY_AFTER_MILLIS, Varint62.encode(1L));
        appendTlv(payload, Protocol.DIAG_RETRY_AFTER_MILLIS, Varint62.encode(2L));
        if (!reason.isEmpty()) {
            appendTlv(payload, Protocol.DIAG_DEBUG_TEXT, reason.getBytes(StandardCharsets.UTF_8));
        }
        return payload.toByteArray();
    }

    private static byte[] invalidUtf8DiagPayload(byte[] basePayload) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        payload.write(basePayload);
        appendTlv(payload, Protocol.DIAG_DEBUG_TEXT, new byte[]{(byte) 0xe2, (byte) 0x82});
        return payload.toByteArray();
    }

    @Test
    void fatalReadProtocolErrorEmitsClose() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(
                ZmuxConfig.builder().build(),
                Protocol.CAPABILITY_OPEN_METADATA
        )) {
            byte[] prefix = FrameCodec.buildOpenMetadataPrefix(
                    Protocol.CAPABILITY_OPEN_METADATA,
                    null,
                    null,
                    "meta".getBytes(StandardCharsets.UTF_8),
                    Settings.defaults().maxFramePayload()
            );
            byte[] appData = "x".getBytes(StandardCharsets.UTF_8);
            byte[] payload = new byte[prefix.length + appData.length];
            System.arraycopy(prefix, 0, payload, 0, prefix.length);
            System.arraycopy(appData, 0, payload, prefix.length, appData.length);

            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    Protocol.FRAME_FLAG_OPEN_METADATA,
                    4L,
                    payload
            ));

            FrameCodec.Frame close = peer.awaitFrameType(FrameType.CLOSE, Duration.ofSeconds(1));
            FrameCodec.ErrorPayload closePayload = FrameCodec.parseErrorPayload(close.payload());
            assertEquals(ErrorCode.PROTOCOL.code(), closePayload.code(), "fatal protocol error should emit CLOSE(PROTOCOL)");
            assertTrue(closePayload.reason().contains("OPEN_METADATA"), "close reason should describe the protocol error");

            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "session should terminate after emitting CLOSE");
            assertEquals(SessionState.FAILED, peer.session().state(), "fatal protocol error should leave the session in FAILED");
        }
    }

    @Test
    void unexpectedPongCountsTowardNoOpControlFlood() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .noOpControlFloodThreshold(1)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            byte[] pongPayload = "12345678".getBytes(StandardCharsets.UTF_8);
            peer.send(new FrameCodec.Frame(FrameType.PONG, 0, 0L, pongPayload));
            peer.send(new FrameCodec.Frame(FrameType.PONG, 0, 0L, pongPayload));

            FrameCodec.Frame close = peer.awaitFrameType(FrameType.CLOSE, Duration.ofSeconds(1));
            FrameCodec.ErrorPayload closePayload = FrameCodec.parseErrorPayload(close.payload());
            assertEquals(ErrorCode.PROTOCOL.code(), closePayload.code(), "repeated unexpected PONG should emit CLOSE(PROTOCOL)");
            assertTrue(closePayload.reason().contains("no-op"), "close reason should report the no-op control flood");

            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "session should terminate after repeated unexpected PONG");
            assertEquals(SessionState.FAILED, peer.session().state(), "unexpected PONG flood should fail the session");
        }
    }

    @Test
    void peerCloseDuplicateStandardDiagDropsReasonButKeepsPrimarySemantics() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L)) {
            byte[] payload = duplicateStandardDiagPayload(
                    Varint62.encode(ErrorCode.PROTOCOL.code()),
                    "peer close"
            );
            peer.send(new FrameCodec.Frame(FrameType.CLOSE, 0, 0L, payload));

            ApplicationError error = assertThrows(
                    ApplicationError.class,
                    () -> peer.session().awaitTerminationOrThrow(Duration.ofSeconds(1)),
                    "peer CLOSE with duplicate singleton DIAG should still terminate with the structured peer-close error"
            );
            assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "returned peer close code mismatch");
            assertEquals("", error.reason(), "duplicate singleton DIAG should clear the returned peer close reason");
            assertEquals(SessionState.FAILED, peer.session().state(), "non-zero peer CLOSE should still fail the session");

            ZmuxNativeSession session = assertInstanceOf(
                    ZmuxNativeSession.class,
                    peer.session(),
                    "raw session should expose the native session surface for peer-close diagnostics"
            );
            ApplicationError peerCloseError = session.peerCloseError();
            assertNotNull(peerCloseError, "peer close should still be retained on the session surface");
            assertEquals(ErrorCode.PROTOCOL.code(), peerCloseError.code(), "retained peer close code mismatch");
            assertEquals("", peerCloseError.reason(), "duplicate singleton DIAG should clear the retained peer close reason");
        }
    }

    @Test
    void peerCloseInvalidUtf8DiagDropsReasonButKeepsPrimarySemantics() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L)) {
            peer.send(new FrameCodec.Frame(
                    FrameType.CLOSE,
                    0,
                    0L,
                    invalidUtf8DiagPayload(Varint62.encode(ErrorCode.PROTOCOL.code()))
            ));

            ApplicationError error = assertThrows(
                    ApplicationError.class,
                    () -> peer.session().awaitTerminationOrThrow(Duration.ofSeconds(1))
            );
            assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "returned peer close code mismatch");
            assertEquals("", error.reason(), "invalid UTF-8 DIAG should clear the returned peer close reason");
            assertEquals(SessionState.FAILED, peer.session().state(), "non-zero peer CLOSE should still fail the session");

            ZmuxNativeSession session = (ZmuxNativeSession) peer.session();
            ApplicationError peerCloseError = session.peerCloseError();
            assertNotNull(peerCloseError, "peer close should still be retained on the session surface");
            assertEquals(ErrorCode.PROTOCOL.code(), peerCloseError.code(), "retained peer close code mismatch");
            assertEquals("", peerCloseError.reason(), "invalid UTF-8 DIAG should clear the retained peer close reason");
        }
    }

    @Test
    void closeWithWrappedStructuredErrorPreservesWireCodeAndReason() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L)) {
            peer.session().closeWithError(new IOException(
                    "wrapped close failure",
                    new ApplicationError(
                            ErrorCode.FRAME_SIZE,
                            "bad frame",
                            ZmuxErrorScope.SESSION,
                            ZmuxErrorSource.LOCAL,
                            ZmuxErrorDirection.BOTH,
                            ZmuxTerminationKind.SESSION_TERMINATION,
                            "close"
                    )
            ));

            FrameCodec.Frame close = peer.awaitFrameType(FrameType.CLOSE, Duration.ofSeconds(1));
            FrameCodec.ErrorPayload payload = FrameCodec.parseErrorPayload(close.payload());
            assertEquals(ErrorCode.FRAME_SIZE.code(), payload.code(), "wrapped structured closeWithError should preserve the wire error code");
            assertEquals("bad frame", payload.reason(), "wrapped structured closeWithError should preserve the structured reason");

            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "session should terminate after emitting the local CLOSE");
            assertEquals(SessionState.FAILED, peer.session().state(), "non-zero local CLOSE should leave the session in FAILED");
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
            }, "protocol-close-raw-open");
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

        FrameCodec.Frame pollFrame(Duration timeout) throws IOException {
            socket.setSoTimeout((int) timeout.toMillis());
            try {
                return FrameCodec.readFrame(input, Settings.defaults().limits());
            } catch (SocketTimeoutException e) {
                return null;
            }
        }

        FrameCodec.Frame awaitFrameType(FrameType expected, Duration timeout) throws Exception {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                FrameCodec.Frame frame = pollFrame(Duration.ofMillis(20));
                if (frame != null && frame.type() == expected) {
                    return frame;
                }
            }
            throw new AssertionError("timed out waiting for frame type " + expected);
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
