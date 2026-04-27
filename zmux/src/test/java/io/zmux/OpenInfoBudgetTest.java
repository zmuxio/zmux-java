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
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class OpenInfoBudgetTest {
    private static final long OPEN_METADATA = Protocol.CAPABILITY_OPEN_METADATA;

    private static FrameCodec.Frame openDataFrame(long streamId, String openInfo, String body) throws IOException {
        byte[] metadata = FrameCodec.buildOpenMetadataPrefix(
                OPEN_METADATA,
                null,
                null,
                openInfo.getBytes(StandardCharsets.UTF_8),
                Settings.defaults().maxFramePayload()
        );
        byte[] data = body.getBytes(StandardCharsets.UTF_8);
        byte[] payload = Arrays.copyOf(metadata, metadata.length + data.length);
        System.arraycopy(data, 0, payload, metadata.length, data.length);
        return new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_OPEN_METADATA, streamId, payload);
    }

    private static String readUtf8(ZmuxStream stream) throws IOException {
        byte[] buf = new byte[64];
        int n = stream.read(buf);
        if (n < 0) {
            return "";
        }
        return new String(buf, 0, n, StandardCharsets.UTF_8);
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
    void visibleAcceptBacklogShedsNewestWhenOpenInfoBudgetExceeded() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .capabilities(OPEN_METADATA)
                .retainedOpenInfoBytesBudget(3L)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, OPEN_METADATA)) {
            peer.send(openDataFrame(4L, "aa", "a"));
            peer.send(openDataFrame(8L, "bb", "b"));

            FrameCodec.Frame refused = peer.awaitFrameType(FrameType.ABORT, Duration.ofSeconds(1));
            assertEquals(8L, refused.streamId(), "open_info budget should refuse newest visible stream");
            FrameCodec.ErrorPayload payload = FrameCodec.parseErrorPayload(refused.payload());
            assertEquals(ErrorCode.REFUSED_STREAM.code(), payload.code(), "open_info budget should use REFUSED_STREAM");

            SessionStats stats = peer.session().stats();
            assertEquals(1L, stats.acceptBacklog().count(), "accept backlog should retain only the oldest visible stream");
            assertEquals(1L, stats.acceptBacklog().refused(), "accept backlog refused counter mismatch");
            assertEquals(2L, stats.retainedOpenInfoBytes(), "retained open_info bytes should drop with the refused stream");
            assertEquals(3L, stats.retainedOpenInfoBudget(), "retained open_info budget mismatch");

            ZmuxStream accepted = peer.session().acceptStream(Duration.ofSeconds(1));
            assertEquals("a", readUtf8(accepted), "oldest visible stream should remain readable");
            assertArrayEquals("aa".getBytes(StandardCharsets.UTF_8), accepted.metadata().openInfo(), "accepted stream open_info mismatch");
            assertFalse(peer.session().state().terminal(), "open_info shedding must not fail the session");
        }
    }

    @Test
    void localOpenBudgetReleasesBytesWhenProvisionalFails() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .capabilities(OPEN_METADATA)
                .retainedOpenInfoBytesBudget(3L)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, OPEN_METADATA)) {
            ZmuxStream first = peer.session().openStream(new OpenOptions(null, null, "ssh".getBytes(StandardCharsets.UTF_8)));
            assertEquals(3L, peer.session().stats().retainedOpenInfoBytes(), "first provisional open should retain its open_info bytes");

            OpenLimitedException error = assertThrows(
                    OpenLimitedException.class,
                    () -> peer.session().openStream(new OpenOptions(null, null, "x".getBytes(StandardCharsets.UTF_8))),
                    "second provisional open should be limited by the retained open_info budget"
            );
            ZmuxException cause = assertInstanceOf(ZmuxException.class, error.getCause());
            assertEquals(OpenLimitedException.MESSAGE, error.getMessage(), "local open budget error mismatch");
            assertEquals(ErrorCode.STREAM_LIMIT.code(), cause.code(), "local open budget cause code mismatch");
            assertEquals("open", cause.operation(), "local open budget cause operation mismatch");
            assertEquals("zmux: open_info budget exceeded", cause.getMessage(), "local open budget cause mismatch");
            assertEquals(ZmuxErrorScope.SESSION, cause.scope(), "local open budget cause scope mismatch");
            assertEquals(ZmuxErrorSource.LOCAL, cause.source(), "local open budget cause source mismatch");
            assertEquals(ZmuxErrorDirection.BOTH, cause.direction(), "local open budget cause direction mismatch");
            assertEquals(ZmuxErrorScope.SESSION, error.scope(), "open_info budget limit scope mismatch");
            assertEquals(ZmuxErrorSource.LOCAL, error.source(), "open_info budget limit source mismatch");
            assertEquals(ZmuxErrorDirection.BOTH, error.direction(), "open_info budget limit direction mismatch");

            first.cancelWrite(ErrorCode.CANCELLED.code());
            assertEquals(0L, peer.session().stats().retainedOpenInfoBytes(), "failing a provisional open should release retained open_info bytes");

            ZmuxStream replacement = peer.session().openStream(new OpenOptions(null, null, "x".getBytes(StandardCharsets.UTF_8)));
            assertArrayEquals("x".getBytes(StandardCharsets.UTF_8), replacement.metadata().openInfo(), "replacement provisional open should succeed after budget release");
        }
    }

    @Test
    void localOpenRejectsOpenInfoWithoutNegotiatedOpenMetadata() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L)) {
            OpenInfoUnavailableException error = assertInstanceOf(
                    OpenInfoUnavailableException.class,
                    assertThrows(
                            IOException.class,
                            () -> peer.session().openStream(new OpenOptions(null, null, "need-metadata".getBytes(StandardCharsets.UTF_8))),
                            "open_info without open_metadata should fail at open time"
                    ),
                    "open_info capability failure should surface a protocol-coded error"
            );
            assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "missing open_metadata should use PROTOCOL");
            assertEquals("open", error.operation(), "open_info capability failure should surface the open operation");
            assertEquals(ZmuxErrorScope.STREAM, error.scope(), "open_info capability failure scope mismatch");
            assertEquals(ZmuxErrorSource.LOCAL, error.source(), "open_info capability failure source mismatch");
            assertEquals(ZmuxErrorDirection.WRITE, error.direction(), "open_info capability failure direction mismatch");
            assertEquals(OpenInfoUnavailableException.MESSAGE, error.getMessage(), "open_info capability error mismatch");
            assertEquals(0L, peer.session().stats().retainedOpenInfoBytes(), "failed local open must not retain open_info bytes");
            assertNull(peer.pollFrame(Duration.ofMillis(100)), "failed local open must not emit frames");
        }
    }

    @Test
    void localOpenRejectsOversizedOpeningMetadataAtOpenTime() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .capabilities(OPEN_METADATA)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, OPEN_METADATA)) {
            byte[] openInfo = new byte[(int) Settings.defaults().maxFramePayload() + 1];
            OpenMetadataTooLargeException error = assertInstanceOf(
                    OpenMetadataTooLargeException.class,
                    assertThrows(
                            IOException.class,
                            () -> peer.session().openStream(new OpenOptions(null, null, openInfo)),
                            "oversized open metadata should fail at open time"
                    ),
                    "oversized opening metadata should surface a protocol-coded error"
            );
            assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "oversized opening metadata should use PROTOCOL");
            assertEquals("open", error.operation(), "oversized opening metadata should surface the open operation");
            assertEquals(ZmuxErrorScope.STREAM, error.scope(), "oversized opening metadata scope mismatch");
            assertEquals(ZmuxErrorSource.LOCAL, error.source(), "oversized opening metadata source mismatch");
            assertEquals(ZmuxErrorDirection.WRITE, error.direction(), "oversized opening metadata direction mismatch");
            assertEquals(OpenMetadataTooLargeException.MESSAGE, error.getMessage(), "oversized opening metadata error mismatch");
            assertEquals(0L, peer.session().stats().retainedOpenInfoBytes(), "failed oversized open must not retain open_info bytes");
            assertNull(peer.pollFrame(Duration.ofMillis(100)), "failed oversized open must not emit frames");
        }
    }

    @Test
    void localOpenIsLimitedBySessionMemoryCap() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .sessionMemoryCap(4_096L)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            OpenLimitedException error = assertThrows(
                    OpenLimitedException.class,
                    () -> peer.session().openStream(),
                    "local open should fail when retained stream state would exceed the session memory cap"
            );
            ZmuxException cause = assertInstanceOf(ZmuxException.class, error.getCause());
            assertEquals(OpenLimitedException.MESSAGE, error.getMessage(), "local open memory-cap error mismatch");
            assertEquals(ErrorCode.STREAM_LIMIT.code(), cause.code(), "local open memory-cap cause code mismatch");
            assertEquals("open", cause.operation(), "local open memory-cap cause operation mismatch");
            assertEquals("zmux: local open limited by session memory cap", cause.getMessage(), "local open memory-cap cause mismatch");
            assertEquals(ZmuxErrorScope.SESSION, cause.scope(), "local open memory-cap cause scope mismatch");
            assertEquals(ZmuxErrorSource.LOCAL, cause.source(), "local open memory-cap cause source mismatch");
            assertEquals(ZmuxErrorDirection.BOTH, cause.direction(), "local open memory-cap cause direction mismatch");
            assertEquals(ZmuxErrorScope.SESSION, error.scope(), "memory-cap limit scope mismatch");
            assertEquals(ZmuxErrorSource.LOCAL, error.source(), "memory-cap limit source mismatch");
            assertEquals(ZmuxErrorDirection.BOTH, error.direction(), "memory-cap limit direction mismatch");
            assertNull(peer.pollFrame(Duration.ofMillis(100)), "locally rejected open must not emit frames");
        }
    }

    @Test
    void inboundDataOverSessionMemoryCapShedsNewestVisibleStreamFirst() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .sessionMemoryCap(4_096L)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    0,
                    4L,
                    new byte[]{1}
            ));

            FrameCodec.Frame refused = peer.awaitFrameType(FrameType.ABORT, Duration.ofSeconds(1));
            assertEquals(4L, refused.streamId(), "memory-pressure shedding should refuse the newest visible stream");
            FrameCodec.ErrorPayload payload = FrameCodec.parseErrorPayload(refused.payload());
            assertEquals(ErrorCode.REFUSED_STREAM.code(), payload.code(), "visible-stream shedding should use REFUSED_STREAM");
            assertFalse(peer.session().state().terminal(), "memory pressure should shed visible accept state before failing the session");
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
            }, "open-info-budget-raw-open");
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
                FrameCodec.Frame frame = pollFrame(Duration.ofMillis(50));
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
