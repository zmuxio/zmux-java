package io.zmux;

import io.zmux.internal.FrameCodec;
import io.zmux.internal.Varint62;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
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

final class PriorityUpdateSemanticsTest {
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

    private static byte[] duplicatePriorityUpdatePayload() throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        Varint62.write(payload, Protocol.EXT_PRIORITY_UPDATE);
        FrameCodec.appendTlv(payload, Protocol.METADATA_STREAM_PRIORITY, Varint62.encode(3L));
        FrameCodec.appendTlv(payload, Protocol.METADATA_STREAM_PRIORITY, Varint62.encode(7L));
        return payload.toByteArray();
    }

    private static byte[] findPreOpenOverflowOpenInfo(long capabilities,
                                                      long maxFramePayload,
                                                      long priority,
                                                      Long group) throws Exception {
        for (int size = (int) maxFramePayload; size >= 0; --size) {
            byte[] candidate = new byte[size];
            Arrays.fill(candidate, (byte) 'x');
            try {
                FrameCodec.buildOpenMetadataPrefix(capabilities, null, null, candidate, maxFramePayload);
            } catch (IOException ignored) {
                continue;
            }
            try {
                FrameCodec.buildOpenMetadataPrefix(capabilities, priority, group, candidate, maxFramePayload);
            } catch (IOException error) {
                if (error instanceof OpenMetadataTooLargeException) {
                    return candidate;
                }
                throw error;
            }
        }
        throw new AssertionError("failed to find open_info that only overflows after metadata update");
    }

    private static void await(Duration timeout, ThrowingBooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10L);
        }
        assertTrue(condition.getAsBoolean(), "condition was not satisfied before timeout");
    }

    @Test
    void emptyMetadataUpdateFailsWithTypedLocalError() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L)) {
            ZmuxStream stream = peer.session().openStream();
            EmptyMetadataUpdateException error = assertThrows(
                    EmptyMetadataUpdateException.class,
                    () -> stream.updateMetadata(new MetadataUpdate(null, null))
            );
            assertEquals(EmptyMetadataUpdateException.MESSAGE, error.getMessage());
            assertEquals("write", error.operation(), "empty metadata update operation mismatch");
            assertEquals(ZmuxErrorScope.STREAM, error.scope(), "empty metadata update scope mismatch");
            assertEquals(ZmuxErrorSource.LOCAL, error.source(), "empty metadata update source mismatch");
            assertEquals(ZmuxErrorDirection.WRITE, error.direction(), "empty metadata update direction mismatch");
        }
    }

    @Test
    void updateMetadataBeforeOpenRequiresOpenMetadataCapability() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L)) {
            ZmuxStream stream = peer.session().openStream();

            PriorityUpdateUnavailableException error = assertInstanceOf(
                    PriorityUpdateUnavailableException.class,
                    assertThrows(IOException.class, () -> stream.updateMetadata(new MetadataUpdate(7L, null)))
            );

            assertEquals(PriorityUpdateUnavailableException.MESSAGE, error.getMessage());
            assertEquals("write", error.operation(), "pre-open capability failure operation mismatch");
            assertEquals(ZmuxErrorScope.STREAM, error.scope());
            assertEquals(ZmuxErrorSource.LOCAL, error.source());
            assertEquals(ZmuxErrorDirection.WRITE, error.direction(), "pre-open capability failure direction mismatch");
            assertEquals(0L, stream.metadata().priority(), "failed local metadata update must not mutate shadow state");

            stream.closeWithError(ErrorCode.CANCELLED.code(), "");
        }
    }

    @Test
    void updateMetadataAfterOpenRequiresNegotiatedPriorityUpdateCapability() throws Exception {
        RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L);
        try {
            ZmuxStream stream = peer.session().openStream();
            stream.write("x".getBytes(StandardCharsets.UTF_8));

            FrameCodec.Frame opener = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.DATA, opener.type(), "expected opening DATA frame");

            PriorityUpdateUnavailableException error = assertInstanceOf(
                    PriorityUpdateUnavailableException.class,
                    assertThrows(IOException.class, () -> stream.updateMetadata(new MetadataUpdate(7L, null)))
            );

            assertEquals(PriorityUpdateUnavailableException.MESSAGE, error.getMessage());
            assertEquals("write", error.operation(), "post-open capability failure operation mismatch");
            assertEquals(ZmuxErrorScope.STREAM, error.scope());
            assertEquals(ZmuxErrorSource.LOCAL, error.source());
            assertEquals(ZmuxErrorDirection.WRITE, error.direction(), "post-open capability failure direction mismatch");
            assertEquals(0L, stream.metadata().priority(), "failed local metadata update must not mutate shadow state");
        } finally {
            try {
                peer.session().closeWithError(ErrorCode.NO_ERROR.code(), "");
            } catch (IOException ignored) {
            }
            peer.socket.close();
        }
    }

    @Test
    void updateMetadataBeforeOpenOverflowDoesNotMutateShadowState() throws Exception {
        long capabilities = Protocol.CAPABILITY_OPEN_METADATA
                | Protocol.CAPABILITY_PRIORITY_HINTS
                | Protocol.CAPABILITY_STREAM_GROUPS;
        Settings peerSettings = Settings.defaults();
        ZmuxConfig config = ZmuxConfig.builder()
                .capabilities(capabilities)
                .build();

        try (RawPeerSession peer = RawPeerSession.open(config, capabilities, peerSettings)) {
            byte[] openInfo = findPreOpenOverflowOpenInfo(capabilities, peerSettings.maxFramePayload(), 7L, 11L);
            ZmuxStream stream = peer.session().openStream(new OpenOptions(null, null, openInfo));

            OpenMetadataTooLargeException error = assertInstanceOf(
                    OpenMetadataTooLargeException.class,
                    assertThrows(IOException.class, () -> stream.updateMetadata(new MetadataUpdate(7L, 11L))),
                    "pre-open metadata overflow should fail before mutating local metadata"
            );

            assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "pre-open metadata overflow code mismatch");
            assertEquals("write", error.operation(), "pre-open metadata overflow operation mismatch");
            assertEquals(ZmuxErrorScope.STREAM, error.scope(), "pre-open metadata overflow scope mismatch");
            assertEquals(ZmuxErrorSource.LOCAL, error.source(), "pre-open metadata overflow source mismatch");
            assertEquals(ZmuxErrorDirection.WRITE, error.direction(), "pre-open metadata overflow direction mismatch");
            assertEquals(OpenMetadataTooLargeException.MESSAGE, error.getMessage(), "pre-open metadata overflow message mismatch");
            assertEquals(0L, stream.metadata().priority(), "failed pre-open metadata update must not mutate priority");
            assertNull(stream.metadata().group(), "failed pre-open metadata update must not mutate group");
            assertArrayEquals(openInfo, stream.metadata().openInfo(), "failed pre-open metadata update must preserve existing open_info");
            assertNull(peer.pollFrame(Duration.ofMillis(100)), "failed pre-open metadata update must not emit any frame");
        }
    }

    @Test
    void unnegotiatedPriorityUpdateIsIgnored() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L)) {
            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    0,
                    4L,
                    "body".getBytes(StandardCharsets.UTF_8)
            ));

            ZmuxStream accepted = peer.session().acceptStream(Duration.ofSeconds(1));
            assertEquals("body", readUtf8(accepted), "accepted payload mismatch");
            assertEquals(0L, accepted.metadata().priority(), "default stream priority mismatch");

            peer.send(new FrameCodec.Frame(
                    FrameType.EXT,
                    0,
                    4L,
                    FrameCodec.buildPriorityUpdatePayload(
                            Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS,
                            9L,
                            null,
                            Settings.defaults().maxExtensionPayloadBytes()
                    )
            ));

            Thread.sleep(50L);
            assertEquals(0L, accepted.metadata().priority(), "unnegotiated PRIORITY_UPDATE must be ignored");
        }
    }

    @Test
    void openMetadataIgnoresUnnegotiatedPriorityAndGroupFields() throws Exception {
        long negotiatedCapabilities = Protocol.CAPABILITY_OPEN_METADATA;
        long payloadCapabilities = Protocol.CAPABILITY_OPEN_METADATA
                | Protocol.CAPABILITY_PRIORITY_HINTS
                | Protocol.CAPABILITY_STREAM_GROUPS;
        ZmuxConfig config = ZmuxConfig.builder()
                .capabilities(negotiatedCapabilities)
                .build();

        try (RawPeerSession peer = RawPeerSession.open(config, negotiatedCapabilities)) {
            byte[] openPrefix = FrameCodec.buildOpenMetadataPrefix(
                    payloadCapabilities,
                    5L,
                    7L,
                    "ssh".getBytes(StandardCharsets.UTF_8),
                    Settings.defaults().maxFramePayload()
            );
            byte[] body = "body".getBytes(StandardCharsets.UTF_8);
            byte[] payload = new byte[openPrefix.length + body.length];
            System.arraycopy(openPrefix, 0, payload, 0, openPrefix.length);
            System.arraycopy(body, 0, payload, openPrefix.length, body.length);

            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    Protocol.FRAME_FLAG_OPEN_METADATA,
                    4L,
                    payload
            ));

            ZmuxStream accepted = peer.session().acceptStream(Duration.ofSeconds(1));
            assertEquals("body", readUtf8(accepted), "accepted payload mismatch");
            assertEquals(0L, accepted.metadata().priority(), "unnegotiated opening priority must be ignored");
            assertNull(accepted.metadata().group(), "unnegotiated opening group must be ignored");
            assertArrayEquals("ssh".getBytes(StandardCharsets.UTF_8), accepted.metadata().openInfo(), "negotiated open_info should still apply");
        }
    }

    @Test
    void priorityUpdateIgnoresUnnegotiatedPriorityAndGroupFields() throws Exception {
        long negotiatedCapabilities = Protocol.CAPABILITY_PRIORITY_UPDATE;
        long payloadCapabilities = Protocol.CAPABILITY_PRIORITY_UPDATE
                | Protocol.CAPABILITY_PRIORITY_HINTS
                | Protocol.CAPABILITY_STREAM_GROUPS;
        ZmuxConfig config = ZmuxConfig.builder()
                .capabilities(negotiatedCapabilities)
                .build();

        try (RawPeerSession peer = RawPeerSession.open(config, negotiatedCapabilities)) {
            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    0,
                    4L,
                    "body".getBytes(StandardCharsets.UTF_8)
            ));

            ZmuxStream accepted = peer.session().acceptStream(Duration.ofSeconds(1));
            assertEquals("body", readUtf8(accepted), "accepted payload mismatch");

            peer.send(new FrameCodec.Frame(
                    FrameType.EXT,
                    0,
                    4L,
                    FrameCodec.buildPriorityUpdatePayload(
                            payloadCapabilities,
                            9L,
                            11L,
                            Settings.defaults().maxExtensionPayloadBytes()
                    )
            ));

            Thread.sleep(50L);
            assertEquals(0L, accepted.metadata().priority(), "unnegotiated PRIORITY_UPDATE priority must be ignored");
            assertNull(accepted.metadata().group(), "unnegotiated PRIORITY_UPDATE group must be ignored");
        }
    }

    @Test
    void priorityUpdateDoesNotReviveTerminalStream() throws Exception {
        long capabilities = Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS;
        ZmuxConfig config = ZmuxConfig.builder()
                .capabilities(capabilities)
                .build();

        try (RawPeerSession peer = RawPeerSession.open(config, capabilities)) {
            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    Protocol.FRAME_FLAG_FIN,
                    4L,
                    "done".getBytes(StandardCharsets.UTF_8)
            ));

            ZmuxStream accepted = peer.session().acceptStream(Duration.ofSeconds(1));
            assertEquals("done", readUtf8(accepted), "accepted payload mismatch");
            assertEquals(-1, accepted.read(new byte[8]), "stream should reach EOF after peer FIN");
            accepted.closeWrite();

            FrameCodec.Frame fin = peer.awaitFrameType(FrameType.DATA, Duration.ofSeconds(1));
            assertEquals(FrameType.DATA, fin.type(), "local closeWrite should emit DATA|FIN");
            assertTrue((fin.flags() & Protocol.FRAME_FLAG_FIN) != 0, "local closeWrite should carry FIN");

            peer.send(new FrameCodec.Frame(
                    FrameType.EXT,
                    0,
                    4L,
                    FrameCodec.buildPriorityUpdatePayload(
                            capabilities,
                            7L,
                            null,
                            Settings.defaults().maxExtensionPayloadBytes()
                    )
            ));

            Thread.sleep(50L);
            assertEquals(0L, accepted.metadata().priority(), "terminal stream must ignore late PRIORITY_UPDATE");
        }
    }

    @Test
    void invalidPriorityUpdateIncrementsDroppedDiagnostic() throws Exception {
        long capabilities = Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS;
        ZmuxConfig config = ZmuxConfig.builder()
                .capabilities(capabilities)
                .build();

        try (RawPeerSession peer = RawPeerSession.open(config, capabilities)) {
            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    0,
                    4L,
                    "body".getBytes(StandardCharsets.UTF_8)
            ));

            ZmuxStream accepted = peer.session().acceptStream(Duration.ofSeconds(1));
            assertEquals("body", readUtf8(accepted), "accepted payload mismatch");

            peer.send(new FrameCodec.Frame(
                    FrameType.EXT,
                    0,
                    4L,
                    duplicatePriorityUpdatePayload()
            ));

            await(Duration.ofSeconds(1), () -> peer.session().stats().diagnostics().droppedPriorityUpdates() == 1L);
            assertEquals(1L, peer.session().stats().diagnostics().droppedPriorityUpdates(), "invalid PRIORITY_UPDATE should count as a dropped diagnostic");
            assertEquals(SessionState.READY, peer.session().state(), "invalid PRIORITY_UPDATE should not fail the session");
            assertEquals(0L, accepted.metadata().priority(), "invalid PRIORITY_UPDATE must not mutate stream metadata");
        }
    }

    @Test
    void partialPriorityUpdatePreservesUnspecifiedFields() throws Exception {
        long capabilities = Protocol.CAPABILITY_OPEN_METADATA
                | Protocol.CAPABILITY_PRIORITY_UPDATE
                | Protocol.CAPABILITY_PRIORITY_HINTS
                | Protocol.CAPABILITY_STREAM_GROUPS;
        ZmuxConfig config = ZmuxConfig.builder()
                .capabilities(capabilities)
                .build();

        try (RawPeerSession peer = RawPeerSession.open(config, capabilities)) {
            byte[] openPrefix = FrameCodec.buildOpenMetadataPrefix(
                    capabilities,
                    5L,
                    7L,
                    new byte[0],
                    Settings.defaults().maxFramePayload()
            );
            byte[] body = "body".getBytes(StandardCharsets.UTF_8);
            byte[] payload = new byte[openPrefix.length + body.length];
            System.arraycopy(openPrefix, 0, payload, 0, openPrefix.length);
            System.arraycopy(body, 0, payload, openPrefix.length, body.length);

            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    Protocol.FRAME_FLAG_OPEN_METADATA,
                    4L,
                    payload
            ));

            ZmuxStream accepted = peer.session().acceptStream(Duration.ofSeconds(1));
            assertEquals("body", readUtf8(accepted), "accepted payload mismatch");
            assertEquals(5L, accepted.metadata().priority(), "opening priority mismatch");
            assertEquals(Long.valueOf(7L), accepted.metadata().group(), "opening group mismatch");

            peer.send(new FrameCodec.Frame(
                    FrameType.EXT,
                    0,
                    4L,
                    FrameCodec.buildPriorityUpdatePayload(
                            capabilities,
                            9L,
                            null,
                            Settings.defaults().maxExtensionPayloadBytes()
                    )
            ));

            Thread.sleep(50L);
            assertEquals(9L, accepted.metadata().priority(), "priority-only update should replace priority");
            assertEquals(Long.valueOf(7L), accepted.metadata().group(), "priority-only update must preserve group");

            peer.send(new FrameCodec.Frame(
                    FrameType.EXT,
                    0,
                    4L,
                    FrameCodec.buildPriorityUpdatePayload(
                            capabilities,
                            null,
                            11L,
                            Settings.defaults().maxExtensionPayloadBytes()
                    )
            ));

            Thread.sleep(50L);
            assertEquals(9L, accepted.metadata().priority(), "group-only update must preserve priority");
            assertEquals(Long.valueOf(11L), accepted.metadata().group(), "group-only update should replace group");

            peer.send(new FrameCodec.Frame(
                    FrameType.EXT,
                    0,
                    4L,
                    FrameCodec.buildPriorityUpdatePayload(
                            capabilities,
                            null,
                            0L,
                            Settings.defaults().maxExtensionPayloadBytes()
                    )
            ));

            await(Duration.ofSeconds(1), () -> accepted.metadata().group() == null);
            assertEquals(9L, accepted.metadata().priority(), "group reset must preserve priority");
            assertNull(accepted.metadata().group(), "stream_group zero should clear the explicit group");
        }
    }

    @Test
    void repeatedEffectiveGroupRebucketChurnFailsSession() throws Exception {
        long capabilities = Protocol.CAPABILITY_PRIORITY_UPDATE
                | Protocol.CAPABILITY_PRIORITY_HINTS
                | Protocol.CAPABILITY_STREAM_GROUPS;
        Settings rawSettings = Settings.defaults().toBuilder()
                .schedulerHints(SchedulerHint.GROUP_FAIR)
                .build();
        ZmuxConfig config = ZmuxConfig.builder()
                .capabilities(capabilities)
                .abuseWindow(Duration.ofHours(1))
                .groupRebucketChurnThreshold(1)
                .build();

        try (RawPeerSession peer = RawPeerSession.open(config, capabilities, rawSettings)) {
            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    0,
                    4L,
                    "body".getBytes(StandardCharsets.UTF_8)
            ));

            ZmuxStream accepted = peer.session().acceptStream(Duration.ofSeconds(1));
            assertEquals("body", readUtf8(accepted), "accepted payload mismatch");

            peer.send(new FrameCodec.Frame(
                    FrameType.EXT,
                    0,
                    4L,
                    FrameCodec.buildPriorityUpdatePayload(
                            capabilities,
                            null,
                            1L,
                            Settings.defaults().maxExtensionPayloadBytes()
                    )
            ));
            Thread.sleep(50L);
            assertEquals(Long.valueOf(1L), accepted.metadata().group(), "first group rebucket should apply");

            peer.send(new FrameCodec.Frame(
                    FrameType.EXT,
                    0,
                    4L,
                    FrameCodec.buildPriorityUpdatePayload(
                            capabilities,
                            null,
                            2L,
                            Settings.defaults().maxExtensionPayloadBytes()
                    )
            ));

            assertTrue(peer.session().awaitTermination(Duration.ofSeconds(1)), "repeated effective group rebucketing should fail the session");
            assertEquals(SessionState.FAILED, peer.session().state(), "group rebucket churn should transition the session to FAILED");
        }
    }

    @Test
    void updateMetadataAfterCloseWriteFailsExplicitly() throws Exception {
        long capabilities = Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS;
        ZmuxConfig config = ZmuxConfig.builder()
                .capabilities(capabilities)
                .build();

        try (RawPeerSession peer = RawPeerSession.open(config, capabilities)) {
            ZmuxStream stream = peer.session().openStream();
            stream.write("x".getBytes(StandardCharsets.UTF_8));

            FrameCodec.Frame opener = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.DATA, opener.type(), "expected opening DATA frame");

            stream.closeWrite();

            FrameCodec.Frame fin = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.DATA, fin.type(), "CloseWrite should emit DATA|FIN");
            assertTrue((fin.flags() & Protocol.FRAME_FLAG_FIN) != 0, "CloseWrite DATA frame must carry FIN");

            WriteClosedException error = assertThrows(
                    WriteClosedException.class,
                    () -> stream.updateMetadata(new MetadataUpdate(7L, null))
            );
            assertEquals(WriteClosedException.MESSAGE, error.getMessage(), "local graceful close should report a typed write-closed error");
            assertEquals(0L, stream.metadata().priority(), "failed metadata update must not mutate local shadow state");

            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    Protocol.FRAME_FLAG_FIN,
                    opener.streamId(),
                    new byte[0]
            ));
            assertEquals(-1, stream.read(new byte[1]), "peer FIN should close the receive side for cleanup");
        }
    }

    @Test
    void updateMetadataAfterPeerStopSendingGracefulFinishUsesLocalGracefulClose() throws Exception {
        long capabilities = Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS;
        ZmuxConfig config = ZmuxConfig.builder()
                .capabilities(capabilities)
                .build();

        try (RawPeerSession peer = RawPeerSession.open(config, capabilities)) {
            ZmuxStream stream = peer.session().openStream();
            stream.write("x".getBytes(StandardCharsets.UTF_8));

            FrameCodec.Frame opener = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.DATA, opener.type(), "expected opening DATA frame");

            peer.send(new FrameCodec.Frame(
                    FrameType.STOP_SENDING,
                    0,
                    opener.streamId(),
                    FrameCodec.buildErrorPayload(ErrorCode.CANCELLED.code(), "", Settings.defaults().maxControlPayloadBytes())
            ));

            FrameCodec.Frame fin = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.DATA, fin.type(), "empty committed tail should conclude with DATA|FIN after peer STOP_SENDING");
            assertTrue((fin.flags() & Protocol.FRAME_FLAG_FIN) != 0, "STOP_SENDING graceful finish must carry FIN");

            WriteClosedException stop = assertInstanceOf(
                    WriteClosedException.class,
                    assertThrows(IOException.class, () -> stream.updateMetadata(new MetadataUpdate(9L, null))),
                    "once STOP_SENDING has converged to DATA|FIN, metadata updates should surface the local graceful write close"
            );
            assertEquals(ZmuxErrorSource.LOCAL, stop.source(), "post-finish metadata update source mismatch");
            assertEquals(ZmuxTerminationKind.GRACEFUL, stop.terminationKind(), "post-finish metadata update termination mismatch");
            assertEquals(0L, stream.metadata().priority(), "failed metadata update must not mutate local shadow state");

            peer.send(new FrameCodec.Frame(
                    FrameType.DATA,
                    Protocol.FRAME_FLAG_FIN,
                    opener.streamId(),
                    new byte[0]
            ));
            assertEquals(-1, stream.read(new byte[1]), "peer FIN should close the receive side for cleanup");
        }
    }

    @FunctionalInterface
    private interface ThrowingBooleanSupplier {
        boolean getAsBoolean() throws Exception;
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
            }, "priority-update-raw-open");
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
