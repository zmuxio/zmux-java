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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class TombstoneMarkerTest {
    private static boolean tombstoneReaped(ZmuxSession session, long streamId, int expectedCount) {
        try {
            synchronized (sessionLock(session)) {
                Object bookkeeping = terminalBookkeeping(session);
                Field tombstonesField = bookkeeping.getClass().getDeclaredField("tombstones");
                tombstonesField.setAccessible(true);
                @SuppressWarnings("unchecked")
                Map<Long, ?> tombstones = (Map<Long, ?>) tombstonesField.get(bookkeeping);
                return tombstones.size() == expectedCount && !tombstones.containsKey(streamId);
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("failed to inspect tombstones", e);
        }
    }

    private static boolean markerOnlyContains(ZmuxSession session, long streamId) {
        try {
            synchronized (sessionLock(session)) {
                Object bookkeeping = terminalBookkeeping(session);
                Field field = bookkeeping.getClass().getDeclaredField("markerOnlyUsedStreams");
                field.setAccessible(true);
                @SuppressWarnings("unchecked")
                Map<Long, ?> markerOnly = (Map<Long, ?>) field.get(bookkeeping);
                return markerOnly.containsKey(streamId);
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("failed to inspect marker-only used streams", e);
        }
    }

    private static Object sessionLock(ZmuxSession session) throws ReflectiveOperationException {
        Field field = session.getClass().getDeclaredField("lock");
        field.setAccessible(true);
        return field.get(session);
    }

    private static Object terminalBookkeeping(ZmuxSession session) throws ReflectiveOperationException {
        Field field = session.getClass().getDeclaredField("terminalBookkeeping");
        field.setAccessible(true);
        return field.get(session);
    }

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

    private static void acceptEmptyUni(ZmuxSession session) throws Exception {
        ZmuxRecvStream stream = session.acceptUniStream(Duration.ofSeconds(1));
        assertEquals(-1, stream.read(new byte[1]), "empty DATA|FIN uni stream should surface EOF once accepted");
        stream.close();
    }

    private static void acceptEmptyBidi(ZmuxSession session) throws Exception {
        ZmuxStream stream = session.acceptStream(Duration.ofSeconds(1));
        assertEquals(-1, stream.read(new byte[1]), "empty DATA|FIN bidi stream should surface EOF once accepted");
        stream.close();
    }

    private static void createMarkerOnlyTombstone(RawPeerSession peer) throws Exception {
        peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 2L, new byte[0]));
        acceptEmptyUni(peer.session());
        peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 6L, new byte[0]));
        acceptEmptyUni(peer.session());
        await(Duration.ofSeconds(1), () -> tombstoneReaped(peer.session(), 2L, 1), "marker-only tombstone");
    }

    private static byte[] controlErrorPayload() throws IOException {
        return FrameCodec.buildErrorPayload(
                ErrorCode.CANCELLED.code(),
                "",
                Settings.defaults().maxControlPayloadBytes()
        );
    }

    private static byte[] varintPayload(long value) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Varint62.write(output, value);
        return output.toByteArray();
    }

    @Test
    void lateDataOnReapedGracefulTombstoneStillAbortsStreamClosed() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .tombstoneLimit(1)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 2L, new byte[0]));
            acceptEmptyUni(peer.session());
            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 6L, new byte[0]));
            acceptEmptyUni(peer.session());

            await(Duration.ofSeconds(1), () -> tombstoneReaped(peer.session(), 2L, 1), "graceful tombstone reap");

            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 2L, new byte[]{1}));

            FrameCodec.Frame abort = peer.awaitFrameType(FrameType.ABORT, Duration.ofSeconds(1));
            assertEquals(2L, abort.streamId(), "marker-only graceful tombstone should still target the original stream");
            FrameCodec.ErrorPayload payload = FrameCodec.parseErrorPayload(abort.payload());
            assertEquals(ErrorCode.STREAM_CLOSED.code(), payload.code(), "late DATA on marker-only graceful tombstone should emit ABORT(STREAM_CLOSED)");
            assertFalse(peer.session().state().terminal(), "late DATA on reaped tombstone must not fail the session");
        }
    }

    @Test
    void lateDataOnReapedGracefulTombstoneCountsAggregateCap() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .tombstoneLimit(1)
                .aggregateLateDataCap(1)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 2L, new byte[0]));
            acceptEmptyUni(peer.session());
            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 6L, new byte[0]));
            acceptEmptyUni(peer.session());

            await(Duration.ofSeconds(1), () -> tombstoneReaped(peer.session(), 2L, 1), "graceful tombstone reap");

            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 2L, new byte[]{1}));
            FrameCodec.Frame abort = peer.awaitFrameType(FrameType.ABORT, Duration.ofSeconds(1));
            assertEquals(2L, abort.streamId(), "first late DATA should still emit STREAM_CLOSED abort");

            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 2L, new byte[]{2}));
            await(Duration.ofSeconds(1), () -> peer.session().state().terminal(), "session failure after tombstone late-data cap breach");
            FrameCodec.Frame close = peer.awaitFrameType(FrameType.CLOSE, Duration.ofSeconds(1));
            FrameCodec.ErrorPayload payload = FrameCodec.parseErrorPayload(close.payload());
            assertEquals(ErrorCode.PROTOCOL.code(), payload.code(), "late tombstone DATA cap breach should fail with PROTOCOL");
            assertTrue(payload.reason().contains("late-data cap exceeded"),
                    "close reason should expose the late-data cap breach");
        }
    }

    @Test
    void lateResetOnMarkerOnlyUsedStreamIsIgnored() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .tombstoneLimit(1)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 2L, new byte[0]));
            acceptEmptyUni(peer.session());
            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 6L, new byte[0]));
            acceptEmptyUni(peer.session());

            await(Duration.ofSeconds(1), () -> tombstoneReaped(peer.session(), 2L, 1), "graceful tombstone reap");

            peer.send(new FrameCodec.Frame(
                    FrameType.RESET,
                    0,
                    2L,
                    FrameCodec.buildErrorPayload(ErrorCode.CANCELLED.code(), "", Settings.defaults().maxControlPayloadBytes())
            ));

            assertNull(peer.pollFrame(Duration.ofMillis(150)), "late RESET on marker-only used stream should be ignored");
            assertFalse(peer.session().state().terminal(), "late RESET on marker-only used stream must not fail the session");
        }
    }

    @Test
    void markerOnlyTerminalControlsDoNotConsumeNoOpBudgets() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .tombstoneLimit(1)
                .abuseWindow(Duration.ofHours(1))
                .noOpControlFloodThreshold(1)
                .noOpMaxDataFloodThreshold(1)
                .noOpBlockedFloodThreshold(1)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            createMarkerOnlyTombstone(peer);

            for (int i = 0; i < 2; i++) {
                peer.send(new FrameCodec.Frame(FrameType.STOP_SENDING, 0, 2L, controlErrorPayload()));
                peer.send(new FrameCodec.Frame(FrameType.RESET, 0, 2L, controlErrorPayload()));
                peer.send(new FrameCodec.Frame(FrameType.ABORT, 0, 2L, controlErrorPayload()));
                peer.send(new FrameCodec.Frame(FrameType.MAX_DATA, 0, 2L, varintPayload(32L)));
                peer.send(new FrameCodec.Frame(FrameType.BLOCKED, 0, 2L, varintPayload(32L)));
            }

            assertNull(peer.pollFrame(Duration.ofMillis(150)), "marker-only terminal controls should be ignored silently");
            assertFalse(peer.session().state().terminal(), "marker-only terminal controls must not trip no-op budgets");
        }
    }

    @Test
    void markerOnlyPriorityUpdateDoesNotConsumeNoOpBudgets() throws Exception {
        long capabilities = Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS;
        ZmuxConfig config = ZmuxConfig.builder()
                .capabilities(capabilities)
                .tombstoneLimit(1)
                .abuseWindow(Duration.ofHours(1))
                .noOpControlFloodThreshold(1)
                .noOpPriorityUpdateFloodThreshold(1)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, capabilities)) {
            createMarkerOnlyTombstone(peer);

            byte[] priorityUpdate = FrameCodec.buildPriorityUpdatePayload(
                    capabilities,
                    7L,
                    null,
                    Settings.defaults().maxExtensionPayloadBytes()
            );
            peer.send(new FrameCodec.Frame(FrameType.EXT, 0, 2L, priorityUpdate));
            peer.send(new FrameCodec.Frame(FrameType.EXT, 0, 2L, priorityUpdate));

            assertNull(peer.pollFrame(Duration.ofMillis(150)), "marker-only PRIORITY_UPDATE should be ignored silently");
            assertFalse(peer.session().state().terminal(), "marker-only PRIORITY_UPDATE must not trip no-op budgets");
        }
    }

    @Test
    void markerOnlyUsedStreamLimitFailureClosesSession() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .tombstoneLimit(1)
                .markerOnlyUsedStreamLimit(1)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 2L, new byte[0]));
            acceptEmptyUni(peer.session());
            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 4L, new byte[0]));
            acceptEmptyBidi(peer.session());

            await(Duration.ofSeconds(1), () -> tombstoneReaped(peer.session(), 2L, 1), "first marker-only reap");

            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 6L, new byte[0]));
            try {
                acceptEmptyUni(peer.session());
            } catch (IOException ignored) {
                // Accept may fail before the raw peer observes CLOSE.
            }

            await(Duration.ofSeconds(1), () -> peer.session().state().terminal(), "session failure after marker-only cap breach");
            FrameCodec.Frame close = peer.awaitFrameType(FrameType.CLOSE, Duration.ofSeconds(1));
            FrameCodec.ErrorPayload payload = FrameCodec.parseErrorPayload(close.payload());
            assertEquals(ErrorCode.INTERNAL.code(), payload.code(), "marker-only cap breach should fail the session with INTERNAL");
            assertTrue(payload.reason().contains("marker-only used-stream cap exceeded"), "close reason should expose the marker-only cap breach");
        }
    }

    @Test
    void markerOnlySequentialStreamIdsStayUnderEntryCap() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .tombstoneLimit(1)
                .markerOnlyUsedStreamLimit(3)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 2L, new byte[0]));
            acceptEmptyUni(peer.session());
            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 6L, new byte[0]));
            acceptEmptyUni(peer.session());

            await(Duration.ofSeconds(1), () -> tombstoneReaped(peer.session(), 2L, 1), "first marker-only reap");

            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 10L, new byte[0]));
            acceptEmptyUni(peer.session());

            assertFalse(peer.session().state().terminal(), "merged marker-only entries should stay under the cap");
            assertNull(peer.pollFrame(Duration.ofMillis(150)), "in-cap marker-only entries should not emit a CLOSE frame");
        }
    }

    @Test
    void aggregateLateDataCapOverrideAppliesToDiscardedLateData() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .aggregateLateDataCap(1)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            peer.send(new FrameCodec.Frame(
                    FrameType.ABORT,
                    0,
                    2L,
                    FrameCodec.buildErrorPayload(ErrorCode.CANCELLED.code(), "", Settings.defaults().maxControlPayloadBytes())
            ));

            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 2L, new byte[]{1, 2}));

            await(Duration.ofSeconds(1), () -> peer.session().state().terminal(), "session failure after aggregate late-data cap breach");
            FrameCodec.Frame close = peer.awaitFrameType(FrameType.CLOSE, Duration.ofSeconds(1));
            FrameCodec.ErrorPayload payload = FrameCodec.parseErrorPayload(close.payload());
            assertEquals(ErrorCode.PROTOCOL.code(), payload.code(), "late-data cap breach should fail the session with PROTOCOL");
            assertTrue(payload.reason().contains("late-data cap exceeded"), "close reason should expose the aggregate late-data cap breach");
        }
    }

    @Test
    void hiddenAbortUnderTrackedMemoryPressureReapsToMarkerOnlyAndIgnoresLateData() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .sessionMemoryCap(64L)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            peer.send(new FrameCodec.Frame(
                    FrameType.ABORT,
                    0,
                    4L,
                    FrameCodec.buildErrorPayload(ErrorCode.CANCELLED.code(), "", Settings.defaults().maxControlPayloadBytes())
            ));

            await(Duration.ofSeconds(1), () -> markerOnlyContains(peer.session(), 4L), "hidden abort marker-only reap under memory pressure");
            assertFalse(peer.session().state().terminal(), "hidden abort bookkeeping should be shed before the session fails");

            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 4L, new byte[]{1, 2}));

            assertNull(peer.pollFrame(Duration.ofMillis(150)), "late DATA on marker-only abortive stream should be ignored");
            assertFalse(peer.session().state().terminal(), "late DATA on marker-only abortive stream must not fail the session");
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
            }, "tombstone-marker-raw-open");
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
