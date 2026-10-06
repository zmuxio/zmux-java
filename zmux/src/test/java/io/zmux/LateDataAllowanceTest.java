package io.zmux;

import io.zmux.protocol.*;
import io.zmux.transport.BasicDuplexConnection;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Late DATA after a local read-stop or local ABORT, DATA after an observed peer FIN and DATA rejected with a
 * stream-local ABORT (SPEC 8, 9.2-9.6; API_SEMANTICS 3/5).
 */
final class LateDataAllowanceTest {
    private static final long BIDI = 4L;
    private static final int STREAM_WINDOW = (int) Settings.defaults().initialMaxStreamDataBidiPeerOpened();
    private static final long SESSION_WINDOW = Settings.defaults().initialMaxData();
    private static final int MAX_FRAME_PAYLOAD = (int) Settings.defaults().maxFramePayload();

    private static void rethrow(Throwable error) throws Exception {
        if (error == null) {
            return;
        }
        if (error instanceof Exception) {
            throw (Exception) error;
        }
        throw new RuntimeException(error);
    }

    private static void await(Duration timeout, BooleanSupplier condition, String description) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(5L);
        }
        throw new AssertionError("timed out waiting for " + description);
    }

    private static Object sessionField(ZmuxSession session, String name) throws ReflectiveOperationException {
        Field field = session.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(session);
    }

    private static boolean hasTombstone(ZmuxSession session, long streamId) {
        try {
            synchronized (sessionField(session, "lock")) {
                Object bookkeeping = sessionField(session, "terminalBookkeeping");
                Field tombstonesField = bookkeeping.getClass().getDeclaredField("tombstones");
                tombstonesField.setAccessible(true);
                return ((Map<?, ?>) tombstonesField.get(bookkeeping)).containsKey(streamId);
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("failed to inspect tombstones", e);
        }
    }

    private static long retainedLateData(ZmuxSession session) throws ReflectiveOperationException {
        synchronized (sessionField(session, "lock")) {
            return (Long) sessionField(session, "aggregateLateDataReceived");
        }
    }

    private static FrameCodec.Frame data(long streamId, int flags, int length) {
        byte[] payload = new byte[length];
        for (int i = 0; i < length; i++) {
            payload[i] = (byte) i;
        }
        return new FrameCodec.Frame(FrameType.DATA, flags, streamId, payload);
    }

    private static FrameCodec.Frame data(long streamId, int flags, String text) {
        return new FrameCodec.Frame(FrameType.DATA, flags, streamId, text.getBytes(StandardCharsets.UTF_8));
    }

    private static long errorCode(FrameCodec.Frame frame) throws IOException {
        return FrameCodec.parseErrorPayload(frame.payload()).code();
    }

    private static long varint(FrameCodec.Frame frame) throws IOException {
        return Varint62.decode(frame.payload(), 0).value();
    }

    private static Predicate<FrameCodec.Frame> frameOn(FrameType type, long streamId) {
        return frame -> frame.type() == type && frame.streamId() == streamId;
    }

    /** Sends late DATA totalling {@code total} bytes in frames no larger than the max frame payload. */
    private static void sendLateTail(RawPeerSession peer, long streamId, int total) throws IOException {
        int remaining = total;
        while (remaining > 0) {
            int chunk = Math.min(remaining, MAX_FRAME_PAYLOAD);
            peer.send(data(streamId, 0, chunk));
            remaining -= chunk;
        }
    }

    private static ZmuxStream acceptAndReadOpener(RawPeerSession peer, long streamId) throws Exception {
        peer.send(data(streamId, 0, "a"));
        ZmuxStream stream = peer.session().acceptStream(Duration.ofSeconds(1));
        byte[] one = new byte[1];
        assertEquals(1, stream.read(one), "opener byte should be readable");
        return stream;
    }

    private static void assertNewStreamRoundTrips(RawPeerSession peer, long streamId) throws Exception {
        peer.send(data(streamId, Protocol.FRAME_FLAG_FIN, "ping"));
        ZmuxStream next = peer.session().acceptStream(Duration.ofSeconds(1));
        byte[] buffer = new byte[4];
        int read = 0;
        while (read < buffer.length) {
            int n = next.read(buffer, read, buffer.length - read);
            assertTrue(n > 0, "new stream should deliver its payload");
            read += n;
        }
        assertEquals("ping", new String(buffer, StandardCharsets.UTF_8));
        assertEquals(-1, next.read(new byte[1]));
    }

    @Test
    void closeReadToleratesFullOutstandingStreamCreditInFlight() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build())) {
            ZmuxStream stream = acceptAndReadOpener(peer, BIDI);
            stream.closeRead();
            peer.await(frameOn(FrameType.STOP_SENDING, BIDI), Duration.ofSeconds(1));

            // A compliant peer may still have the whole outstanding stream credit (one byte already received)
            // in flight, which is several max-size frames and far above the repository late-data floor.
            int outstanding = STREAM_WINDOW - 1;
            sendLateTail(peer, BIDI, outstanding);

            long expectedSessionLimit = SESSION_WINDOW + outstanding;
            FrameCodec.Frame sessionMaxData = peer.await(
                    frame -> frame.type() == FrameType.MAX_DATA && frame.streamId() == 0L && varintUnchecked(frame) >= expectedSessionLimit,
                    Duration.ofSeconds(2)
            );
            assertEquals(expectedSessionLimit, varint(sessionMaxData), "discarded late tail should be released to the session window");
            assertTrue(peer.seen(frame -> frame.type() == FrameType.CLOSE).isEmpty(), "in-credit late tail must not close the session");
            assertTrue(peer.seen(frameOn(FrameType.MAX_DATA, BIDI)).isEmpty(), "stopped direction must not be replenished");
            assertFalse(peer.session().state().terminal(), "in-credit late tail must not fail the session");

            // One byte beyond the advertised stream credit violates stream flow control.
            peer.send(data(BIDI, 0, 1));
            FrameCodec.Frame abort = peer.await(frameOn(FrameType.ABORT, BIDI), Duration.ofSeconds(1));
            assertEquals(ErrorCode.FLOW_CONTROL.code(), errorCode(abort), "overrun on a stopped direction should abort FLOW_CONTROL");
            assertFalse(peer.session().state().terminal(), "stream-local overrun must not fail the session");

            assertNewStreamRoundTrips(peer, BIDI + 4L);
        }
    }

    @Test
    void localAbortToleratesFullOutstandingStreamCreditInFlight() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build())) {
            ZmuxStream stream = acceptAndReadOpener(peer, BIDI);
            stream.closeWithError(ErrorCode.CANCELLED.code(), "");
            peer.await(frameOn(FrameType.ABORT, BIDI), Duration.ofSeconds(1));

            int outstanding = STREAM_WINDOW - 1;
            sendLateTail(peer, BIDI, outstanding);

            long expectedSessionLimit = SESSION_WINDOW + outstanding;
            peer.await(
                    frame -> frame.type() == FrameType.MAX_DATA && frame.streamId() == 0L && varintUnchecked(frame) >= expectedSessionLimit,
                    Duration.ofSeconds(2)
            );
            assertTrue(peer.seen(frame -> frame.type() == FrameType.CLOSE).isEmpty(), "in-credit tail after local ABORT must not close the session");
            assertFalse(peer.session().state().terminal(), "in-credit tail after local ABORT must not fail the session");
            assertNewStreamRoundTrips(peer, BIDI + 4L);

            // Only a peer that ignored its stream credit can exceed the captured allowance; that keeps the
            // repository escalation.
            peer.send(data(BIDI, 0, 1));
            FrameCodec.Frame close = peer.await(frame -> frame.type() == FrameType.CLOSE, Duration.ofSeconds(1));
            assertEquals(ErrorCode.PROTOCOL.code(), errorCode(close));
        }
    }

    @Test
    void sequentialStoppedStreamsWithLateTailsKeepSessionOpen() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .tombstoneLimit(2)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config)) {
            int tail = 6000;
            for (int i = 0; i < 32; i++) {
                long streamId = BIDI + 4L * i;
                ZmuxStream stream = acceptAndReadOpener(peer, streamId);
                stream.close();
                peer.await(frameOn(FrameType.STOP_SENDING, streamId), Duration.ofSeconds(1));
                sendLateTail(peer, streamId, tail);
                peer.send(new FrameCodec.Frame(
                        FrameType.RESET,
                        0,
                        streamId,
                        FrameCodec.buildErrorPayload(ErrorCode.CANCELLED.code(), "", Settings.defaults().maxControlPayloadBytes())
                ));
                await(Duration.ofSeconds(1), () -> hasTombstone(peer.session(), streamId), "stream " + streamId + " compaction");
                assertFalse(peer.session().state().terminal(), "late tail " + i + " must not fail the session");
                assertTrue(retainedLateData(peer.session()) <= 3L * tail,
                        "aggregate must only count late tails of retained tombstones, not a lifetime total");
            }
            assertTrue(peer.seen(frame -> frame.type() == FrameType.CLOSE).isEmpty(), "sequential late tails must not close the session");
            assertNewStreamRoundTrips(peer, BIDI + 4L * 32);
        }
    }

    @Test
    void dataAfterPeerFinOnReadStoppedLiveStreamAbortsStreamClosed() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build())) {
            ZmuxStream stream = acceptAndReadOpener(peer, BIDI);
            stream.closeRead();
            peer.await(frameOn(FrameType.STOP_SENDING, BIDI), Duration.ofSeconds(1));

            peer.send(data(BIDI, Protocol.FRAME_FLAG_FIN, "x"));
            peer.send(data(BIDI, 0, "late"));

            FrameCodec.Frame abort = peer.await(frameOn(FrameType.ABORT, BIDI), Duration.ofSeconds(1));
            assertEquals(ErrorCode.STREAM_CLOSED.code(), errorCode(abort), "DATA after FIN must abort STREAM_CLOSED even after read-stop");
            assertFalse(peer.session().state().terminal());
        }
    }

    @Test
    void dataAfterPeerFinOnReadStoppedTombstoneAbortsStreamClosed() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build())) {
            ZmuxStream stream = acceptAndReadOpener(peer, BIDI);
            stream.close();
            peer.await(frameOn(FrameType.STOP_SENDING, BIDI), Duration.ofSeconds(1));

            peer.send(data(BIDI, Protocol.FRAME_FLAG_FIN, "x"));
            await(Duration.ofSeconds(1), () -> hasTombstone(peer.session(), BIDI), "read-stopped stream compaction after peer FIN");
            peer.send(data(BIDI, 0, "late"));

            FrameCodec.Frame abort = peer.await(frameOn(FrameType.ABORT, BIDI), Duration.ofSeconds(1));
            assertEquals(ErrorCode.STREAM_CLOSED.code(), errorCode(abort), "tombstone of a FIN-completed read-stopped stream must abort STREAM_CLOSED");
            assertFalse(peer.session().state().terminal());
        }
    }

    @Test
    void dataAfterPeerFinOnUnacceptedUniStreamAbortsStreamClosed() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build())) {
            peer.send(data(2L, Protocol.FRAME_FLAG_FIN, "abc"));
            peer.send(data(2L, 0, "late"));

            FrameCodec.Frame abort = peer.await(frameOn(FrameType.ABORT, 2L), Duration.ofSeconds(1));
            assertEquals(ErrorCode.STREAM_CLOSED.code(), errorCode(abort),
                    "DATA after FIN on a fully terminal, still accept-queued stream must abort STREAM_CLOSED");
            assertFalse(peer.session().state().terminal());
        }
    }

    @Test
    void dataAfterPeerFinOnFullyTerminalUnreadStreamAbortsStreamClosed() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build())) {
            ZmuxStream stream = acceptAndReadOpener(peer, BIDI);
            stream.closeWrite();
            peer.await(frame -> frame.type() == FrameType.DATA && frame.streamId() == BIDI
                    && (frame.flags() & Protocol.FRAME_FLAG_FIN) != 0, Duration.ofSeconds(1));

            peer.send(data(BIDI, Protocol.FRAME_FLAG_FIN, "abc"));
            peer.send(data(BIDI, 0, "late"));

            FrameCodec.Frame abort = peer.await(frameOn(FrameType.ABORT, BIDI), Duration.ofSeconds(1));
            assertEquals(ErrorCode.STREAM_CLOSED.code(), errorCode(abort),
                    "DATA after FIN on a fully terminal stream with unread bytes must abort STREAM_CLOSED");
            assertFalse(peer.session().state().terminal());
        }
    }

    @Test
    void dataAfterPeerFinOnLiveStreamReleasesSessionCredit() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build())) {
            peer.send(data(BIDI, Protocol.FRAME_FLAG_FIN, "abc"));
            peer.send(data(BIDI, 0, 100));

            FrameCodec.Frame abort = peer.await(frameOn(FrameType.ABORT, BIDI), Duration.ofSeconds(1));
            assertEquals(ErrorCode.STREAM_CLOSED.code(), errorCode(abort));
            FrameCodec.Frame sessionMaxData = peer.await(frameOn(FrameType.MAX_DATA, 0L), Duration.ofSeconds(1));
            assertEquals(SESSION_WINDOW + 100L, varint(sessionMaxData),
                    "payload rejected with ABORT must still be counted and released as session credit");
            assertFalse(peer.session().state().terminal());
        }
    }

    @Test
    void dataAfterPeerFinOverrunningSessionWindowFailsWithFlowControl() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .settings(Settings.defaults().toBuilder().initialMaxData(1000L).build())
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config)) {
            peer.send(data(BIDI, Protocol.FRAME_FLAG_FIN, "abc"));
            peer.send(data(BIDI, 0, 1000));

            FrameCodec.Frame close = peer.await(frame -> frame.type() == FrameType.CLOSE, Duration.ofSeconds(1));
            assertEquals(ErrorCode.FLOW_CONTROL.code(), errorCode(close),
                    "DATA after FIN that overruns the session window must fail the session with FLOW_CONTROL");
        }
    }

    @Test
    void streamFlowControlOverrunReleasesSessionCredit() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .settings(Settings.defaults().toBuilder().initialMaxStreamDataBidiPeerOpened(10L).build())
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config)) {
            peer.send(data(BIDI, 0, 11));

            FrameCodec.Frame abort = peer.await(frameOn(FrameType.ABORT, BIDI), Duration.ofSeconds(1));
            assertEquals(ErrorCode.FLOW_CONTROL.code(), errorCode(abort));
            FrameCodec.Frame sessionMaxData = peer.await(frameOn(FrameType.MAX_DATA, 0L), Duration.ofSeconds(1));
            assertEquals(SESSION_WINDOW + 11L, varint(sessionMaxData),
                    "payload rejected by a stream FLOW_CONTROL abort must still be released as session credit");
            assertFalse(peer.session().state().terminal());
        }
    }

    @Test
    void cancelReadDuringBulkTransferKeepsJavaSessionOpen() throws Exception {
        try (SessionPair pair = SessionPair.open(ZmuxConfig.builder().build(), ZmuxConfig.builder().build())) {
            for (int round = 0; round < 12; round++) {
                ZmuxStream outbound = pair.client().openStream();
                AtomicReference<Throwable> writeError = new AtomicReference<>();
                CountDownLatch writeDone = new CountDownLatch(1);
                Thread writer = new Thread(() -> {
                    try {
                        outbound.write(new byte[4 << 20]);
                    } catch (Throwable error) {
                        writeError.set(error);
                    } finally {
                        writeDone.countDown();
                    }
                }, "late-data-bulk-writer-" + round);
                writer.start();

                ZmuxStream inbound = pair.server().acceptStream(Duration.ofSeconds(2));
                byte[] buffer = new byte[1000];
                int read = 0;
                while (read < buffer.length) {
                    int n = inbound.read(buffer, read, buffer.length - read);
                    assertTrue(n > 0, "server should receive the start of the transfer");
                    read += n;
                }
                if ((round & 1) == 0) {
                    inbound.closeRead();
                } else {
                    inbound.closeWithError(ErrorCode.CANCELLED.code(), "");
                }

                assertTrue(writeDone.await(5, TimeUnit.SECONDS), "writer should observe the stop");
                writer.join(TimeUnit.SECONDS.toMillis(1));
                outbound.closeWithError(ErrorCode.CANCELLED.code(), "");
                inbound.close();
                assertFalse(pair.server().state().terminal(), "server session must survive cancelled read " + round);
                assertFalse(pair.client().state().terminal(), "client session must survive cancelled read " + round);
            }

            ZmuxStream outbound = pair.client().openStream();
            outbound.writeFinal("ok".getBytes(StandardCharsets.UTF_8));
            ZmuxStream inbound = pair.server().acceptStream(Duration.ofSeconds(2));
            byte[] buffer = new byte[2];
            int read = 0;
            while (read < 2) {
                int n = inbound.read(buffer, read, 2 - read);
                assertTrue(n > 0);
                read += n;
            }
            assertEquals("ok", new String(buffer, StandardCharsets.UTF_8));
        }
    }

    private static long varintUnchecked(FrameCodec.Frame frame) {
        try {
            return varint(frame);
        } catch (IOException e) {
            throw new AssertionError("malformed varint frame", e);
        }
    }

    static final class RawPeerSession implements AutoCloseable {
        private final ZmuxSession session;
        private final Socket socket;
        private final BufferedInputStream input;
        private final BufferedOutputStream output;
        private final List<FrameCodec.Frame> seen = new ArrayList<>();

        private RawPeerSession(ZmuxSession session, Socket socket, BufferedInputStream input, BufferedOutputStream output) {
            this.session = session;
            this.socket = socket;
            this.input = input;
            this.output = output;
        }

        static RawPeerSession open(ZmuxConfig sessionConfig) throws Exception {
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
            }, "late-data-raw-open");
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
            return new RawPeerSession(sessionRef.get(), peerSocket, peerInput, peerOutput);
        }

        ZmuxSession session() {
            return session;
        }

        void send(FrameCodec.Frame frame) throws IOException {
            FrameCodec.writeFrame(output, frame, Settings.defaults().limits());
            output.flush();
        }

        private FrameCodec.Frame pollFrame(Duration timeout) throws IOException {
            FrameCodec.Frame frame = RawFrameReads.readFrameIfStarted(socket, input, timeout);
            if (frame != null) {
                seen.add(frame);
            }
            return frame;
        }

        /** Returns the first frame (already seen or newly read) matching {@code predicate}. */
        FrameCodec.Frame await(Predicate<FrameCodec.Frame> predicate, Duration timeout) throws Exception {
            for (FrameCodec.Frame frame : seen) {
                if (predicate.test(frame)) {
                    seen.remove(frame);
                    return frame;
                }
            }
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                FrameCodec.Frame frame = pollFrame(Duration.ofMillis(20));
                if (frame != null && predicate.test(frame)) {
                    seen.remove(frame);
                    return frame;
                }
            }
            throw new AssertionError("timed out waiting for matching frame; seen=" + seen.size());
        }

        List<FrameCodec.Frame> seen(Predicate<FrameCodec.Frame> predicate) throws IOException {
            while (pollFrame(Duration.ofMillis(20)) != null) {
                // drain what is already on the wire
            }
            List<FrameCodec.Frame> matches = new ArrayList<>();
            for (FrameCodec.Frame frame : seen) {
                if (predicate.test(frame)) {
                    matches.add(frame);
                }
            }
            return matches;
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
            if (error != null && !session.state().terminal()) {
                throw error;
            }
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
            }, "late-data-client-open");
            Thread serverThread = new Thread(() -> {
                try {
                    serverRef.set(Zmux.server(rightConn, serverConfig));
                } catch (Throwable error) {
                    errorRef.compareAndSet(null, error);
                } finally {
                    established.countDown();
                }
            }, "late-data-server-open");
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
            IOException error = null;
            try {
                client.closeWithError(ErrorCode.NO_ERROR.code(), "");
            } catch (IOException e) {
                error = e;
            }
            try {
                server.closeWithError(ErrorCode.NO_ERROR.code(), "");
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
