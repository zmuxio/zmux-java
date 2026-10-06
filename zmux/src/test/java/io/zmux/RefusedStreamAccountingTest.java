package io.zmux;

import io.zmux.protocol.*;
import io.zmux.transport.BasicDuplexConnection;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Peer streams refused at open (local GOAWAY watermark or incoming-stream limit): frames racing the refusal never fail
 * the session, ABORT(REFUSED_STREAM) is sent once per ID, and refused DATA is counted in and released to the session
 * window (SPEC 3.1, 6.9, 8).
 */
final class RefusedStreamAccountingTest {
    private static final long SESSION_WINDOW = Settings.defaults().initialMaxData();

    private static void rethrow(Throwable error) throws Exception {
        if (error == null) {
            return;
        }
        if (error instanceof Exception) {
            throw (Exception) error;
        }
        throw new RuntimeException(error);
    }

    private static FrameCodec.Frame data(long streamId, int flags, int length) {
        byte[] payload = new byte[length];
        for (int i = 0; i < length; i++) {
            payload[i] = (byte) i;
        }
        return new FrameCodec.Frame(FrameType.DATA, flags, streamId, payload);
    }

    private static FrameCodec.Frame control(FrameType type, long streamId) throws IOException {
        return new FrameCodec.Frame(
                type,
                0,
                streamId,
                FrameCodec.buildErrorPayload(ErrorCode.CANCELLED.code(), "", Settings.defaults().maxControlPayloadBytes())
        );
    }

    private static FrameCodec.Frame varintFrame(FrameType type, long streamId, long value) throws IOException {
        return new FrameCodec.Frame(type, 0, streamId, Varint62.encode(value));
    }

    private static long errorCode(FrameCodec.Frame frame) throws IOException {
        return FrameCodec.parseErrorPayload(frame.payload()).code();
    }

    private static long varint(FrameCodec.Frame frame) {
        try {
            return Varint62.decode(frame.payload(), 0).value();
        } catch (IOException e) {
            throw new AssertionError("malformed varint frame", e);
        }
    }

    private static Predicate<FrameCodec.Frame> frameOn(FrameType type, long streamId) {
        return frame -> frame.type() == type && frame.streamId() == streamId;
    }

    private static long sessionLimitAdvertised(RawPeerSession peer) throws IOException {
        long limit = SESSION_WINDOW;
        for (FrameCodec.Frame frame : peer.seen(frameOn(FrameType.MAX_DATA, 0L))) {
            limit = Math.max(limit, varint(frame));
        }
        return limit;
    }

    private static void assertSessionLimitReaches(RawPeerSession peer, long expected) throws Exception {
        peer.await(frame -> frame.type() == FrameType.MAX_DATA && frame.streamId() == 0L && varint(frame) >= expected,
                Duration.ofSeconds(2));
        assertEquals(expected, sessionLimitAdvertised(peer), "refused DATA should be released exactly once");
    }

    @Test
    void goAwayRefusedStreamIgnoresRacingFramesAndReleasesItsData() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L)) {
            // An accepted stream keeps draining while later streams are refused.
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 4L, "a".getBytes(StandardCharsets.UTF_8)));
            ZmuxStream accepted = peer.session().acceptStream(Duration.ofSeconds(1));
            ((ZmuxNativeSession) peer.session()).goAway(4L, 0L);
            peer.await(frame -> frame.type() == FrameType.GOAWAY, Duration.ofSeconds(1));

            // The peer opened 8 and 2 before it saw GOAWAY and keeps using them until it sees the refusal.
            peer.send(data(8L, 0, 1000));
            peer.send(control(FrameType.STOP_SENDING, 8L));
            peer.send(varintFrame(FrameType.BLOCKED, 8L, 1000L));
            peer.send(varintFrame(FrameType.MAX_DATA, 8L, 70_000L));
            peer.send(control(FrameType.RESET, 8L));
            peer.send(data(8L, 0, 1000));
            peer.send(data(8L, Protocol.FRAME_FLAG_FIN, 1000));
            peer.send(data(2L, 0, 500));
            peer.send(control(FrameType.RESET, 2L));
            peer.send(data(2L, Protocol.FRAME_FLAG_FIN, 500));
            peer.send(control(FrameType.ABORT, 12L));

            assertSessionLimitReaches(peer, SESSION_WINDOW + 4000L);
            // The refusal of 12 answers the last frame sent, so it can trail the final MAX_DATA.
            peer.await(frameOn(FrameType.ABORT, 12L), Duration.ofSeconds(2));
            List<FrameCodec.Frame> aborts = peer.seen(frame -> frame.type() == FrameType.ABORT);
            assertEquals(3, aborts.size(), "one ABORT per refused stream ID: " + aborts.size());
            for (FrameCodec.Frame abort : aborts) {
                assertEquals(ErrorCode.REFUSED_STREAM.code(), errorCode(abort));
            }
            assertEquals(1, peer.seen(frameOn(FrameType.ABORT, 8L)).size());
            assertEquals(1, peer.seen(frameOn(FrameType.ABORT, 2L)).size());
            assertTrue(peer.seen(frame -> frame.type() == FrameType.CLOSE).isEmpty(), "racing frames must not close the session");
            assertEquals(SessionState.DRAINING, peer.session().state());
            assertEquals(1L + 4000L, peer.session().stats().pressure().recvSessionReceivedBytes(),
                    "refused DATA counts toward the session received total");

            // The draining stream is unaffected.
            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 4L, "b".getBytes(StandardCharsets.UTF_8)));
            assertArrayEquals("ab".getBytes(StandardCharsets.UTF_8), accepted.readAllBytes());
            accepted.close();
        }
    }

    @Test
    void limitRefusedOpenerApplicationBytesAreReleased() throws Exception {
        long capabilities = Protocol.CAPABILITY_OPEN_METADATA;
        ZmuxConfig config = ZmuxConfig.builder()
                .capabilities(capabilities)
                .settings(Settings.defaults().toBuilder().maxIncomingStreamsBidi(0L).build())
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, capabilities)) {
            byte[] prefix = FrameCodec.buildOpenMetadataPrefix(
                    capabilities,
                    null,
                    null,
                    "open-info".getBytes(StandardCharsets.UTF_8),
                    Settings.defaults().maxFramePayload()
            );
            byte[] opener = new byte[prefix.length + 1000];
            System.arraycopy(prefix, 0, opener, 0, prefix.length);
            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_OPEN_METADATA, 4L, opener));
            peer.send(data(4L, 0, 1000));
            peer.send(data(4L, Protocol.FRAME_FLAG_FIN, 1000));
            peer.send(data(8L, 0, 700));

            assertSessionLimitReaches(peer, SESSION_WINDOW + 3700L);
            assertEquals(1, peer.seen(frameOn(FrameType.ABORT, 4L)).size(), "the refused stream is aborted once");
            assertEquals(1, peer.seen(frameOn(FrameType.ABORT, 8L)).size());
            assertTrue(peer.seen(frame -> frame.type() == FrameType.CLOSE).isEmpty());
            assertEquals(3700L, peer.session().stats().pressure().recvSessionReceivedBytes(),
                    "only application bytes after the OPEN_METADATA prefix count");
            assertFalse(peer.session().state().terminal());
        }
    }

    @Test
    void goAwayRefusedOpenerSkipsWellFormedOpenMetadataWithoutApplyingIt() throws Exception {
        long capabilities = Protocol.CAPABILITY_OPEN_METADATA;
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().capabilities(capabilities).build(), capabilities)) {
            ((ZmuxNativeSession) peer.session()).goAway(0L, 0L);
            peer.await(frame -> frame.type() == FrameType.GOAWAY, Duration.ofSeconds(1));

            // metadata_len = 9: an unknown TLV and a duplicated priority TLV (structurally valid, so the envelope
            // accepts the frame), followed by three application bytes.
            byte[] payload = new byte[]{0x09, 0x10, 0x01, 'z', 0x01, 0x01, 0x05, 0x01, 0x01, 0x06, 'a', 'b', 'c'};
            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_OPEN_METADATA, 4L, payload));

            FrameCodec.Frame abort = peer.await(frameOn(FrameType.ABORT, 4L), Duration.ofSeconds(2));
            assertEquals(ErrorCode.REFUSED_STREAM.code(), errorCode(abort));
            assertTrue(peer.seen(frame -> frame.type() == FrameType.CLOSE).isEmpty(),
                    "a structurally valid metadata block on a refused opener must not fail the session");
            assertEquals(3L, peer.session().stats().pressure().recvSessionReceivedBytes(),
                    "only the application bytes after the OPEN_METADATA prefix count");
            assertFalse(peer.session().state().terminal());
        }
    }

    @Test
    void goAwayRefusedOpenerWithTruncatedOpenMetadataTlvFailsWithFrameSize() throws Exception {
        long capabilities = Protocol.CAPABILITY_OPEN_METADATA;
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().capabilities(capabilities).build(), capabilities)) {
            ((ZmuxNativeSession) peer.session()).goAway(0L, 0L);
            peer.await(frame -> frame.type() == FrameType.GOAWAY, Duration.ofSeconds(1));

            // DATA|OPEN_METADATA on stream 4: metadata_len = 1 covers a TLV whose length varint is missing. Envelope
            // validation rejects the frame (D1 FRAME_SIZE) before the GOAWAY refusal is considered.
            peer.sendRaw(new byte[]{0x07, 0x21, 0x04, 0x01, 0x01, 'a', 'b', 'c'});

            FrameCodec.Frame close = peer.await(frame -> frame.type() == FrameType.CLOSE, Duration.ofSeconds(2));
            assertEquals(ErrorCode.FRAME_SIZE.code(), errorCode(close), "truncated OPEN_METADATA TLV on a refused opener");
            assertTrue(peer.seen(frameOn(FrameType.ABORT, 4L)).isEmpty(), "a malformed opener is not refused stream-locally");
        }
    }

    @Test
    void refusedOpenerBeyondSessionWindowFailsWithFlowControl() throws Exception {
        Settings smallWindow = Settings.defaults().toBuilder().initialMaxData(100L).maxIncomingStreamsBidi(0L).build();
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().settings(smallWindow).build(), 0L)) {
            peer.send(data(4L, 0, 1000));
            FrameCodec.Frame close = peer.await(frame -> frame.type() == FrameType.CLOSE, Duration.ofSeconds(2));
            assertEquals(ErrorCode.FLOW_CONTROL.code(), errorCode(close), "limit-refused opener over the session window");
        }

        Settings smallSessionWindow = Settings.defaults().toBuilder().initialMaxData(100L).build();
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().settings(smallSessionWindow).build(), 0L)) {
            ((ZmuxNativeSession) peer.session()).goAway(0L, 0L);
            peer.await(frame -> frame.type() == FrameType.GOAWAY, Duration.ofSeconds(1));
            peer.send(data(4L, 0, 200));
            FrameCodec.Frame close = peer.await(frame -> frame.type() == FrameType.CLOSE, Duration.ofSeconds(2));
            assertEquals(ErrorCode.FLOW_CONTROL.code(), errorCode(close), "GOAWAY-refused DATA over the session window");
        }
    }

    @Test
    void refusalsBeyondSessionWindowDoNotStallAcceptedStream() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .settings(Settings.defaults().toBuilder().maxIncomingStreamsBidi(1L).build())
                .build();
        try (RawPeerSession peer = RawPeerSession.open(config, 0L)) {
            long sent = 1L;
            peer.send(new FrameCodec.Frame(FrameType.DATA, 0, 4L, "a".getBytes(StandardCharsets.UTF_8)));
            ZmuxStream accepted = peer.session().acceptStream(Duration.ofSeconds(1));

            // A credit-respecting sender keeps opening streams that are refused, for well over one session window.
            int frame = 16 * 1024;
            long nextId = 8L;
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (sent < 2L * SESSION_WINDOW) {
                if (sent + frame > sessionLimitAdvertised(peer)) {
                    assertTrue(System.nanoTime() < deadline, "sender stalled on leaked session credit at " + sent + " bytes");
                    peer.poll(Duration.ofMillis(20));
                    continue;
                }
                peer.send(data(nextId, Protocol.FRAME_FLAG_FIN, frame));
                nextId += 4L;
                sent += frame;
            }

            byte[] tail = new byte[frame];
            while (sent + tail.length > sessionLimitAdvertised(peer)) {
                assertTrue(System.nanoTime() < deadline, "sender stalled on leaked session credit before the tail");
                peer.poll(Duration.ofMillis(20));
            }
            peer.send(new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 4L, tail));
            assertEquals(1 + tail.length, accepted.readAllBytes().length, "the accepted stream still receives a full frame");
            assertTrue(peer.seen(f -> f.type() == FrameType.CLOSE).isEmpty());
            accepted.close();
        }
    }

    private static final class RawPeerSession implements AutoCloseable {
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

        static RawPeerSession open(ZmuxConfig sessionConfig, long rawCapabilities) throws Exception {
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
            }, "refused-stream-raw-open");
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

        void sendRaw(byte[] bytes) throws IOException {
            output.write(bytes);
            output.flush();
        }

        /** Reads at most one frame into the seen list. */
        void poll(Duration timeout) throws IOException {
            FrameCodec.Frame frame = RawFrameReads.readFrameIfStarted(socket, input, timeout);
            if (frame != null) {
                seen.add(frame);
            }
        }

        FrameCodec.Frame await(Predicate<FrameCodec.Frame> predicate, Duration timeout) throws Exception {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (true) {
                for (FrameCodec.Frame frame : seen) {
                    if (predicate.test(frame)) {
                        return frame;
                    }
                }
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("timed out waiting for matching frame; seen=" + seen.size());
                }
                poll(Duration.ofMillis(20));
            }
        }

        List<FrameCodec.Frame> seen(Predicate<FrameCodec.Frame> predicate) throws IOException {
            int before;
            try {
                do {
                    before = seen.size();
                    poll(Duration.ofMillis(20));
                } while (seen.size() != before);
            } catch (EOFException closed) {
                // The session closed the transport: everything it sent has been seen.
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
}
