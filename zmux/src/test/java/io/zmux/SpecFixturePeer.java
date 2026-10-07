package io.zmux;

import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;
import io.zmux.protocol.Preface;
import io.zmux.protocol.Protocol;
import io.zmux.transport.BasicDuplexConnection;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Raw initiator peer in front of a real Java responder session, shared by the zmux-spec fixture runners.
 *
 * <p>Because the peer is the initiator, peer-owned stream IDs are 4, 8, ... (bidi) and 2, 6, ... (uni), and the
 * session's own IDs are 1, 5, ... (bidi) and 3, 7, ... (uni), which is the role assignment the fixtures assume.
 */
final class SpecFixturePeer implements AutoCloseable {
    static final Duration WAIT = Duration.ofSeconds(2);
    static final long PEER_BIDI = 4L;
    static final long PEER_UNI = 2L;
    static final long LOCAL_BIDI = 1L;
    static final long LOCAL_UNI = 3L;

    private final ZmuxNativeSession session;
    private final Socket socket;
    private final BufferedInputStream input;
    private final BufferedOutputStream output;
    private final List<FrameCodec.Frame> unconsumed = new ArrayList<>();
    private final List<FrameCodec.Frame> received = new ArrayList<>();
    private long pingCounter;
    private boolean transportClosed;

    private SpecFixturePeer(ZmuxNativeSession session, Socket socket, BufferedInputStream input, BufferedOutputStream output) {
        this.session = session;
        this.socket = socket;
        this.input = input;
        this.output = output;
    }

    static SpecFixturePeer open(ZmuxConfig sessionConfig) throws Exception {
        return open(sessionConfig, 0L, Settings.defaults());
    }

    static SpecFixturePeer open(ZmuxConfig sessionConfig, long peerCapabilities, Settings peerSettings) throws Exception {
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
        }, "spec-fixture-raw-open");
        sessionThread.start();

        FrameCodec.writePreface(peerOutput, new Preface(
                Protocol.PREFACE_VERSION,
                Role.INITIATOR,
                0L,
                Protocol.PROTO_VERSION,
                Protocol.PROTO_VERSION,
                peerCapabilities,
                peerSettings
        ));
        peerOutput.flush();
        FrameCodec.readPreface(peerInput);
        established.await();
        rethrow(errorRef.get());
        return new SpecFixturePeer(sessionRef.get(), peerSocket, peerInput, peerOutput);
    }

    static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String describe(List<FrameCodec.Frame> frames) {
        StringBuilder out = new StringBuilder("[");
        for (FrameCodec.Frame frame : frames) {
            if (out.length() > 1) {
                out.append(", ");
            }
            out.append(frame.type()).append('@').append(frame.streamId());
        }
        return out.append(']').toString();
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

    ZmuxNativeSession session() {
        return session;
    }

    void send(FrameType type, int flags, long streamId, byte[] payload) throws IOException {
        send(new FrameCodec.Frame(type, flags, streamId, payload));
    }

    void send(FrameCodec.Frame frame) throws IOException {
        FrameCodec.writeFrame(output, frame, Settings.defaults().limits());
        output.flush();
    }

    void sendRaw(byte[] bytes) throws IOException {
        output.write(bytes);
        output.flush();
    }

    /**
     * Opens peer-owned bidi {@code streamId} with DATA and returns the accepted Java stream.
     */
    ZmuxStream openPeerStream(long streamId) throws Exception {
        send(FrameType.DATA, 0, streamId, bytes("x"));
        ZmuxStream stream = session.acceptStream(WAIT);
        assertEquals(streamId, stream.streamId(), "accepted stream ID");
        return stream;
    }

    /**
     * Round-trips a PING so every frame sent before it has been processed by the session.
     */
    void sync() throws Exception {
        byte[] token = new byte[8];
        long value = ++pingCounter ^ 0x5a5a5a5a00000000L;
        for (int i = 0; i < token.length; i++) {
            token[i] = (byte) (value >>> (56 - 8 * i));
        }
        send(FrameType.PING, 0, 0L, token);
        await(frame -> frame.type() == FrameType.PONG
                && frame.payload().length >= token.length
                && Arrays.equals(Arrays.copyOf(frame.payload(), token.length), token), "PONG for sync PING");
    }

    /**
     * Waits for the session's CLOSE, checks that the session failed, and returns the CLOSE code.
     */
    ErrorCode awaitSessionClose() throws Exception {
        FrameCodec.Frame close = await(frame -> frame.type() == FrameType.CLOSE, "CLOSE");
        long code = FrameCodec.parseErrorPayload(close.payload()).code();
        assertTrue(session.awaitTermination(WAIT), "session should terminate after CLOSE");
        assertEquals(SessionState.FAILED, session.state(), "a session-scope error must fail the session");
        return ErrorCode.fromCode(code);
    }

    /**
     * Waits for an ABORT on {@code streamId}, checks that the session is still usable, and returns the code.
     */
    ErrorCode awaitStreamAbort(long streamId) throws Exception {
        FrameCodec.Frame abort = await(frame -> frame.type() == FrameType.ABORT && frame.streamId() == streamId, "ABORT on stream " + streamId);
        long code = FrameCodec.parseErrorPayload(abort.payload()).code();
        sync();
        assertFalse(session.state().terminal(), "a stream-scope error must not end the session");
        return ErrorCode.fromCode(code);
    }

    /**
     * Asserts that nothing the session sent so far (and not yet consumed) is a CLOSE or targets {@code streamId}.
     */
    void assertNothingSentOn(long streamId) throws IOException {
        for (FrameCodec.Frame frame : drain()) {
            assertFalse(frame.type() == FrameType.CLOSE, "unexpected CLOSE");
            assertFalse(frame.streamId() == streamId, "unexpected " + frame.type() + " on stream " + streamId);
        }
    }

    FrameCodec.Frame await(Predicate<FrameCodec.Frame> predicate, String what) throws Exception {
        Iterator<FrameCodec.Frame> iterator = unconsumed.iterator();
        while (iterator.hasNext()) {
            FrameCodec.Frame frame = iterator.next();
            if (predicate.test(frame)) {
                iterator.remove();
                return frame;
            }
        }
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (System.nanoTime() < deadline && !transportClosed) {
            FrameCodec.Frame frame = poll();
            if (frame == null) {
                continue;
            }
            if (predicate.test(frame)) {
                return frame;
            }
            unconsumed.add(frame);
        }
        throw new AssertionError("timed out waiting for " + what + "; transportClosed=" + transportClosed
                + ", session=" + session.state() + ", unconsumed=" + describe(unconsumed));
    }

    /**
     * Reads whatever is already on the wire and returns every frame no await consumed, in arrival order.
     */
    List<FrameCodec.Frame> drain() throws IOException {
        while (!transportClosed) {
            FrameCodec.Frame frame = poll();
            if (frame == null) {
                break;
            }
            unconsumed.add(frame);
        }
        return new ArrayList<>(unconsumed);
    }

    /**
     * Forgets frames that are already read but unconsumed.
     */
    void discardUnconsumed() throws IOException {
        drain();
        unconsumed.clear();
    }

    /**
     * Every frame read from the session so far, consumed or not, in arrival order.
     */
    List<FrameCodec.Frame> received() {
        return new ArrayList<>(received);
    }

    /**
     * Reads one frame if one has started to arrive. Only the wait for the first byte uses the short poll timeout
     * (see {@link RawFrameReads}), so a timeout can never strike mid-frame and desynchronize the stream.
     */
    private FrameCodec.Frame poll() throws IOException {
        try {
            FrameCodec.Frame frame = RawFrameReads.readFrameIfStarted(socket, input, 20);
            if (frame != null) {
                received.add(frame);
            }
            return frame;
        } catch (EOFException | SocketException e) {
            transportClosed = true;
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
        if (error != null && !session.state().terminal()) {
            throw error;
        }
    }
}
