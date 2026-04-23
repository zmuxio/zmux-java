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

final class WritevFinalTest {
    private static int findOpenInfoLengthForPrefixSize(long capabilities, int targetSize) throws Exception {
        for (int length = 1; length <= targetSize; length++) {
            byte[] openInfo = new byte[length];
            byte[] prefix = FrameCodec.buildOpenMetadataPrefix(capabilities, null, null, openInfo, targetSize);
            if (prefix.length == targetSize) {
                return length;
            }
        }
        return -1;
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
    void writevFinalPacksSmallPartsIntoSingleFinFrame() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L, Settings.defaults())) {
            ZmuxStream stream = peer.session().openStream();
            int written = stream.writevFinal(
                    "hello".getBytes(StandardCharsets.UTF_8),
                    new byte[0],
                    "world".getBytes(StandardCharsets.UTF_8)
            );

            assertEquals(10, written, "writevFinal byte count mismatch");

            FrameCodec.Frame frame = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.DATA, frame.type(), "multipart final write should emit DATA");
            assertTrue((frame.flags() & Protocol.FRAME_FLAG_FIN) != 0, "multipart final write should carry FIN");
            assertEquals("helloworld", new String(frame.payload(), StandardCharsets.UTF_8), "multipart final payload mismatch");
            assertEquals(1L, frame.streamId(), "unexpected local stream id");
        }
    }

    @Test
    void writeFinalEmptyOpensAndFinishesStream() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L, Settings.defaults())) {
            ZmuxStream stream = peer.session().openStream();
            int written = stream.writeFinal(new byte[0]);

            assertEquals(0, written, "empty final write should report zero app bytes");

            FrameCodec.Frame frame = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.DATA, frame.type(), "empty final write should emit DATA");
            assertTrue((frame.flags() & Protocol.FRAME_FLAG_FIN) != 0, "empty final write should carry FIN");
            assertEquals(0, frame.payload().length, "empty final write should not carry app payload");
            assertEquals(1L, frame.streamId(), "unexpected local stream id");
        }
    }

    @Test
    void zeroLengthWriteDoesNotEmitOpeningFrame() throws Exception {
        long capabilities = Protocol.CAPABILITY_OPEN_METADATA;
        ZmuxConfig config = ZmuxConfig.builder().capabilities(capabilities).settings(Settings.defaults()).build();
        try (RawPeerSession peer = RawPeerSession.open(config, capabilities, Settings.defaults())) {
            ZmuxStream stream = peer.session().openStream(new OpenOptions(null, null, "meta".getBytes(StandardCharsets.UTF_8)));
            stream.write(new byte[0]);
            peer.assertNoFrame(Duration.ofMillis(200));
        }
    }

    @Test
    void zeroLengthOrdinaryWriteRemainsNoOpAfterCloseWrite() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L, Settings.defaults())) {
            ZmuxStream stream = peer.session().openStream();
            stream.closeWrite();

            FrameCodec.Frame fin = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.DATA, fin.type(), "closeWrite should emit DATA");
            assertTrue((fin.flags() & Protocol.FRAME_FLAG_FIN) != 0, "closeWrite should carry FIN");

            stream.write(new byte[0], 0, 0);
            peer.assertNoFrame(Duration.ofMillis(200));
            assertThrows(WriteClosedException.class, () -> stream.write("x".getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Test
    void writevFinalSplitsWhenOpenMetadataConsumesWholeFirstFrame() throws Exception {
        long capabilities = Protocol.CAPABILITY_OPEN_METADATA;
        Settings settings = Settings.defaults();
        int maxFramePayload = (int) settings.maxFramePayload();
        int openInfoLength = findOpenInfoLengthForPrefixSize(capabilities, maxFramePayload);
        assertTrue(openInfoLength > 0, "expected to find an open_info length that fills one whole frame payload");

        byte[] openInfo = new byte[openInfoLength];
        Arrays.fill(openInfo, (byte) 'm');
        byte[] payload = "x".getBytes(StandardCharsets.UTF_8);
        ZmuxConfig config = ZmuxConfig.builder().capabilities(capabilities).settings(settings).build();

        try (RawPeerSession peer = RawPeerSession.open(config, capabilities, settings)) {
            ZmuxStream stream = peer.session().openStream(new OpenOptions(null, null, openInfo));
            int written = stream.writevFinal(payload);
            assertEquals(payload.length, written, "writevFinal byte count mismatch");

            FrameCodec.Frame opener = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.DATA, opener.type(), "opening frame type mismatch");
            assertTrue((opener.flags() & Protocol.FRAME_FLAG_OPEN_METADATA) != 0, "opening frame should carry OPEN_METADATA");
            assertEquals(maxFramePayload, opener.payload().length, "opening metadata payload should saturate the first frame");
            FrameCodec.DataPayload parsedOpener = FrameCodec.parseDataPayload(opener.payload(), opener.flags());
            assertArrayEquals(openInfo, parsedOpener.openInfo(), "opening frame open_info mismatch");
            assertEquals(0, parsedOpener.appData().length, "opening frame should not overrun with app payload");

            FrameCodec.Frame fin = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.DATA, fin.type(), "payload continuation should still use DATA");
            assertEquals(opener.streamId(), fin.streamId(), "continuation stream id mismatch");
            assertTrue((fin.flags() & Protocol.FRAME_FLAG_FIN) != 0, "continuation should carry FIN");
            assertArrayEquals(payload, fin.payload(), "continuation payload mismatch");
        }
    }

    @Test
    void writevFinalRejectsAggregateLengthOverflowWithoutPartialWrite() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L, Settings.defaults())) {
            ZmuxStream stream = peer.session().openStream();
            byte[] chunk = new byte[1 << 20];
            Arrays.fill(chunk, (byte) 'x');
            byte[][] parts = new byte[2048][];
            Arrays.fill(parts, chunk);

            IOException error = assertThrows(IOException.class, () -> stream.writevFinal(parts));
            ZmuxException frameSize = assertInstanceOf(ZmuxException.class, error);
            assertEquals(ErrorCode.FRAME_SIZE.code(), frameSize.code(), "overflowing multipart write should surface FRAME_SIZE");
            ZmuxErrorDetails details = ZmuxErrors.details(frameSize);
            assertNotNull(details, "overflowing multipart write should expose structured error details");
            assertEquals("writevFinal", details.operation());
            assertEquals(ZmuxErrorScope.STREAM, details.scope());
            assertEquals(ZmuxErrorSource.LOCAL, details.source());
            assertEquals(ZmuxErrorDirection.WRITE, details.direction());

            peer.assertNoFrame(Duration.ofMillis(200));

            int written = stream.writeFinal("ok".getBytes(StandardCharsets.UTF_8));
            assertEquals(2, written, "stream should remain writable after rejecting overflowing multipart write");

            FrameCodec.Frame frame = peer.readFrame(Duration.ofSeconds(1));
            assertEquals(FrameType.DATA, frame.type(), "recovery write should still emit DATA");
            assertTrue((frame.flags() & Protocol.FRAME_FLAG_FIN) != 0, "recovery write should carry FIN");
            assertEquals("ok", new String(frame.payload(), StandardCharsets.UTF_8), "recovery payload mismatch");
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
            }, "writev-final-raw-open");
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

        FrameCodec.Frame readFrame(Duration timeout) throws Exception {
            socket.setSoTimeout((int) timeout.toMillis());
            try {
                return FrameCodec.readFrame(input, Settings.defaults().limits());
            } catch (SocketTimeoutException e) {
                throw new AssertionError("timed out waiting for frame", e);
            }
        }

        void assertNoFrame(Duration timeout) throws Exception {
            socket.setSoTimeout((int) timeout.toMillis());
            try {
                FrameCodec.readFrame(input, Settings.defaults().limits());
                throw new AssertionError("unexpected frame received");
            } catch (SocketTimeoutException expected) {
                return;
            }
        }

        @Override
        public void close() throws Exception {
            IOException error = null;
            try {
                session.closeWithError(ErrorCode.NO_ERROR.code(), "");
            } catch (IOException e) {
                error = e;
            }
            try {
                input.close();
            } catch (IOException e) {
                if (error == null) {
                    error = e;
                }
            }
            try {
                output.close();
            } catch (IOException e) {
                if (error == null) {
                    error = e;
                }
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
