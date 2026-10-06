package io.zmux;

import io.zmux.protocol.*;
import io.zmux.transport.BasicDuplexConnection;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Outbound DATA must be fragmented to the currently available stream/session credit and to what the local
 * send queue can admit, instead of waiting for a fixed fragment size to fit (CONFORMANCE §5,
 * IMPLEMENTATION §2.2). BLOCKED is only sent once a limit has actually been reached (SPEC §6.6).
 */
final class CreditFragmentationTest {
    private static final long SERVER_LOCAL_BIDI = 1L;

    private static void rethrow(Throwable error) throws Exception {
        if (error == null) {
            return;
        }
        if (error instanceof Exception) {
            throw (Exception) error;
        }
        throw new RuntimeException(error);
    }

    private static byte[] pattern(int length) {
        byte[] payload = new byte[length];
        for (int i = 0; i < length; i++) {
            payload[i] = (byte) (i * 31 + 7);
        }
        return payload;
    }

    private static long varintValue(FrameCodec.Frame frame) throws IOException {
        return Varint62.decode(frame.payload(), 0).value();
    }

    private static void assertDataFrame(FrameCodec.Frame frame, long streamId, int expectedLength, String message) {
        assertEquals(FrameType.DATA, frame.type(), message + ": frame type");
        assertEquals(streamId, frame.streamId(), message + ": stream id");
        assertEquals(expectedLength, frame.payload().length, message + ": payload length");
    }

    private static void assertBlockedFrame(FrameCodec.Frame frame, long streamId, long offset, String message)
            throws IOException {
        assertEquals(FrameType.BLOCKED, frame.type(), message + ": frame type");
        assertEquals(streamId, frame.streamId(), message + ": stream id");
        assertEquals(offset, varintValue(frame), message + ": BLOCKED offset");
    }

    private static int readDataUntil(RawPeerSession peer, long streamId, int expectedBytes) throws Exception {
        int received = 0;
        while (received < expectedBytes) {
            FrameCodec.Frame frame = peer.readFrame(Duration.ofSeconds(2));
            if (frame.type() == FrameType.BLOCKED) {
                continue;
            }
            assertEquals(FrameType.DATA, frame.type(), "only DATA (or BLOCKED) expected while draining the write");
            assertEquals(streamId, frame.streamId(), "drained DATA stream id mismatch");
            received += frame.payload().length;
        }
        assertEquals(expectedBytes, received, "drained DATA byte count mismatch");
        return received;
    }

    @Test
    void openingWriteLargerThanStreamWindowCarriesAvailableCreditInOpener() throws Exception {
        Settings peerSettings = Settings.defaults().toBuilder()
                .initialMaxStreamDataBidiPeerOpened(1000L)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L, peerSettings)) {
            AsyncWrite write = AsyncWrite.start(() -> {
                ZmuxStream stream = peer.session().openStream();
                stream.write(pattern(4000));
                return stream;
            });

            FrameCodec.Frame opener = peer.readFrame(Duration.ofSeconds(2));
            assertDataFrame(opener, SERVER_LOCAL_BIDI, 1000, "opener should carry exactly the advertised stream window");
            assertEquals(0, opener.flags() & Protocol.FRAME_FLAG_FIN, "partial opener must not carry FIN");
            assertArrayEquals(Arrays.copyOf(pattern(4000), 1000), opener.payload(), "opener payload mismatch");
            assertBlockedFrame(peer.readFrame(Duration.ofSeconds(2)), SERVER_LOCAL_BIDI, 1000L, "stream BLOCKED once the window is used up");
            assertFalse(write.isDone(), "writer should wait for more stream credit");

            peer.send(new FrameCodec.Frame(FrameType.MAX_DATA, 0, SERVER_LOCAL_BIDI, Varint62.encode(4000L)));
            readDataUntil(peer, SERVER_LOCAL_BIDI, 3000);
            write.await(Duration.ofSeconds(2)).close();
        }
    }

    @Test
    void openingWriteLargerThanSessionWindowCarriesAvailableCreditInOpener() throws Exception {
        Settings peerSettings = Settings.defaults().toBuilder()
                .initialMaxData(1000L)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L, peerSettings)) {
            AsyncWrite write = AsyncWrite.start(() -> {
                ZmuxStream stream = peer.session().openStream();
                stream.write(pattern(4000));
                return stream;
            });

            FrameCodec.Frame opener = peer.readFrame(Duration.ofSeconds(2));
            assertDataFrame(opener, SERVER_LOCAL_BIDI, 1000, "opener should carry exactly the advertised session window");
            assertBlockedFrame(peer.readFrame(Duration.ofSeconds(2)), 0L, 1000L, "session BLOCKED once the session window is used up");
            assertFalse(write.isDone(), "writer should wait for more session credit");

            peer.send(new FrameCodec.Frame(FrameType.MAX_DATA, 0, 0L, Varint62.encode(4000L)));
            readDataUntil(peer, SERVER_LOCAL_BIDI, 3000);
            write.await(Duration.ofSeconds(2)).close();
        }
    }

    @Test
    void writeStraddlingStreamWindowSendsRemainingCreditBeforeBlocked() throws Exception {
        Settings peerSettings = Settings.defaults().toBuilder()
                .initialMaxStreamDataBidiPeerOpened(1000L)
                .build();
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L, peerSettings)) {
            ZmuxStream stream = peer.session().openStream();
            stream.write(pattern(10));
            assertDataFrame(peer.readFrame(Duration.ofSeconds(2)), SERVER_LOCAL_BIDI, 10, "first small write");

            AsyncWrite write = AsyncWrite.start(() -> {
                stream.write(pattern(4000));
                return stream;
            });
            assertDataFrame(peer.readFrame(Duration.ofSeconds(2)), SERVER_LOCAL_BIDI, 990, "remaining stream credit must be used before BLOCKED");
            assertBlockedFrame(peer.readFrame(Duration.ofSeconds(2)), SERVER_LOCAL_BIDI, 1000L, "stream BLOCKED only after the window is used up");

            peer.send(new FrameCodec.Frame(FrameType.MAX_DATA, 0, SERVER_LOCAL_BIDI, Varint62.encode(4010L)));
            readDataUntil(peer, SERVER_LOCAL_BIDI, 3010);
            write.await(Duration.ofSeconds(2));
            stream.close();
        }
    }

    @Test
    void writevFinalFragmentsToStreamWindowWithOpenMetadataPrefix() throws Exception {
        Settings peerSettings = Settings.defaults().toBuilder()
                .initialMaxStreamDataBidiPeerOpened(1000L)
                .build();
        long capabilities = Protocol.CAPABILITY_OPEN_METADATA;
        byte[] openInfo = "route=a".getBytes(StandardCharsets.UTF_8);
        byte[] prefix = FrameCodec.buildOpenMetadataPrefix(
                capabilities,
                null,
                null,
                openInfo,
                Settings.defaults().maxFramePayload()
        );
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), capabilities, peerSettings)) {
            AtomicInteger written = new AtomicInteger(-1);
            AsyncWrite write = AsyncWrite.start(() -> {
                ZmuxStream stream = peer.session().openStream(OpenOptions.builder().openInfo(openInfo).build());
                written.set(stream.writevFinal(pattern(1500), pattern(1500), pattern(1000)));
                return stream;
            });

            FrameCodec.Frame opener = peer.readFrame(Duration.ofSeconds(2));
            assertEquals(FrameType.DATA, opener.type(), "first frame should open the stream");
            assertEquals(SERVER_LOCAL_BIDI, opener.streamId(), "opener stream id mismatch");
            assertNotEquals(0, opener.flags() & Protocol.FRAME_FLAG_OPEN_METADATA, "opener should carry OPEN_METADATA");
            assertEquals(0, opener.flags() & Protocol.FRAME_FLAG_FIN, "partial opener must not carry FIN");
            assertEquals(prefix.length + 1000, opener.payload().length, "opener should carry the metadata prefix plus the stream window");
            assertBlockedFrame(peer.readFrame(Duration.ofSeconds(2)), SERVER_LOCAL_BIDI, 1000L, "stream BLOCKED once the window is used up");

            peer.send(new FrameCodec.Frame(FrameType.MAX_DATA, 0, SERVER_LOCAL_BIDI, Varint62.encode(4000L)));
            int received = 0;
            boolean sawFin = false;
            while (!sawFin) {
                FrameCodec.Frame frame = peer.readFrame(Duration.ofSeconds(2));
                assertDataFrame(frame, SERVER_LOCAL_BIDI, frame.payload().length, "writev tail");
                received += frame.payload().length;
                sawFin = (frame.flags() & Protocol.FRAME_FLAG_FIN) != 0;
            }
            assertEquals(3000, received, "writev tail byte count mismatch");
            write.await(Duration.ofSeconds(2)).close();
            assertEquals(4000, written.get(), "writevFinal should report all bytes");
        }
    }

    @Test
    void javaPeerWithSmallStreamWindowReceivesLargeWrite() throws Exception {
        ZmuxConfig serverConfig = ZmuxConfig.builder()
                .settings(Settings.defaults().toBuilder().initialMaxStreamDataBidiPeerOpened(1000L).build())
                .build();
        assertPairTransfers(ZmuxConfig.builder().build(), serverConfig, 64 * 1024);
    }

    @Test
    void peerMaxFramePayloadAboveQueueWatermarkDoesNotStallWrites() throws Exception {
        ZmuxConfig serverConfig = ZmuxConfig.builder()
                .settings(Settings.defaults().toBuilder()
                        .maxFramePayload(1L << 20)
                        .initialMaxData(64L << 20)
                        .initialMaxStreamDataBidiPeerOpened(16L << 20)
                        .build())
                .build();
        assertPairTransfers(ZmuxConfig.builder().build(), serverConfig, 300 * 1024);
        assertPairTransfers(ZmuxConfig.builder().build(), serverConfig, 1 << 20);
    }

    @Test
    void peerMaxFramePayloadAboveQueueWatermarkWithDefaultWindows() throws Exception {
        ZmuxConfig serverConfig = ZmuxConfig.builder()
                .settings(Settings.defaults().toBuilder().maxFramePayload(1L << 20).build())
                .build();
        assertPairTransfers(ZmuxConfig.builder().build(), serverConfig, 100 * 1024);
        assertPairTransfers(ZmuxConfig.builder().build(), serverConfig, 1 << 20);
    }

    @Test
    void perStreamQueueWatermarkBelowOneFrameDoesNotStallWrites() throws Exception {
        ZmuxConfig clientConfig = ZmuxConfig.builder().perStreamQueuedDataHwm(4096L).build();
        assertPairTransfers(clientConfig, ZmuxConfig.builder().build(), 60_000);
    }

    @Test
    void writeLargerThanQueueWatermarkWakesWriterWithLargeWindows() throws Exception {
        ZmuxConfig serverConfig = ZmuxConfig.builder()
                .settings(Settings.defaults().toBuilder()
                        .initialMaxData(64L << 20)
                        .initialMaxStreamDataBidiPeerOpened(16L << 20)
                        .build())
                .build();
        assertPairTransfers(ZmuxConfig.builder().build(), serverConfig, 300 * 1024);
    }

    private static void assertPairTransfers(ZmuxConfig clientConfig, ZmuxConfig serverConfig, int length) throws Exception {
        byte[] payload = pattern(length);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (SessionPair pair = SessionPair.open(clientConfig, serverConfig)) {
            Future<byte[]> serverRead = executor.submit(() -> {
                ZmuxStream accepted = pair.server().acceptStream(Duration.ofSeconds(5));
                byte[] received = accepted.readAllBytes();
                accepted.close();
                return received;
            });
            Future<ZmuxStream> clientWrite = executor.submit(() -> {
                ZmuxStream stream = pair.client().openStream();
                stream.write(payload);
                stream.closeWrite();
                return stream;
            });
            ZmuxStream clientStream;
            try {
                clientStream = clientWrite.get(5, TimeUnit.SECONDS);
            } catch (TimeoutException timeout) {
                throw new AssertionError("write of " + length + " bytes stalled", timeout);
            }
            byte[] received = serverRead.get(5, TimeUnit.SECONDS);
            assertArrayEquals(payload, received, "server should read every byte of a " + length + " byte write");
            clientStream.close();
        } finally {
            executor.shutdownNow();
        }
    }

    @FunctionalInterface
    private interface StreamAction {
        Object run() throws Exception;
    }

    private static final class AsyncWrite {
        private final Thread thread;
        private final AtomicReference<Object> result = new AtomicReference<>();
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private final CountDownLatch done = new CountDownLatch(1);

        private AsyncWrite(StreamAction action) {
            this.thread = new Thread(() -> {
                try {
                    result.set(action.run());
                } catch (Throwable t) {
                    error.set(t);
                } finally {
                    done.countDown();
                }
            }, "credit-fragmentation-writer");
        }

        static AsyncWrite start(StreamAction action) {
            AsyncWrite write = new AsyncWrite(action);
            write.thread.start();
            return write;
        }

        boolean isDone() {
            return done.getCount() == 0;
        }

        ZmuxStream await(Duration timeout) throws Exception {
            assertTrue(done.await(timeout.toMillis(), TimeUnit.MILLISECONDS), "writer did not finish");
            rethrow(error.get());
            return (ZmuxStream) result.get();
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
            }, "credit-fragmentation-raw-open");
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
            return RawFrameReads.readFrame(socket, input, timeout);
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
            }, "credit-fragmentation-client-open");
            Thread serverThread = new Thread(() -> {
                try {
                    serverRef.set(Zmux.server(rightConn, serverConfig));
                } catch (Throwable error) {
                    errorRef.compareAndSet(null, error);
                } finally {
                    established.countDown();
                }
            }, "credit-fragmentation-server-open");
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
