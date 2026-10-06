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
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Within a stream class, local stream IDs must reach the wire in the order they were assigned
 * (SPEC §3.1 no peer-observable gaps), and a stream's first frame must be opening-eligible
 * (SPEC §6.7, §9.1): never RESET or STOP_SENDING.
 */
final class OpenerOrderTest {
    private static final int THREADS = 100;
    private static final int ROUNDS = 10;

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
    void concurrentOpenWritesKeepStreamIdOrder() throws Exception {
        assertConcurrentOpensStayOrdered(index -> OpenOptions.empty(), 100, Settings.defaults());
    }

    @Test
    void concurrentOpenWritesWithMixedPrioritiesKeepStreamIdOrder() throws Exception {
        assertConcurrentOpensStayOrdered(
                index -> OpenOptions.builder().initialPriority(index % 16).build(),
                100,
                Settings.defaults()
        );
    }

    @Test
    void concurrentOpenWritesLargerThanAvailableCreditKeepStreamIdOrder() throws Exception {
        // A small session window makes most first writes wait for credit right after their stream opens.
        Settings smallSessionWindow = Settings.defaults().toBuilder().initialMaxData(48L * 1024L).build();
        assertConcurrentOpensStayOrdered(index -> OpenOptions.empty(), 40_000, smallSessionWindow);
    }

    @Test
    void cancelWriteRacingFirstWriteNeverMakesResetTheFirstFrame() throws Exception {
        try (RawPeerSession peer = RawPeerSession.open(ZmuxConfig.builder().build(), 0L, Settings.defaults())) {
            ExecutorService writers = Executors.newSingleThreadExecutor();
            try {
                for (int iteration = 0; iteration < 120; iteration++) {
                    ZmuxStream stream = peer.session().openStream();
                    Future<?> write = writers.submit(() -> {
                        stream.write(new byte[10]);
                        return null;
                    });
                    long spinUntil = System.nanoTime() + TimeUnit.MICROSECONDS.toNanos(50L * (iteration % 12));
                    while (System.nanoTime() < spinUntil) {
                        Thread.yield();
                    }
                    try {
                        stream.cancelWrite(ErrorCode.CANCELLED.code());
                    } catch (IOException ignored) {
                        // The write may already have finished the stream or failed it; both are fine here.
                    }
                    try {
                        write.get(2, TimeUnit.SECONDS);
                    } catch (ExecutionException ignored) {
                        // cancelWrite may fail the in-flight write.
                    }
                }
            } finally {
                writers.shutdownNow();
            }

            Map<Long, FrameType> firstFrames = new LinkedHashMap<>();
            long lastOpened = -1L;
            FrameCodec.Frame frame;
            while ((frame = peer.pollFrame(Duration.ofMillis(300))) != null) {
                long streamId = frame.streamId();
                if (streamId == 0L || firstFrames.containsKey(streamId)) {
                    continue;
                }
                firstFrames.put(streamId, frame.type());
                assertTrue(
                        frame.type() == FrameType.DATA || frame.type() == FrameType.ABORT,
                        "first frame for stream " + streamId + " must be opening-eligible, was " + frame.type()
                );
                assertTrue(streamId > lastOpened, "stream " + streamId + " opened after " + lastOpened);
                assertEquals(lastOpened < 0L ? 1L : lastOpened + 4L, streamId, "local stream IDs must open without gaps");
                lastOpened = streamId;
            }
            assertFalse(firstFrames.isEmpty(), "the race should have opened streams on the wire");
        }
    }

    private static void assertConcurrentOpensStayOrdered(IntFunction<OpenOptions> options,
                                                         int payloadLength,
                                                         Settings serverSettings) throws Exception {
        byte[] payload = new byte[payloadLength];
        Arrays.fill(payload, (byte) 7);
        ExecutorService readers = Executors.newCachedThreadPool();
        ExecutorService writers = Executors.newFixedThreadPool(THREADS);
        List<Long> acceptedIds = Collections.synchronizedList(new ArrayList<>());
        List<Future<Integer>> reads = Collections.synchronizedList(new ArrayList<>());
        // Leave room for every writer to hold a provisional open at once, and for streams of earlier
        // rounds that are still draining.
        ZmuxConfig clientConfig = ZmuxConfig.builder().acceptBacklogLimit(4 * THREADS).build();
        ZmuxConfig serverConfig = ZmuxConfig.builder()
                .acceptBacklogLimit(4 * THREADS)
                .settings(serverSettings.toBuilder().maxIncomingStreamsBidi(4L * THREADS * ROUNDS).build())
                .build();
        try (SessionPair pair = SessionPair.open(clientConfig, serverConfig)) {
            Thread acceptor = new Thread(() -> {
                try {
                    for (int i = 0; i < THREADS * ROUNDS; i++) {
                        ZmuxStream accepted = pair.server().acceptStream(Duration.ofSeconds(10));
                        acceptedIds.add(accepted.streamId());
                        reads.add(readers.submit(() -> {
                            byte[] received = accepted.readAllBytes();
                            accepted.close();
                            return received.length;
                        }));
                    }
                } catch (Exception ignored) {
                    // Reported through the accepted count and session state below.
                }
            }, "opener-order-acceptor");
            acceptor.start();

            for (int round = 0; round < ROUNDS; round++) {
                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> writes = new ArrayList<>(THREADS);
                for (int i = 0; i < THREADS; i++) {
                    OpenOptions openOptions = options.apply(i);
                    writes.add(writers.submit(() -> {
                        start.await();
                        ZmuxStream stream = pair.client().openStream(openOptions);
                        stream.write(payload);
                        stream.closeWrite();
                        return null;
                    }));
                }
                start.countDown();
                for (Future<?> write : writes) {
                    try {
                        write.get(20, TimeUnit.SECONDS);
                    } catch (ExecutionException failure) {
                        fail("concurrent open+write failed in round " + round
                                + " (server state " + pair.server().state() + ")", failure.getCause());
                    }
                }
            }

            acceptor.join(TimeUnit.SECONDS.toMillis(20));
            assertFalse(pair.server().isClosed(), "server session must not fail on out-of-order stream IDs, state=" + pair.server().state());
            assertFalse(pair.client().isClosed(), "client session must stay open, state=" + pair.client().state());
            assertEquals(THREADS * ROUNDS, acceptedIds.size(), "server should accept every opened stream");
            for (int i = 1; i < acceptedIds.size(); i++) {
                assertEquals(acceptedIds.get(i - 1) + 4L, acceptedIds.get(i), "accepted stream IDs must be consecutive and ascending");
            }
            synchronized (reads) {
                for (Future<Integer> read : reads) {
                    assertEquals(payloadLength, read.get(20, TimeUnit.SECONDS), "server should read every stream payload");
                }
            }
        } finally {
            writers.shutdownNow();
            readers.shutdownNow();
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
            }, "opener-order-raw-open");
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

        FrameCodec.Frame pollFrame(Duration timeout) throws IOException {
            return RawFrameReads.readFrameIfStarted(socket, input, timeout);
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
            }, "opener-order-client-open");
            Thread serverThread = new Thread(() -> {
                try {
                    serverRef.set(Zmux.server(rightConn, serverConfig));
                } catch (Throwable error) {
                    errorRef.compareAndSet(null, error);
                } finally {
                    established.countDown();
                }
            }, "opener-order-server-open");
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
