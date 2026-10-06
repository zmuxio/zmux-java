package io.zmux;

import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;
import io.zmux.protocol.Preface;
import io.zmux.protocol.Protocol;
import io.zmux.transport.DuplexConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Liveness against a transport that stops draining (IMPLEMENTATION 4 and 8): a peer that stops reading must not be
 * able to pin a session, its writer thread or its transport after the local side decided to tear it down.
 */
final class StalledTransportLivenessTest {
    private static void rethrow(Throwable error) throws Exception {
        if (error == null) {
            return;
        }
        if (error instanceof Exception) {
            throw (Exception) error;
        }
        throw new RuntimeException(error);
    }

    /**
     * Socket transport whose output can be stalled: once {@link #stall()} is called every write blocks until the
     * connection is closed, like a peer that stopped reading with a full TCP window.
     *
     * <p>With {@code closeWaitsForWrites} the close behaves like an orderly TLS close without SO_LINGER: it does not
     * release a stalled write, and it waits until no write is in progress (an SSLSocket's close_notify needs the
     * record lock the blocked write holds). Only {@link #release()} ends the stall then.
     */
    private static final class StallableConnection implements DuplexConnection {
        private final Socket socket;
        private final InputStream input;
        private final OutputStream socketOutput;
        private final boolean closeWaitsForWrites;
        private final Object gate = new Object();
        private final CountDownLatch writerBlocked = new CountDownLatch(1);
        private final CountDownLatch closeStarted = new CountDownLatch(1);
        private final CountDownLatch closed = new CountDownLatch(1);
        private boolean stalled;
        private boolean closing;
        private boolean released;
        private int writesInProgress;
        private final OutputStream output = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                write(new byte[]{(byte) b}, 0, 1);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                beginWrite();
                try {
                    socketOutput.write(b, off, len);
                } finally {
                    endWrite();
                }
            }

            @Override
            public void flush() throws IOException {
                beginWrite();
                try {
                    socketOutput.flush();
                } finally {
                    endWrite();
                }
            }
        };

        StallableConnection(Socket socket) throws IOException {
            this(socket, false);
        }

        StallableConnection(Socket socket, boolean closeWaitsForWrites) throws IOException {
            this.socket = socket;
            this.input = socket.getInputStream();
            this.socketOutput = socket.getOutputStream();
            this.closeWaitsForWrites = closeWaitsForWrites;
        }

        void stall() {
            synchronized (gate) {
                stalled = true;
            }
        }

        boolean awaitWriterBlocked(Duration timeout) throws InterruptedException {
            return writerBlocked.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        boolean awaitClosed(Duration timeout) throws InterruptedException {
            return closed.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        boolean awaitCloseStarted(Duration timeout) throws InterruptedException {
            return closeStarted.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        /** Ends the stall: blocked writes fail and a waiting close completes. */
        void release() {
            synchronized (gate) {
                released = true;
                gate.notifyAll();
            }
        }

        private void beginWrite() throws IOException {
            synchronized (gate) {
                writesInProgress++;
                try {
                    while (stalled && !released && !(closing && !closeWaitsForWrites)) {
                        writerBlocked.countDown();
                        gate.wait();
                    }
                } catch (InterruptedException interrupted) {
                    endWriteLocked();
                    Thread.currentThread().interrupt();
                    throw new IOException("stalled write interrupted", interrupted);
                }
                if (closing || released) {
                    endWriteLocked();
                    throw new IOException("transport closed");
                }
            }
        }

        private void endWrite() {
            synchronized (gate) {
                endWriteLocked();
            }
        }

        private void endWriteLocked() {
            writesInProgress--;
            gate.notifyAll();
        }

        @Override
        public InputStream input() {
            return input;
        }

        @Override
        public OutputStream output() {
            return output;
        }

        @Override
        public void close() throws IOException {
            synchronized (gate) {
                closing = true;
                gate.notifyAll();
                closeStarted.countDown();
                while (closeWaitsForWrites && writesInProgress > 0) {
                    try {
                        gate.wait();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IOException("close interrupted", interrupted);
                    }
                }
            }
            closed.countDown();
            socket.close();
        }
    }

    private static final class StalledPeer implements AutoCloseable {
        private final ZmuxNativeSession session;
        private final StallableConnection connection;
        private final Socket peerSocket;

        private StalledPeer(ZmuxNativeSession session, StallableConnection connection, Socket peerSocket) {
            this.session = session;
            this.connection = connection;
            this.peerSocket = peerSocket;
        }

        /** Establishes a server session with a raw, silent initiator peer that grants plenty of credit. */
        static StalledPeer open(ZmuxConfig config) throws Exception {
            return open(config, false);
        }

        static StalledPeer open(ZmuxConfig config, boolean closeWaitsForWrites) throws Exception {
            ServerSocket listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Socket peerSocket = new Socket("127.0.0.1", listener.getLocalPort());
            Socket sessionSocket = listener.accept();
            listener.close();
            StallableConnection connection = new StallableConnection(sessionSocket, closeWaitsForWrites);

            AtomicReference<ZmuxNativeSession> sessionRef = new AtomicReference<>();
            AtomicReference<Throwable> errorRef = new AtomicReference<>();
            CountDownLatch established = new CountDownLatch(1);
            Thread opener = new Thread(() -> {
                try {
                    sessionRef.set(Zmux.server(connection, config));
                } catch (Throwable error) {
                    errorRef.set(error);
                } finally {
                    established.countDown();
                }
            }, "stalled-transport-open");
            opener.start();

            BufferedOutputStream peerOutput = new BufferedOutputStream(peerSocket.getOutputStream());
            FrameCodec.writePreface(peerOutput, new Preface(
                    Protocol.PREFACE_VERSION,
                    Role.INITIATOR,
                    0L,
                    Protocol.PROTO_VERSION,
                    Protocol.PROTO_VERSION,
                    0L,
                    Settings.defaults().toBuilder()
                            .initialMaxData(64L << 20)
                            .initialMaxStreamDataBidiPeerOpened(64L << 20)
                            .initialMaxStreamDataBidiLocallyOpened(64L << 20)
                            .build()
            ));
            peerOutput.flush();
            FrameCodec.readPreface(new BufferedInputStream(peerSocket.getInputStream()));
            established.await();
            rethrow(errorRef.get());
            return new StalledPeer(sessionRef.get(), connection, peerSocket);
        }

        /** Stalls the transport, then starts a bulk stream write that blocks the session writer on it. */
        Thread stallWriterWithBulkWrite() throws Exception {
            connection.stall();
            ZmuxStream stream = session.openStream();
            Thread writer = new Thread(() -> {
                try {
                    stream.write(new byte[1 << 20]);
                } catch (IOException expected) {
                    // The session is torn down underneath the write.
                }
            }, "stalled-transport-bulk-write");
            writer.setDaemon(true);
            writer.start();
            assertTrue(connection.awaitWriterBlocked(Duration.ofSeconds(5)), "session writer should block on the stalled transport");
            return writer;
        }

        @Override
        public void close() throws Exception {
            try {
                connection.release();
                connection.close();
            } finally {
                peerSocket.close();
            }
        }
    }

    @Test
    void closeWithErrorClosesTransportWhenWriterIsStalled() throws Exception {
        try (StalledPeer peer = StalledPeer.open(ZmuxConfig.builder().build())) {
            Thread bulkWriter = peer.stallWriterWithBulkWrite();

            long startedAtNanos = System.nanoTime();
            peer.session.closeWithError(5L, "x");
            assertTrue(peer.session.awaitTermination(Duration.ofSeconds(4)), "stalled CLOSE must not keep the session from terminating");
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
            assertTrue(elapsedMillis < 2_500L, "termination should follow the bounded close-frame wait, took " + elapsedMillis + "ms");
            assertTrue(peer.connection.awaitClosed(Duration.ZERO), "the transport must be closed so the stalled writer is released");

            Optional<IOException> cause = peer.session.terminationCause();
            assertTrue(cause.isPresent(), "failed session should keep its cause");
            assertEquals(5L, ZmuxErrors.code(cause.get(), -1L), "transport-close noise must not replace the committed close cause");
            assertEquals(1L, peer.session.stats().diagnostics().closeFrameFlushTimeouts(), "close-frame flush timeout should be counted");

            bulkWriter.join(2_000L);
            assertFalse(bulkWriter.isAlive(), "stream writers blocked behind the stalled transport should be released");
            peer.session.close();
        }
    }

    @Test
    void gracefulCloseIsBoundedWhenWriterIsStalled() throws Exception {
        try (StalledPeer peer = StalledPeer.open(ZmuxConfig.builder().gracefulCloseDrainTimeout(Duration.ofMillis(200)).build())) {
            peer.stallWriterWithBulkWrite();

            long startedAtNanos = System.nanoTime();
            assertThrows(GracefulCloseTimeoutException.class, peer.session::close, "a close that could not drain should report it");
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
            assertTrue(elapsedMillis < 3_000L, "graceful close must not block on a stalled writer, took " + elapsedMillis + "ms");
            assertTrue(peer.session.awaitTermination(Duration.ZERO), "close() should return only after the session finished");
            assertTrue(peer.connection.awaitClosed(Duration.ZERO), "graceful close should close the stalled transport");
        }
    }

    @Test
    void keepaliveTimeoutFiresWhileWriterIsStalled() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .keepaliveInterval(Duration.ofMillis(300))
                .keepaliveTimeout(Duration.ofSeconds(1))
                .build();
        try (StalledPeer peer = StalledPeer.open(config)) {
            peer.stallWriterWithBulkWrite();

            assertTrue(
                    peer.session.awaitTermination(Duration.ofSeconds(6)),
                    "keepalive timeout must be evaluated while the writer is blocked on the transport"
            );
            Optional<IOException> cause = peer.session.terminationCause();
            assertTrue(cause.isPresent(), "keepalive timeout should leave a cause");
            assertEquals(ErrorCode.IDLE_TIMEOUT.code(), ZmuxErrors.code(cause.get(), -1L), "keepalive timeout should fail with IDLE_TIMEOUT");
            assertEquals(1L, peer.session.stats().diagnostics().keepaliveTimeouts(), "keepalive timeout should be counted");
            assertTrue(peer.connection.awaitClosed(Duration.ZERO), "keepalive timeout should close the stalled transport");
        }
    }

    @Test
    void pingHonoursItsDeadlineWhileWriterIsStalled() throws Exception {
        try (StalledPeer peer = StalledPeer.open(ZmuxConfig.builder().build())) {
            peer.stallWriterWithBulkWrite();

            long startedAtNanos = System.nanoTime();
            assertThrows(PingTimeoutException.class, () -> peer.session.ping(new byte[0], Duration.ofMillis(300)));
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
            assertTrue(elapsedMillis < 2_000L, "ping deadline should hold while the writer is stalled, took " + elapsedMillis + "ms");
        }
    }

    @Test
    void closeIsBoundedWhenTransportCloseWaitsForStalledWrite() throws Exception {
        try (StalledPeer peer = StalledPeer.open(ZmuxConfig.builder().build(), true)) {
            Thread bulkWriter = peer.stallWriterWithBulkWrite();

            long startedAtNanos = System.nanoTime();
            assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
                peer.session.closeWithError(5L, "x");
                peer.session.close();
            }, "a transport close that waits for the stalled write must not hold close()");
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
            assertTrue(elapsedMillis < 2_500L, "close() should follow the bounded close-frame wait, took " + elapsedMillis + "ms");
            assertTrue(peer.session.awaitTermination(Duration.ZERO), "close() should return only after the session finished");
            assertTrue(peer.connection.awaitCloseStarted(Duration.ZERO), "the forced finish should start the transport close");
            assertFalse(peer.connection.awaitClosed(Duration.ZERO), "the test transport close is still waiting for the stalled write");

            // The transport close runs off the session lock: the session stays fully usable while it is blocked.
            assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
                assertEquals(SessionState.FAILED, peer.session.state());
                assertEquals(1L, peer.session.stats().diagnostics().closeFrameFlushTimeouts(), "close-frame flush timeout should be counted");
                Optional<IOException> cause = peer.session.terminationCause();
                assertTrue(cause.isPresent(), "failed session should keep its cause");
                assertEquals(5L, ZmuxErrors.code(cause.get(), -1L), "the committed close cause should be kept");
                assertThrows(IOException.class, peer.session::openStream, "a finished session must reject opens");
                assertTrue(peer.session.awaitTermination(null), "awaitTermination must not wait for the blocked transport close");
                peer.session.close();
            }, "session operations must not block behind the transport close");

            bulkWriter.join(2_000L);
            assertFalse(bulkWriter.isAlive(), "stream writers queued behind the stalled transport should be released");

            peer.connection.release();
            assertTrue(peer.connection.awaitClosed(Duration.ofSeconds(2)), "the transport close should complete once the write returns");
        }
    }

    @Test
    void keepaliveTimeoutIsBoundedWhenTransportCloseWaitsForStalledWrite() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .keepaliveInterval(Duration.ofMillis(300))
                .keepaliveTimeout(Duration.ofSeconds(1))
                .build();
        try (StalledPeer peer = StalledPeer.open(config, true)) {
            peer.stallWriterWithBulkWrite();

            assertTrue(
                    peer.session.awaitTermination(Duration.ofSeconds(6)),
                    "keepalive timeout must finish the session although the transport close blocks"
            );
            assertTrue(peer.connection.awaitCloseStarted(Duration.ZERO), "keepalive timeout should start the transport close");
            assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
                Optional<IOException> cause = peer.session.terminationCause();
                assertTrue(cause.isPresent(), "keepalive timeout should leave a cause");
                assertEquals(ErrorCode.IDLE_TIMEOUT.code(), ZmuxErrors.code(cause.get(), -1L), "keepalive timeout should fail with IDLE_TIMEOUT");
                peer.session.close();
                peer.session.stats();
            }, "session operations must not block behind the transport close");
        }
    }

    /**
     * Keepalive checks share one JVM-wide timer: a session whose lock stays held must not delay the keepalive timeout
     * of another session whose writer is stalled (only the timer can detect that timeout).
     */
    @Test
    void keepaliveTimeoutIsNotDelayedByAnotherSessionsHeldLock() throws Exception {
        ZmuxConfig stalledConfig = ZmuxConfig.builder()
                .keepaliveInterval(Duration.ofMillis(300))
                .keepaliveTimeout(Duration.ofSeconds(1))
                .build();
        ZmuxConfig busyConfig = ZmuxConfig.builder()
                .keepaliveInterval(Duration.ofMillis(100))
                .keepaliveTimeout(Duration.ofMillis(400))
                .build();
        try (StalledPeer stalled = StalledPeer.open(stalledConfig)) {
            stalled.stallWriterWithBulkWrite();
            try (StalledPeer busy = StalledPeer.open(busyConfig)) {
                java.lang.reflect.Field lockField = busy.session.getClass().getDeclaredField("lock");
                lockField.setAccessible(true);
                Object busyLock = lockField.get(busy.session);
                CountDownLatch lockHeld = new CountDownLatch(1);
                CountDownLatch releaseLock = new CountDownLatch(1);
                Thread holder = new Thread(() -> {
                    synchronized (busyLock) {
                        lockHeld.countDown();
                        try {
                            releaseLock.await(10, TimeUnit.SECONDS);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }, "stalled-transport-lock-holder");
                holder.setDaemon(true);
                holder.start();
                try {
                    assertTrue(lockHeld.await(2, TimeUnit.SECONDS), "lock holder should start");
                    // The busy session's keepalive timer fires within ~100ms and has to wait for its held lock.
                    assertTrue(
                            stalled.session.awaitTermination(Duration.ofSeconds(4)),
                            "the stalled session's keepalive timeout must not wait for another session's lock"
                    );
                    Optional<IOException> cause = stalled.session.terminationCause();
                    assertTrue(cause.isPresent(), "keepalive timeout should leave a cause");
                    assertEquals(ErrorCode.IDLE_TIMEOUT.code(), ZmuxErrors.code(cause.get(), -1L), "keepalive timeout should fail with IDLE_TIMEOUT");
                } finally {
                    releaseLock.countDown();
                    holder.join(2_000L);
                }
            }
        }
    }

    /**
     * A server session created with {@code Zmux.server(Socket)} over a real loopback socket (TLS when a key store
     * directory is given) with a raw initiator peer on the other end.
     */
    private static final class SocketPeer implements AutoCloseable {
        private final ZmuxNativeSession session;
        private final Socket sessionSocket;
        private final Socket peerSocket;
        private final BufferedInputStream peerInput;

        private SocketPeer(ZmuxNativeSession session, Socket sessionSocket, Socket peerSocket, BufferedInputStream peerInput) {
            this.session = session;
            this.sessionSocket = sessionSocket;
            this.peerSocket = peerSocket;
            this.peerInput = peerInput;
        }

        static SocketPeer openPlain() throws Exception {
            return open(ServerSocketFactory.getDefault(), SocketFactory.getDefault());
        }

        static SocketPeer openTls(Path keyStoreDir) throws Exception {
            KeyStore keyStore = selfSignedKeyStore(keyStoreDir);
            KeyManagerFactory keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagers.init(keyStore, KEYSTORE_PASSWORD);
            SSLContext serverContext = SSLContext.getInstance("TLS");
            serverContext.init(keyManagers.getKeyManagers(), null, null);
            TrustManagerFactory trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagers.init(keyStore);
            SSLContext clientContext = SSLContext.getInstance("TLS");
            clientContext.init(null, trustManagers.getTrustManagers(), null);
            return open(serverContext.getServerSocketFactory(), clientContext.getSocketFactory());
        }

        private static SocketPeer open(ServerSocketFactory serverSockets, SocketFactory sockets) throws Exception {
            InetAddress loopback = InetAddress.getLoopbackAddress();
            Socket peerSocket = null;
            Socket sessionSocket = null;
            try {
                try (ServerSocket listener = serverSockets.createServerSocket(0, 1, loopback)) {
                    // Small buffers so a peer that stops reading stalls the session writer quickly.
                    listener.setReceiveBufferSize(16 * 1024);
                    peerSocket = sockets.createSocket();
                    peerSocket.setReceiveBufferSize(16 * 1024);
                    peerSocket.connect(new InetSocketAddress(loopback, listener.getLocalPort()), 5_000);
                    sessionSocket = listener.accept();
                }
                sessionSocket.setSendBufferSize(16 * 1024);
                Socket acceptedSocket = sessionSocket;
                if (peerSocket instanceof SSLSocket) {
                    AtomicReference<Throwable> handshakeError = new AtomicReference<>();
                    Thread handshake = new Thread(() -> {
                        try {
                            ((SSLSocket) acceptedSocket).startHandshake();
                        } catch (Throwable error) {
                            handshakeError.set(error);
                        }
                    }, "stalled-transport-tls-handshake");
                    handshake.start();
                    ((SSLSocket) peerSocket).startHandshake();
                    handshake.join(10_000L);
                    rethrow(handshakeError.get());
                }

                AtomicReference<ZmuxNativeSession> sessionRef = new AtomicReference<>();
                AtomicReference<Throwable> errorRef = new AtomicReference<>();
                CountDownLatch established = new CountDownLatch(1);
                Thread opener = new Thread(() -> {
                    try {
                        sessionRef.set(Zmux.server(acceptedSocket, ZmuxConfig.builder().build()));
                    } catch (Throwable error) {
                        errorRef.set(error);
                    } finally {
                        established.countDown();
                    }
                }, "stalled-transport-socket-open");
                opener.start();
                writeGenerousPeerPreface(peerSocket.getOutputStream());
                BufferedInputStream peerInput = new BufferedInputStream(peerSocket.getInputStream());
                FrameCodec.readPreface(peerInput);
                assertTrue(established.await(10, TimeUnit.SECONDS), "socket session should establish");
                rethrow(errorRef.get());
                return new SocketPeer(sessionRef.get(), sessionSocket, peerSocket, peerInput);
            } catch (Throwable error) {
                if (peerSocket != null) {
                    peerSocket.close();
                }
                if (sessionSocket != null) {
                    sessionSocket.close();
                }
                throw error;
            }
        }

        @Override
        public void close() throws Exception {
            try {
                peerSocket.close();
            } finally {
                sessionSocket.close();
            }
        }
    }

    /**
     * A real TLS socket whose peer stopped reading: an orderly SSLSocket close would wait forever for the record lock
     * held by the blocked session writer, so the transport must be reset instead and the writer released.
     */
    @Test
    void tlsSocketTransportIsResetWhenWriterIsStalled(@TempDir Path tempDir) throws Exception {
        try (SocketPeer peer = SocketPeer.openTls(tempDir)) {
            assertStalledSocketCloseIsBounded(peer);
        }
    }

    @Test
    void plainSocketTransportIsClosedWhenWriterIsStalled() throws Exception {
        try (SocketPeer peer = SocketPeer.openPlain()) {
            assertStalledSocketCloseIsBounded(peer);
        }
    }

    /** The peer never reads: stall the session writer inside the socket, then close with an error. */
    private static void assertStalledSocketCloseIsBounded(SocketPeer peer) throws Exception {
        ZmuxNativeSession session = peer.session;
        ZmuxStream stream = session.openStream();
        Thread bulkWriter = new Thread(() -> {
            byte[] chunk = new byte[64 * 1024];
            try {
                while (true) {
                    stream.write(chunk);
                }
            } catch (IOException expected) {
                // The session is torn down underneath the write.
            }
        }, "stalled-transport-socket-bulk-write");
        bulkWriter.setDaemon(true);
        bulkWriter.start();
        awaitSentBytesPlateau(session);

        long startedAtNanos = System.nanoTime();
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            session.closeWithError(5L, "x");
            session.close();
        }, "closing over a stalled socket must be bounded");
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
        assertTrue(elapsedMillis < 2_500L, "close() should follow the bounded close-frame wait, took " + elapsedMillis + "ms");
        assertTrue(session.awaitTermination(Duration.ofSeconds(1)), "the session must finish");
        assertTrue(peer.sessionSocket.isClosed(), "the stalled socket must be closed, not left open");
        Optional<IOException> cause = session.terminationCause();
        assertTrue(cause.isPresent(), "failed session should keep its cause");
        assertEquals(5L, ZmuxErrors.code(cause.get(), -1L), "transport-close noise must not replace the committed close cause");

        bulkWriter.join(2_000L);
        assertFalse(bulkWriter.isAlive(), "stream writers blocked behind the stalled socket should be released");
    }

    /** Without a write in progress the TLS close stays orderly: the peer gets CLOSE, then close_notify (EOF). */
    @Test
    void tlsSocketGracefulCloseStaysOrderly(@TempDir Path tempDir) throws Exception {
        try (SocketPeer peer = SocketPeer.openTls(tempDir)) {
            assertTimeoutPreemptively(Duration.ofSeconds(5), peer.session::close, "graceful close over TLS");
            assertTrue(peer.session.awaitTermination(Duration.ofSeconds(1)), "the session must finish");
            assertTrue(peer.sessionSocket.isClosed(), "close() should close the TLS socket");

            peer.peerSocket.setSoTimeout(5_000);
            FrameCodec.Frame close = null;
            while (close == null) {
                FrameCodec.Frame frame = FrameCodec.readFrame(peer.peerInput, Settings.defaults().limits());
                if (frame.type() == FrameType.CLOSE) {
                    close = frame;
                }
            }
            assertEquals(ErrorCode.NO_ERROR.code(), FrameCodec.parseErrorPayload(close.payload()).code());
            assertEquals(-1, peer.peerInput.read(), "an orderly TLS close ends with close_notify (EOF), not a reset");
        }
    }

    private static final char[] KEYSTORE_PASSWORD = "changeit".toCharArray();

    /** Creates a throwaway self-signed key pair with the running JDK's keytool. */
    private static KeyStore selfSignedKeyStore(Path dir) throws Exception {
        boolean windows = System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT).contains("win");
        Path keytool = Paths.get(System.getProperty("java.home"), "bin", windows ? "keytool.exe" : "keytool");
        assumeTrue(Files.isExecutable(keytool), "the JDK keytool is needed to create a test certificate");
        Path keystorePath = dir.resolve("zmux-test.p12");
        Process process = new ProcessBuilder(
                keytool.toString(),
                "-genkeypair",
                "-alias", "zmux",
                "-keyalg", "EC",
                "-keysize", "256",
                "-dname", "CN=localhost",
                "-validity", "2",
                "-storetype", "PKCS12",
                "-keystore", keystorePath.toString(),
                "-storepass", new String(KEYSTORE_PASSWORD),
                "-keypass", new String(KEYSTORE_PASSWORD)
        ).redirectErrorStream(true).start();
        ByteArrayOutputStream log = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        try (InputStream output = process.getInputStream()) {
            int read;
            while ((read = output.read(buffer)) >= 0) {
                log.write(buffer, 0, read);
            }
        }
        assertTrue(process.waitFor(60, TimeUnit.SECONDS), "keytool should finish");
        assertEquals(0, process.exitValue(), "keytool failed: " + new String(log.toByteArray(), StandardCharsets.UTF_8));
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keystorePath)) {
            keyStore.load(in, KEYSTORE_PASSWORD);
        }
        return keyStore;
    }

    private static void writeGenerousPeerPreface(OutputStream output) throws IOException {
        BufferedOutputStream peerOutput = new BufferedOutputStream(output);
        FrameCodec.writePreface(peerOutput, new Preface(
                Protocol.PREFACE_VERSION,
                Role.INITIATOR,
                0L,
                Protocol.PROTO_VERSION,
                Protocol.PROTO_VERSION,
                0L,
                Settings.defaults().toBuilder()
                        .initialMaxData(1L << 40)
                        .initialMaxStreamDataBidiPeerOpened(1L << 40)
                        .initialMaxStreamDataBidiLocallyOpened(1L << 40)
                        .build()
        ));
        peerOutput.flush();
    }

    /** Waits until the session has written data and then made no progress for a while: its writer is blocked. */
    private static void awaitSentBytesPlateau(ZmuxSession session) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        long last = -1L;
        long stableSince = System.nanoTime();
        while (System.nanoTime() < deadline) {
            long sent = session.stats().sentDataBytes();
            if (sent != last) {
                last = sent;
                stableSince = System.nanoTime();
            } else if (sent > 0L && System.nanoTime() - stableSince > TimeUnit.MILLISECONDS.toNanos(500)) {
                return;
            }
            Thread.sleep(20L);
        }
        fail("session writer did not stall on the non-reading peer; sentDataBytes=" + last);
    }
}
