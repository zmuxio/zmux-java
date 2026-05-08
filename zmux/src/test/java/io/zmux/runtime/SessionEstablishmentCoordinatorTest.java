package io.zmux.runtime;

import io.zmux.ErrorCode;
import io.zmux.ReadTimeoutException;
import io.zmux.Role;
import io.zmux.Settings;
import io.zmux.WriteTimeoutException;
import io.zmux.ZmuxErrorDirection;
import io.zmux.ZmuxErrorScope;
import io.zmux.ZmuxErrorSource;
import io.zmux.ZmuxException;
import io.zmux.ZmuxTerminationKind;
import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.Negotiated;
import io.zmux.protocol.Preface;
import io.zmux.protocol.Protocol;
import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class SessionEstablishmentCoordinatorTest {
    private static byte[] encodePreface(Preface preface) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        FrameCodec.writePreface(output, preface);
        return output.toByteArray();
    }

    private static Preface preface(Role role, long nonce) {
        return new Preface(
                Protocol.PREFACE_VERSION,
                role,
                nonce,
                Protocol.PROTO_VERSION,
                Protocol.PROTO_VERSION,
                0L,
                Settings.defaults()
        );
    }

    @Test
    void successfulEstablishmentStartsWriterLoopFromPrefaceWriterThread() throws Exception {
        TestOwner owner = new TestOwner(
                preface(Role.INITIATOR, 1L),
                preface(Role.RESPONDER, 2L),
                new CountDownLatch(0)
        );
        SessionEstablishmentCoordinator coordinator = new SessionEstablishmentCoordinator(
                owner,
                Duration.ofMillis(50),
                Duration.ofMillis(100),
                Duration.ofMillis(1)
        );

        coordinator.establish();

        assertTrue(owner.readyMarked.get(), "successful establishment must mark the session ready");
        assertTrue(owner.readerStartedLatch.await(1L, TimeUnit.SECONDS), "reader loop should start after establishment");
        assertTrue(owner.writerStartedLatch.await(1L, TimeUnit.SECONDS), "writer loop should start after establishment");
        assertEquals("zmux-reader", owner.readerThreadName.get(), "reader loop thread name mismatch");
        assertEquals("zmux-writer", owner.writerThreadName.get(), "writer loop thread name mismatch");
        assertNull(owner.writeDeadline.get(), "successful establishment must clear the temporary write deadline");
    }

    @Test
    void successfulEstablishmentClearsTemporaryReadDeadline() throws Exception {
        AtomicReference<Instant> readDeadline = new AtomicReference<>();
        TestOwner owner = new TestOwner(
                preface(Role.INITIATOR, 1L),
                preface(Role.RESPONDER, 2L),
                new CountDownLatch(0),
                true,
                readDeadline
        );
        SessionEstablishmentCoordinator coordinator = new SessionEstablishmentCoordinator(
                owner,
                Duration.ofMillis(50),
                Duration.ofMillis(100),
                Duration.ofMillis(1)
        );

        coordinator.establish();

        assertTrue(owner.readyMarked.get(), "successful establishment must mark the session ready");
        assertEquals(1, owner.readDeadlineSetCalls.get(), "establishment should arm one temporary read deadline");
        assertEquals(1, owner.readDeadlineClearCalls.get(), "establishment should clear the temporary read deadline");
        assertNull(owner.readDeadline.get(), "successful establishment must clear the temporary read deadline");
    }

    @Test
    void delayedPeerPrefaceFallsBackToFreshWriterAfterCarrierExpiry() throws Exception {
        CountDownLatch peerPrefaceReadAttempted = new CountDownLatch(1);
        CountDownLatch releasePeerPreface = new CountDownLatch(1);
        TestOwner owner = new TestOwner(
                preface(Role.INITIATOR, 1L),
                new DelayedInputStream(
                        encodePreface(preface(Role.RESPONDER, 2L)),
                        peerPrefaceReadAttempted,
                        releasePeerPreface
                ),
                new CountDownLatch(0)
        );
        SessionEstablishmentCoordinator coordinator = new SessionEstablishmentCoordinator(
                owner,
                Duration.ofMillis(50),
                Duration.ofMillis(20),
                Duration.ofMillis(1)
        );
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread establishThread = new Thread(() -> {
            try {
                coordinator.establish();
            } catch (Throwable error) {
                failure.set(error);
            }
        }, "test-delayed-establishment");

        establishThread.start();
        assertTrue(
                peerPrefaceReadAttempted.await(1L, TimeUnit.SECONDS),
                "establishment should start reading the peer preface"
        );
        assertFalse(
                owner.writerStartedLatch.await(80L, TimeUnit.MILLISECONDS),
                "writer loop must wait for successful preface negotiation"
        );

        releasePeerPreface.countDown();
        establishThread.join(1_000L);

        assertFalse(establishThread.isAlive(), "establishment should finish after the delayed peer preface arrives");
        assertNull(failure.get(), "delayed preface establishment should succeed");
        assertTrue(owner.readyMarked.get(), "delayed establishment must mark the session ready");
        assertTrue(owner.readerStartedLatch.await(1L, TimeUnit.SECONDS), "reader loop should start after delayed establishment");
        assertTrue(owner.writerStartedLatch.await(1L, TimeUnit.SECONDS), "writer loop should start after carrier expiry");
        assertNull(owner.writeDeadline.get(), "delayed establishment must clear the temporary write deadline before starting runtime writers");
    }

    @Test
    void stalledLocalPrefaceWriteFailsSuccessfulEstablishmentAttempt() throws Exception {
        Preface localPreface = preface(Role.INITIATOR, 1L);
        Preface remotePreface = preface(Role.RESPONDER, 2L);
        CountDownLatch releaseWrites = new CountDownLatch(1);
        TestOwner owner = new TestOwner(localPreface, remotePreface, releaseWrites);
        SessionEstablishmentCoordinator coordinator = new SessionEstablishmentCoordinator(
                owner,
                Duration.ofMillis(50),
                Duration.ofMillis(100),
                Duration.ofMillis(1)
        );

        ZmuxException error = assertInstanceOf(
                ZmuxException.class,
                assertThrows(IOException.class, coordinator::establish),
                "stalled preface write must fail establishment"
        );

        assertEquals(ErrorCode.INTERNAL.code(), error.code(), "stalled preface write code mismatch");
        assertEquals("write preface", error.operation(), "stalled preface write operation mismatch");
        assertEquals(
                "local preface write stalled during establishment",
                error.getMessage(),
                "stalled preface write reason mismatch"
        );
        assertEquals(ZmuxErrorScope.SESSION, error.scope(), "stalled preface write scope mismatch");
        assertEquals(ZmuxErrorSource.LOCAL, error.source(), "stalled preface write source mismatch");
        assertEquals(ZmuxErrorDirection.BOTH, error.direction(), "stalled preface write direction mismatch");
        assertEquals(
                ZmuxTerminationKind.SESSION_TERMINATION,
                error.terminationKind(),
                "stalled preface write termination mismatch"
        );
        assertTrue(owner.transportClosed.get(), "establishment failure must close the transport");
        assertFalse(owner.readyMarked.get(), "stalled preface write must not mark the session ready");
        assertFalse(owner.readerStarted.get(), "stalled preface write must not start the reader loop");
        assertFalse(owner.writerStarted.get(), "stalled preface write must not start the writer loop");
    }

    @Test
    void negotiationFailureSurfacesBeforeStalledLocalPrefaceWrite() throws Exception {
        CountDownLatch releaseWrites = new CountDownLatch(1);
        TestOwner owner = new TestOwner(
                preface(Role.INITIATOR, 1L),
                preface(Role.INITIATOR, 2L),
                releaseWrites
        );
        SessionEstablishmentCoordinator coordinator = new SessionEstablishmentCoordinator(
                owner,
                Duration.ofMillis(40),
                Duration.ofMillis(200),
                Duration.ofMillis(1)
        );

        try {
            ZmuxException error = assertInstanceOf(
                    ZmuxException.class,
                    assertThrows(IOException.class, coordinator::establish),
                    "same-role conflict should fail before a stalled local preface write masks it"
            );

            assertEquals(ErrorCode.ROLE_CONFLICT.code(), error.code(), "same-role conflict code mismatch");
            assertEquals("resolve roles", error.operation(), "same-role conflict operation mismatch");
            assertTrue(owner.transportClosed.get(), "establishment failure must close the transport");
            assertFalse(owner.readyMarked.get(), "failed establishment must not mark the session ready");
            assertFalse(owner.readerStarted.get(), "failed establishment must not start the reader loop");
            assertFalse(owner.writerStarted.get(), "failed establishment must not start the writer loop");
        } finally {
            releaseWrites.countDown();
        }
    }

    @Test
    void stalledPeerPrefaceReadFailsSuccessfulEstablishmentAttempt() throws Exception {
        AtomicReference<Instant> readDeadline = new AtomicReference<>();
        TestOwner owner = new TestOwner(
                preface(Role.INITIATOR, 1L),
                new ReadDeadlineTimeoutInputStream(readDeadline),
                new CountDownLatch(0),
                true,
                readDeadline
        );
        SessionEstablishmentCoordinator coordinator = new SessionEstablishmentCoordinator(
                owner,
                Duration.ofMillis(50),
                Duration.ofMillis(40),
                Duration.ofMillis(1)
        );

        ZmuxException error = assertInstanceOf(
                ZmuxException.class,
                assertThrows(IOException.class, coordinator::establish),
                "stalled peer preface read must fail establishment"
        );

        assertEquals(ErrorCode.INTERNAL.code(), error.code(), "stalled preface read code mismatch");
        assertEquals("read preface", error.operation(), "stalled preface read operation mismatch");
        assertEquals(
                "peer preface read stalled during establishment",
                error.getMessage(),
                "stalled preface read reason mismatch"
        );
        assertEquals(ZmuxErrorScope.SESSION, error.scope(), "stalled preface read scope mismatch");
        assertEquals(ZmuxErrorSource.LOCAL, error.source(), "stalled preface read source mismatch");
        assertEquals(ZmuxErrorDirection.BOTH, error.direction(), "stalled preface read direction mismatch");
        assertEquals(
                ZmuxTerminationKind.SESSION_TERMINATION,
                error.terminationKind(),
                "stalled preface read termination mismatch"
        );
        assertEquals(1, owner.readDeadlineSetCalls.get(), "establishment should arm one temporary read deadline");
        assertEquals(1, owner.readDeadlineClearCalls.get(), "failed establishment should clear the read deadline");
        assertNull(owner.readDeadline.get(), "failed establishment must not leave a read deadline armed");
        assertTrue(owner.transportClosed.get(), "establishment failure must close the transport");
        assertFalse(owner.readyMarked.get(), "stalled preface read must not mark the session ready");
        assertFalse(owner.readerStarted.get(), "stalled preface read must not start the reader loop");
        assertFalse(owner.writerStarted.get(), "stalled preface read must not start the writer loop");
    }

    @Test
    void blockedEstablishmentFailureCloseWriteIsBoundedByDeadline() throws Exception {
        BlockingCloseWriteOwner owner = new BlockingCloseWriteOwner(
                preface(Role.INITIATOR, 1L),
                preface(Role.INITIATOR, 2L)
        );
        SessionEstablishmentCoordinator coordinator = new SessionEstablishmentCoordinator(
                owner,
                Duration.ofMillis(40L),
                Duration.ofMillis(200L),
                Duration.ofMillis(1L)
        );
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread establishThread = new Thread(() -> {
            try {
                coordinator.establish();
                failure.set(new AssertionError("same-role conflict should fail establishment"));
            } catch (Throwable error) {
                failure.set(error);
            }
        }, "test-blocked-establishment-close");

        establishThread.start();
        try {
            assertTrue(
                    owner.closeWriteAttempted.await(1L, TimeUnit.SECONDS),
                    "establishment failure must attempt to send a fatal CLOSE"
            );
            establishThread.join(1_000L);
            assertFalse(
                    establishThread.isAlive(),
                    "blocked establishment failure CLOSE write must be bounded by failureWriteWait"
            );
            Throwable error = failure.get();
            assertNotNull(error, "same-role conflict should surface locally");
            ZmuxException zmuxError = assertInstanceOf(ZmuxException.class, error);
            assertEquals(ErrorCode.ROLE_CONFLICT.code(), zmuxError.code(), "same-role conflict code mismatch");
            assertTrue(owner.closeWriteSawDeadline.get(), "fatal CLOSE write should observe its own write deadline");
            assertTrue(owner.transportClosed.get(), "establishment failure must close the transport");
            assertFalse(owner.readyMarked.get(), "failed establishment must not mark the session ready");
            assertFalse(owner.readerStarted.get(), "failed establishment must not start the reader loop");
            assertFalse(owner.writerStarted.get(), "failed establishment must not start the writer loop");
        } finally {
            owner.releaseCloseWrite.countDown();
            establishThread.join(1_000L);
        }
    }

    @Test
    void establishmentFailureSkipsFatalCloseWithoutWriteDeadlineSupport() throws Exception {
        NoWriteDeadlineFailureOwner owner = new NoWriteDeadlineFailureOwner(
                preface(Role.INITIATOR, 1L),
                preface(Role.INITIATOR, 2L)
        );
        SessionEstablishmentCoordinator coordinator = new SessionEstablishmentCoordinator(
                owner,
                Duration.ofMillis(40L),
                Duration.ofMillis(200L),
                Duration.ofMillis(1L)
        );

        ZmuxException error = assertInstanceOf(
                ZmuxException.class,
                assertThrows(IOException.class, coordinator::establish),
                "same-role conflict should fail establishment"
        );

        assertEquals(ErrorCode.ROLE_CONFLICT.code(), error.code(), "same-role conflict code mismatch");
        assertTrue(
                owner.transportWrites.get() <= 1,
                "unsupported write deadline must not emit a fatal CLOSE write"
        );
        assertTrue(owner.transportClosed.get(), "establishment failure must close the transport");
        assertFalse(owner.readyMarked.get(), "failed establishment must not mark the session ready");
        assertFalse(owner.readerStarted.get(), "failed establishment must not start the reader loop");
        assertFalse(owner.writerStarted.get(), "failed establishment must not start the writer loop");
    }

    private static final class TestOwner implements SessionEstablishmentCoordinator.Owner {
        private final FrameCodec.Decoder input;
        private final BufferedOutputStream output;
        private final Preface localPreface;
        private final CountDownLatch releaseWrites;
        private final Object lock = new Object();
        private final AtomicBoolean readyMarked = new AtomicBoolean();
        private final AtomicBoolean transportClosed = new AtomicBoolean();
        private final AtomicBoolean readerStarted = new AtomicBoolean();
        private final AtomicBoolean writerStarted = new AtomicBoolean();
        private final CountDownLatch readerStartedLatch = new CountDownLatch(1);
        private final CountDownLatch writerStartedLatch = new CountDownLatch(1);
        private final AtomicReference<Instant> writeDeadline = new AtomicReference<>();
        private final AtomicReference<Instant> readDeadline;
        private final boolean supportsReadDeadline;
        private final AtomicInteger readDeadlineSetCalls = new AtomicInteger();
        private final AtomicInteger readDeadlineClearCalls = new AtomicInteger();
        private final AtomicReference<String> readerThreadName = new AtomicReference<>();
        private final AtomicReference<String> writerThreadName = new AtomicReference<>();

        private TestOwner(Preface localPreface, Preface remotePreface, CountDownLatch releaseWrites) throws IOException {
            this(localPreface, new ByteArrayInputStream(encodePreface(remotePreface)), releaseWrites);
        }

        private TestOwner(Preface localPreface,
                          Preface remotePreface,
                          CountDownLatch releaseWrites,
                          boolean supportsReadDeadline,
                          AtomicReference<Instant> readDeadline) throws IOException {
            this(
                    localPreface,
                    new ByteArrayInputStream(encodePreface(remotePreface)),
                    releaseWrites,
                    supportsReadDeadline,
                    readDeadline
            );
        }

        private TestOwner(Preface localPreface, InputStream input, CountDownLatch releaseWrites) {
            this(localPreface, input, releaseWrites, false, new AtomicReference<>());
        }

        private TestOwner(Preface localPreface,
                          InputStream input,
                          CountDownLatch releaseWrites,
                          boolean supportsReadDeadline,
                          AtomicReference<Instant> readDeadline) {
            this.localPreface = localPreface;
            this.releaseWrites = releaseWrites;
            this.supportsReadDeadline = supportsReadDeadline;
            this.readDeadline = readDeadline;
            this.input = FrameCodec.decoder(input);
            this.output = new BufferedOutputStream(new OutputStream() {
                @Override
                public void write(int value) throws IOException {
                    awaitRelease();
                }

                @Override
                public void write(byte[] buffer, int offset, int length) throws IOException {
                    awaitRelease();
                }

                private void awaitRelease() throws IOException {
                    try {
                        while (TestOwner.this.releaseWrites.getCount() > 0L) {
                            Instant deadline = TestOwner.this.writeDeadline.get();
                            if (deadline == null) {
                                TestOwner.this.releaseWrites.await();
                                return;
                            }
                            Instant now = Instant.now();
                            if (!deadline.isAfter(now)) {
                                throw new WriteTimeoutException();
                            }
                            long waitMillis = Math.max(1L, Math.min(10L, Duration.between(now, deadline).toMillis()));
                            TestOwner.this.releaseWrites.await(waitMillis, TimeUnit.MILLISECONDS);
                        }
                    } catch (InterruptedException interruptedException) {
                        Thread.currentThread().interrupt();
                        throw new IOException("synthetic blocked preface write interrupted", interruptedException);
                    }
                }
            });
        }

        @Override
        public FrameCodec.Decoder input() {
            return this.input;
        }

        @Override
        public BufferedOutputStream output() {
            return this.output;
        }

        @Override
        public Preface localPreface() {
            return this.localPreface;
        }

        @Override
        public Object lock() {
            return this.lock;
        }

        @Override
        public void markReadyLocked(Preface remotePreface, Negotiated negotiated, long readyAtNanos) {
            this.readyMarked.set(true);
        }

        @Override
        public void notifyLockWaiters() {
        }

        @Override
        public boolean supportsWriteDeadline() {
            return true;
        }

        @Override
        public void setWriteDeadline(Instant deadline) {
            this.writeDeadline.set(deadline);
        }

        @Override
        public boolean supportsReadDeadline() {
            return this.supportsReadDeadline;
        }

        @Override
        public void setReadDeadline(Instant deadline) {
            if (!this.supportsReadDeadline) {
                throw new AssertionError("unsupported read deadline must not be armed");
            }
            this.readDeadline.set(deadline);
            if (deadline == null) {
                this.readDeadlineClearCalls.incrementAndGet();
            } else {
                this.readDeadlineSetCalls.incrementAndGet();
            }
        }

        @Override
        public Runnable readerLoopTask() {
            return () -> {
                this.readerThreadName.set(Thread.currentThread().getName());
                this.readerStarted.set(true);
                this.readerStartedLatch.countDown();
            };
        }

        @Override
        public Runnable writerLoopTask() {
            return () -> {
                this.writerThreadName.set(Thread.currentThread().getName());
                this.writerStarted.set(true);
                this.writerStartedLatch.countDown();
            };
        }

        @Override
        public IOException sessionInternalError(String operation, String message) {
            return SessionRuntime.sessionInternalError(operation, message);
        }

        @Override
        public IOException sessionInternalError(String operation, String message, Throwable cause) {
            return SessionRuntime.sessionInternalError(operation, message, cause);
        }

        @Override
        public void closeTransport() {
            this.transportClosed.set(true);
            this.releaseWrites.countDown();
        }
    }

    private static final class NoWriteDeadlineFailureOwner implements SessionEstablishmentCoordinator.Owner {
        private final FrameCodec.Decoder input;
        private final BufferedOutputStream output;
        private final Preface localPreface;
        private final Object lock = new Object();
        private final AtomicInteger transportWrites = new AtomicInteger();
        private final AtomicBoolean readyMarked = new AtomicBoolean();
        private final AtomicBoolean transportClosed = new AtomicBoolean();
        private final AtomicBoolean readerStarted = new AtomicBoolean();
        private final AtomicBoolean writerStarted = new AtomicBoolean();

        private NoWriteDeadlineFailureOwner(Preface localPreface, Preface remotePreface) throws IOException {
            this.localPreface = localPreface;
            this.input = FrameCodec.decoder(new ByteArrayInputStream(encodePreface(remotePreface)));
            this.output = new BufferedOutputStream(new OutputStream() {
                @Override
                public void write(int value) {
                    NoWriteDeadlineFailureOwner.this.transportWrites.incrementAndGet();
                }

                @Override
                public void write(byte[] buffer, int offset, int length) {
                    NoWriteDeadlineFailureOwner.this.transportWrites.incrementAndGet();
                }
            });
        }

        @Override
        public FrameCodec.Decoder input() {
            return this.input;
        }

        @Override
        public BufferedOutputStream output() {
            return this.output;
        }

        @Override
        public Preface localPreface() {
            return this.localPreface;
        }

        @Override
        public Object lock() {
            return this.lock;
        }

        @Override
        public void markReadyLocked(Preface remotePreface, Negotiated negotiated, long readyAtNanos) {
            this.readyMarked.set(true);
        }

        @Override
        public void notifyLockWaiters() {
        }

        @Override
        public boolean supportsWriteDeadline() {
            return false;
        }

        @Override
        public void setWriteDeadline(Instant deadline) {
            throw new AssertionError("unsupported write deadline must not be armed");
        }

        @Override
        public boolean supportsReadDeadline() {
            return false;
        }

        @Override
        public void setReadDeadline(Instant deadline) {
            throw new AssertionError("unsupported read deadline must not be armed");
        }

        @Override
        public Runnable readerLoopTask() {
            return () -> this.readerStarted.set(true);
        }

        @Override
        public Runnable writerLoopTask() {
            return () -> this.writerStarted.set(true);
        }

        @Override
        public IOException sessionInternalError(String operation, String message) {
            return SessionRuntime.sessionInternalError(operation, message);
        }

        @Override
        public IOException sessionInternalError(String operation, String message, Throwable cause) {
            return SessionRuntime.sessionInternalError(operation, message, cause);
        }

        @Override
        public void closeTransport() {
            this.transportClosed.set(true);
        }
    }

    private static final class BlockingCloseWriteOwner implements SessionEstablishmentCoordinator.Owner {
        private final FrameCodec.Decoder input;
        private final BufferedOutputStream output;
        private final Preface localPreface;
        private final Object lock = new Object();
        private final AtomicInteger transportWrites = new AtomicInteger();
        private final CountDownLatch closeWriteAttempted = new CountDownLatch(1);
        private final CountDownLatch releaseCloseWrite = new CountDownLatch(1);
        private final AtomicBoolean closeWriteSawDeadline = new AtomicBoolean();
        private final AtomicBoolean readyMarked = new AtomicBoolean();
        private final AtomicBoolean transportClosed = new AtomicBoolean();
        private final AtomicBoolean readerStarted = new AtomicBoolean();
        private final AtomicBoolean writerStarted = new AtomicBoolean();
        private final AtomicReference<Instant> writeDeadline = new AtomicReference<>();

        private BlockingCloseWriteOwner(Preface localPreface, Preface remotePreface) throws IOException {
            this.localPreface = localPreface;
            this.input = FrameCodec.decoder(new ByteArrayInputStream(encodePreface(remotePreface)));
            this.output = new BufferedOutputStream(new OutputStream() {
                @Override
                public void write(int value) throws IOException {
                    acceptTransportWrite();
                }

                @Override
                public void write(byte[] buffer, int offset, int length) throws IOException {
                    acceptTransportWrite();
                }

                private void acceptTransportWrite() throws IOException {
                    if (BlockingCloseWriteOwner.this.transportWrites.incrementAndGet() == 1) {
                        return;
                    }
                    BlockingCloseWriteOwner.this.closeWriteAttempted.countDown();
                    awaitCloseWriteDeadline();
                }

                private void awaitCloseWriteDeadline() throws IOException {
                    try {
                        while (BlockingCloseWriteOwner.this.releaseCloseWrite.getCount() > 0L) {
                            Instant deadline = BlockingCloseWriteOwner.this.writeDeadline.get();
                            if (deadline == null) {
                                BlockingCloseWriteOwner.this.releaseCloseWrite.await(10L, TimeUnit.MILLISECONDS);
                                continue;
                            }
                            BlockingCloseWriteOwner.this.closeWriteSawDeadline.set(true);
                            Instant now = Instant.now();
                            if (!deadline.isAfter(now)) {
                                throw new WriteTimeoutException();
                            }
                            long waitMillis = Math.max(
                                    1L,
                                    Math.min(10L, Duration.between(now, deadline).toMillis())
                            );
                            BlockingCloseWriteOwner.this.releaseCloseWrite.await(waitMillis, TimeUnit.MILLISECONDS);
                        }
                    } catch (InterruptedException interruptedException) {
                        Thread.currentThread().interrupt();
                        throw new IOException("synthetic blocked close write interrupted", interruptedException);
                    }
                }
            });
        }

        @Override
        public FrameCodec.Decoder input() {
            return this.input;
        }

        @Override
        public BufferedOutputStream output() {
            return this.output;
        }

        @Override
        public Preface localPreface() {
            return this.localPreface;
        }

        @Override
        public Object lock() {
            return this.lock;
        }

        @Override
        public void markReadyLocked(Preface remotePreface, Negotiated negotiated, long readyAtNanos) {
            this.readyMarked.set(true);
        }

        @Override
        public void notifyLockWaiters() {
        }

        @Override
        public boolean supportsWriteDeadline() {
            return true;
        }

        @Override
        public void setWriteDeadline(Instant deadline) {
            this.writeDeadline.set(deadline);
        }

        @Override
        public boolean supportsReadDeadline() {
            return false;
        }

        @Override
        public void setReadDeadline(Instant deadline) {
            throw new AssertionError("unsupported read deadline must not be armed");
        }

        @Override
        public Runnable readerLoopTask() {
            return () -> this.readerStarted.set(true);
        }

        @Override
        public Runnable writerLoopTask() {
            return () -> this.writerStarted.set(true);
        }

        @Override
        public IOException sessionInternalError(String operation, String message) {
            return SessionRuntime.sessionInternalError(operation, message);
        }

        @Override
        public IOException sessionInternalError(String operation, String message, Throwable cause) {
            return SessionRuntime.sessionInternalError(operation, message, cause);
        }

        @Override
        public void closeTransport() {
            this.transportClosed.set(true);
            this.releaseCloseWrite.countDown();
        }
    }

    private static final class ReadDeadlineTimeoutInputStream extends InputStream {
        private final AtomicReference<Instant> deadline;

        private ReadDeadlineTimeoutInputStream(AtomicReference<Instant> deadline) {
            this.deadline = deadline;
        }

        @Override
        public int read() throws IOException {
            awaitDeadline();
            throw new AssertionError("awaitDeadline should only return by throwing");
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            awaitDeadline();
            throw new AssertionError("awaitDeadline should only return by throwing");
        }

        private void awaitDeadline() throws IOException {
            try {
                while (true) {
                    Instant current = deadline.get();
                    if (current == null) {
                        TimeUnit.MILLISECONDS.sleep(1L);
                        continue;
                    }
                    Instant now = Instant.now();
                    if (!current.isAfter(now)) {
                        throw new ReadTimeoutException();
                    }
                    long waitMillis = Math.max(1L, Math.min(10L, Duration.between(now, current).toMillis()));
                    TimeUnit.MILLISECONDS.sleep(waitMillis);
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("synthetic blocked preface read interrupted", interrupted);
            }
        }
    }

    private static final class DelayedInputStream extends InputStream {
        private final byte[] bytes;
        private final CountDownLatch readAttempted;
        private final CountDownLatch releaseRead;
        private int offset;
        private boolean released;

        private DelayedInputStream(byte[] bytes, CountDownLatch readAttempted, CountDownLatch releaseRead) {
            this.bytes = bytes;
            this.readAttempted = readAttempted;
            this.releaseRead = releaseRead;
        }

        @Override
        public int read() throws IOException {
            awaitRelease();
            if (offset >= bytes.length) {
                return -1;
            }
            return bytes[offset++] & 0xff;
        }

        @Override
        public int read(byte[] buffer, int bufferOffset, int length) throws IOException {
            awaitRelease();
            if (offset >= bytes.length) {
                return -1;
            }
            int copied = Math.min(length, bytes.length - offset);
            System.arraycopy(bytes, offset, buffer, bufferOffset, copied);
            offset += copied;
            return copied;
        }

        private void awaitRelease() throws IOException {
            if (released) {
                return;
            }
            readAttempted.countDown();
            try {
                releaseRead.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("synthetic delayed preface read interrupted", interrupted);
            }
            released = true;
        }
    }
}
