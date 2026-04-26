package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
        private final AtomicReference<String> readerThreadName = new AtomicReference<>();
        private final AtomicReference<String> writerThreadName = new AtomicReference<>();

        private TestOwner(Preface localPreface, Preface remotePreface, CountDownLatch releaseWrites) throws IOException {
            this(localPreface, new ByteArrayInputStream(encodePreface(remotePreface)), releaseWrites);
        }

        private TestOwner(Preface localPreface, InputStream input, CountDownLatch releaseWrites) {
            this.localPreface = localPreface;
            this.releaseWrites = releaseWrites;
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
                        TestOwner.this.releaseWrites.await();
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
