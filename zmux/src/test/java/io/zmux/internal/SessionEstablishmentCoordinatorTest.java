package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

final class SessionEstablishmentCoordinatorTest {
    private static byte[] encodePreface(Preface preface) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        FrameCodec.writePreface(output, preface);
        return output.toByteArray();
    }

    @Test
    void stalledLocalPrefaceWriteFailsSuccessfulEstablishmentAttempt() throws Exception {
        Preface localPreface = new Preface(
                Protocol.PREFACE_VERSION,
                Role.INITIATOR,
                1L,
                Protocol.PROTO_VERSION,
                Protocol.PROTO_VERSION,
                0L,
                Settings.defaults()
        );
        Preface remotePreface = new Preface(
                Protocol.PREFACE_VERSION,
                Role.RESPONDER,
                2L,
                Protocol.PROTO_VERSION,
                Protocol.PROTO_VERSION,
                0L,
                Settings.defaults()
        );
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

        private TestOwner(Preface localPreface, Preface remotePreface, CountDownLatch releaseWrites) throws IOException {
            this.localPreface = localPreface;
            this.releaseWrites = releaseWrites;
            this.input = FrameCodec.decoder(new ByteArrayInputStream(encodePreface(remotePreface)));
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
            this.releaseWrites.countDown();
        }
    }
}
