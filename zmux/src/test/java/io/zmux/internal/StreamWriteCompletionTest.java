package io.zmux.internal;

import io.zmux.BasicDuplexConnection;
import io.zmux.Settings;
import io.zmux.WriteTimeoutException;
import io.zmux.ZmuxConfig;
import io.zmux.ZmuxStream;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class StreamWriteCompletionTest {
    @Test
    void writeWaitsUntilWriterCompletesUnderlyingTransportWrite() throws Exception {
        BlockingOutputStream output = new BlockingOutputStream();
        SessionRuntime runtime = newRuntime(output);
        Thread writer = startWriter(runtime);
        try {
            ZmuxStream stream = runtime.openStream();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch writeReturned = new CountDownLatch(1);

            Thread caller = new Thread(() -> {
                try {
                    stream.write("x".getBytes(StandardCharsets.UTF_8));
                } catch (Throwable throwable) {
                    failure.set(throwable);
                } finally {
                    writeReturned.countDown();
                }
            }, "stream-write-completion-caller");
            caller.start();

            assertTrue(output.awaitWriteEntered(), "writer should reach the underlying transport");
            assertFalse(writeReturned.await(100L, TimeUnit.MILLISECONDS), "write must not return while transport write is blocked");

            output.release();
            assertTrue(writeReturned.await(1L, TimeUnit.SECONDS), "write should return after transport write completes");
            assertNull(failure.get(), "write should complete successfully");
        } finally {
            closeRuntime(runtime, writer);
        }
    }

    @Test
    void writeReturnsUnderlyingTransportFailure() throws Exception {
        IOException transportFailure = new IOException("transport boom");
        BlockingOutputStream output = new BlockingOutputStream(transportFailure);
        SessionRuntime runtime = newRuntime(output);
        Thread writer = startWriter(runtime);
        try {
            ZmuxStream stream = runtime.openStream();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch writeReturned = new CountDownLatch(1);

            Thread caller = new Thread(() -> {
                try {
                    stream.write("x".getBytes(StandardCharsets.UTF_8));
                } catch (Throwable throwable) {
                    failure.set(throwable);
                } finally {
                    writeReturned.countDown();
                }
            }, "stream-write-failure-caller");
            caller.start();

            assertTrue(output.awaitWriteEntered(), "writer should reach the underlying transport");
            output.release();

            assertTrue(writeReturned.await(1L, TimeUnit.SECONDS), "write should return after transport failure");
            Throwable error = failure.get();
            assertNotNull(error, "write should surface the transport failure");
            assertTrue(containsMessage(error, "transport boom"), "write should preserve the transport failure reason");
        } finally {
            closeRuntime(runtime, writer);
        }
    }

    @Test
    void writeAsyncCompletesAfterQueueAdmissionWithoutWriterTransport() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        SessionRuntime runtime = newRuntime(output);
        try {
            StreamRuntime stream = (StreamRuntime) runtime.openStream();

            CompletionStage<Void> write = stream.writeAsync("async".getBytes(StandardCharsets.UTF_8));
            write.toCompletableFuture().get(1L, TimeUnit.SECONDS);

            synchronized (runtime.lock()) {
                assertFalse(runtime.dataQueueInternal().isEmpty(), "async write should be accepted into the zmux send queue");
                assertEquals("async".length(), stream.queuedDataBytesLocked(), "async write should count queued payload bytes");
            }
            assertEquals(0, output.size(), "async write should not require the underlying writer to run");
        } finally {
            closeRuntime(runtime, null);
        }
    }

    @Test
    void writeTimeoutAfterQueueAdmissionCancelsQueuedWriteBeforeWriterOwnsIt() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        SessionRuntime runtime = newRuntime(output);
        Thread writer = null;
        try {
            StreamRuntime stream = (StreamRuntime) runtime.openStream();
            stream.setWriteTimeout(Duration.ofMillis(100L));

            assertThrows(
                    WriteTimeoutException.class,
                    () -> stream.write("x".getBytes(StandardCharsets.UTF_8)),
                    "write should time out while waiting for a queued frame to reach the transport"
            );

            synchronized (runtime.lock()) {
                assertTrue(runtime.dataQueueInternal().isEmpty(), "timed-out queued write should be removed before writer ownership");
                assertFalse(stream.openingFramePendingLocked(), "canceled opening write should allow a later opener to be emitted");
                assertEquals(0L, stream.queuedDataBytesLocked(), "canceled write should release queued-data accounting");
                assertEquals(0L, stream.reservedSendBytes(), "canceled write should release flow-control reservations");
            }
            assertFalse(runtime.awaitTermination(Duration.ofMillis(50L)), "canceling a queued write should not fail the session");

            stream.clearWriteDeadline();
            writer = startWriter(runtime);
            stream.write("y".getBytes(StandardCharsets.UTF_8));
        } finally {
            closeRuntime(runtime, writer);
        }
    }

    @Test
    void writeDeadlineShortenedAfterQueueAdmissionCancelsQueuedWrite() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        SessionRuntime runtime = newRuntime(output);
        Thread writer = null;
        try {
            StreamRuntime stream = (StreamRuntime) runtime.openStream();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch writeReturned = new CountDownLatch(1);

            Thread caller = new Thread(() -> {
                try {
                    stream.write("x".getBytes(StandardCharsets.UTF_8));
                } catch (Throwable throwable) {
                    failure.set(throwable);
                } finally {
                    writeReturned.countDown();
                }
            }, "stream-write-shortened-deadline-caller");
            caller.start();

            waitUntilQueued(runtime);
            assertFalse(writeReturned.await(50L, TimeUnit.MILLISECONDS), "write should wait without an initial deadline");

            stream.setWriteDeadline(Instant.now());

            assertTrue(writeReturned.await(1L, TimeUnit.SECONDS), "shortened deadline should wake the waiting write");
            assertTrue(failure.get() instanceof WriteTimeoutException, "queued write should return the new timeout");
            synchronized (runtime.lock()) {
                assertTrue(runtime.dataQueueInternal().isEmpty(), "timed-out queued write should be removed");
                assertEquals(0L, stream.queuedDataBytesLocked(), "timed-out queued write should release accounting");
                assertEquals(0L, stream.reservedSendBytes(), "timed-out queued write should release send reservations");
            }

            stream.clearWriteDeadline();
            writer = startWriter(runtime);
            stream.write("y".getBytes(StandardCharsets.UTF_8));
        } finally {
            closeRuntime(runtime, writer);
        }
    }

    @Test
    void writeDeadlineExtendedAfterQueueAdmissionDoesNotUseStaleTimeout() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        SessionRuntime runtime = newRuntime(output);
        Thread writer = null;
        try {
            StreamRuntime stream = (StreamRuntime) runtime.openStream();
            stream.setWriteTimeout(Duration.ofMillis(100L));
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch writeReturned = new CountDownLatch(1);

            Thread caller = new Thread(() -> {
                try {
                    stream.write("x".getBytes(StandardCharsets.UTF_8));
                } catch (Throwable throwable) {
                    failure.set(throwable);
                } finally {
                    writeReturned.countDown();
                }
            }, "stream-write-extended-deadline-caller");
            caller.start();

            waitUntilQueued(runtime);
            stream.setWriteTimeout(Duration.ofSeconds(2L));

            assertFalse(writeReturned.await(250L, TimeUnit.MILLISECONDS), "old deadline should not cancel the queued write");
            assertNull(failure.get(), "write should still be waiting after the original timeout");

            writer = startWriter(runtime);
            assertTrue(writeReturned.await(1L, TimeUnit.SECONDS), "write should complete after writer drains the queue");
            assertNull(failure.get(), "extended deadline should allow successful write completion");
        } finally {
            closeRuntime(runtime, writer);
        }
    }

    @Test
    void writeTimeoutAfterWriterOwnsBatchDoesNotCancelInflightWrite() throws Exception {
        BlockingOutputStream output = new BlockingOutputStream();
        SessionRuntime runtime = newRuntime(output);
        Thread writer = startWriter(runtime);
        try {
            ZmuxStream stream = runtime.openStream();
            stream.setWriteTimeout(Duration.ofMillis(150L));
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch writeReturned = new CountDownLatch(1);

            Thread caller = new Thread(() -> {
                try {
                    stream.write("x".getBytes(StandardCharsets.UTF_8));
                } catch (Throwable throwable) {
                    failure.set(throwable);
                } finally {
                    writeReturned.countDown();
                }
            }, "stream-write-timeout-caller");
            caller.start();

            assertTrue(output.awaitWriteEntered(), "writer should reach the underlying transport before the timeout");
            assertFalse(writeReturned.await(50L, TimeUnit.MILLISECONDS), "write should still be waiting for transport completion");

            assertFalse(writeReturned.await(250L, TimeUnit.MILLISECONDS), "inflight writes should ignore later deadline expiry");
            assertFalse(runtime.awaitTermination(Duration.ofMillis(50L)), "an inflight write timeout should not fail the session");
            output.release();
            assertTrue(writeReturned.await(1L, TimeUnit.SECONDS), "write should return after the inflight transport write completes");
            assertNull(failure.get(), "successful inflight transport write should return success");
            assertTrue(output.awaitWriteCompleted(), "the already queued write may still complete on the transport");
            assertFalse(runtime.awaitTermination(Duration.ofMillis(50L)), "the completed inflight write should not fail the session");
        } finally {
            output.release();
            closeRuntime(runtime, writer);
        }
    }

    private static SessionRuntime newRuntime(OutputStream output) throws Exception {
        return SessionRuntimeTestSupport.newReadyRuntime(
                new BasicDuplexConnection(SessionRuntimeTestSupport.emptyInput(), output),
                ZmuxConfig.builder().role(io.zmux.Role.RESPONDER).build(),
                0L,
                Settings.defaults()
        );
    }

    private static Thread startWriter(SessionRuntime runtime) {
        Thread writer = new Thread(runtime.writerLoopTaskInternal(), "stream-write-completion-writer");
        writer.setDaemon(true);
        writer.start();
        return writer;
    }

    private static void closeRuntime(SessionRuntime runtime, Thread writer) throws Exception {
        runtime.closeWithError(0L, "");
        if (writer != null) {
            writer.join(1_000L);
        }
    }

    private static void waitUntilQueued(SessionRuntime runtime) throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
        while (System.nanoTime() < deadlineNanos) {
            synchronized (runtime.lock()) {
                if (!runtime.dataQueueInternal().isEmpty()) {
                    return;
                }
            }
            Thread.sleep(1L);
        }
        throw new AssertionError("write did not enter the outbound queue");
    }

    private static boolean containsMessage(Throwable error, String expected) {
        Throwable current = error;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && message.contains(expected)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static final class BlockingOutputStream extends OutputStream {
        private final IOException failure;
        private final CountDownLatch writeEntered = new CountDownLatch(1);
        private final CountDownLatch writeCompleted = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        private BlockingOutputStream() {
            this(null);
        }

        private BlockingOutputStream(IOException failure) {
            this.failure = failure;
        }

        @Override
        public void write(int value) throws IOException {
            byte[] single = new byte[]{(byte) value};
            write(single, 0, 1);
        }

        @Override
        public void write(byte[] buffer, int offset, int length) throws IOException {
            writeEntered.countDown();
            try {
                release.await();
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted", interruptedException);
            } finally {
                writeCompleted.countDown();
            }
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public void close() {
            release();
        }

        private boolean awaitWriteEntered() throws InterruptedException {
            return writeEntered.await(1L, TimeUnit.SECONDS);
        }

        private boolean awaitWriteCompleted() throws InterruptedException {
            return writeCompleted.await(1L, TimeUnit.SECONDS);
        }

        private void release() {
            release.countDown();
        }
    }
}
