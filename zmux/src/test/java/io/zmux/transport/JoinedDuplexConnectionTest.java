package io.zmux.transport;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

final class JoinedDuplexConnectionTest {
    private static void waitUntilPaused(JoinedDuplexConnection connection) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
        while (System.nanoTime() < deadline) {
            if (connection.inputHalf() == null) {
                return;
            }
            Thread.sleep(1L);
        }
        throw new AssertionError("connection did not enter paused input state");
    }

    private static void waitUntilOutputPaused(JoinedDuplexConnection connection) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
        while (System.nanoTime() < deadline) {
            if (connection.outputHalf() == null) {
                return;
            }
            Thread.sleep(1L);
        }
        throw new AssertionError("connection did not enter paused output state");
    }

    @Test
    void readDeadlineDuringPauseReachesAttachedActiveReadHalf() throws Exception {
        BlockingReadHalf readHalf = new BlockingReadHalf();
        JoinedDuplexConnection connection = new JoinedDuplexConnection(readHalf, (WriteHalf) null);

        FutureTask<IOException> readTask = new FutureTask<>(() -> {
            try {
                connection.input().read(new byte[1]);
                return null;
            } catch (IOException error) {
                return error;
            }
        });
        Thread readThread = new Thread(readTask, "joined-read-deadline-test-read");
        readThread.start();
        assertTrue(readHalf.readEntered.await(1L, TimeUnit.SECONDS), "read did not enter half");

        FutureTask<JoinedDuplexConnection.PausedInput> pauseTask = new FutureTask<>(
                () -> connection.pauseInput(Duration.ofSeconds(2))
        );
        Thread pauseThread = new Thread(pauseTask, "joined-read-deadline-test-pause");
        pauseThread.start();
        waitUntilPaused(connection);

        try {
            connection.setReadDeadline(Instant.now());
            assertTrue(readHalf.deadlineApplied.await(1L, TimeUnit.SECONDS), "deadline was not applied to paused active half");
            IOException readError = readTask.get(1L, TimeUnit.SECONDS);
            assertTrue(readError instanceof SocketTimeoutException, "read should be released by deadline");
            JoinedDuplexConnection.PausedInput paused = pauseTask.get(1L, TimeUnit.SECONDS);
            assertNotNull(paused.currentReadHalf(), "pause should detach the original read half");
            assertEquals(1, readHalf.deadlineCount.get(), "deadline should be applied once");
        } finally {
            readHalf.releaseRead.countDown();
            connection.close();
            readThread.join(1000L);
            pauseThread.join(1000L);
        }
    }

    @Test
    void writeDeadlineDuringPauseReachesAttachedActiveWriteHalf() throws Exception {
        BlockingWriteHalf writeHalf = new BlockingWriteHalf();
        JoinedDuplexConnection connection = new JoinedDuplexConnection((ReadHalf) null, writeHalf);

        FutureTask<IOException> writeTask = new FutureTask<>(() -> {
            try {
                connection.output().write(new byte[]{1});
                return null;
            } catch (IOException error) {
                return error;
            }
        });
        Thread writeThread = new Thread(writeTask, "joined-write-deadline-test-write");
        writeThread.start();
        assertTrue(writeHalf.writeEntered.await(1L, TimeUnit.SECONDS), "write did not enter half");

        FutureTask<JoinedDuplexConnection.PausedOutput> pauseTask = new FutureTask<>(
                () -> connection.pauseOutput(Duration.ofSeconds(2))
        );
        Thread pauseThread = new Thread(pauseTask, "joined-write-deadline-test-pause");
        pauseThread.start();
        waitUntilOutputPaused(connection);

        try {
            connection.setWriteDeadline(Instant.now());
            assertTrue(writeHalf.deadlineApplied.await(1L, TimeUnit.SECONDS), "deadline was not applied to paused active half");
            IOException writeError = writeTask.get(1L, TimeUnit.SECONDS);
            assertTrue(writeError instanceof SocketTimeoutException, "write should be released by deadline");
            JoinedDuplexConnection.PausedOutput paused = pauseTask.get(1L, TimeUnit.SECONDS);
            assertNotNull(paused.currentWriteHalf(), "pause should detach the original write half");
            assertEquals(1, writeHalf.deadlineCount.get(), "deadline should be applied once");
        } finally {
            writeHalf.releaseWrite.countDown();
            connection.close();
            writeThread.join(1000L);
            pauseThread.join(1000L);
        }
    }

    @Test
    void readDeadlineFailureRollsBackJoinedDeadline() throws Exception {
        RejectingDeadlineReadHalf readHalf = new RejectingDeadlineReadHalf();
        JoinedDuplexConnection connection = new JoinedDuplexConnection(readHalf, (WriteHalf) null);

        IOException error = assertThrows(
                IOException.class,
                () -> connection.setReadDeadline(Instant.now()),
                "read deadline failure should be surfaced"
        );
        assertSame(readHalf.failure, error, "read deadline should surface the underlying failure");

        JoinedDuplexConnection.PausedInput paused = connection.pauseRead(Duration.ofSeconds(1));
        FutureTask<Object> readTask = new FutureTask<>(() -> {
            try {
                return connection.input().read(new byte[1]);
            } catch (IOException readError) {
                return readError;
            }
        });
        Thread readThread = new Thread(readTask, "joined-read-deadline-rollback-read");
        readThread.start();

        try {
            Thread.sleep(80L);
            assertFalse(readTask.isDone(), "failed read deadline must not remain armed while input is paused");
            paused.resume();
            assertEquals(-1, readTask.get(1L, TimeUnit.SECONDS), "read should resume with the original no-deadline state");
            assertEquals(1, readHalf.rejectedDeadlines.get(), "only the rejected non-null read deadline should fail");
            assertEquals(1, readHalf.clearedDeadlines.get(), "resume should apply the rolled-back null read deadline");
        } finally {
            connection.close();
            readThread.join(1000L);
        }
    }

    @Test
    void writeDeadlineFailureRollsBackJoinedDeadline() throws Exception {
        RejectingDeadlineWriteHalf writeHalf = new RejectingDeadlineWriteHalf();
        JoinedDuplexConnection connection = new JoinedDuplexConnection((ReadHalf) null, writeHalf);

        IOException error = assertThrows(
                IOException.class,
                () -> connection.setWriteDeadline(Instant.now()),
                "write deadline failure should be surfaced"
        );
        assertSame(writeHalf.failure, error, "write deadline should surface the underlying failure");

        JoinedDuplexConnection.PausedOutput paused = connection.pauseWrite(Duration.ofSeconds(1));
        FutureTask<IOException> writeTask = new FutureTask<>(() -> {
            try {
                connection.output().write(1);
                return null;
            } catch (IOException writeError) {
                return writeError;
            }
        });
        Thread writeThread = new Thread(writeTask, "joined-write-deadline-rollback-write");
        writeThread.start();

        try {
            Thread.sleep(80L);
            assertFalse(writeTask.isDone(), "failed write deadline must not remain armed while output is paused");
            paused.resume();
            assertNull(writeTask.get(1L, TimeUnit.SECONDS), "write should resume with the original no-deadline state");
            assertEquals(1, writeHalf.rejectedDeadlines.get(), "only the rejected non-null write deadline should fail");
            assertEquals(1, writeHalf.clearedDeadlines.get(), "resume should apply the rolled-back null write deadline");
        } finally {
            connection.close();
            writeThread.join(1000L);
        }
    }

    @Test
    void closeUsesUnderlyingCloseForAttachedHalves() throws Exception {
        CountingReadHalf readHalf = new CountingReadHalf();
        CountingWriteHalf writeHalf = new CountingWriteHalf();
        JoinedDuplexConnection connection = new JoinedDuplexConnection(readHalf, writeHalf);

        connection.close();

        assertEquals(1, readHalf.closeCount.get(), "read half close() should release its underlying resource");
        assertEquals(0, readHalf.closeReadCount.get(), "connection close must not degrade to closeRead()");
        assertEquals(1, writeHalf.closeCount.get(), "write half close() should release its underlying resource");
        assertEquals(0, writeHalf.closeWriteCount.get(), "connection close must not degrade to closeWrite()");
    }

    @Test
    void closeUsesSharedUnderlyingCloseOnlyOnce() throws Exception {
        CountingDuplexHalf half = new CountingDuplexHalf();
        JoinedDuplexConnection connection = new JoinedDuplexConnection((ReadHalf) half, (WriteHalf) half);

        connection.close();

        assertEquals(1, half.closeCount.get(), "shared underlying resource should be closed once");
        assertEquals(0, half.closeReadCount.get(), "shared connection close must not call closeRead()");
        assertEquals(0, half.closeWriteCount.get(), "shared connection close must not call closeWrite()");
    }

    @Test
    void directionalCloseKeepsHalfCloseSemantics() throws Exception {
        CountingReadHalf readHalf = new CountingReadHalf();
        CountingWriteHalf writeHalf = new CountingWriteHalf();
        JoinedDuplexConnection connection = new JoinedDuplexConnection(readHalf, writeHalf);

        connection.closeRead();
        connection.closeWrite();

        assertEquals(1, readHalf.closeReadCount.get(), "closeRead should close only the read side");
        assertEquals(0, readHalf.closeCount.get(), "closeRead should not fully close the read resource");
        assertEquals(1, writeHalf.closeWriteCount.get(), "closeWrite should close only the write side");
        assertEquals(0, writeHalf.closeCount.get(), "closeWrite should not fully close the write resource");
    }

    private static final class BlockingReadHalf implements ReadHalf {
        private final CountDownLatch readEntered = new CountDownLatch(1);
        private final CountDownLatch releaseRead = new CountDownLatch(1);
        private final CountDownLatch deadlineApplied = new CountDownLatch(1);
        private final AtomicInteger deadlineCount = new AtomicInteger();

        @Override
        public int read(byte[] dst, int offset, int length) throws IOException {
            readEntered.countDown();
            try {
                releaseRead.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted", interrupted);
            }
            throw new SocketTimeoutException("deadline");
        }

        @Override
        public void closeRead() {
            releaseRead.countDown();
        }

        @Override
        public void setReadDeadline(Instant deadline) {
            deadlineCount.incrementAndGet();
            deadlineApplied.countDown();
            releaseRead.countDown();
        }

        @Override
        public SocketAddress localAddress() {
            return ZmuxSocketAddress.localPending();
        }

        @Override
        public SocketAddress remoteAddress() {
            return ZmuxSocketAddress.remotePending();
        }
    }

    private static final class CountingReadHalf implements ReadHalf {
        private final AtomicInteger closeReadCount = new AtomicInteger();
        private final AtomicInteger closeCount = new AtomicInteger();

        @Override
        public int read(byte[] dst, int offset, int length) {
            return -1;
        }

        @Override
        public void closeRead() {
            closeReadCount.incrementAndGet();
        }

        @Override
        public void setReadDeadline(Instant deadline) {
        }

        @Override
        public SocketAddress localAddress() {
            return ZmuxSocketAddress.localPending();
        }

        @Override
        public SocketAddress remoteAddress() {
            return ZmuxSocketAddress.remotePending();
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }
    }

    private static final class RejectingDeadlineReadHalf implements ReadHalf {
        private final IOException failure = new IOException("reject read deadline");
        private final AtomicInteger rejectedDeadlines = new AtomicInteger();
        private final AtomicInteger clearedDeadlines = new AtomicInteger();

        @Override
        public int read(byte[] dst, int offset, int length) {
            return -1;
        }

        @Override
        public void closeRead() {
        }

        @Override
        public void setReadDeadline(Instant deadline) throws IOException {
            if (deadline == null) {
                clearedDeadlines.incrementAndGet();
                return;
            }
            rejectedDeadlines.incrementAndGet();
            throw failure;
        }
    }

    private static final class RejectingDeadlineWriteHalf implements WriteHalf {
        private final IOException failure = new IOException("reject write deadline");
        private final AtomicInteger rejectedDeadlines = new AtomicInteger();
        private final AtomicInteger clearedDeadlines = new AtomicInteger();

        @Override
        public void write(byte[] src, int offset, int length) {
        }

        @Override
        public void closeWrite() {
        }

        @Override
        public void setWriteDeadline(Instant deadline) throws IOException {
            if (deadline == null) {
                clearedDeadlines.incrementAndGet();
                return;
            }
            rejectedDeadlines.incrementAndGet();
            throw failure;
        }
    }

    private static final class CountingWriteHalf implements WriteHalf {
        private final AtomicInteger closeWriteCount = new AtomicInteger();
        private final AtomicInteger closeCount = new AtomicInteger();

        @Override
        public void write(byte[] src, int offset, int length) {
        }

        @Override
        public void closeWrite() {
            closeWriteCount.incrementAndGet();
        }

        @Override
        public void setWriteDeadline(Instant deadline) {
        }

        @Override
        public SocketAddress localAddress() {
            return ZmuxSocketAddress.localPending();
        }

        @Override
        public SocketAddress remoteAddress() {
            return ZmuxSocketAddress.remotePending();
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }
    }

    private static final class CountingDuplexHalf implements ReadHalf, WriteHalf {
        private final AtomicInteger closeReadCount = new AtomicInteger();
        private final AtomicInteger closeWriteCount = new AtomicInteger();
        private final AtomicInteger closeCount = new AtomicInteger();

        @Override
        public int read(byte[] dst, int offset, int length) {
            return -1;
        }

        @Override
        public void write(byte[] src, int offset, int length) {
        }

        @Override
        public void closeRead() {
            closeReadCount.incrementAndGet();
        }

        @Override
        public void closeWrite() {
            closeWriteCount.incrementAndGet();
        }

        @Override
        public void setReadDeadline(Instant deadline) {
        }

        @Override
        public void setWriteDeadline(Instant deadline) {
        }

        @Override
        public SocketAddress localAddress() {
            return ZmuxSocketAddress.localPending();
        }

        @Override
        public SocketAddress remoteAddress() {
            return ZmuxSocketAddress.remotePending();
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }
    }

    private static final class BlockingWriteHalf implements WriteHalf {
        private final CountDownLatch writeEntered = new CountDownLatch(1);
        private final CountDownLatch releaseWrite = new CountDownLatch(1);
        private final CountDownLatch deadlineApplied = new CountDownLatch(1);
        private final AtomicInteger deadlineCount = new AtomicInteger();

        @Override
        public void write(byte[] src, int offset, int length) throws IOException {
            writeEntered.countDown();
            try {
                releaseWrite.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted", interrupted);
            }
            throw new SocketTimeoutException("deadline");
        }

        @Override
        public void closeWrite() {
            releaseWrite.countDown();
        }

        @Override
        public void setWriteDeadline(Instant deadline) {
            deadlineCount.incrementAndGet();
            deadlineApplied.countDown();
            releaseWrite.countDown();
        }

        @Override
        public SocketAddress localAddress() {
            return ZmuxSocketAddress.localPending();
        }

        @Override
        public SocketAddress remoteAddress() {
            return ZmuxSocketAddress.remotePending();
        }
    }
}
