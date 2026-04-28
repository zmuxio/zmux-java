package io.zmux;

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
