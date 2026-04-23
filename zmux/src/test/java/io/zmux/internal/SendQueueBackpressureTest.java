package io.zmux.internal;

import io.zmux.Role;
import io.zmux.Settings;
import io.zmux.ZmuxConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class SendQueueBackpressureTest {
    private static Settings largeSendWindowSettings() {
        return Settings.defaults().toBuilder()
                .initialMaxData(1L << 20)
                .initialMaxStreamDataBidiPeerOpened(1L << 20)
                .build();
    }

    private static void makePeerVisible(SessionRuntime runtime, StreamRuntime stream) throws Exception {
        SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "beginLocalOpenLocked",
                new Class<?>[]{StreamRuntime.class},
                stream
        );
        runtime.markLocalStreamOpeningCommittedLocked(stream);
        SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "markPeerVisibleLocked",
                new Class<?>[]{StreamRuntime.class},
                stream
        );
    }

    private static void reserveSend(SessionRuntime runtime, StreamRuntime stream, int bytes) throws Exception {
        synchronized (runtime.lock()) {
            runtime.reserveSendLocked(stream, bytes);
        }
    }

    private static void setQueuedBytesLocked(SessionRuntime runtime, StreamRuntime stream, long streamQueued, long sessionQueued) throws Exception {
        SessionRuntimeTestSupport.setLongField(stream, "queuedDataBytes", streamQueued);
        SessionRuntimeTestSupport.setLongField(runtime, "sessionQueuedDataBytes", sessionQueued);
    }

    @Test
    void sessionQueuedDataHwmBlocksCrossStreamWritesUntilQueuedBytesCrossLowWatermark() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .sessionQueuedDataHwm(8L)
                .perStreamQueuedDataHwm(64L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, largeSendWindowSettings());
        StreamRuntime holder = (StreamRuntime) runtime.openStream();
        StreamRuntime blocked = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, holder);
            makePeerVisible(runtime, blocked);
            setQueuedBytesLocked(runtime, holder, 8L, 8L);
        }

        WriteAttempt blockedWrite = WriteAttempt.start(() -> reserveSend(runtime, blocked, 1));
        try {
            assertFalse(blockedWrite.await(Duration.ofMillis(150)), "cross-stream reservation should wait while session queued bytes remain at HWM");

            synchronized (runtime.lock()) {
                setQueuedBytesLocked(runtime, holder, 4L, 4L);
                runtime.lock().notifyAll();
            }

            assertTrue(blockedWrite.await(Duration.ofSeconds(1)), "cross-stream reservation should resume after queued bytes cross the session low watermark");
            assertNull(blockedWrite.error(), "cross-stream reservation should complete cleanly");
        } finally {
            synchronized (runtime.lock()) {
                setQueuedBytesLocked(runtime, holder, 0L, 0L);
                runtime.lock().notifyAll();
            }
            blockedWrite.join();
        }
    }

    @Test
    void perStreamQueuedDataHwmBlocksSameStreamButNotOtherStreams() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .sessionQueuedDataHwm(64L)
                .perStreamQueuedDataHwm(8L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, largeSendWindowSettings());
        StreamRuntime blocked = (StreamRuntime) runtime.openStream();
        StreamRuntime other = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, blocked);
            makePeerVisible(runtime, other);
            setQueuedBytesLocked(runtime, blocked, 8L, 8L);
        }

        WriteAttempt sameStreamBlocked = WriteAttempt.start(() -> reserveSend(runtime, blocked, 1));
        try {
            assertFalse(sameStreamBlocked.await(Duration.ofMillis(150)), "same-stream reservation should wait while that stream remains at the queued-data HWM");

            reserveSend(runtime, other, 1);
            assertFalse(sameStreamBlocked.await(Duration.ofMillis(150)), "same-stream reservation should remain blocked after an unrelated stream reserves send credit");

            synchronized (runtime.lock()) {
                setQueuedBytesLocked(runtime, blocked, 4L, 4L);
                runtime.lock().notifyAll();
            }

            assertTrue(sameStreamBlocked.await(Duration.ofSeconds(1)), "same-stream reservation should resume after queued bytes cross the per-stream low watermark");
            assertNull(sameStreamBlocked.error(), "same-stream reservation should complete cleanly");
        } finally {
            synchronized (runtime.lock()) {
                setQueuedBytesLocked(runtime, blocked, 0L, 0L);
                runtime.lock().notifyAll();
            }
            sameStreamBlocked.join();
        }
    }

    @FunctionalInterface
    private interface CheckedRunnable {
        void run() throws Exception;
    }

    private static final class WriteAttempt {
        private final Thread thread;
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private final CountDownLatch done = new CountDownLatch(1);

        private WriteAttempt(CheckedRunnable runnable) {
            this.thread = new Thread(() -> {
                try {
                    runnable.run();
                } catch (Throwable t) {
                    error.set(t);
                } finally {
                    done.countDown();
                }
            }, "send-queue-backpressure-write");
        }

        static WriteAttempt start(CheckedRunnable runnable) {
            WriteAttempt attempt = new WriteAttempt(runnable);
            attempt.thread.start();
            return attempt;
        }

        boolean await(Duration timeout) throws InterruptedException {
            return done.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        Throwable error() {
            return error.get();
        }

        void join() throws InterruptedException {
            thread.join(1000L);
        }
    }
}
