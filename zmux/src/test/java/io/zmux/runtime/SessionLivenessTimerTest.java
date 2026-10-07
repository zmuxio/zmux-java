package io.zmux.runtime;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;

final class SessionLivenessTimerTest {
    @Test
    void blockedTaskDoesNotDelayLaterTasks() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch blockedStarted = new CountDownLatch(1);
        CountDownLatch laterRan = new CountDownLatch(1);
        try {
            // Like a keepalive check waiting for a session lock that another thread holds.
            SessionLivenessTimer.schedule(() -> {
                blockedStarted.countDown();
                try {
                    release.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }, 0L);
            assertTrue(blockedStarted.await(2, TimeUnit.SECONDS), "first task should start");

            SessionLivenessTimer.schedule(laterRan::countDown, TimeUnit.MILLISECONDS.toNanos(10L));
            assertTrue(
                    laterRan.await(2, TimeUnit.SECONDS),
                    "a timer task blocked on one session must not hold up the timer for other sessions"
            );
        } finally {
            release.countDown();
        }
    }

    @Test
    void tasksDoNotRunOnTheTimerThread() throws Exception {
        AtomicReference<String> threadName = new AtomicReference<>();
        CountDownLatch ran = new CountDownLatch(1);
        SessionLivenessTimer.schedule(() -> {
            threadName.set(Thread.currentThread().getName());
            ran.countDown();
        }, 0L);
        assertTrue(ran.await(2, TimeUnit.SECONDS), "task should run");
        assertTrue(threadName.get().startsWith("zmux-liveness-worker-"), "task ran on " + threadName.get());
    }
}
