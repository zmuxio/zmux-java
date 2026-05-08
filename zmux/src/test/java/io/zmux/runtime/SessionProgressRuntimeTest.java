package io.zmux.runtime;

import io.zmux.Role;
import io.zmux.SessionStats;
import io.zmux.Settings;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class SessionProgressRuntimeTest {
    private static StreamRuntime createPeerOpenedBidi(SessionRuntime runtime) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField("readerRuntime");
        field.setAccessible(true);
        Object readerRuntime = field.get(runtime);
        synchronized (runtime.lock()) {
            return (StreamRuntime) SessionRuntimeTestSupport.invokePrivate(
                    readerRuntime,
                    "createPeerOpenedStreamLocked",
                    new Class<?>[]{long.class},
                    SessionRuntime.firstPeerStreamId(Role.RESPONDER, true)
            );
        }
    }

    private static void awaitWaiting(Thread thread) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
        while (thread.isAlive() && System.nanoTime() < deadlineNanos) {
            Thread.State state = thread.getState();
            if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
                return;
            }
            Thread.sleep(5L);
        }
        Thread.State state = thread.getState();
        assertTrue(
                state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING,
                "writer should enter blocked wait before credit is restored, state=" + state
        );
    }

    @Test
    void localWriteRecordsProgressAndOpenLatency() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            stream.setProvisionalCreatedAtNanosLocked(System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(25L));
        }

        SessionRuntimeTestSupport.queueWrite(stream, "local".getBytes(StandardCharsets.UTF_8));

        SessionStats stats = runtime.stats();
        assertNotNull(stats.progress().streamProgressAt(), "local write should record stream progress");
        assertNotNull(stats.progress().applicationProgressAt(), "local write should record application progress");
        assertTrue(
                stats.lastOpenLatencyNanos() >= TimeUnit.MILLISECONDS.toNanos(20L),
                "open latency should include the provisional age up to opening commit"
        );
    }

    @Test
    void readRecordsProgress() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime);

        synchronized (runtime.lock()) {
            stream.receiveDataLocked("peer".getBytes(StandardCharsets.UTF_8));
        }

        byte[] buffer = new byte[8];
        assertEquals(4, stream.read(buffer), "peer payload length mismatch");

        SessionStats stats = runtime.stats();
        assertNotNull(stats.progress().streamProgressAt(), "read should record stream progress");
        assertNotNull(stats.progress().applicationProgressAt(), "read should record application progress");
    }

    @Test
    void blockedWriteAndCompletedFlushPopulateStats() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setLongField(runtime, "sessionSendLimit", 0L);
        }

        Thread writer = new Thread(() -> {
            try {
                SessionRuntimeTestSupport.queueWrite(stream, "x".getBytes(StandardCharsets.UTF_8));
            } catch (Throwable throwable) {
                failure.set(throwable);
            }
        }, "session-progress-blocked-write");
        writer.start();

        awaitWaiting(writer);
        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setLongField(runtime, "sessionSendLimit", 1024L);
            runtime.lock().notifyAll();
        }

        writer.join(1_000L);
        assertFalse(writer.isAlive(), "blocked writer should resume once session credit is restored");
        assertNull(failure.get(), "blocked write should complete without surfacing an error");

        synchronized (runtime.lock()) {
            long completedAtNanos = System.nanoTime();
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "noteCompletedWriteBatchLocked",
                    new Class<?>[]{int.class, long.class, long.class, long.class},
                    2,
                    128L,
                    completedAtNanos - TimeUnit.MILLISECONDS.toNanos(5L),
                    completedAtNanos
            );
        }

        SessionStats stats = runtime.stats();
        assertTrue(stats.blockedWriteTotalNanos() > 0L, "blocked write wait time should accumulate");
        assertEquals(1L, stats.flush().count(), "completed write batch should increment flush count");
        assertEquals(2, stats.flush().lastFrames(), "flush should retain the last batch frame count");
        assertEquals(128L, stats.flush().lastBytes(), "flush should retain the last batch byte count");
        assertNotNull(stats.flush().lastAt(), "flush should retain the last completion timestamp");
        assertNotNull(stats.progress().transportWriteAt(), "completed write batch should record transport progress");
    }
}
