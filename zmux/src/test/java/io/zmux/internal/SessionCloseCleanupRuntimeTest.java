package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class SessionCloseCleanupRuntimeTest {
    private static final int MAX_PENDING_READ_LOOP_PROTOCOL_TASKS = 256;

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

    @SuppressWarnings("unchecked")
    private static Deque<Object> readLoopProtocolTasks(SessionRuntime runtime) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField("readLoopProtocolTasks");
        field.setAccessible(true);
        return (Deque<Object>) field.get(runtime);
    }

    private static Object newReadLoopProtocolTask() throws Exception {
        Class<?> outboundFrameType = Class.forName("io.zmux.internal.SessionRuntime$OutboundFrame");
        Class<?> taskType = Class.forName("io.zmux.internal.SessionRuntime$ReadLoopProtocolTask");
        Constructor<?> constructor = taskType.getDeclaredConstructor(outboundFrameType);
        constructor.setAccessible(true);
        return constructor.newInstance(new Object[]{null});
    }

    @Test
    void finishSessionClearsProtocolBacklogAndPendingWriterQueues() throws Exception {
        long capabilities = Protocol.CAPABILITY_PRIORITY_HINTS | Protocol.CAPABILITY_PRIORITY_UPDATE;
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(capabilities, Settings.defaults());
        StreamRuntime ordinary = (StreamRuntime) runtime.openStream();
        StreamRuntime advisory = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, ordinary);
            makePeerVisible(runtime, advisory);

            ordinary.write("ordinary".getBytes(StandardCharsets.UTF_8));
            advisory.updateMetadata(MetadataUpdate.priority(7L));
            runtime.enqueueControlLocked(new FrameCodec.Frame(FrameType.PING, 0, 0L, new byte[]{1}));

            SessionRuntimeTestSupport.setField(runtime, "readLoopProtocolWorkerStarted", true);
            SessionRuntime.OutboundFrame protocolFrame = new SessionRuntime.OutboundFrame(
                    new FrameCodec.Frame(FrameType.PONG, 0, 0L, new byte[]{2}),
                    null,
                    0,
                    false,
                    false
            );
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "enqueueReadLoopProtocolFrameLocked",
                    new Class<?>[]{SessionRuntime.OutboundFrame.class, boolean.class},
                    protocolFrame,
                    false
            );

            assertFalse(SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue").isEmpty(),
                    "ordinary lane should hold queued stream data before close cleanup");
            assertFalse(SessionRuntimeTestSupport.advisoryQueue(runtime).isEmpty(),
                    "advisory lane should hold queued priority update before close cleanup");
            assertFalse(SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue").isEmpty(),
                    "urgent lane should hold queued control before close cleanup");
            assertFalse(readLoopProtocolTasks(runtime).isEmpty(),
                    "protocol backlog should hold queued read-loop work before close cleanup");
            assertTrue(runtime.outboundQueueBookkeepingInternal().pendingPriorityBytesLocked() > 0L,
                    "priority backlog should retain pending bytes before close cleanup");
            assertTrue(SessionRuntimeTestSupport.getLongField(runtime, "sessionQueuedDataBytes") > 0L,
                    "queued ordinary data should contribute to tracked session queue bytes before close cleanup");

            runtime.finishSessionLocked(new SessionClosedException(ZmuxErrorSource.LOCAL), SessionState.CLOSED);

            assertTrue(SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue").isEmpty(),
                    "ordinary lane must be drained on session close");
            assertTrue(SessionRuntimeTestSupport.advisoryQueue(runtime).isEmpty(),
                    "advisory lane must be drained on session close");
            assertTrue(SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue").isEmpty(),
                    "urgent lane must be drained on session close");
            assertTrue(readLoopProtocolTasks(runtime).isEmpty(),
                    "protocol backlog must be cleared on session close");
            assertEquals(0L, runtime.outboundQueueBookkeepingInternal().urgentQueuedControlBytesLocked(),
                    "urgent queued control bytes must reset after session close cleanup");
            assertEquals(0L, runtime.outboundQueueBookkeepingInternal().pendingPriorityBytesLocked(),
                    "pending priority bytes must reset after session close cleanup");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "sessionQueuedDataBytes"),
                    "queued data accounting must reset after session close cleanup");
        }
    }

    @Test
    void readLoopProtocolDrainReleasesFullBacklogDeque() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        ArrayDeque<Object> oversizedBacklog = new ArrayDeque<>(MAX_PENDING_READ_LOOP_PROTOCOL_TASKS * 2);
        for (int i = 0; i < MAX_PENDING_READ_LOOP_PROTOCOL_TASKS; i++) {
            oversizedBacklog.addLast(newReadLoopProtocolTask());
        }

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setField(runtime, "readLoopProtocolTasks", oversizedBacklog);
        }

        CountDownLatch drained = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                SessionRuntimeTestSupport.invokePrivate(runtime, "readLoopProtocolLoop", new Class<?>[0]);
            } catch (Throwable error) {
                workerFailure.set(error);
            } finally {
                finished.countDown();
            }
        }, "test-protocol-drain");
        worker.start();

        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
        while (System.nanoTime() < deadlineNanos) {
            synchronized (runtime.lock()) {
                if (readLoopProtocolTasks(runtime).isEmpty()
                        && readLoopProtocolTasks(runtime) != oversizedBacklog) {
                    drained.countDown();
                    SessionRuntimeTestSupport.setField(runtime, "state", SessionState.CLOSED);
                    runtime.lock().notifyAll();
                    break;
                }
            }
            Thread.sleep(10L);
        }

        assertTrue(drained.await(1L, TimeUnit.SECONDS),
                "protocol worker should drain a full backlog and replace the retained deque backing");
        assertTrue(finished.await(1L, TimeUnit.SECONDS), "protocol worker should stop once the session is closed");
        assertNull(workerFailure.get(), "protocol worker should drain synthetic close-write tasks without failing");
    }
}
