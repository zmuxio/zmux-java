package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Deque;

import static org.junit.jupiter.api.Assertions.*;

final class SessionCloseCleanupRuntimeTest {
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
}
