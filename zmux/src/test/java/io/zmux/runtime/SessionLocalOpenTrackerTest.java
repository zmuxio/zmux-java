package io.zmux.runtime;

import io.zmux.Settings;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

final class SessionLocalOpenTrackerTest {
    private static SessionLocalOpenTracker tracker(SessionRuntime runtime) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField("localOpenTracker");
        field.setAccessible(true);
        return (SessionLocalOpenTracker) field.get(runtime);
    }

    private static Object queue(SessionLocalOpenTracker tracker, String name) throws Exception {
        Field field = SessionLocalOpenTracker.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(tracker);
    }

    @Test
    void drainedLargeProvisionalQueueReleasesDequeStorage() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        List<StreamRuntime> streams = new ArrayList<>();

        synchronized (runtime.lock()) {
            SessionLocalOpenTracker tracker = tracker(runtime);
            for (int i = 0; i < 1100; i++) {
                StreamRuntime stream = new StreamRuntime(runtime, true, true, null);
                tracker.appendProvisionalLocked(stream);
                streams.add(stream);
            }
            Object retainedQueue = queue(tracker, "provisionalBidi");

            for (StreamRuntime stream : streams) {
                tracker.removeProvisionalLocked(stream);
            }

            assertEquals(0, tracker.provisionalCountLocked(true), "provisional queue should be empty after draining");
            assertNotSame(retainedQueue, queue(tracker, "provisionalBidi"),
                    "drained oversized provisional queue should release ArrayDeque backing");
        }
    }

    @Test
    void drainedLargeUnseenLocalQueueReleasesDequeStorage() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        List<StreamRuntime> streams = new ArrayList<>();

        synchronized (runtime.lock()) {
            SessionLocalOpenTracker tracker = tracker(runtime);
            long streamId = SessionRuntime.firstLocalStreamId(runtime.localRole(), true);
            for (int i = 0; i < 1100; i++) {
                StreamRuntime stream = new StreamRuntime(runtime, true, true, null);
                stream.initializeLocalOpenedLocked(streamId + i * 4L, 1024L, 1024L);
                tracker.appendUnseenLocalLocked(stream);
                streams.add(stream);
            }
            Object retainedQueue = queue(tracker, "unseenLocalBidi");

            for (StreamRuntime stream : streams) {
                tracker.removeUnseenLocalLocked(stream);
            }

            assertNotSame(retainedQueue, queue(tracker, "unseenLocalBidi"),
                    "drained oversized unseen-local queue should release ArrayDeque backing");
        }
    }
}
