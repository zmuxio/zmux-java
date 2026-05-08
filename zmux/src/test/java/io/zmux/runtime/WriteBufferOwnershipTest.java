package io.zmux.runtime;

import io.zmux.Settings;
import io.zmux.ZmuxStream;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

final class WriteBufferOwnershipTest {
    private static int invokeWritev(StreamRuntime stream, byte[][] parts, boolean fin) throws Exception {
        Field field = StreamRuntime.class.getDeclaredField("writeCoordinator");
        field.setAccessible(true);
        Object coordinator = field.get(stream);
        Method method = coordinator.getClass().getDeclaredMethod("writev", byte[][].class, boolean.class);
        method.setAccessible(true);
        return (Integer) method.invoke(coordinator, parts, fin);
    }

    private static int queueSize(SessionRuntime runtime, String name) throws Exception {
        Deque<Object> queue = SessionRuntimeTestSupport.outboundQueue(runtime, name);
        return queue.size();
    }

    private static Object awaitLastOutbound(SessionRuntime runtime, String name) throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
        while (System.nanoTime() < deadlineNanos) {
            Object outbound = SessionRuntimeTestSupport.outboundQueue(runtime, name).peekLast();
            if (outbound != null) {
                return outbound;
            }
            Thread.sleep(1L);
        }
        throw new AssertionError("timed out waiting for outbound frame");
    }

    @Test
    void openingWriteRetainsBorrowedPayloadBeforeReturn() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        ZmuxStream stream = runtime.openStream();

        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        SessionRuntimeTestSupport.queueWrite(stream, payload);
        payload[0] = 'x';

        Object outbound = SessionRuntimeTestSupport.pollLastOutboundQueue(runtime, "dataQueue");
        assertNotNull(outbound, "expected opening DATA to be queued");
        assertEquals("hello", new String(SessionRuntimeTestSupport.outboundPayload(outbound), StandardCharsets.UTF_8), "queued opening payload must not alias the caller buffer");
    }

    @Test
    void postOpenWriteRetainsBorrowedPayloadBeforeReturn() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        ZmuxStream stream = runtime.openStream();
        SessionRuntimeTestSupport.queueWrite(stream, "open".getBytes(StandardCharsets.UTF_8));

        byte[] payload = "later".getBytes(StandardCharsets.UTF_8);
        SessionRuntimeTestSupport.queueWrite(stream, payload);
        payload[0] = 'x';

        Object outbound = SessionRuntimeTestSupport.pollLastOutboundQueue(runtime, "dataQueue");
        assertNotNull(outbound, "expected post-open DATA to be queued");
        assertEquals("later", new String(SessionRuntimeTestSupport.outboundPayload(outbound), StandardCharsets.UTF_8), "queued post-open payload must not alias the caller buffer");
    }

    @Test
    void asyncWriteRetainsPayloadBeforeFutureCompletion() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        byte[] payload = "async".getBytes(StandardCharsets.UTF_8);
        CompletionStage<Void> write = stream.writeAsync(payload);
        payload[0] = 'x';

        Object outbound = awaitLastOutbound(runtime, "dataQueue");
        assertEquals("async", new String(SessionRuntimeTestSupport.outboundPayload(outbound), StandardCharsets.UTF_8),
                "async write must not alias the caller buffer before the future completes");

        runtime.closeWithError(0L, "");
        assertThrows(ExecutionException.class, () -> write.toCompletableFuture().get(1L, TimeUnit.SECONDS));
    }

    @Test
    void writevFinalRetainsBorrowedPartsWithoutFlatteningIntoSingleChunk() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        ZmuxStream stream = runtime.openStream();

        byte[] first = "he".getBytes(StandardCharsets.UTF_8);
        byte[] second = "llo".getBytes(StandardCharsets.UTF_8);
        SessionRuntimeTestSupport.queueWritevFinal(stream, first, second);
        first[0] = 'x';
        second[0] = 'y';

        Object outbound = SessionRuntimeTestSupport.pollLastOutboundQueue(runtime, "dataQueue");
        assertNotNull(outbound, "expected multipart DATA to be queued");
        assertEquals("hello", new String(SessionRuntimeTestSupport.outboundPayload(outbound), StandardCharsets.UTF_8), "queued multipart payload must not alias caller buffers");
        assertTrue(SessionRuntimeTestSupport.outboundPayloadPartCount(outbound) >= 2, "multipart write should retain segmented owned parts instead of eagerly flattening");
    }

    @Test
    void writevFinalMergesManyTinyBorrowedParts() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        ZmuxStream stream = runtime.openStream();

        byte[][] parts = new byte[20][];
        StringBuilder expected = new StringBuilder(parts.length);
        for (int i = 0; i < parts.length; ++i) {
            char next = (char) ('a' + i);
            expected.append(next);
            parts[i] = new byte[]{(byte) next};
        }
        SessionRuntimeTestSupport.queueWritevFinal(stream, parts);
        parts[0][0] = 'x';

        Object outbound = SessionRuntimeTestSupport.pollLastOutboundQueue(runtime, "dataQueue");
        assertNotNull(outbound, "expected multipart DATA to be queued");
        assertEquals(expected.toString(), new String(SessionRuntimeTestSupport.outboundPayload(outbound), StandardCharsets.UTF_8),
                "merged tiny multipart payload must not alias caller buffers");
        assertEquals(0, SessionRuntimeTestSupport.outboundPayloadPartCount(outbound),
                "many tiny borrowed parts should be merged to avoid retaining many small arrays");
    }

    @Test
    void ordinaryZeroLengthWritevDoesNotObserveClosedWriteSide() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        stream.closeWrite();

        int dataQueuedBefore;
        int urgentQueuedBefore;
        synchronized (runtime.lock()) {
            dataQueuedBefore = queueSize(runtime, "dataQueue");
            urgentQueuedBefore = queueSize(runtime, "urgentQueue");
        }

        assertEquals(0, invokeWritev(stream, new byte[][]{new byte[0]}, false));

        synchronized (runtime.lock()) {
            assertEquals(dataQueuedBefore, queueSize(runtime, "dataQueue"), "ordinary empty writev must not enqueue data after closeWrite");
            assertEquals(urgentQueuedBefore, queueSize(runtime, "urgentQueue"), "ordinary empty writev must not enqueue urgent control after closeWrite");
        }
    }
}
