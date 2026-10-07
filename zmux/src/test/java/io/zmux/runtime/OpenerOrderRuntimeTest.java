package io.zmux.runtime;

import io.zmux.ErrorCode;
import io.zmux.Settings;
import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;
import io.zmux.protocol.Varint62;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Writer-side ordering of local opening frames: within a stream class the peer must observe new stream IDs
 * in assignment order (SPEC §3.1), and a stream's first frame must be opening-eligible (SPEC §6.7, §9.1).
 */
final class OpenerOrderRuntimeTest {
    @SuppressWarnings("unchecked")
    private static List<Object> collectReadyBatch(SessionRuntime runtime) throws Exception {
        return (List<Object>) SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "collectReadyBatchLocked",
                new Class<?>[0]
        );
    }

    @SuppressWarnings("unchecked")
    private static List<Object> collectStagedOrdinaryBatch(SessionRuntime runtime) throws Exception {
        Object readyBatch = SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "collectReadyBatchStateLocked",
                new Class<?>[]{boolean.class, boolean.class},
                false,
                true
        );
        return (List<Object>) SessionRuntimeTestSupport.invokePrivate(readyBatch, "frames", new Class<?>[0]);
    }

    @SuppressWarnings("unchecked")
    private static List<Object> filterWritableBatch(SessionRuntime runtime, List<Object> batch) throws Exception {
        java.lang.reflect.Field writerRuntimeField = SessionRuntime.class.getDeclaredField("writerRuntime");
        writerRuntimeField.setAccessible(true);
        Object writerRuntime = writerRuntimeField.get(runtime);
        return (List<Object>) SessionRuntimeTestSupport.invokePrivate(
                writerRuntime,
                "filterWritableBatchLocked",
                new Class<?>[]{List.class},
                batch
        );
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void setStagedBatch(SessionRuntime runtime, List<Object> batch) {
        runtime.setStagedOrdinaryBatchInternal((List) batch);
    }

    private static String describe(List<?> batch) throws Exception {
        StringBuilder out = new StringBuilder("[");
        for (Object outbound : batch) {
            FrameCodec.Frame frame = SessionRuntimeTestSupport.outboundFrame(outbound);
            if (out.length() > 1) {
                out.append(", ");
            }
            out.append(frame.type()).append('(').append(frame.streamId()).append(')');
            if (SessionRuntimeTestSupport.outboundOpeningFrame(outbound)) {
                out.append("*");
            }
        }
        return out.append(']').toString();
    }

    private static SessionRuntime.OutboundFrame dataFrame(StreamRuntime stream, boolean opening) {
        return new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.DATA, 0, stream.streamIdInternal(), new byte[]{1}),
                stream,
                1,
                opening,
                false
        );
    }

    @Test
    void ordinaryReorderingNeverPutsHigherOpenerAheadOfLowerOpener() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime first = (StreamRuntime) runtime.openStream();
        StreamRuntime second = (StreamRuntime) runtime.openStream();
        synchronized (runtime.lock()) {
            runtime.beginLocalOpenLocked(first);
            runtime.beginLocalOpenLocked(second);
            SessionRuntime.OutboundFrame sessionMaxData = new SessionRuntime.OutboundFrame(
                    new FrameCodec.Frame(FrameType.MAX_DATA, 0, 0L, Varint62.encode(1L)),
                    null,
                    0,
                    false,
                    false
            );
            // As a priority scheduler could emit them: the later stream first, followed by its next DATA.
            List<SessionRuntime.OutboundFrame> batch = new ArrayList<>(java.util.Arrays.asList(
                    dataFrame(second, true),
                    dataFrame(second, false),
                    sessionMaxData,
                    dataFrame(first, true),
                    dataFrame(first, false)
            ));

            SessionWriterBatchOrderer.keepOpeningFramesInStreamIdOrder(batch);

            assertEquals(
                    "[MAX_DATA(0), DATA(1)*, DATA(5)*, DATA(5), DATA(1)]",
                    describe(batch),
                    "the higher opener and its stream's later frames should wait for the lower opener, nothing else moves"
            );
        }
    }

    @Test
    void urgentOpenerPullsLowerQueuedOpenersAheadOfIt() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime first = (StreamRuntime) runtime.openStream();
        StreamRuntime second = (StreamRuntime) runtime.openStream();
        SessionRuntimeTestSupport.queueWrite(first, "a".getBytes());
        SessionRuntimeTestSupport.queueWrite(second, "b".getBytes());

        // RESET needs the second stream's opener on the urgent path while the first opener is still queued.
        second.cancelWrite(ErrorCode.CANCELLED.code());

        synchronized (runtime.lock()) {
            List<Object> batch = collectReadyBatch(runtime);
            assertEquals(
                    "[DATA(1)*, DATA(5)*, RESET(5)]",
                    describe(batch),
                    "the lower queued opener must reach the wire before the urgent higher opener"
            );
            assertTrue(runtime.dataQueueInternal().isEmpty(), "the lower opener should have been pulled from the data queue");
        }
    }

    @Test
    void cancelWriteDuringCoalescingWaitKeepsStagedOpenerAheadOfReset() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        SessionRuntimeTestSupport.queueWrite(stream, "x".getBytes());

        List<Object> staged;
        synchronized (runtime.lock()) {
            staged = collectStagedOrdinaryBatch(runtime);
            assertEquals("[DATA(1)*]", describe(staged), "test requires the opener to be staged by the writer");
            // What the writer publishes while it releases the monitor to coalesce.
            setStagedBatch(runtime, staged);
        }

        stream.cancelWrite(ErrorCode.CANCELLED.code());

        synchronized (runtime.lock()) {
            assertTrue(stream.openingFramePendingLocked(), "the staged opener should still count as pending");
            Deque<Object> urgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
            assertEquals("[RESET(1)]", describe(new ArrayList<>(urgentQueue)), "cancelWrite should queue RESET without a duplicate opener");

            setStagedBatch(runtime, null);
            List<Object> filtered = filterWritableBatch(runtime, staged);
            assertEquals("[DATA(1)*]", describe(filtered), "the staged opener must still open the stream ahead of RESET");
            assertEquals(0, SessionRuntimeTestSupport.outboundPayload(filtered.get(0)).length, "the reset write's payload must not be sent");
            assertEquals(0, SessionRuntimeTestSupport.outboundDataBytes(filtered.get(0)), "the zero-length opener carries no flow-controlled bytes");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "sessionQueuedDataBytes"), "dropped payload should release queued-data accounting");
            assertEquals(0L, stream.reservedSendBytes(), "dropped payload should release send credit");
        }
    }

    @Test
    void abortedStagedOpenerStillOpensBeforeLaterStagedOpener() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime first = (StreamRuntime) runtime.openStream();
        StreamRuntime second = (StreamRuntime) runtime.openStream();
        SessionRuntimeTestSupport.queueWrite(first, "a".getBytes());
        SessionRuntimeTestSupport.queueWrite(second, "b".getBytes());

        List<Object> staged;
        synchronized (runtime.lock()) {
            staged = collectStagedOrdinaryBatch(runtime);
            assertEquals("[DATA(1)*, DATA(5)*]", describe(staged), "test requires both openers to be staged");
            setStagedBatch(runtime, staged);
        }

        first.closeWithError(ErrorCode.CANCELLED.code(), "");

        synchronized (runtime.lock()) {
            setStagedBatch(runtime, null);
            List<Object> filtered = filterWritableBatch(runtime, staged);
            assertEquals(
                    "[DATA(1)*, DATA(5)*]",
                    describe(filtered),
                    "stream 1 must open before stream 5 even though its ABORT is only queued afterwards"
            );
            assertEquals(0, SessionRuntimeTestSupport.outboundPayload(filtered.get(0)).length, "the aborted payload must not be sent");
            Deque<Object> urgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
            assertEquals("[ABORT(1)*]", describe(new ArrayList<>(urgentQueue)), "the ABORT still follows");
        }
    }
}
