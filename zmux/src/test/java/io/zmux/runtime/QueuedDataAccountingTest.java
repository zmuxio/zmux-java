package io.zmux.runtime;

import io.zmux.Settings;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class QueuedDataAccountingTest {
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
    private static List<Object> collectReadyBatch(SessionRuntime runtime) throws Exception {
        return (List<Object>) SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "collectReadyBatchLocked",
                new Class<?>[0]
        );
    }

    @Test
    void successfulDataWriteReleasesQueuedBytesOnlyOnce() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        byte[] payload = "body".getBytes(StandardCharsets.UTF_8);

        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
        }
        SessionRuntimeTestSupport.queueWrite(stream, payload);

        synchronized (runtime.lock()) {
            List<Object> batch = collectReadyBatch(runtime);
            assertEquals(1, batch.size(), "test requires exactly one queued DATA frame");

            Object outbound = batch.get(0);
            int dataBytes = SessionRuntimeTestSupport.outboundDataBytes(outbound);
            assertEquals(payload.length, dataBytes, "queued DATA frame size mismatch");

            // Synthetic tail keeps one frame queued after completion.
            stream.reserveQueuedDataBytesLocked(dataBytes);
            stream.reserveSendBytesLocked(dataBytes);
            SessionRuntimeTestSupport.setLongField(
                    runtime,
                    "sessionQueuedDataBytes",
                    SessionRuntimeTestSupport.getLongField(runtime, "sessionQueuedDataBytes") + dataBytes
            );
            SessionRuntimeTestSupport.setLongField(
                    runtime,
                    "sessionReservedSendBytes",
                    SessionRuntimeTestSupport.getLongField(runtime, "sessionReservedSendBytes") + dataBytes
            );

            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "trackQueuedDataRemovedLocked",
                    new Class<?>[]{outbound.getClass()},
                    outbound
            );
            runtime.onDataFrameWrittenLocked(stream, dataBytes);

            assertEquals(dataBytes, stream.queuedDataBytesLocked(), "successful DATA write should release only the written queued bytes");
            assertEquals(dataBytes, SessionRuntimeTestSupport.getLongField(runtime, "sessionQueuedDataBytes"), "session queued bytes should retain the unwritten tail");
            assertEquals(dataBytes, stream.reservedSendBytes(), "reserved send bytes should retain the unwritten tail");
            assertEquals(dataBytes, SessionRuntimeTestSupport.getLongField(runtime, "sessionReservedSendBytes"), "session reserved bytes should retain the unwritten tail");
            assertEquals(dataBytes, stream.sentBytes(), "stream sent-byte accounting mismatch");
        }
    }
}
