package io.zmux.runtime;

import io.zmux.Settings;
import io.zmux.protocol.Frame;
import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;
import io.zmux.protocol.Protocol;
import io.zmux.protocol.Varint62;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class PriorityUpdateReaderRuntimeTest {
    private static StreamRuntime newTerminalLocalStream(SessionRuntime runtime) throws Exception {
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        SessionRuntimeTestSupport.queueWrite(stream, "body".getBytes(StandardCharsets.UTF_8));
        stream.closeWithError(41L, "");
        synchronized (runtime.lock()) {
            assertTrue(stream.fullyTerminalLocked(), "closeWithError should fully terminate the stream for this test");
        }
        return stream;
    }

    private static void handleExtFrame(SessionRuntime runtime, FrameCodec.Frame frame) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField("readerRuntime");
        field.setAccessible(true);
        Object readerRuntime = field.get(runtime);
        SessionRuntimeTestSupport.invokePrivate(
                readerRuntime,
                "handleExtFrame",
                new Class<?>[]{FrameCodec.Frame.class},
                frame
        );
    }

    private static int getIntField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getInt(target);
    }

    private static byte[] duplicatePriorityUpdatePayload() throws Exception {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        Varint62.write(payload, Protocol.EXT_PRIORITY_UPDATE);
        FrameCodec.appendTlv(payload, Protocol.METADATA_STREAM_PRIORITY, Varint62.encode(3L));
        FrameCodec.appendTlv(payload, Protocol.METADATA_STREAM_PRIORITY, Varint62.encode(7L));
        return payload.toByteArray();
    }

    private static byte[] malformedPriorityUpdatePayload() throws Exception {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        Varint62.write(payload, Protocol.EXT_PRIORITY_UPDATE);
        payload.write((int) Protocol.METADATA_STREAM_PRIORITY);
        return payload.toByteArray();
    }

    private static long priorityCapabilities() {
        return Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS;
    }

    @Test
    void duplicatePriorityUpdateOnTerminalStreamCountsAsDroppedNotNoOp() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(priorityCapabilities(), Settings.defaults());
        StreamRuntime stream = newTerminalLocalStream(runtime);

        handleExtFrame(runtime, new FrameCodec.Frame(
                FrameType.EXT,
                0,
                stream.streamIdInternal(),
                duplicatePriorityUpdatePayload()
        ));

        synchronized (runtime.lock()) {
            assertTrue(stream.fullyTerminalLocked(), "test requires the stream to stay terminal");
            assertEquals(1L, SessionRuntimeTestSupport.getLongField(runtime, "droppedPriorityUpdateCount"), "duplicate PRIORITY_UPDATE should count as dropped");
            assertEquals(0, getIntField(runtime, "noOpPriorityUpdateCount"), "duplicate PRIORITY_UPDATE should no longer be misclassified as no-op on a terminal stream");
        }
    }

    @Test
    void unknownExtSubtypeOnTerminalStreamSkipsPriorityAccounting() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(priorityCapabilities(), Settings.defaults());
        StreamRuntime stream = newTerminalLocalStream(runtime);

        handleExtFrame(runtime, new FrameCodec.Frame(
                FrameType.EXT,
                0,
                stream.streamIdInternal(),
                Varint62.encode(99L)
        ));

        synchronized (runtime.lock()) {
            assertTrue(stream.fullyTerminalLocked(), "test requires the stream to stay terminal");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "droppedPriorityUpdateCount"), "unknown EXT subtype must not be counted as dropped PRIORITY_UPDATE");
            assertEquals(0, getIntField(runtime, "noOpPriorityUpdateCount"), "unknown EXT subtype must bypass PRIORITY_UPDATE no-op accounting");
        }
    }

    @Test
    void malformedExtSubtypeIsRejectedWithoutPriorityCapability() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        InvocationTargetException error = assertThrows(
                InvocationTargetException.class,
                () -> handleExtFrame(runtime, new FrameCodec.Frame(
                        FrameType.EXT,
                        0,
                        4L,
                        new byte[]{0x40}
                ))
        );

        assertInstanceOf(IOException.class, error.getCause(), "malformed EXT subtype must still be parsed before capability-specific ignore");
    }

    @Test
    void directMalformedPriorityUpdateIsIgnoredAfterCloseStart() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(priorityCapabilities(), Settings.defaults());

        synchronized (runtime.lock()) {
            runtime.setCloseFrameQueuedInternal(true);
        }

        handleExtFrame(runtime, new FrameCodec.Frame(
                FrameType.EXT,
                0,
                4L,
                malformedPriorityUpdatePayload()
        ));

        synchronized (runtime.lock()) {
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "droppedPriorityUpdateCount"), "closing direct handler should ignore malformed PRIORITY_UPDATE without diagnostics side effects");
            assertEquals(0, getIntField(runtime, "noOpPriorityUpdateCount"), "closing direct handler should not count ignored PRIORITY_UPDATE as no-op");
        }
    }
}
