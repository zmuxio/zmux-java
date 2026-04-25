package io.zmux.internal;

import io.zmux.OpenMetadataTooLargeException;
import io.zmux.Protocol;
import io.zmux.Settings;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

final class CloseWriteRuntimeTest {
    @Test
    void closeWriteKeepsLocalWriteOpenWhenOpeningMetadataValidationFails() throws Exception {
        Settings peerSettings = Settings.defaults().toBuilder()
                .maxFramePayload(16L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(Protocol.CAPABILITY_OPEN_METADATA, peerSettings);
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "beginLocalOpenLocked",
                    new Class<?>[]{StreamRuntime.class},
                    stream
            );
            stream.applyOpenMetadataLocked(0L, null, "oversized-open-info".getBytes(StandardCharsets.UTF_8));
        }

        OpenMetadataTooLargeException error = assertInstanceOf(
                OpenMetadataTooLargeException.class,
                assertThrows(IOException.class, stream::closeWrite, "oversized opener metadata should fail local CloseWrite"),
                "CloseWrite open-metadata validation failure should surface a typed protocol error"
        );
        assertEquals("write", error.operation(), "CloseWrite open-metadata failure operation mismatch");

        synchronized (runtime.lock()) {
            assertFalse(stream.writeClosed(), "CloseWrite failure must not commit local write closure");
            assertTrue(stream.halfStateInternal().sendOpen(), "CloseWrite failure must leave the send side open");
            assertFalse(stream.openedOnWire(), "failed opener validation must not commit local stream visibility");
            assertFalse(stream.peerVisible(), "failed opener validation must not mark the stream peer-visible");
            assertFalse(stream.openingFramePendingLocked(), "failed opener validation must not leave an opening frame pending");
            assertTrue(SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue").isEmpty(), "CloseWrite failure must not queue urgent frames");
            assertTrue(SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue").isEmpty(), "CloseWrite failure must not queue DATA frames");
        }
    }

    @Test
    void writeKeepsLocalWriteOpenWhenOpeningMetadataValidationFails() throws Exception {
        Settings peerSettings = Settings.defaults().toBuilder()
                .maxFramePayload(16L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(Protocol.CAPABILITY_OPEN_METADATA, peerSettings);
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "beginLocalOpenLocked",
                    new Class<?>[]{StreamRuntime.class},
                    stream
            );
            stream.applyOpenMetadataLocked(0L, null, "oversized-open-info".getBytes(StandardCharsets.UTF_8));
        }

        OpenMetadataTooLargeException error = assertInstanceOf(
                OpenMetadataTooLargeException.class,
                assertThrows(IOException.class, () -> stream.write("x".getBytes(StandardCharsets.UTF_8))),
                "oversized opener metadata should fail local write before queueing DATA"
        );
        assertEquals("write", error.operation(), "write open-metadata failure operation mismatch");

        synchronized (runtime.lock()) {
            assertFalse(stream.writeClosed(), "write failure must not close the local write side");
            assertTrue(stream.halfStateInternal().sendOpen(), "write failure must leave the send side open");
            assertFalse(stream.openedOnWire(), "failed opener validation must not commit local stream visibility");
            assertFalse(stream.peerVisible(), "failed opener validation must not mark the stream peer-visible");
            assertFalse(stream.openingFramePendingLocked(), "failed opener validation must not leave an opening frame pending");
            assertTrue(SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue").isEmpty(), "write failure must not queue urgent frames");
            assertTrue(SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue").isEmpty(), "write failure must not queue DATA frames");
        }
    }
}
