package io.zmux.internal;

import io.zmux.ErrorCode;
import io.zmux.FrameType;
import io.zmux.Settings;
import io.zmux.ZmuxErrorSource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

final class CloseReaderRuntimeTest {
    private static void handleCloseFrame(SessionRuntime runtime, FrameCodec.Frame frame) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField("readerRuntime");
        field.setAccessible(true);
        Object readerRuntime = field.get(runtime);
        try {
            SessionRuntimeTestSupport.invokePrivate(
                    readerRuntime,
                    "handleCloseFrame",
                    new Class<?>[]{FrameCodec.Frame.class},
                    frame
            );
        } catch (InvocationTargetException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw exception;
        }
    }

    private static int getIntField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getInt(target);
    }

    @Test
    void duplicateMalformedCloseIsIgnoredBeforeParseAndAccounting() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        byte[] closePayload = FrameCodec.buildErrorPayload(
                ErrorCode.PROTOCOL.code(),
                "peer-close",
                Settings.defaults().maxControlPayloadBytes()
        );

        handleCloseFrame(runtime, new FrameCodec.Frame(FrameType.CLOSE, 0, 0L, closePayload));
        assertNotNull(runtime.peerCloseError(), "initial peer CLOSE should be retained");
        assertEquals(ErrorCode.PROTOCOL.code(), runtime.peerCloseError().code(), "initial peer close code mismatch");
        assertEquals("peer-close", runtime.peerCloseError().reason(), "initial peer close reason mismatch");
        assertEquals(ZmuxErrorSource.REMOTE, runtime.peerCloseError().source(), "initial peer close source mismatch");

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setIntField(runtime, "inboundControlFrameCount", 7);
            SessionRuntimeTestSupport.setIntField(runtime, "inboundMixedFrameCount", 9);
            SessionRuntimeTestSupport.setIntField(runtime, "noOpControlCount", 11);
        }

        handleCloseFrame(runtime, new FrameCodec.Frame(FrameType.CLOSE, 0, 0L, new byte[]{(byte) 0xff}));

        assertEquals(ErrorCode.PROTOCOL.code(), runtime.peerCloseError().code(), "duplicate malformed CLOSE must preserve the first peer close");
        assertEquals("peer-close", runtime.peerCloseError().reason(), "duplicate malformed CLOSE must preserve the first close reason");
        synchronized (runtime.lock()) {
            assertEquals(7, getIntField(runtime, "inboundControlFrameCount"), "ignored duplicate CLOSE must not alter control budget");
            assertEquals(9, getIntField(runtime, "inboundMixedFrameCount"), "ignored duplicate CLOSE must not alter mixed budget");
            assertEquals(11, getIntField(runtime, "noOpControlCount"), "ignored duplicate CLOSE must not alter no-op budget");
        }
    }
}
