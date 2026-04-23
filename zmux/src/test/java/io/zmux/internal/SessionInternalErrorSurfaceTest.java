package io.zmux.internal;

import io.zmux.ErrorCode;
import io.zmux.Settings;
import io.zmux.ZmuxException;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;

import static org.junit.jupiter.api.Assertions.*;

final class SessionInternalErrorSurfaceTest {
    @Test
    void encodeVarintPreservesStructuredCauseInsideUncheckedWrapper() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        InvocationTargetException error = assertInstanceOf(
                InvocationTargetException.class,
                org.junit.jupiter.api.Assertions.assertThrows(
                        InvocationTargetException.class,
                        () -> SessionRuntimeTestSupport.invokePrivate(
                                runtime,
                                "encodeVarint",
                                new Class<?>[]{long.class},
                                -1L
                        )
                )
        );

        UncheckedIOException unchecked = assertInstanceOf(UncheckedIOException.class, error.getCause());
        assertTrue(unchecked.getMessage().contains("encode control varint"));
        ZmuxException cause = assertInstanceOf(ZmuxException.class, unchecked.getCause());
        assertEquals(ErrorCode.PROTOCOL.code(), cause.code());
        assertEquals("varint length", cause.operation());
        assertTrue(cause.getMessage().contains("varint62 value out of range"));
    }

    @Test
    void pendingControlValueBytesPreservesStructuredCauseInsideUncheckedWrapper() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        InvocationTargetException error = assertInstanceOf(
                InvocationTargetException.class,
                org.junit.jupiter.api.Assertions.assertThrows(
                        InvocationTargetException.class,
                        () -> SessionRuntimeTestSupport.invokePrivate(
                                runtime,
                                "pendingControlValueBytesLocked",
                                new Class<?>[]{long.class},
                                -1L
                        )
                )
        );

        UncheckedIOException unchecked = assertInstanceOf(UncheckedIOException.class, error.getCause());
        assertTrue(unchecked.getMessage().contains("compute pending control value bytes"));
        ZmuxException cause = assertInstanceOf(ZmuxException.class, unchecked.getCause());
        assertEquals(ErrorCode.PROTOCOL.code(), cause.code());
        assertEquals("varint length", cause.operation());
    }
}
