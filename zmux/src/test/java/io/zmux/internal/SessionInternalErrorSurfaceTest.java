package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

final class SessionInternalErrorSurfaceTest {
    private static void awaitState(SessionRuntime runtime, SessionState expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
        while (System.nanoTime() < deadline) {
            if (runtime.state() == expected) {
                return;
            }
            Thread.sleep(10L);
        }
        assertEquals(expected, runtime.state());
    }

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

    @Test
    void asyncSessionFailureRunsOnSessionProtocolWorker() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        IOException failure = new ZmuxException(
                ErrorCode.INTERNAL.code(),
                "late data",
                "synthetic async failure",
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        );

        runtime.failSessionAsync(failure);

        awaitState(runtime, SessionState.FAILED);
        IOException cause = runtime.terminationCause().orElseThrow(AssertionError::new);
        assertEquals(ErrorCode.INTERNAL.code(), ZmuxErrors.code(cause, -1L));
        assertEquals("late data", ZmuxErrors.operation(cause));
        assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, ZmuxErrors.terminationKind(cause));
    }
}
