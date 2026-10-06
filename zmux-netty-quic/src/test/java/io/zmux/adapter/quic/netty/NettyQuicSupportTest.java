package io.zmux.adapter.quic.netty;

import io.netty.handler.codec.quic.QuicClosedChannelException;
import io.netty.handler.codec.quic.QuicConnectionCloseEvent;
import io.netty.handler.codec.quic.QuicException;
import io.netty.handler.codec.quic.QuicTransportError;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static io.zmux.adapter.quic.netty.TestLists.listOf;
import static org.junit.jupiter.api.Assertions.*;

class NettyQuicSupportTest {
    private static ZmuxErrorDetails requireDetails(Throwable error) {
        ZmuxErrorDetails details = ZmuxErrors.details(error);
        assertNotNull(details);
        return details;
    }

    private static QuicConnectionCloseEvent newCloseEvent(boolean applicationClose, int error, byte[] reason) throws Exception {
        Constructor<QuicConnectionCloseEvent> constructor = QuicConnectionCloseEvent.class.getDeclaredConstructor(
                boolean.class,
                int.class,
                byte[].class
        );
        constructor.setAccessible(true);
        return constructor.newInstance(applicationClose, error, reason);
    }

    private static QuicClosedChannelException newClosedChannelException(QuicConnectionCloseEvent event) throws Exception {
        Constructor<QuicClosedChannelException> constructor = QuicClosedChannelException.class.getDeclaredConstructor(
                QuicConnectionCloseEvent.class
        );
        constructor.setAccessible(true);
        return constructor.newInstance(event);
    }

    @Test
    void acceptedPreludeTimeoutDefaultsForNullAndZero() {
        assertEquals(
                NettyQuic.DEFAULT_ACCEPTED_PRELUDE_READ_TIMEOUT,
                NettyQuicSupport.normalizeAcceptedPreludeReadTimeout(null)
        );
        assertEquals(
                NettyQuic.DEFAULT_ACCEPTED_PRELUDE_READ_TIMEOUT,
                NettyQuicSupport.normalizeAcceptedPreludeReadTimeout(Duration.ZERO)
        );
    }

    @Test
    void acceptedPreludeTimeoutNegativeDisablesAdapterTimeout() {
        assertNull(NettyQuicSupport.normalizeAcceptedPreludeReadTimeout(Duration.ofMillis(-1)));
    }

    @Test
    void acceptedPreludeConcurrencyFallsBackToSharedDefault() {
        assertEquals(NettyQuic.defaultAcceptedPreludeMaxConcurrent(), NettyQuicSupport.normalizeAcceptedPreludeMaxConcurrent(0));
        assertEquals(3, NettyQuicSupport.normalizeAcceptedPreludeMaxConcurrent(3));

        int previousDefault = NettyQuic.defaultAcceptedPreludeMaxConcurrent();
        try {
            NettyQuic.setDefaultAcceptedPreludeMaxConcurrent(NettyQuic.MAX_ACCEPTED_PRELUDE_MAX_CONCURRENT + 1);
            assertEquals(
                    NettyQuic.MAX_ACCEPTED_PRELUDE_MAX_CONCURRENT,
                    NettyQuic.defaultAcceptedPreludeMaxConcurrent()
            );
            assertEquals(
                    NettyQuic.MAX_ACCEPTED_PRELUDE_MAX_CONCURRENT,
                    NettyQuicSupport.normalizeAcceptedPreludeMaxConcurrent(NettyQuic.MAX_ACCEPTED_PRELUDE_MAX_CONCURRENT + 1)
            );
        } finally {
            NettyQuic.setDefaultAcceptedPreludeMaxConcurrent(previousDefault);
        }
    }

    @Test
    void sessionOptionsFactoriesAndWithersPreserveOtherFields() {
        NettyQuicSessionOptions timeoutOnly = NettyQuicSessionOptions.ofAcceptedPreludeReadTimeout(Duration.ofMillis(25));
        assertEquals(Duration.ofMillis(25), timeoutOnly.acceptedPreludeReadTimeout());
        assertEquals(0, timeoutOnly.acceptedPreludeMaxConcurrent());

        NettyQuicSessionOptions concurrencyOnly = NettyQuicSessionOptions.ofAcceptedPreludeMaxConcurrent(7);
        assertEquals(Duration.ZERO, concurrencyOnly.acceptedPreludeReadTimeout());
        assertEquals(7, concurrencyOnly.acceptedPreludeMaxConcurrent());

        NettyQuicSessionOptions updated = timeoutOnly.withAcceptedPreludeMaxConcurrent(3);
        assertEquals(Duration.ofMillis(25), updated.acceptedPreludeReadTimeout());
        assertEquals(3, updated.acceptedPreludeMaxConcurrent());
        assertEquals(0, timeoutOnly.acceptedPreludeMaxConcurrent(), "withers must keep the original options immutable");
    }

    @Test
    void wrapSessionConvenienceOverloadsPreserveClosedSafeNullWrapper() throws Exception {
        ZmuxSession timeoutSession = NettyQuic.wrapSession(null, Duration.ofMillis(25));
        ZmuxSession concurrencySession = NettyQuic.wrapSession(null, 3);
        ZmuxSession combinedSession = NettyQuic.wrapSession(null, Duration.ofMillis(25), 3);
        ZmuxSession optionsSession = NettyQuic.wrapSessionWithOptions(
                null,
                NettyQuicSessionOptions.defaults()
                        .withAcceptedPreludeReadTimeout(Duration.ofMillis(25))
                        .withAcceptedPreludeMaxConcurrent(3)
        );

        assertSame(Zmux.closedSession(), timeoutSession);
        assertTrue(timeoutSession.isClosed());
        assertEquals(SessionState.INVALID, timeoutSession.state());
        assertEquals(SessionState.INVALID, timeoutSession.stats().state());
        assertTrue(timeoutSession.awaitTermination(Duration.ofMillis(1)));
        timeoutSession.close();

        assertSame(Zmux.closedSession(), concurrencySession);
        assertTrue(concurrencySession.isClosed());
        assertEquals(SessionState.INVALID, concurrencySession.state());
        assertTrue(concurrencySession.awaitTermination(Duration.ofMillis(1)));
        concurrencySession.close();

        assertSame(Zmux.closedSession(), combinedSession);
        assertTrue(combinedSession.isClosed());
        assertEquals(SessionState.INVALID, combinedSession.state());
        assertTrue(combinedSession.awaitTermination(Duration.ofMillis(1)));
        combinedSession.close();

        assertSame(Zmux.closedSession(), optionsSession);
        assertTrue(optionsSession.isClosed());
        assertEquals(SessionState.INVALID, optionsSession.state());
        assertTrue(optionsSession.awaitTermination(Duration.ofMillis(1)));
        optionsSession.close();
    }

    @Test
    void acceptedPreludePendingCapacityIsBounded() {
        assertEquals(
                NettyQuicSupport.ACCEPT_PRELUDE_PENDING_MIN_CAPACITY,
                NettyQuicSupport.acceptedPreludePendingCapacity(1)
        );
        assertEquals(128, NettyQuicSupport.acceptedPreludePendingCapacity(32));
        assertEquals(
                NettyQuicSupport.ACCEPT_PRELUDE_PENDING_MAX_CAPACITY,
                NettyQuicSupport.acceptedPreludePendingCapacity(Integer.MAX_VALUE)
        );
    }

    @Test
    void positiveIntegerPropertyClampsOversizedConfiguration() {
        String name = "io.zmux.test.boundedIntegerProperty";
        String previous = System.getProperty(name);
        try {
            System.setProperty(name, "999999");
            assertEquals(64, NettyQuicSupport.positiveIntegerProperty(name, 8, 64));

            System.setProperty(name, "-1");
            assertEquals(8, NettyQuicSupport.positiveIntegerProperty(name, 8, 64));

            System.setProperty(name, "not-an-integer");
            assertEquals(8, NettyQuicSupport.positiveIntegerProperty(name, 8, 64));
        } finally {
            if (previous == null) {
                System.clearProperty(name);
            } else {
                System.setProperty(name, previous);
            }
        }
    }

    @Test
    void elapsedHelpersPreserveNanoTimeWrapAndClampBackwardsClock() {
        assertEquals(21L, NettyQuicSupport.elapsedNanos(Long.MIN_VALUE + 10L, Long.MAX_VALUE - 10L));
        assertEquals(Long.MAX_VALUE, NettyQuicSupport.elapsedNanos(Long.MAX_VALUE, Long.MIN_VALUE + 1L));
        assertEquals(0L, NettyQuicSupport.elapsedNanos(100L, 200L));
        assertEquals(1L, NettyQuicSupport.positiveElapsedNanos(7L, 7L));
    }

    @Test
    void saturatingMulDivFloorAvoidsRateOverflow() {
        assertEquals(2L, NettyQuicSupport.saturatingMulDivFloor(5L, 1_000_000_000L, 2_000_000_000L));
        assertEquals(Long.MAX_VALUE, NettyQuicSupport.saturatingMulDivFloor(Long.MAX_VALUE, 1_000_000_000L, 1L));
    }

    @Test
    void sessionAtomicCountersSaturate() throws Exception {
        java.lang.reflect.Method method = NettyQuicSession.class.getDeclaredMethod("saturatingIncrement", AtomicLong.class);
        method.setAccessible(true);
        AtomicLong counter = new AtomicLong(Long.MAX_VALUE);
        method.invoke(null, counter);
        assertEquals(Long.MAX_VALUE, counter.get());

        counter.set(Long.MAX_VALUE - 1L);
        method.invoke(null, counter);
        assertEquals(Long.MAX_VALUE, counter.get());
    }

    @Test
    void supportAtomicCountersSaturate() {
        AtomicLong counter = new AtomicLong(Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, NettyQuicSupport.saturatingIncrement(counter));
        assertEquals(Long.MAX_VALUE, counter.get());

        counter.set(Long.MAX_VALUE - 1L);
        assertEquals(Long.MAX_VALUE, NettyQuicSupport.saturatingIncrement(counter));
        assertEquals(Long.MAX_VALUE, counter.get());
    }

    @Test
    void sessionReasonCountersBoundDistinctCodesAndCountOverflow() throws Exception {
        java.lang.reflect.Method method = NettyQuicSession.class.getDeclaredMethod(
                "noteReasonLocked",
                ConcurrentHashMap.class,
                AtomicLong.class,
                long.class
        );
        method.setAccessible(true);
        ConcurrentHashMap<Long, AtomicLong> counters = new ConcurrentHashMap<>();
        AtomicLong overflow = new AtomicLong();
        int trackedCodes = SessionStats.ReasonStats.MAX_TRACKED_CODES;

        for (int i = 0; i < trackedCodes + 5; i++) {
            method.invoke(null, counters, overflow, 30_000L + i);
        }
        method.invoke(null, counters, overflow, 30_000L);

        assertEquals(trackedCodes, counters.size());
        assertEquals(5L, overflow.get());
        assertEquals(2L, counters.get(30_000L).get());
    }

    @Test
    void acceptQueueCloseWithDiscardHandlesEmptyQueueWithoutCallbacks() {
        NettyQuicSupport.AcceptQueue<Integer> queue = new NettyQuicSupport.AcceptQueue<>(2);
        ArrayList<Integer> discarded = new ArrayList<>();

        queue.close(discarded::add);

        assertTrue(discarded.isEmpty());
        assertEquals(0, queue.size());
    }

    @Test
    void acceptQueueCloseWithDiscardDrainsSingleQueuedValue() throws Exception {
        NettyQuicSupport.AcceptQueue<Integer> queue = new NettyQuicSupport.AcceptQueue<>(2);
        queue.put(7);
        ArrayList<Integer> discarded = new ArrayList<>();

        queue.close(discarded::add);

        assertEquals(listOf(7), discarded);
        assertEquals(0, queue.size());
    }

    @Test
    void acceptQueueCloseWithDiscardPreservesFifoOrder() throws Exception {
        NettyQuicSupport.AcceptQueue<Integer> queue = new NettyQuicSupport.AcceptQueue<>(4);
        queue.put(1);
        queue.put(2);
        queue.put(3);
        ArrayList<Integer> discarded = new ArrayList<>();

        queue.close(discarded::add);

        assertEquals(listOf(1, 2, 3), discarded);
        assertEquals(0, queue.size());
    }

    @Test
    void acceptQueueCloseWithoutDiscardStillAllowsQueuedDrain() throws Exception {
        NettyQuicSupport.AcceptQueue<Integer> queue = new NettyQuicSupport.AcceptQueue<>(2);
        queue.put(11);

        queue.close();

        assertEquals(11, queue.take(Duration.ZERO, null));
        assertThrows(SessionClosedException.class, () -> queue.take(Duration.ZERO, null));
    }

    @Test
    void remoteTransportClosePreservesStructuredSessionMetadata() throws Exception {
        ZmuxException error = assertInstanceOf(
                ZmuxException.class,
                NettyQuicSupport.translateThrowable(
                        newClosedChannelException(newCloseEvent(false, (int) QuicTransportError.PROTOCOL_VIOLATION.code(), "bad".getBytes()))
                )
        );
        ZmuxErrorDetails details = requireDetails(error);
        assertFalse(details.hasCode(), "QUIC transport codes must not occupy the zmux code slot");
        assertNull(ZmuxErrors.code(error));
        assertEquals(io.zmux.ZmuxErrorScope.SESSION, details.scope());
        assertEquals(io.zmux.ZmuxErrorSource.REMOTE, details.source());
        assertEquals(io.zmux.ZmuxErrorDirection.BOTH, details.direction());
        assertEquals(io.zmux.ZmuxTerminationKind.SESSION_TERMINATION, details.terminationKind());
        assertEquals("QUIC transport error PROTOCOL_VIOLATION (0xa): bad", error.getMessage());
    }

    @Test
    void remoteTransportCloseWithNullNettyReasonUsesErrorLabel() throws Exception {
        ZmuxException error = assertInstanceOf(
                ZmuxException.class,
                NettyQuicSupport.connectionCloseError(
                        newCloseEvent(false, (int) QuicTransportError.PROTOCOL_VIOLATION.code(), null),
                        null
                )
        );

        assertEquals(-1L, error.code());
        assertFalse(ZmuxErrors.hasCode(error));
        assertEquals("QUIC transport error PROTOCOL_VIOLATION (0xa)", error.getMessage());
    }

    @Test
    void gracefulApplicationCloseAllowsNullNettyReason() throws Exception {
        QuicConnectionCloseEvent closeEvent = newCloseEvent(true, 0, null);

        assertTrue(NettyQuicSupport.isGracefulApplicationClose(closeEvent));
        assertNull(NettyQuicSupport.applicationCloseError(closeEvent));
    }

    @Test
    void quicTransportExceptionPreservesStructuredTransportMetadata() {
        ZmuxException error = assertInstanceOf(
                ZmuxException.class,
                NettyQuicSupport.translateThrowable(new QuicException(QuicTransportError.FLOW_CONTROL_ERROR))
        );

        ZmuxErrorDetails details = requireDetails(error);
        assertFalse(details.hasCode(), "QUIC transport codes must not occupy the zmux code slot");
        assertFalse(ZmuxErrors.isCode(error, ErrorCode.STREAM_LIMIT));
        assertEquals(io.zmux.ZmuxErrorScope.SESSION, details.scope());
        assertEquals(io.zmux.ZmuxErrorSource.TRANSPORT, details.source());
        assertEquals(io.zmux.ZmuxErrorDirection.BOTH, details.direction());
        assertEquals(io.zmux.ZmuxTerminationKind.SESSION_TERMINATION, details.terminationKind());
        assertTrue(error.getMessage().contains("FLOW_CONTROL_ERROR"));
    }

    @Test
    void quicTransportCloseCodesDoNotAliasZmuxErrorCodes() throws Exception {
        // QUIC INTERNAL_ERROR(0x1) / FLOW_CONTROL_ERROR(0x3) / PROTOCOL_VIOLATION(0xa) share numbers with
        // zmux PROTOCOL / STREAM_LIMIT / FRAME_SIZE; TLS alerts (0x100+) look like application codes.
        Object[][] cases = {
                {QuicTransportError.INTERNAL_ERROR.code(), ErrorCode.PROTOCOL, "INTERNAL_ERROR (0x1)"},
                {QuicTransportError.FLOW_CONTROL_ERROR.code(), ErrorCode.STREAM_LIMIT, "FLOW_CONTROL_ERROR (0x3)"},
                {QuicTransportError.PROTOCOL_VIOLATION.code(), ErrorCode.FRAME_SIZE, "PROTOCOL_VIOLATION (0xa)"},
                {0x128L, null, "CRYPTO_ERROR (0x128)"},
        };
        for (Object[] testCase : cases) {
            long quicCode = (Long) testCase[0];
            ErrorCode aliased = (ErrorCode) testCase[1];
            String label = (String) testCase[2];
            for (IOException error : listOf(
                    NettyQuicSupport.connectionCloseError(newCloseEvent(false, (int) quicCode, null), null),
                    NettyQuicSupport.translateThrowable(newClosedChannelException(newCloseEvent(false, (int) quicCode, null)))
            )) {
                ZmuxException transport = assertInstanceOf(ZmuxException.class, error, label);
                assertFalse(ZmuxErrors.hasCode(transport), label);
                assertNull(ZmuxErrors.code(transport), label);
                assertEquals(-1L, ZmuxErrors.code(transport, -1L), label);
                if (aliased != null) {
                    assertFalse(ZmuxErrors.isCode(transport, aliased), label);
                }
                assertNull(ZmuxErrors.applicationError(transport), label);
                assertEquals(ZmuxErrorSource.REMOTE, transport.source(), label);
                assertEquals(ZmuxErrorScope.SESSION, transport.scope(), label);
                assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, transport.terminationKind(), label);
                assertTrue(transport.getMessage().contains(label), transport.getMessage());
            }
        }

        ApplicationError application = assertInstanceOf(
                ApplicationError.class,
                NettyQuicSupport.connectionCloseError(newCloseEvent(true, 0x128, null), null)
        );
        assertEquals(0x128L, application.code(), "peer application close codes keep the zmux code slot");
    }

    @Test
    void quicTransportExceptionCodesDoNotAliasZmuxErrorCodes() {
        for (QuicTransportError transportError : listOf(
                QuicTransportError.INTERNAL_ERROR,
                QuicTransportError.FLOW_CONTROL_ERROR,
                QuicTransportError.PROTOCOL_VIOLATION
        )) {
            IOException error = NettyQuicSupport.translateThrowable(new QuicException(transportError));

            assertFalse(ZmuxErrors.hasCode(error), transportError.name());
            assertNull(ZmuxErrors.code(error), transportError.name());
            assertNull(ZmuxErrors.applicationError(error), transportError.name());
            assertEquals(ZmuxErrorSource.TRANSPORT, ZmuxErrors.source(error));
            assertTrue(error.getMessage().contains(transportError.name()), error.getMessage());
        }
    }

    @Test
    void sessionOperationErrorKeepsCodeLessTransportErrorsCodeLess() throws Exception {
        IOException transport = NettyQuicSupport.connectionCloseError(
                newCloseEvent(false, (int) QuicTransportError.INTERNAL_ERROR.code(), "peer-bug".getBytes()),
                null
        );

        IOException read = NettyQuicSupport.sessionOperationError("read", transport);

        assertFalse(ZmuxErrors.hasCode(read), "a code-less transport close must not be rewrapped as zmux INTERNAL");
        assertNull(ZmuxErrors.code(read));
        assertEquals("read", ZmuxErrors.operation(read));
        assertEquals(ZmuxErrorSource.REMOTE, ZmuxErrors.source(read));
        assertTrue(ZmuxErrors.reason(read).contains("peer-bug"));

        IOException coded = NettyQuicSupport.sessionOperationError(
                "write",
                NettyQuicSupport.translateThrowable(new RuntimeException("adapter-runtime-failure"))
        );
        assertEquals(ErrorCode.INTERNAL, ZmuxErrors.code(coded));
    }

    @Test
    void quicApplicationCodeIsLimitedToNonNegativeJavaInt() throws Exception {
        assertEquals(0, NettyQuicSupport.requireQuicApplicationCode(0L, "write", ZmuxErrorScope.STREAM, ZmuxErrorDirection.WRITE));
        assertEquals(
                Integer.MAX_VALUE,
                NettyQuicSupport.requireQuicApplicationCode(0x7fff_ffffL, "write", ZmuxErrorScope.STREAM, ZmuxErrorDirection.WRITE)
        );
        // Netty sign-extends the int it is given, so 2^31.. would reach quiche above varint62 and abort the JVM.
        for (long code : new long[]{-1L, 0x8000_0000L, 0xffff_ffffL, 0x1_0000_0000L, Long.MAX_VALUE}) {
            AdapterUnsupportedException error = assertThrows(
                    AdapterUnsupportedException.class,
                    () -> NettyQuicSupport.requireQuicApplicationCode(code, "close", ZmuxErrorScope.SESSION, ZmuxErrorDirection.BOTH),
                    Long.toHexString(code)
            );
            assertTrue(error.getMessage().contains("31-bit"), error.getMessage());
            assertEquals("close", error.operation());
            assertEquals(ZmuxErrorScope.SESSION, error.scope());
            assertEquals(ZmuxErrorDirection.BOTH, error.direction());
        }
    }

    @Test
    void nestedStructuredIOExceptionIsUnwrappedFromRuntimeWrapper() {
        ApplicationError nested = new ApplicationError(
                77L,
                "nested",
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.REMOTE,
                ZmuxErrorDirection.WRITE,
                ZmuxTerminationKind.RESET
        );

        ApplicationError translated = assertInstanceOf(
                ApplicationError.class,
                NettyQuicSupport.translateThrowable(new RuntimeException("wrapper", nested))
        );

        assertEquals(77L, translated.code());
        assertEquals("nested", translated.reason());
        assertEquals(ZmuxErrorSource.REMOTE, translated.source());
        assertEquals(ZmuxErrorDirection.WRITE, translated.direction());
        assertEquals(ZmuxTerminationKind.RESET, translated.terminationKind());
    }

    @Test
    void suppressedStructuredIOExceptionIsUnwrappedFromRuntimeWrapper() {
        ApplicationError nested = new ApplicationError(
                79L,
                "suppressed-nested",
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.REMOTE,
                ZmuxErrorDirection.READ,
                ZmuxTerminationKind.ABORT
        );
        RuntimeException wrapper = new RuntimeException("wrapper");
        wrapper.addSuppressed(nested);

        ApplicationError translated = assertInstanceOf(
                ApplicationError.class,
                NettyQuicSupport.translateThrowable(wrapper)
        );

        assertEquals(79L, translated.code());
        assertEquals("suppressed-nested", translated.reason());
        assertEquals(ZmuxErrorSource.REMOTE, translated.source());
        assertEquals(ZmuxErrorDirection.READ, translated.direction());
        assertEquals(ZmuxTerminationKind.ABORT, translated.terminationKind());
    }

    @Test
    void plainClosedChannelExceptionFallsBackToStructuredSessionClose() {
        SessionClosedException error = assertInstanceOf(
                SessionClosedException.class,
                NettyQuicSupport.translateThrowable(new ClosedChannelException())
        );

        ZmuxErrorDetails details = requireDetails(error);
        assertEquals(ZmuxErrorScope.SESSION, details.scope());
        assertEquals(ZmuxErrorSource.TRANSPORT, details.source());
        assertEquals(ZmuxErrorDirection.BOTH, details.direction());
        assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, details.terminationKind());
    }

    @Test
    void nullTransportFailureFallsBackToTransportSessionClose() {
        SessionClosedException error = assertInstanceOf(
                SessionClosedException.class,
                NettyQuicSupport.translateThrowable(null)
        );

        ZmuxErrorDetails details = requireDetails(error);
        assertEquals(ZmuxErrorScope.SESSION, details.scope());
        assertEquals(ZmuxErrorSource.TRANSPORT, details.source());
        assertEquals(ZmuxErrorDirection.BOTH, details.direction());
        assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, details.terminationKind());
    }

    @Test
    void missingConnectionCloseEventFallsBackToTransportSessionClose() {
        SessionClosedException error = assertInstanceOf(
                SessionClosedException.class,
                NettyQuicSupport.connectionCloseError(null, null)
        );

        ZmuxErrorDetails details = requireDetails(error);
        assertEquals(ZmuxErrorScope.SESSION, details.scope());
        assertEquals(ZmuxErrorSource.TRANSPORT, details.source());
        assertEquals(ZmuxErrorDirection.BOTH, details.direction());
        assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, details.terminationKind());
    }

    @Test
    void cancelledTransportFailureFallsBackToStructuredSessionClose() {
        SessionClosedException error = assertInstanceOf(
                SessionClosedException.class,
                NettyQuicSupport.translateThrowable(new CancellationException("cancelled"))
        );

        ZmuxErrorDetails details = requireDetails(error);
        assertEquals(ZmuxErrorScope.SESSION, details.scope());
        assertEquals(ZmuxErrorSource.TRANSPORT, details.source());
        assertEquals(ZmuxErrorDirection.BOTH, details.direction());
        assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, details.terminationKind());
    }

    @Test
    void unknownRuntimeTransportFailureKeepsStructuredSessionMetadata() {
        ZmuxException error = assertInstanceOf(
                ZmuxException.class,
                NettyQuicSupport.translateThrowable(new RuntimeException("adapter-runtime-failure"))
        );

        ZmuxErrorDetails details = requireDetails(error);
        assertEquals(io.zmux.ErrorCode.INTERNAL.code(), details.code());
        assertEquals(ZmuxErrorScope.SESSION, details.scope());
        assertEquals(ZmuxErrorSource.TRANSPORT, details.source());
        assertEquals(ZmuxErrorDirection.BOTH, details.direction());
        assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, details.terminationKind());
        assertEquals("adapter-runtime-failure", error.getMessage());
    }

    @Test
    void cyclicRuntimeCauseTranslatesAsBoundedTransportFailure() {
        ZmuxException error = assertInstanceOf(
                ZmuxException.class,
                NettyQuicSupport.translateThrowable(new SelfCauseRuntimeException("cyclic-runtime"))
        );

        ZmuxErrorDetails details = requireDetails(error);
        assertEquals(io.zmux.ErrorCode.INTERNAL.code(), details.code());
        assertEquals(ZmuxErrorSource.TRANSPORT, details.source());
        assertEquals("cyclic-runtime", error.getMessage());
    }

    @Test
    void cyclicNestedIoCauseTranslatesAsBoundedTransportFailure() {
        IOException first = new IOException("first-io");
        IOException second = new IOException("second-io");
        first.initCause(second);
        second.initCause(first);

        ZmuxException error = assertInstanceOf(
                ZmuxException.class,
                NettyQuicSupport.translateThrowable(first)
        );

        ZmuxErrorDetails details = requireDetails(error);
        assertEquals(io.zmux.ErrorCode.INTERNAL.code(), details.code());
        assertEquals(ZmuxErrorSource.TRANSPORT, details.source());
    }

    @Test
    void failedFutureAwaitUsesUncheckedIoWithStructuredCause() {
        DefaultPromise<Void> promise = new DefaultPromise<>(ImmediateEventExecutor.INSTANCE);
        promise.setFailure(new QuicException(QuicTransportError.STREAM_LIMIT_ERROR));

        UncheckedIOException error = assertThrows(
                UncheckedIOException.class,
                () -> NettyQuicSupport.awaitFutureUninterruptibly(promise)
        );

        OpenLimitedException cause = assertInstanceOf(OpenLimitedException.class, error.getCause());
        assertTrue(error.getMessage().contains("Netty operation failed"));
        assertEquals(ZmuxErrorScope.SESSION, cause.scope());
        assertEquals(ZmuxErrorSource.TRANSPORT, cause.source());
        assertEquals(ZmuxErrorDirection.BOTH, cause.direction());
    }

    private static final class SelfCauseRuntimeException extends RuntimeException {
        private SelfCauseRuntimeException(String message) {
            super(message);
        }

        @Override
        public synchronized Throwable getCause() {
            return this;
        }
    }
}
