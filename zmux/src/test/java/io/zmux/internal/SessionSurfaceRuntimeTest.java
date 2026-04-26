package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class SessionSurfaceRuntimeTest {
    private static final long SPLIT_MIX_GAMMA = -7046029254386353131L;

    private static long splitMix64(long state) {
        long z = state + SPLIT_MIX_GAMMA;
        z = (z ^ (z >>> 30)) * -4658895280553007687L;
        z = (z ^ (z >>> 27)) * -7723592293110705685L;
        return z ^ (z >>> 31);
    }

    private static long readBigEndianLong(byte[] payload) {
        long value = 0L;
        for (int i = 0; i < Long.BYTES; i++) {
            value = (value << 8) | (payload[i] & 0xffL);
        }
        return value;
    }

    private static boolean blockedOnOpenTurn(Thread thread) {
        Thread.State state = thread.getState();
        return state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING;
    }

    private static StreamRuntime createPeerOpenedBidi(SessionRuntime runtime, long streamId) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField("readerRuntime");
        field.setAccessible(true);
        Object readerRuntime = field.get(runtime);
        synchronized (runtime.lock()) {
            return (StreamRuntime) SessionRuntimeTestSupport.invokePrivate(
                    readerRuntime,
                    "createPeerOpenedStreamLocked",
                    new Class<?>[]{long.class},
                    streamId
            );
        }
    }

    private static void awaitBlockedThread(Thread thread, String message) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        while (System.nanoTime() < deadlineNanos) {
            Thread.State state = thread.getState();
            if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
                return;
            }
            Thread.sleep(10L);
        }
        fail(message);
    }

    private static void failRuntimeWithPeerClose(SessionRuntime runtime, long code, String reason) throws Exception {
        SessionRuntimeTestSupport.setField(
                runtime,
                "peerCloseError",
                new ApplicationError(
                        code,
                        reason,
                        ZmuxErrorScope.SESSION,
                        ZmuxErrorSource.REMOTE,
                        ZmuxErrorDirection.BOTH,
                        ZmuxTerminationKind.SESSION_TERMINATION
                )
        );
        SessionRuntimeTestSupport.setField(runtime, "state", SessionState.FAILED);
    }

    @Test
    void stateAndStatsStayInvalidBeforeReady() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newRuntime(ZmuxConfig.builder().build());

        assertEquals(SessionState.INVALID, runtime.state(), "unestablished runtime must not expose an internal establishing state");
        assertEquals(SessionState.INVALID, runtime.stats().state(), "stats() should mirror the public invalid state before readiness");
    }

    @Test
    void peerReasonUtf8LengthMatchesJdkEncodingWithoutAllocatingForAccounting() throws Exception {
        String[] values = {
                "",
                "ascii",
                "\u00e9",
                "\u20ac",
                "\uD83D\uDE00",
                "\uD800",
                "\uDC00",
                "a\uD83D\uDE00\uD800z"
        };
        for (String value : values) {
            assertEquals(
                    value.getBytes(StandardCharsets.UTF_8).length,
                    SessionRuntime.utf8EncodedLength(value),
                    "UTF-8 accounting length mismatch for " + value
            );
        }
    }

    @Test
    void peerReasonRetentionTrimsMultibyteReasonsOnUtf8Boundary() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                ZmuxConfig.builder().retainedPeerReasonBytesBudget(3L).build(),
                0L,
                Settings.defaults()
        );

        String retained;
        synchronized (runtime.lock()) {
            retained = runtime.retainPeerReasonLocked(0L, "\u20acx");
        }

        assertEquals("\u20ac", retained, "retained peer reason should keep only complete UTF-8 code points");
        assertEquals(3L, runtime.stats().retainedPeerReasonBytes(), "retained peer reason byte accounting mismatch");
    }

    @Test
    void sessionRuntimeWrapsRawInputStreamInStatefulCodecDecoder() throws Exception {
        InputStream rawInput = new InputStream() {
            @Override
            public int read() {
                return -1;
            }
        };
        SessionRuntime runtime = SessionRuntimeTestSupport.newRuntime(
                new BasicDuplexConnection(rawInput, SessionRuntimeTestSupport.discardingOutput()),
                ZmuxConfig.builder().build()
        );

        Field field = SessionRuntime.class.getDeclaredField("input");
        field.setAccessible(true);

        assertInstanceOf(
                FrameCodec.Decoder.class,
                field.get(runtime),
                "session runtime should wrap connection input in a stateful codec decoder before preface/frame reads"
        );
    }

    @Test
    void acceptAndPingTimeoutsUseTypedLocalExceptions() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        assertThrows(AcceptTimeoutException.class, () -> runtime.acceptStream(Duration.ofMillis(20)));
        assertThrows(AcceptTimeoutException.class, () -> runtime.acceptStream(Duration.ZERO));
        assertThrows(
                PingTimeoutException.class,
                () -> runtime.ping("timeout".getBytes(StandardCharsets.UTF_8), Duration.ofMillis(20))
        );
    }

    @Test
    void expiredOpenTimeoutFailsBeforeTrackingProvisionalStreams() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        assertThrows(OpenTimeoutException.class, () -> runtime.openStreamWithTimeout(Duration.ZERO));
        assertThrows(OpenTimeoutException.class, () -> runtime.openUniStreamWithTimeout(Duration.ZERO));

        SessionStats stats = runtime.stats();
        assertEquals(0, stats.provisionals().bidi(), "expired bidi open timeout should not allocate a provisional stream");
        assertEquals(0, stats.provisionals().uni(), "expired uni open timeout should not allocate a provisional stream");
        assertEquals(0L, stats.provisionals().limited(), "expired open timeout should not count as open-limit pressure");
    }

    @Test
    void zeroPingTimeoutFailsBeforeQueueingOutstandingPing() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        assertThrows(
                PingTimeoutException.class,
                () -> runtime.ping("timeout".getBytes(StandardCharsets.UTF_8), Duration.ZERO)
        );

        SessionStats stats = runtime.stats();
        assertFalse(stats.keepalive().pingOutstanding(), "expired ping timeout should not leave an active ping behind");
        assertEquals(0L, stats.pressure().outstandingPingBytes(), "expired ping timeout should not pin ping payload bytes");
    }

    @Test
    void pingInterruptionClearsOutstandingSlotAndSurfacesTypedError() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                runtime.ping("interrupt".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(5));
            } catch (Throwable throwable) {
                error.set(throwable);
            }
        }, "session-ping-interrupt");

        waiter.start();
        awaitBlockedThread(waiter, "ping should block while waiting for a response");
        waiter.interrupt();
        waiter.join(Duration.ofSeconds(1).toMillis());

        assertFalse(waiter.isAlive(), "interrupted ping waiter should exit promptly");
        ZmuxInterruptedException interrupted = assertInstanceOf(
                ZmuxInterruptedException.class,
                error.get(),
                "ping interruption should surface the typed local interrupted error"
        );
        assertEquals("ping", interrupted.operation(), "ping interruption operation mismatch");
        assertEquals(ZmuxErrorScope.SESSION, interrupted.scope(), "ping interruption scope mismatch");
        assertEquals(ZmuxErrorSource.LOCAL, interrupted.source(), "ping interruption source mismatch");
        assertEquals(ZmuxErrorDirection.BOTH, interrupted.direction(), "ping interruption direction mismatch");
        assertEquals(ZmuxTerminationKind.INTERRUPTED, interrupted.terminationKind(), "ping interruption termination mismatch");

        SessionStats stats = runtime.stats();
        assertFalse(stats.keepalive().pingOutstanding(), "interrupted ping must release its outstanding slot");
        assertEquals(0L, stats.pressure().outstandingPingBytes(), "interrupted ping must release retained ping bytes");
    }

    @Test
    void pingNonceUsesSplitMixStateAndKeepsEchoSuffix() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setLongField(runtime, "pingNonceState", 1L);

            byte[] first = runtime.buildPingPayloadLocked(new byte[]{9});
            byte[] second = runtime.buildPingPayloadLocked(new byte[]{8});

            assertEquals(splitMix64(1L), readBigEndianLong(first), "first PING nonce should match Go SplitMix64 state advance");
            assertEquals(splitMix64(1L + SPLIT_MIX_GAMMA), readBigEndianLong(second), "second PING nonce should advance from retained state");
            assertEquals(9, first[8], "PING echo suffix must remain after the nonce prefix");
            assertEquals(8, second[8], "PING echo suffix must remain after the nonce prefix");
        }
    }

    @Test
    void acceptWithHugeTimeoutOnClosedSessionSurfacesSessionError() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        SessionRuntimeTestSupport.setField(runtime, "state", SessionState.CLOSED);

        assertThrows(
                IOException.class,
                () -> runtime.acceptStream(Duration.ofSeconds(Long.MAX_VALUE))
        );
    }

    @Test
    void localCloseStartRejectsAcceptPingAndDeadlineUpdatesBeforeTermination() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime, SessionRuntime.firstPeerStreamId(io.zmux.Role.RESPONDER, true));
        AtomicReference<Throwable> acceptFailure = new AtomicReference<>();
        Thread acceptor = new Thread(() -> {
            try {
                runtime.acceptStream();
            } catch (Throwable throwable) {
                acceptFailure.set(throwable);
            }
        }, "session-close-start-accept");
        acceptor.start();

        awaitBlockedThread(acceptor, "accept waiter should block before local close starts");
        runtime.closeWithError(ErrorCode.NO_ERROR.code(), "");

        acceptor.join(Duration.ofSeconds(1).toMillis());
        assertFalse(acceptor.isAlive(), "accept waiter should wake once the local close frame has been queued");
        SessionClosedException acceptError = assertInstanceOf(
                SessionClosedException.class,
                acceptFailure.get(),
                "accept waiter should surface a typed local session-close error once close starts"
        );
        assertEquals(ZmuxErrorSource.LOCAL, acceptError.source(), "accept waiter close source mismatch");
        assertEquals(SessionState.CLOSING, runtime.state(), "close start should leave the session in CLOSING until the close frame flushes");

        SessionClosedException pingError = assertInstanceOf(
                SessionClosedException.class,
                assertThrows(IOException.class, () -> runtime.ping("probe".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(1))),
                "ping should fail immediately once local close has started"
        );
        assertEquals(ZmuxErrorSource.LOCAL, pingError.source(), "ping close source mismatch");

        SessionClosedException goAwayError = assertInstanceOf(
                SessionClosedException.class,
                assertThrows(IOException.class, () -> runtime.goAway(0L, 0L, ErrorCode.NO_ERROR.code(), "")),
                "GOAWAY should fail immediately once local close has started"
        );
        assertEquals(ZmuxErrorSource.LOCAL, goAwayError.source(), "GOAWAY close source mismatch");
        assertEquals("goAway", goAwayError.operation(), "GOAWAY close operation mismatch");

        SessionClosedException readDeadlineError = assertInstanceOf(
                SessionClosedException.class,
                assertThrows(IOException.class, () -> stream.setReadDeadline(java.time.Instant.now())),
                "read deadline updates should fail once local close has started"
        );
        assertEquals(ZmuxErrorSource.LOCAL, readDeadlineError.source(), "setReadDeadline close source mismatch");

        SessionClosedException writeDeadlineError = assertInstanceOf(
                SessionClosedException.class,
                assertThrows(IOException.class, () -> stream.setWriteDeadline(java.time.Instant.now())),
                "write deadline updates should fail once local close has started"
        );
        assertEquals(ZmuxErrorSource.LOCAL, writeDeadlineError.source(), "setWriteDeadline close source mismatch");
    }

    @Test
    void localCloseStartUnblocksBlockedReadAndWriteWaitersBeforeTermination() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        long firstPeerBidi = SessionRuntime.firstPeerStreamId(io.zmux.Role.RESPONDER, true);
        StreamRuntime readStream = createPeerOpenedBidi(runtime, firstPeerBidi);
        StreamRuntime writeStream = createPeerOpenedBidi(runtime, firstPeerBidi + 4L);
        AtomicReference<Object> readOutcome = new AtomicReference<>();
        AtomicReference<Throwable> writeFailure = new AtomicReference<>();

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setLongField(runtime, "sessionSendLimit", 0L);
        }

        Thread reader = new Thread(() -> {
            try {
                readOutcome.set(readStream.read(new byte[1]));
            } catch (Throwable throwable) {
                readOutcome.set(throwable);
            }
        }, "session-close-start-read");
        Thread writer = new Thread(() -> {
            try {
                writeStream.write("x".getBytes(StandardCharsets.UTF_8));
            } catch (Throwable throwable) {
                writeFailure.set(throwable);
            }
        }, "session-close-start-write");
        reader.start();
        writer.start();

        awaitBlockedThread(reader, "read waiter should block before local close starts");
        awaitBlockedThread(writer, "write waiter should block before local close starts");
        runtime.closeWithError(ErrorCode.NO_ERROR.code(), "");

        reader.join(Duration.ofSeconds(1).toMillis());
        writer.join(Duration.ofSeconds(1).toMillis());
        assertFalse(reader.isAlive(), "blocked read should wake once the local close frame has been queued");
        assertFalse(writer.isAlive(), "blocked write should wake once the local close frame has been queued");

        Object readResult = readOutcome.get();
        if (readResult instanceof Throwable) {
            Throwable throwable = (Throwable) readResult;
            SessionClosedException readError = assertInstanceOf(
                    SessionClosedException.class,
                    throwable,
                    "blocked read failure should surface a typed local session-close error once close starts"
            );
            assertEquals(ZmuxErrorSource.LOCAL, readError.source(), "blocked read close source mismatch");
        } else {
            assertEquals(-1, readResult, "graceful local close may also release a blocked read as EOF, but it must not report payload bytes");
        }
        SessionClosedException writeError = assertInstanceOf(
                SessionClosedException.class,
                writeFailure.get(),
                "blocked write should surface a typed local session-close error once close starts"
        );
        assertEquals(ZmuxErrorSource.LOCAL, writeError.source(), "blocked write close source mismatch");
    }

    @Test
    void localCloseStartRejectsDirectStreamOperationsWithoutMutatingState() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime, SessionRuntime.firstPeerStreamId(io.zmux.Role.RESPONDER, true));

        runtime.closeWithError(ErrorCode.NO_ERROR.code(), "");

        StreamHalfState.SendState sendStateAfterCloseStart;
        StreamHalfState.RecvState recvStateAfterCloseStart;
        IOException localErrorAfterCloseStart;
        synchronized (runtime.lock()) {
            sendStateAfterCloseStart = stream.halfStateInternal().sendState();
            recvStateAfterCloseStart = stream.halfStateInternal().recvState();
            localErrorAfterCloseStart = stream.terminalStateInternal().localError();
        }

        SessionClosedException writeError = assertInstanceOf(
                SessionClosedException.class,
                assertThrows(IOException.class, () -> stream.write("x".getBytes(StandardCharsets.UTF_8))),
                "write should fail immediately once local close has started"
        );
        assertEquals(ZmuxErrorSource.LOCAL, writeError.source(), "write close source mismatch");

        SessionClosedException closeReadError = assertInstanceOf(
                SessionClosedException.class,
                assertThrows(IOException.class, stream::closeRead),
                "closeRead should fail immediately once local close has started"
        );
        assertEquals(ZmuxErrorSource.LOCAL, closeReadError.source(), "closeRead close source mismatch");

        SessionClosedException abortError = assertInstanceOf(
                SessionClosedException.class,
                assertThrows(IOException.class, () -> stream.closeWithError(ErrorCode.CANCELLED.code(), "local abort")),
                "closeWithError should fail immediately once local close has started"
        );
        assertEquals(ZmuxErrorSource.LOCAL, abortError.source(), "closeWithError close source mismatch");

        SessionClosedException closeError = assertInstanceOf(
                SessionClosedException.class,
                assertThrows(IOException.class, stream::close),
                "stream.close should fail immediately once local close has started"
        );
        assertEquals(ZmuxErrorSource.LOCAL, closeError.source(), "stream.close close source mismatch");

        synchronized (runtime.lock()) {
            assertEquals(sendStateAfterCloseStart, stream.halfStateInternal().sendState(), "rejected stream operations must not mutate send half-state beyond the session-close baseline");
            assertEquals(recvStateAfterCloseStart, stream.halfStateInternal().recvState(), "rejected stream operations must not mutate recv half-state beyond the session-close baseline");
            assertSame(localErrorAfterCloseStart, stream.terminalStateInternal().localError(), "rejected stream operations must not stash a new local terminal error");
        }
    }

    @Test
    void gracefulDrainRejectsNewLocalOpensBeforeTerminal() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setField(runtime, "gracefulCloseActive", true);
            SessionRuntimeTestSupport.setField(runtime, "state", SessionState.DRAINING);
        }

        SessionClosedException bidiOpenError = assertInstanceOf(
                SessionClosedException.class,
                assertThrows(IOException.class, runtime::openStream, "graceful drain should reject new bidirectional opens"),
                "bidirectional local open should fail with a typed session-close error while draining"
        );
        assertEquals(ZmuxErrorSource.LOCAL, bidiOpenError.source(), "bidirectional local open close source mismatch");

        SessionClosedException uniOpenError = assertInstanceOf(
                SessionClosedException.class,
                assertThrows(IOException.class, runtime::openUniStream, "graceful drain should reject new unidirectional opens"),
                "unidirectional local open should fail with a typed session-close error while draining"
        );
        assertEquals(ZmuxErrorSource.LOCAL, uniOpenError.source(), "unidirectional local open close source mismatch");
    }

    @Test
    void gracefulDrainRejectsBlockedProvisionalCommitWaiterAndReclaimsSlot() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime first = (StreamRuntime) runtime.openStream();
        StreamRuntime second = (StreamRuntime) runtime.openStream();
        byte[] payload = "blocked-open".getBytes(StandardCharsets.UTF_8);
        AtomicReference<Object> outcome = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            try {
                second.write(payload);
                outcome.set(Boolean.TRUE);
            } catch (Throwable failure) {
                outcome.set(failure);
            }
        }, "session-open-turn-graceful-drain");
        writer.start();

        long waitDeadlineNanos = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < waitDeadlineNanos) {
            if (blockedOnOpenTurn(writer) && runtime.stats().provisionals().bidi() == 2) {
                break;
            }
            Thread.sleep(10L);
        }
        assertTrue(blockedOnOpenTurn(writer), "second local open should block behind the provisional head before graceful drain starts");

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setField(runtime, "gracefulCloseActive", true);
            SessionRuntimeTestSupport.setField(runtime, "state", SessionState.DRAINING);
            runtime.lock().notifyAll();
        }

        writer.join(Duration.ofSeconds(1).toMillis());
        assertFalse(writer.isAlive(), "blocked provisional commit waiter should exit promptly once graceful drain starts");

        ApplicationError refused = assertInstanceOf(
                ApplicationError.class,
                outcome.get(),
                "graceful drain should reject a blocked provisional commit waiter with the stored stream-local failure"
        );
        assertEquals(ErrorCode.REFUSED_STREAM.code(), refused.code(), "graceful-drain provisional refusal code mismatch");
        assertEquals("", refused.reason(), "graceful-drain provisional refusal reason mismatch");
        assertEquals(ZmuxErrorScope.STREAM, refused.scope(), "graceful-drain provisional refusal scope mismatch");
        assertEquals(ZmuxErrorSource.LOCAL, refused.source(), "graceful-drain provisional refusal source mismatch");
        assertEquals(ZmuxErrorDirection.BOTH, refused.direction(), "graceful-drain provisional refusal direction mismatch");
        assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, refused.terminationKind(), "graceful-drain provisional refusal termination mismatch");
        assertEquals(1, runtime.stats().provisionals().bidi(), "rejected provisional commit waiter should release its provisional slot immediately");

        ApplicationError repeated = assertInstanceOf(
                ApplicationError.class,
                assertThrows(IOException.class, () -> second.write(payload)),
                "rejected provisional stream should retain the graceful-drain refusal"
        );
        assertEquals(ErrorCode.REFUSED_STREAM.code(), repeated.code(), "stored graceful-drain refusal code mismatch");
        assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, repeated.terminationKind(), "stored graceful-drain refusal termination mismatch");

        synchronized (runtime.lock()) {
            assertTrue(first.provisionalTracked(), "the earlier provisional head should remain tracked until graceful-close reclaim handles it");
            assertFalse(second.provisionalTracked(), "the rejected provisional waiter should no longer stay tracked");
        }
    }

    @Test
    void statsExposeExpandedRuntimeSnapshot() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(io.zmux.Role.RESPONDER)
                .keepaliveInterval(Duration.ofSeconds(2))
                .keepaliveMaxPingInterval(Duration.ofSeconds(9))
                .keepaliveTimeout(Duration.ofSeconds(6))
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());

        synchronized (runtime.lock()) {
            long nowNanos = System.nanoTime();
            SessionRuntimeTestSupport.setLongField(runtime, "sessionQueuedDataBytes", 123L);
            SessionRuntimeTestSupport.setLongField(runtime, "sessionReservedSendBytes", 17L);
            SessionRuntimeTestSupport.setLongField(runtime, "urgentQueuedControlBytes", 11L);
            SessionRuntimeTestSupport.setLongField(runtime, "ordinaryQueuedControlBytes", 13L);
            SessionRuntimeTestSupport.setLongField(runtime, "pendingControlBytes", 19L);
            SessionRuntimeTestSupport.setLongField(runtime, "pendingPriorityBytes", 23L);
            SessionRuntimeTestSupport.setLongField(runtime, "writerHeldRetainedBytes", 29L);
            SessionRuntimeTestSupport.setLongField(runtime, "bufferedReceiveBytes", 31L);
            SessionRuntimeTestSupport.setLongField(runtime, "bufferedReceiveStorageBytes", 31L);
            SessionRuntimeTestSupport.setLongField(runtime, "recvSessionAdvertised", 100L);
            SessionRuntimeTestSupport.setLongField(runtime, "recvSessionReceivedBytes", 45L);
            SessionRuntimeTestSupport.setLongField(runtime, "recvSessionPending", 7L);
            SessionRuntimeTestSupport.setLongField(runtime, "retainedOpenInfoBytes", 5L);
            SessionRuntimeTestSupport.setLongField(runtime, "retainedPeerReasonBytes", 6L);
            SessionRuntimeTestSupport.setLongField(runtime, "lastInboundFrameAtNanos", nowNanos - 7L);
            SessionRuntimeTestSupport.setLongField(runtime, "lastControlProgressAtNanos", nowNanos - 6L);
            SessionRuntimeTestSupport.setLongField(runtime, "lastTransportWriteAtNanos", nowNanos - 5L);
            SessionRuntimeTestSupport.setLongField(runtime, "lastStreamProgressAtNanos", nowNanos - 4L);
            SessionRuntimeTestSupport.setLongField(runtime, "lastApplicationProgressAtNanos", nowNanos - 3L);
            SessionRuntimeTestSupport.setLongField(runtime, "lastPingSentAtNanos", nowNanos - Duration.ofSeconds(4).toNanos());
            SessionRuntimeTestSupport.setLongField(runtime, "lastPongAtNanos", nowNanos - 2L);
            SessionRuntimeTestSupport.setLongField(runtime, "lastPingRttNanos", 41L);
            SessionRuntimeTestSupport.setLongField(runtime, "sendRateEstimateBytesPerSecond", 2_000L);
            SessionRuntimeTestSupport.setLongField(runtime, "flushCount", 7L);
            SessionRuntimeTestSupport.setLongField(runtime, "lastFlushAtNanos", nowNanos - 1L);
            SessionRuntimeTestSupport.setIntField(runtime, "lastFlushFrames", 3);
            SessionRuntimeTestSupport.setLongField(runtime, "lastFlushBytes", 4_096L);
            SessionRuntimeTestSupport.setLongField(runtime, "blockedWriteTotalNanos", 55L);
            SessionRuntimeTestSupport.setLongField(runtime, "lastOpenLatencyNanos", 77L);
            SessionRuntimeTestSupport.setField(
                    runtime,
                    "activePing",
                    SessionRuntimeTestSupport.newPendingPing(
                            System.nanoTime() - Duration.ofSeconds(4).toNanos(),
                            new byte[]{1, 2, 3}
                    )
            );
        }

        SessionStats stats = runtime.stats();

        assertEquals(SessionState.READY, stats.state(), "ready runtime should still report READY");
        assertTrue(stats.keepalive().enabled(), "configured keepalive interval should surface as enabled");
        assertEquals(Duration.ofSeconds(2).toNanos(), stats.keepalive().intervalNanos(), "keepalive interval mismatch");
        assertEquals(Duration.ofSeconds(9).toNanos(), stats.keepalive().maxPingIntervalNanos(), "keepalive max ping interval mismatch");
        assertEquals(Duration.ofSeconds(6).toNanos(), stats.keepalive().timeoutNanos(), "keepalive timeout mismatch");
        assertTrue(stats.keepalive().pingOutstanding(), "active ping should be visible in stats");
        assertTrue(stats.keepalive().pingStalled(), "half-timeout overrun should mark the ping stalled");
        assertEquals(2_000L, stats.keepalive().sendRateEstimateBytesPerSecond(), "send-rate estimate mismatch");
        assertNotNull(stats.progress().inboundFrameAt(), "last inbound frame timestamp should be surfaced");
        assertNotNull(stats.progress().controlProgressAt(), "control progress timestamp should be surfaced");
        assertNotNull(stats.progress().transportWriteAt(), "transport write timestamp should be surfaced");
        assertNotNull(stats.progress().streamProgressAt(), "stream progress timestamp should be surfaced");
        assertNotNull(stats.progress().applicationProgressAt(), "application progress timestamp should be surfaced");
        assertNotNull(stats.progress().pingSentAt(), "ping sent timestamp should be surfaced");
        assertNotNull(stats.progress().pongAt(), "pong timestamp should be surfaced");
        assertEquals(7L, stats.flush().count(), "flush count mismatch");
        assertNotNull(stats.flush().lastAt(), "last flush timestamp should be surfaced");
        assertEquals(3, stats.flush().lastFrames(), "last flush frame count mismatch");
        assertEquals(4_096L, stats.flush().lastBytes(), "last flush byte count mismatch");
        assertEquals(55L, stats.blockedWriteTotalNanos(), "blocked write total mismatch");
        assertEquals(77L, stats.lastOpenLatencyNanos(), "last open latency mismatch");

        assertEquals(123L, stats.queues().queuedDataBytes(), "queued data bytes mismatch");
        assertEquals(17L, stats.queues().reservedSendBytes(), "reserved send bytes mismatch");
        assertEquals(11L, stats.queues().urgentQueuedControlBytes(), "urgent queued control bytes mismatch");
        assertEquals(13L, stats.queues().ordinaryQueuedControlBytes(), "ordinary queued control bytes mismatch");
        assertEquals(19L, stats.queues().pendingControlBytes(), "pending control bytes mismatch");
        assertEquals(23L, stats.queues().pendingPriorityBytes(), "pending priority bytes mismatch");
        assertEquals(29L, stats.queues().writerHeldRetainedBytes(), "writer-held retained bytes mismatch");

        assertEquals(32, stats.provisionals().softCap(), "provisional soft cap should expose the current runtime default");
        assertEquals(64, stats.provisionals().hardCap(), "provisional hard cap should expose the current runtime default");
        assertEquals(Duration.ofSeconds(5).toNanos(), stats.provisionals().maxAgeNanos(), "provisional max age mismatch");
        assertEquals(0L, stats.provisionals().limited(), "synthetic snapshot should not report provisional-limit events");
        assertEquals(0L, stats.provisionals().expired(), "synthetic snapshot should not report provisional-expiry events");
        assertEquals(32, stats.hiddenState().softCap(), "hidden-state soft cap mismatch");
        assertEquals(64, stats.hiddenState().hardCap(), "hidden-state hard cap mismatch");
        assertTrue(stats.reasons().reset().isEmpty(), "synthetic snapshot should start with empty reset reasons");
        assertTrue(stats.reasons().abort().isEmpty(), "synthetic snapshot should start with empty abort reasons");
        assertEquals(0L, stats.diagnostics().droppedPriorityUpdates(), "synthetic snapshot should not report dropped PRIORITY_UPDATEs");
        assertEquals(0L, stats.diagnostics().droppedLocalPriorityUpdates(), "synthetic snapshot should not report dropped local PRIORITY_UPDATEs");
        assertEquals(0L, stats.diagnostics().visibleTerminalChurnEvents(), "synthetic snapshot should not report visible terminal churn diagnostics");
        assertEquals(0L, stats.diagnostics().groupRebucketEvents(), "synthetic snapshot should not report group rebucket diagnostics");
        assertEquals(0L, stats.diagnostics().protocolBacklogBlocked(), "synthetic snapshot should not report protocol backlog rejections");
        assertEquals(0L, stats.diagnostics().skippedCloseOnDeadIO(), "synthetic snapshot should not report skipped close-on-dead-IO diagnostics");
        assertEquals(0L, stats.diagnostics().closeFrameFlushErrors(), "synthetic snapshot should not report close flush failures");
        assertEquals(0L, stats.diagnostics().closeCompletionTimeouts(), "synthetic snapshot should not report close completion timeout diagnostics");
        assertEquals(0L, stats.diagnostics().gracefulCloseTimeouts(), "synthetic snapshot should not report graceful close timeout diagnostics");
        assertEquals(0L, stats.diagnostics().keepaliveTimeouts(), "synthetic snapshot should not report keepalive timeout diagnostics");
        assertEquals(0L, stats.hiddenState().refused(), "synthetic snapshot should not report hidden refusals");
        assertEquals(0L, stats.hiddenState().reaped(), "synthetic snapshot should not report hidden reaps");
        assertEquals(0L, stats.hiddenState().unreadBytesDiscarded(), "synthetic snapshot should not report hidden unread discards");

        assertEquals(31L, stats.pressure().bufferedReceiveBytes(), "buffered receive bytes mismatch");
        assertEquals(31L, stats.pressure().bufferedReceiveStorageBytes(), "buffered receive storage bytes mismatch");
        assertEquals(100L, stats.pressure().recvSessionAdvertisedBytes(), "advertised receive window mismatch");
        assertEquals(45L, stats.pressure().recvSessionReceivedBytes(), "received bytes mismatch");
        assertEquals(7L, stats.pressure().recvSessionPendingBytes(), "pending receive bytes mismatch");
        assertEquals(3L, stats.pressure().outstandingPingBytes(), "outstanding ping bytes mismatch");
        assertEquals(157L, stats.pressure().trackedSessionMemoryBytes(), "tracked session memory should include queue, retained, and ping bytes");
        assertEquals(0L, stats.pressure().trackedRetainedStateMemoryBytes(), "retained state memory should stay zero without tombstones or provisional entries");
        assertEquals(0L, stats.pressure().retainedStateBreakdown().hiddenControl().count(), "synthetic snapshot should start with zero hidden retained items");
        assertEquals(0L, stats.pressure().retainedStateBreakdown().acceptBacklog().bytes(), "synthetic snapshot should start with zero accept-backlog retained bytes");
        assertEquals(0L, stats.pressure().retainedStateBreakdown().provisionals().count(), "synthetic snapshot should start with zero provisional retained items");
        assertEquals(0L, stats.pressure().retainedStateBreakdown().visibleTombstones().count(), "synthetic snapshot should start with zero visible tombstones");
        assertEquals(0L, stats.pressure().retainedStateBreakdown().markerOnly().count(), "synthetic snapshot should start with zero marker-only retained items");
        assertFalse(stats.pressure().memoryPressureHigh(), "synthetic snapshot should stay under the memory pressure threshold");
        assertTrue(
                stats.pressure().sessionMemoryHardCapBytes() >= stats.pressure().sessionMemoryHighThresholdBytes(),
                "memory hard cap should not be below its high threshold"
        );
    }

    @Test
    void terminalStatsClearKeepaliveLivenessSurface() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(io.zmux.Role.RESPONDER)
                .keepaliveInterval(Duration.ofSeconds(2))
                .keepaliveMaxPingInterval(Duration.ofSeconds(9))
                .keepaliveTimeout(Duration.ofSeconds(6))
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());

        synchronized (runtime.lock()) {
            long nowNanos = System.nanoTime();
            SessionRuntimeTestSupport.setLongField(runtime, "lastPingSentAtNanos", nowNanos - Duration.ofSeconds(4).toNanos());
            SessionRuntimeTestSupport.setLongField(runtime, "lastPongAtNanos", nowNanos - Duration.ofSeconds(1).toNanos());
            SessionRuntimeTestSupport.setLongField(runtime, "lastPingRttNanos", Duration.ofSeconds(2).toNanos());
            SessionRuntimeTestSupport.setLongField(runtime, "sendRateEstimateBytesPerSecond", 2_000L);
            SessionRuntimeTestSupport.setField(
                    runtime,
                    "activePing",
                    SessionRuntimeTestSupport.newPendingPing(
                            nowNanos - Duration.ofSeconds(4).toNanos(),
                            new byte[]{1, 2, 3}
                    )
            );
            runtime.finishSessionLocked(null, SessionState.CLOSED);
        }

        SessionStats stats = runtime.stats();

        assertEquals(SessionState.CLOSED, stats.state(), "terminal stats should report the closed public state");
        assertFalse(stats.keepalive().enabled(), "terminal stats should not report keepalive as enabled");
        assertEquals(0L, stats.keepalive().intervalNanos(), "terminal stats should clear the keepalive interval");
        assertEquals(0L, stats.keepalive().maxPingIntervalNanos(), "terminal stats should clear the max ping interval");
        assertEquals(Duration.ofSeconds(6).toNanos(), stats.keepalive().timeoutNanos(), "configured keepalive timeout should remain surfaced");
        assertFalse(stats.keepalive().pingOutstanding(), "terminal stats should not report an active ping");
        assertFalse(stats.keepalive().pingStalled(), "terminal stats should not report a stalled ping");
        assertEquals(0L, stats.keepalive().lastPingRttNanos(), "terminal stats should clear retained ping RTT");
        assertEquals(2_000L, stats.keepalive().sendRateEstimateBytesPerSecond(), "terminal stats should keep the transport send-rate estimate");
        assertNull(stats.progress().pingSentAt(), "terminal stats should clear the last ping timestamp");
        assertNull(stats.progress().pongAt(), "terminal stats should clear the last pong timestamp");
        assertEquals(0L, stats.pressure().outstandingPingBytes(), "terminal stats should not retain outstanding ping bytes");
    }

    @Test
    void terminalStatsClearOpenAndAcceptSurfaceCounts() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                ZmuxConfig.builder().role(io.zmux.Role.RESPONDER).build(),
                0L,
                Settings.defaults()
        );
        runtime.openStream();

        synchronized (runtime.lock()) {
            runtime.enqueueAcceptedLocked(createPeerOpenedBidi(
                    runtime,
                    SessionRuntime.firstPeerStreamId(io.zmux.Role.RESPONDER, true)
            ));
            runtime.enqueueAcceptedLocked(createPeerOpenedBidi(
                    runtime,
                    SessionRuntime.firstPeerStreamId(io.zmux.Role.RESPONDER, true) + 4L
            ));
        }

        runtime.acceptStream();
        SessionStats before = runtime.stats();

        assertEquals(2L, before.openStreams(), "pre-close stats should report peer-visible live streams");
        assertEquals(1L, before.acceptedStreams(), "pre-close stats should retain accepted stream count");
        assertEquals(1L, before.acceptBacklog().count(), "pre-close stats should retain the remaining accept backlog");

        synchronized (runtime.lock()) {
            runtime.finishSessionLocked(null, SessionState.CLOSED);
        }

        SessionStats after = runtime.stats();

        assertEquals(SessionState.CLOSED, after.state(), "terminal stats should report the closed public state");
        assertEquals(0L, after.openStreams(), "terminal stats should clear live stream counts");
        assertEquals(0L, after.acceptedStreams(), "terminal stats should clear accepted stream counts");
        assertEquals(0L, after.acceptBacklog().count(), "terminal stats should clear accept backlog counts");
        assertEquals(0L, after.acceptBacklog().bytes(), "terminal stats should clear accept backlog bytes");
    }

    @Test
    void provisionalStatsTrackVisibleAcceptBacklogLimitAndObservedRtt() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(io.zmux.Role.RESPONDER)
                .acceptBacklogLimit(200)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setLongField(runtime, "lastPingRttNanos", Duration.ofSeconds(2).toNanos());
        }

        SessionStats stats = runtime.stats();

        assertEquals(50, stats.provisionals().softCap(), "provisional soft cap should follow the visible pending-inbound limit");
        assertEquals(100, stats.provisionals().hardCap(), "provisional hard cap should follow the visible pending-inbound limit");
        assertEquals(
                Duration.ofMillis(12_250).toNanos(),
                stats.provisionals().maxAgeNanos(),
                "provisional max age should widen from observed RTT"
        );
    }

    @Test
    void localOpenLimitUsesDerivedProvisionalHardCap() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(io.zmux.Role.RESPONDER)
                .acceptBacklogLimit(8)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());

        for (int i = 0; i < 32; i++) {
            runtime.openStream();
        }

        assertThrows(
                OpenLimitedException.class,
                runtime::openStream,
                "provisional open hard cap should derive from the visible pending-inbound limit"
        );
        assertEquals(1L, runtime.stats().provisionals().limited(), "rejected local open should increment provisional-limit diagnostics");
    }

    @Test
    void peerIncomingLimitRefusesProvisionalOpenWithApplicationError() throws Exception {
        Settings peerSettings = Settings.defaults()
                .toBuilder()
                .maxIncomingStreamsBidi(1L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, peerSettings);

        runtime.openStream();

        ApplicationError refused = assertInstanceOf(
                ApplicationError.class,
                assertThrows(IOException.class, runtime::openStream),
                "peer stream-limit refusal should surface as an application error"
        );
        assertEquals(ErrorCode.REFUSED_STREAM.code(), refused.code(), "peer stream-limit refusal code mismatch");
        assertEquals("", refused.reason(), "peer stream-limit refusal reason mismatch");
        assertEquals(ZmuxErrorScope.SESSION, refused.scope(), "peer stream-limit refusal scope mismatch");
        assertEquals(ZmuxErrorSource.REMOTE, refused.source(), "peer stream-limit refusal source mismatch");
        assertEquals(ZmuxErrorDirection.BOTH, refused.direction(), "peer stream-limit refusal direction mismatch");
        assertEquals(ZmuxTerminationKind.UNKNOWN, refused.terminationKind(), "peer stream-limit refusal termination mismatch");
    }

    @Test
    void expiredProvisionalSurfaceIsStructuredAndCounted() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            stream.setProvisionalCreatedAtNanosLocked(
                    System.nanoTime() - runtime.provisionalOpenMaxAgeNanosLocked() - 1L
            );
        }

        ApplicationError expired = assertInstanceOf(
                ApplicationError.class,
                assertThrows(IOException.class, () -> stream.write("x".getBytes(StandardCharsets.UTF_8))),
                "expired provisional commit should surface the abortive stream error"
        );
        assertEquals(ErrorCode.CANCELLED.code(), expired.code(), "expired provisional error code mismatch");
        assertEquals(OpenExpiredException.MESSAGE, expired.reason(), "expired provisional reason mismatch");
        assertEquals(ZmuxErrorScope.STREAM, expired.scope(), "expired provisional scope mismatch");
        assertEquals(ZmuxErrorSource.LOCAL, expired.source(), "expired provisional source mismatch");
        assertEquals(ZmuxErrorDirection.BOTH, expired.direction(), "expired provisional direction mismatch");
        assertEquals(ZmuxTerminationKind.ABORT, expired.terminationKind(), "expired provisional termination mismatch");
        assertEquals(0L, stream.streamId(), "expired provisional open must not consume a stream id");
        assertEquals(0, runtime.stats().provisionals().bidi(), "expired provisional should be reclaimed immediately");
        assertEquals(0L, runtime.stats().provisionals().limited(), "expired provisional should not count as a limit rejection");
        assertEquals(1L, runtime.stats().provisionals().expired(), "expired provisional should increment expiry diagnostics");

        ApplicationError repeated = assertInstanceOf(
                ApplicationError.class,
                assertThrows(IOException.class, () -> stream.write("x".getBytes(StandardCharsets.UTF_8))),
                "expired provisional stream should retain the abortive failure"
        );
        assertEquals(ErrorCode.CANCELLED.code(), repeated.code(), "stored expired provisional code mismatch");
        assertEquals(OpenExpiredException.MESSAGE, repeated.reason(), "stored expired provisional reason mismatch");
    }

    @Test
    void interruptWhileWaitingForOpenTurnFailsAndReclaimsProvisionalSlot() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime first = (StreamRuntime) runtime.openStream();
        StreamRuntime second = (StreamRuntime) runtime.openStream();
        byte[] payload = "blocked-open".getBytes(StandardCharsets.UTF_8);
        AtomicReference<Object> outcome = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            try {
                second.write(payload);
                outcome.set(Boolean.TRUE);
            } catch (Throwable failure) {
                outcome.set(failure);
            }
        }, "session-open-turn-interrupt");
        writer.start();

        long waitDeadlineNanos = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < waitDeadlineNanos) {
            if (blockedOnOpenTurn(writer) && runtime.stats().provisionals().bidi() == 2) {
                break;
            }
            Thread.sleep(10L);
        }
        assertTrue(blockedOnOpenTurn(writer), "second local open should block behind the provisional head");

        writer.interrupt();
        writer.join(Duration.ofSeconds(2).toMillis());
        assertFalse(writer.isAlive(), "interrupted open-turn waiter should exit promptly");

        ZmuxInterruptedIOException interrupted = assertInstanceOf(
                ZmuxInterruptedIOException.class,
                outcome.get(),
                "interrupting open-turn wait should surface the typed write interruption"
        );
        assertEquals("write", interrupted.operation(), "interrupted operation mismatch");
        assertEquals(ZmuxErrorScope.STREAM, interrupted.scope(), "interrupted scope mismatch");
        assertEquals(ZmuxErrorDirection.WRITE, interrupted.direction(), "interrupted direction mismatch");
        assertEquals(ZmuxTerminationKind.INTERRUPTED, interrupted.terminationKind(), "interrupted termination kind mismatch");
        assertEquals(1, runtime.stats().provisionals().bidi(), "interrupted provisional open should be reclaimed immediately");

        ZmuxInterruptedIOException repeated = assertInstanceOf(
                ZmuxInterruptedIOException.class,
                assertThrows(IOException.class, () -> second.write(payload)),
                "failed provisional stream should retain its local interruption error"
        );
        assertEquals("write", repeated.operation(), "stored interruption operation mismatch");
        assertEquals(ZmuxErrorScope.STREAM, repeated.scope(), "stored interruption scope mismatch");
        assertEquals(ZmuxErrorDirection.WRITE, repeated.direction(), "stored interruption direction mismatch");

        first.close();
    }

    @Test
    void interruptWhileCloseWriteWaitsForOpenTurnUsesCloseSurface() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime first = (StreamRuntime) runtime.openStream();
        StreamRuntime second = (StreamRuntime) runtime.openStream();
        AtomicReference<Object> outcome = new AtomicReference<>();
        Thread closer = new Thread(() -> {
            try {
                second.closeWrite();
                outcome.set(Boolean.TRUE);
            } catch (Throwable failure) {
                outcome.set(failure);
            }
        }, "session-open-turn-close-interrupt");
        closer.start();

        long waitDeadlineNanos = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < waitDeadlineNanos) {
            if (blockedOnOpenTurn(closer) && runtime.stats().provisionals().bidi() == 2) {
                break;
            }
            Thread.sleep(10L);
        }
        assertTrue(blockedOnOpenTurn(closer), "closeWrite should block behind the earlier provisional head");

        closer.interrupt();
        closer.join(Duration.ofSeconds(2).toMillis());
        assertFalse(closer.isAlive(), "interrupted closeWrite waiter should exit promptly");

        ZmuxInterruptedIOException interrupted = assertInstanceOf(
                ZmuxInterruptedIOException.class,
                outcome.get(),
                "interrupting closeWrite open-turn wait should use the close surface"
        );
        assertEquals("close", interrupted.operation(), "closeWrite interruption operation mismatch");
        assertEquals(ZmuxErrorScope.STREAM, interrupted.scope(), "closeWrite interruption scope mismatch");
        assertEquals(ZmuxErrorDirection.BOTH, interrupted.direction(), "closeWrite interruption direction mismatch");
        assertEquals(ZmuxTerminationKind.INTERRUPTED, interrupted.terminationKind(), "closeWrite interruption termination mismatch");
        assertEquals(1, runtime.stats().provisionals().bidi(), "interrupted provisional closeWrite should be reclaimed immediately");

        ZmuxInterruptedIOException repeated = assertInstanceOf(
                ZmuxInterruptedIOException.class,
                assertThrows(IOException.class, second::closeWrite),
                "failed provisional closeWrite should retain the close-surface interruption"
        );
        assertEquals("close", repeated.operation(), "stored closeWrite interruption operation mismatch");
        assertEquals(ZmuxErrorScope.STREAM, repeated.scope(), "stored closeWrite interruption scope mismatch");
        assertEquals(ZmuxErrorDirection.BOTH, repeated.direction(), "stored closeWrite interruption direction mismatch");

        first.close();
    }

    @Test
    void sessionFailureBeatsPeerStopForMetadataUpdateSurface() throws Exception {
        long capabilities = Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS;
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(capabilities, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            stream.stopSendingFromPeerLocked(ErrorCode.CANCELLED.code(), "peer stop", 0L);
            failRuntimeWithPeerClose(runtime, ErrorCode.INTERNAL.code(), "peer closing");
        }

        ApplicationError error = assertInstanceOf(
                ApplicationError.class,
                assertThrows(IOException.class, () -> stream.updateMetadata(new MetadataUpdate(7L, null))),
                "session termination should outrank the earlier peer STOP_SENDING surface for metadata updates"
        );
        assertEquals(ErrorCode.INTERNAL.code(), error.code(), "metadata update session-close code mismatch");
        assertEquals("peer closing", error.reason(), "metadata update session-close reason mismatch");
        assertEquals(ZmuxErrorScope.SESSION, error.scope(), "metadata update session-close scope mismatch");
        assertEquals(ZmuxErrorSource.REMOTE, error.source(), "metadata update session-close source mismatch");
        assertEquals(ZmuxErrorDirection.BOTH, error.direction(), "metadata update session-close direction mismatch");
        assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, error.terminationKind(), "metadata update session-close termination mismatch");
    }

    @Test
    void localCloseFrameQueuedRejectsMetadataUpdateWithoutMutatingShadowState() throws Exception {
        long capabilities = Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS;
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(capabilities, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        stream.write("x".getBytes(StandardCharsets.UTF_8));

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(runtime, "markPeerVisibleLocked", new Class<?>[]{StreamRuntime.class}, stream);
            runtime.setCloseFrameQueuedInternal(true);
        }

        SessionClosedException error = assertInstanceOf(
                SessionClosedException.class,
                assertThrows(IOException.class, () -> stream.updateMetadata(new MetadataUpdate(7L, null))),
                "metadata update should fail once the session has started closing local non-close control"
        );
        assertEquals(ZmuxErrorSource.LOCAL, error.source(), "queued-close metadata update should surface a local session-closed error");
        assertEquals(0L, stream.metadata().priority(), "failed metadata update must not mutate local metadata");

        synchronized (runtime.lock()) {
            assertFalse(stream.hasPendingPriorityUpdateLocked(), "rejected metadata update must not leave a staged PRIORITY_UPDATE behind");
        }
    }

    @Test
    void sessionFailureBeatsCloseReadWithoutMutatingReadStop() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            failRuntimeWithPeerClose(runtime, ErrorCode.INTERNAL.code(), "peer closing");
        }

        ApplicationError error = assertInstanceOf(
                ApplicationError.class,
                assertThrows(IOException.class, stream::closeRead),
                "closeRead on a terminated session should surface the session error first"
        );
        assertEquals(ErrorCode.INTERNAL.code(), error.code(), "closeRead session-close code mismatch");
        assertEquals("peer closing", error.reason(), "closeRead session-close reason mismatch");

        synchronized (runtime.lock()) {
            assertFalse(stream.readClosed(), "failed closeRead on a terminated session must not latch local read-stop state");
            assertFalse(stream.halfStateInternal().readStopSent(), "failed closeRead on a terminated session must not mutate recv half-state");
        }
    }

    @Test
    void sessionFailureBeatsCloseWithErrorWithoutMutatingLocalAbort() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            failRuntimeWithPeerClose(runtime, ErrorCode.INTERNAL.code(), "peer closing");
        }

        ApplicationError error = assertInstanceOf(
                ApplicationError.class,
                assertThrows(IOException.class, () -> stream.closeWithError(ErrorCode.CANCELLED.code(), "local abort")),
                "closeWithError on a terminated session should surface the session error first"
        );
        assertEquals(ErrorCode.INTERNAL.code(), error.code(), "closeWithError session-close code mismatch");
        assertEquals("peer closing", error.reason(), "closeWithError session-close reason mismatch");

        synchronized (runtime.lock()) {
            assertFalse(stream.sendTerminalLocked(), "failed closeWithError on a terminated session must not mutate send terminal state");
            assertFalse(stream.recvAbortiveLocked(), "failed closeWithError on a terminated session must not inject a local abort into the recv half");
            assertNull(stream.terminalStateInternal().localError(), "failed closeWithError on a terminated session must not stash a local abort");
        }
    }

    @Test
    void sessionFailureBeatsCompositeStreamCloseWithoutSuppressedNoise() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            failRuntimeWithPeerClose(runtime, ErrorCode.INTERNAL.code(), "peer closing");
        }

        ApplicationError error = assertInstanceOf(
                ApplicationError.class,
                assertThrows(IOException.class, stream::close),
                "stream.close on a terminated session should return the session error directly"
        );
        assertEquals(ErrorCode.INTERNAL.code(), error.code(), "stream.close session-close code mismatch");
        assertEquals("peer closing", error.reason(), "stream.close session-close reason mismatch");
        assertEquals(0, error.getSuppressed().length, "stream.close should not accumulate redundant suppressed session errors");
    }

    @Test
    void sessionFailureBeatsPerHalfDeadlineUpdates() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            failRuntimeWithPeerClose(runtime, ErrorCode.INTERNAL.code(), "peer closing");
        }

        ApplicationError readError = assertInstanceOf(
                ApplicationError.class,
                assertThrows(IOException.class, () -> stream.setReadDeadline(java.time.Instant.now())),
                "read deadline updates on a terminated session should surface the session error"
        );
        assertEquals(ErrorCode.INTERNAL.code(), readError.code(), "setReadDeadline session-close code mismatch");
        assertEquals("peer closing", readError.reason(), "setReadDeadline session-close reason mismatch");

        ApplicationError writeError = assertInstanceOf(
                ApplicationError.class,
                assertThrows(IOException.class, () -> stream.setWriteDeadline(java.time.Instant.now())),
                "write deadline updates on a terminated session should surface the session error"
        );
        assertEquals(ErrorCode.INTERNAL.code(), writeError.code(), "setWriteDeadline session-close code mismatch");
        assertEquals("peer closing", writeError.reason(), "setWriteDeadline session-close reason mismatch");
    }

    @Test
    void sessionFailureBeatsCombinedDeadlineUpdate() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            failRuntimeWithPeerClose(runtime, ErrorCode.INTERNAL.code(), "peer closing");
        }

        ApplicationError error = assertInstanceOf(
                ApplicationError.class,
                assertThrows(IOException.class, () -> stream.setDeadline(java.time.Instant.now())),
                "combined deadline updates on a terminated session should surface the session error"
        );
        assertEquals(ErrorCode.INTERNAL.code(), error.code(), "setDeadline session-close code mismatch");
        assertEquals("peer closing", error.reason(), "setDeadline session-close reason mismatch");
        assertEquals("read", error.operation(), "bidi setDeadline should preserve read-first deadline operation");
    }

    @Test
    void sessionFailureBeatsSendOnlyCombinedDeadlineUpdateAsWriteOperation() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        ZmuxNativeSendStream stream = runtime.openUniStream();

        synchronized (runtime.lock()) {
            failRuntimeWithPeerClose(runtime, ErrorCode.INTERNAL.code(), "peer closing");
        }

        ApplicationError error = assertInstanceOf(
                ApplicationError.class,
                assertThrows(IOException.class, () -> stream.setDeadline(java.time.Instant.now())),
                "send-only combined deadline updates on a terminated session should surface the session error"
        );
        assertEquals(ErrorCode.INTERNAL.code(), error.code(), "send-only setDeadline session-close code mismatch");
        assertEquals("peer closing", error.reason(), "send-only setDeadline session-close reason mismatch");
        assertEquals("write", error.operation(), "send-only setDeadline should preserve write deadline operation");
    }

    @Test
    void finalizedGracefulSessionIgnoresLateFinishErrors() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        IOException lateTransportError = new ZmuxException(
                ErrorCode.INTERNAL.code(),
                "write",
                "late transport write failure",
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.TRANSPORT,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        );

        synchronized (runtime.lock()) {
            runtime.finishSessionLocked(null, SessionState.CLOSED);
            runtime.finishSessionLocked(lateTransportError, SessionState.CLOSED);
        }

        assertEquals(SessionState.CLOSED, runtime.state(), "late finish error must not change graceful terminal state");
        assertFalse(runtime.terminationCause().isPresent(), "late finish error must not become the cause of a graceful close");
    }

    @Test
    void finalizedFailedSessionKeepsOriginalTerminalCause() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        ApplicationError original = new ApplicationError(
                ErrorCode.INTERNAL.code(),
                "peer failed",
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.REMOTE,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION,
                "read"
        );
        IOException lateTransportError = new ZmuxException(
                ErrorCode.INTERNAL.code(),
                "write",
                "late transport write failure",
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.TRANSPORT,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        );

        synchronized (runtime.lock()) {
            runtime.finishSessionLocked(original, SessionState.FAILED);
            runtime.finishSessionLocked(lateTransportError, SessionState.FAILED);
        }

        IOException cause = runtime.terminationCause()
                .orElseThrow(() -> new AssertionError("failed session should expose its terminal cause"));
        assertSame(original, cause, "late finish error must not overwrite the first terminal cause");
        assertEquals(SessionState.FAILED, runtime.state(), "late finish error must not change failed terminal state");
    }

    @Test
    void gracefulCloseDrainTimeoutUsesObservedRttWhenUnset() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                ZmuxConfig.builder().role(io.zmux.Role.RESPONDER).build(),
                0L,
                Settings.defaults()
        );

        Duration defaultTimeout;
        synchronized (runtime.lock()) {
            defaultTimeout = (Duration) SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "gracefulCloseDrainTimeout",
                    new Class<?>[0]
            );
        }
        assertEquals(Duration.ofMillis(500), defaultTimeout, "default graceful-close drain timeout should use the repository base");

        Duration adaptiveTimeout;
        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setLongField(runtime, "lastPingRttNanos", Duration.ofMillis(600).toNanos());
            adaptiveTimeout = (Duration) SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "gracefulCloseDrainTimeout",
                    new Class<?>[0]
            );
        }
        assertEquals(Duration.ofMillis(2_500), adaptiveTimeout, "observed RTT should widen the default graceful-close drain timeout");

        Duration cappedTimeout;
        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setLongField(runtime, "lastPingRttNanos", Duration.ofSeconds(2).toNanos());
            cappedTimeout = (Duration) SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "gracefulCloseDrainTimeout",
                    new Class<?>[0]
            );
        }
        assertEquals(Duration.ofSeconds(5), cappedTimeout, "default graceful-close drain timeout should honor the adaptive cap");
    }

    @Test
    void explicitGracefulCloseDrainTimeoutOverrideWinsOverObservedRtt() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                ZmuxConfig.builder()
                        .role(io.zmux.Role.RESPONDER)
                        .gracefulCloseDrainTimeout(Duration.ofMillis(200))
                        .build(),
                0L,
                Settings.defaults()
        );

        Duration timeout;
        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setLongField(runtime, "lastPingRttNanos", Duration.ofSeconds(2).toNanos());
            timeout = (Duration) SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "gracefulCloseDrainTimeout",
                    new Class<?>[0]
            );
        }

        assertEquals(Duration.ofMillis(200), timeout, "explicit graceful-close drain timeout should override RTT-derived defaults");
    }
}
