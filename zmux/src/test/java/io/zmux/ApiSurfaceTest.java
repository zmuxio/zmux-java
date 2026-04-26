package io.zmux;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class ApiSurfaceTest {
    private static void rethrow(Throwable error) throws Exception {
        if (error == null) {
            return;
        }
        if (error instanceof Exception) {
            throw (Exception) error;
        }
        throw new RuntimeException(error);
    }

    private static void awaitBlockingState(Thread thread, Duration timeout) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            Thread.State state = thread.getState();
            if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
                return;
            }
            Thread.sleep(5L);
        }
        assertTrue(
                thread.getState() == Thread.State.WAITING || thread.getState() == Thread.State.TIMED_WAITING,
                "thread did not enter a blocking wait state"
        );
    }

    private static void assertStructuredInterrupted(ZmuxInterruptedException interrupted, String operation) {
        assertEquals(operation, interrupted.operation());
        assertEquals(operation, ZmuxErrors.operation(interrupted));
        assertEquals(ZmuxErrorScope.SESSION, interrupted.scope());
        assertEquals(ZmuxErrorSource.LOCAL, interrupted.source());
        assertEquals(ZmuxErrorDirection.BOTH, interrupted.direction());
        assertEquals(ZmuxTerminationKind.INTERRUPTED, interrupted.terminationKind());
        assertTrue(ZmuxErrors.interrupted(interrupted));
        assertFalse(ZmuxErrors.timeout(interrupted));
    }

    @Test
    void openUniStreamExposesSendOnlySurface() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            ZmuxSendStream stream = pair.client().openUniStream();

            assertTrue(stream instanceof ZmuxNativeSendStream, "native client should expose native send-only view");
            assertFalse(stream instanceof ZmuxRecvStream, "send-only stream must not also expose recv methods");
            assertFalse(stream instanceof ZmuxNativeRecvStream, "send-only stream must not also expose native recv methods");
        }
    }

    @Test
    void javaStyleTimeoutOpenAliasesDelegateToWithTimeoutVariants() throws Exception {
        RecordingDefaultSendStream send = new RecordingDefaultSendStream();
        DefaultRecordingNativeSession session = new DefaultRecordingNativeSession(send);
        OpenOptions options = OpenOptions.priority(7L);
        Duration timeout = Duration.ofMillis(25);

        assertSame(session.bidiStream, session.openStream(timeout));
        assertEquals(1, session.openStreamWithTimeoutCalls);
        assertSame(OpenOptions.empty(), session.lastOpenStreamOptions);
        assertEquals(timeout, session.lastOpenStreamTimeout);

        assertSame(session.bidiStream, session.openStream(options, timeout));
        assertEquals(2, session.openStreamWithTimeoutCalls);
        assertSame(options, session.lastOpenStreamOptions);
        assertEquals(timeout, session.lastOpenStreamTimeout);

        assertSame(send, session.openUniStream(timeout));
        assertEquals(1, session.openUniStreamWithTimeoutCalls);
        assertSame(OpenOptions.empty(), session.lastOpenUniStreamOptions);
        assertEquals(timeout, session.lastOpenUniStreamTimeout);

        assertSame(send, session.openUniStream(options, timeout));
        assertEquals(2, session.openUniStreamWithTimeoutCalls);
        assertSame(options, session.lastOpenUniStreamOptions);
        assertEquals(timeout, session.lastOpenUniStreamTimeout);
    }

    @Test
    void javaStyleTimeoutSendAliasesDelegateToExistingTimedHelpers() throws Exception {
        RecordingDefaultSendStream send = new RecordingDefaultSendStream();
        DefaultRecordingNativeSession session = new DefaultRecordingNativeSession(send);
        OpenOptions options = OpenOptions.priority(11L);
        Duration timeout = Duration.ofMillis(40);

        assertSame(session.bidiStream, session.openAndSend(timeout, new byte[]{1}));
        assertEquals(1, session.openStreamWithTimeoutCalls);
        assertSame(OpenOptions.empty(), session.lastOpenStreamOptions);
        assertEquals(timeout, session.lastOpenStreamTimeout);

        assertSame(session.bidiStream, session.openAndSend(options, timeout, new byte[]{2}));
        assertEquals(2, session.openStreamWithTimeoutCalls);
        assertSame(options, session.lastOpenStreamOptions);
        assertEquals(timeout, session.lastOpenStreamTimeout);

        assertSame(send, session.openUniAndSend(timeout, new byte[]{3}));
        assertEquals(1, session.openUniStreamWithTimeoutCalls);
        assertSame(OpenOptions.empty(), session.lastOpenUniStreamOptions);
        assertEquals(timeout, session.lastOpenUniStreamTimeout);

        assertSame(send, session.openUniAndSend(options, timeout, new byte[]{4}));
        assertEquals(2, session.openUniStreamWithTimeoutCalls);
        assertSame(options, session.lastOpenUniStreamOptions);
        assertEquals(timeout, session.lastOpenUniStreamTimeout);
    }

    @Test
    void acceptUniStreamExposesRecvOnlySurface() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            ZmuxSendStream outbound = pair.client().openUniStream();
            outbound.writeFinal("hi".getBytes(StandardCharsets.UTF_8));

            ZmuxRecvStream inbound = pair.server().acceptUniStream(Duration.ofSeconds(1));

            assertTrue(inbound instanceof ZmuxNativeRecvStream, "native server should expose native recv-only view");
            assertFalse(inbound instanceof ZmuxSendStream, "recv-only stream must not also expose send methods");
            assertFalse(inbound instanceof ZmuxNativeSendStream, "recv-only stream must not also expose native send methods");
        }
    }

    @Test
    void asSessionNullReturnsClosedSafeStableSession() throws Exception {
        ZmuxSession session = Zmux.closedSession();

        assertNotNull(session);
        assertTrue(session.isClosed());
        assertEquals(SessionState.INVALID, session.state());
        assertEquals(SessionState.INVALID, session.stats().state());
        assertTrue(session.awaitTermination());
        assertTrue(session.awaitTermination(Duration.ofMillis(1)));
        assertFalse(session.awaitTerminationCause().isPresent());
        assertThrows(SessionClosedException.class, session::openStream);
        session.closeWithError(7L, "ignored");
        session.closeWithError((Throwable) null);
        session.close();
        assertSame(session, Zmux.asSession(null), "asSession(null) should reuse the canonical closed-safe session");
    }

    @Test
    void asNativeSessionNullReturnsClosedSafeNativeSession() throws Exception {
        ZmuxNativeSession session = Zmux.closedNativeSession();
        Settings zeroSettings = new Settings(
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                SchedulerHint.UNSPECIFIED_OR_BALANCED
        );

        assertNotNull(session);
        assertSame(Zmux.closedSession(), session, "closed native session should reuse the canonical closed-safe session");
        assertTrue(session.isClosed());
        assertEquals(SessionState.INVALID, session.state());
        assertEquals(SessionState.INVALID, session.stats().state());
        assertTrue(session.awaitTermination());
        assertTrue(session.awaitTermination(Duration.ofMillis(1)));
        assertFalse(session.awaitTerminationCause().isPresent());
        assertNull(session.peerGoAwayError());
        assertNull(session.peerCloseError());
        assertEquals(new Preface((byte) 0, Role.INITIATOR, 0L, 0L, 0L, 0L, zeroSettings), session.localPreface());
        assertEquals(new Preface((byte) 0, Role.INITIATOR, 0L, 0L, 0L, 0L, zeroSettings), session.peerPreface());
        assertEquals(new Negotiated(0L, 0L, Role.INITIATOR, Role.INITIATOR, zeroSettings), session.negotiated());
        assertThrows(SessionClosedException.class, session::openStream);
        assertThrows(SessionClosedException.class, session::ping);
        assertThrows(SessionClosedException.class, () -> session.goAway(0L, 0L));
        session.closeWithError(7L, "ignored");
        session.closeWithError((Throwable) null);
        session.close();
        assertSame(session, Zmux.asNativeSession(null), "asNativeSession(null) should reuse the canonical closed-safe native session");
    }

    @Test
    void asSessionReturnsSameNonNullReference() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            assertSame(pair.client(), Zmux.asSession(pair.client()));
            assertSame(pair.server(), Zmux.asSession(pair.server()));
        }
    }

    @Test
    void zmuxJoinHelpersDelegateToConnectionJoinAdapters() throws Exception {
        InetSocketAddress local = InetSocketAddress.createUnresolved("join.local", 3131);
        InetSocketAddress remote = InetSocketAddress.createUnresolved("join.remote", 4141);
        ReadHalf recv = new ReadHalf() {
            @Override
            public int read(byte[] dst, int offset, int length) {
                return -1;
            }

            @Override
            public void closeRead() {
            }

            @Override
            public void setReadDeadline(Instant deadline) {
            }

            @Override
            public SocketAddress localAddress() {
                return local;
            }

            @Override
            public SocketAddress remoteAddress() {
                return remote;
            }
        };
        WriteHalf send = new WriteHalf() {
            @Override
            public void write(byte[] src, int offset, int length) {
            }

            @Override
            public void closeWrite() {
            }

            @Override
            public void setWriteDeadline(Instant deadline) {
            }

            @Override
            public SocketAddress localAddress() {
                return local;
            }

            @Override
            public SocketAddress remoteAddress() {
                return remote;
            }
        };
        ByteArrayInputStream input = new ByteArrayInputStream(new byte[]{1});
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try (JoinedDuplexConnection fromHalves = Zmux.join((ReadHalf) recv, (WriteHalf) send);
             JoinedDuplexConnection fromIo = Zmux.join(input, output, local, remote)) {
            assertSame(local, fromHalves.localAddress());
            assertSame(remote, fromHalves.remoteAddress());
            assertSame(local, fromIo.localAddress());
            assertSame(remote, fromIo.remoteAddress());
        }

        try (JoinedDuplexConnection fromStreams = Zmux.join(new RecordingDefaultRecvStream(), new RecordingDefaultSendStream())) {
            assertNotNull(fromStreams);
        }
    }

    @Test
    void asNativeSessionReturnsSameNonNullReference() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            assertSame(pair.client(), Zmux.asNativeSession(pair.client()));
            assertSame(pair.server(), Zmux.asNativeSession(pair.server()));
        }
    }

    @Test
    void openStreamExposesNativeBidiSurface() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            ZmuxStream stream = pair.client().openStream();

            assertTrue(stream instanceof ZmuxNativeStream, "bidi stream should expose native bidi state queries");
        }
    }

    @Test
    void streamAddressesFallBackWhenTransportDoesNotExposeSocketAddresses() throws Exception {
        try (SessionPair pair = SessionPair.openWithoutAddresses()) {
            ZmuxNativeStream outbound = pair.client().openStream();
            outbound.write("x".getBytes(StandardCharsets.UTF_8));
            ZmuxNativeStream inbound = pair.server().acceptStream(Duration.ofSeconds(1));

            SocketAddress outboundLocal = outbound.localAddress();
            SocketAddress outboundRemote = outbound.remoteAddress();
            SocketAddress inboundLocal = inbound.localAddress();
            SocketAddress inboundRemote = inbound.remoteAddress();

            assertEquals(ZmuxSocketAddress.localStream(outbound.streamId()), outboundLocal);
            assertEquals(ZmuxSocketAddress.remoteStream(outbound.streamId()), outboundRemote);
            assertEquals(ZmuxSocketAddress.localStream(inbound.streamId()), inboundLocal);
            assertEquals(ZmuxSocketAddress.remoteStream(inbound.streamId()), inboundRemote);

            outbound.close();
            inbound.close();
        }
    }

    @Test
    void openWithExpiredTimeoutFailsBeforeCoreProvisionalOpen() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            assertThrows(OpenTimeoutException.class, () -> pair.client().openStreamWithTimeout(Duration.ZERO));
        }
    }

    @Test
    void readClosedReflectsCommittedRecvTerminalStateBeforeBufferDrain() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            ZmuxNativeStream outbound = pair.client().openStream();
            outbound.writeFinal("hi".getBytes(StandardCharsets.UTF_8));

            ZmuxNativeStream inbound = pair.server().acceptStream(Duration.ofSeconds(1));

            assertTrue(
                    inbound.readClosed(),
                    "native readClosed should reflect committed recv terminal state even before buffered bytes are drained"
            );

            byte[] buffer = new byte[8];
            assertEquals(2, inbound.read(buffer));
            assertEquals(-1, inbound.read(buffer));

            inbound.close();
            outbound.close();
        }
    }

    @Test
    void openUniAndSendUsesWriteFinalSemanticsForPayloadAndEmptyPayload() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            ZmuxNativeSendStream first = (ZmuxNativeSendStream) pair.client().openUniAndSend("hi".getBytes(StandardCharsets.UTF_8));
            ZmuxRecvStream firstInbound = pair.server().acceptUniStream(Duration.ofSeconds(5));
            byte[] firstBuffer = new byte[8];
            assertEquals(2, firstInbound.read(firstBuffer));
            assertEquals(-1, firstInbound.read(firstBuffer));
            assertTrue(first.writeClosed(), "openUniAndSend should close the send side after writing payload");
            firstInbound.close();
            first.close();

            ZmuxNativeSendStream second = (ZmuxNativeSendStream) pair.client().openUniAndSend(new byte[0]);
            ZmuxRecvStream secondInbound = pair.server().acceptUniStream(Duration.ofSeconds(5));
            byte[] secondBuffer = new byte[1];
            assertEquals(-1, secondInbound.read(secondBuffer));
            assertTrue(second.writeClosed(), "openUniAndSend should close the send side even for an empty payload");
            secondInbound.close();
            second.close();
        }
    }

    @Test
    void sessionSendHelpersAcceptSlicesAndByteBuffers() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            byte[] bidiSource = "xhelloz".getBytes(StandardCharsets.UTF_8);
            ZmuxNativeStream bidi = pair.client().openAndSend(bidiSource, 1, 5);
            bidi.closeWrite();
            ZmuxStream inboundBidi = pair.server().acceptStream(Duration.ofSeconds(1));
            assertEquals("hello", new String(inboundBidi.readAllBytes(), StandardCharsets.UTF_8));

            ByteBuffer uniSource = ByteBuffer.wrap("!event?".getBytes(StandardCharsets.UTF_8));
            uniSource.position(1);
            uniSource.limit(6);
            ZmuxNativeSendStream uni = pair.client().openUniAndSend(uniSource);
            ZmuxRecvStream inboundUni = pair.server().acceptUniStream(Duration.ofSeconds(1));
            assertEquals("event", new String(inboundUni.readAllBytes(), StandardCharsets.UTF_8));

            ByteBuffer timedBidiSource = ByteBuffer.wrap("ab".getBytes(StandardCharsets.UTF_8));
            ZmuxNativeStream timedBidi = pair.client().openAndSendWithTimeout(Duration.ofSeconds(1), timedBidiSource);
            timedBidi.closeWrite();
            ZmuxStream inboundTimedBidi = pair.server().acceptStream(Duration.ofSeconds(1));
            assertEquals("ab", new String(inboundTimedBidi.readAllBytes(), StandardCharsets.UTF_8));

            byte[] timedUniSource = "pqrs".getBytes(StandardCharsets.UTF_8);
            ZmuxNativeSendStream timedUni = pair.client().openUniAndSendWithTimeout(
                    Duration.ofSeconds(1),
                    timedUniSource,
                    1,
                    2
            );
            ZmuxRecvStream inboundTimedUni = pair.server().acceptUniStream(Duration.ofSeconds(1));
            assertEquals("qr", new String(inboundTimedUni.readAllBytes(), StandardCharsets.UTF_8));
            assertEquals(6, uniSource.position(), "openUniAndSend(ByteBuffer) should advance the source position");
            assertEquals(2, timedBidiSource.position(), "openAndSendWithTimeout(ByteBuffer) should advance the source position");

            inboundBidi.close();
            inboundUni.close();
            inboundTimedBidi.close();
            inboundTimedUni.close();
            bidi.close();
            uni.close();
            timedBidi.close();
            timedUni.close();
        }
    }

    @Test
    void readDeadlineTimesOutLocallyAndCanBeCleared() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            ZmuxStream outbound = pair.client().openStream();
            outbound.write("x".getBytes(StandardCharsets.UTF_8));

            ZmuxStream inbound = pair.server().acceptStream(Duration.ofSeconds(1));
            byte[] buffer = new byte[1];
            assertEquals(1, inbound.read(buffer));

            inbound.setReadTimeout(Duration.ofMillis(50));
            ReadTimeoutException timeout = assertThrows(ReadTimeoutException.class, () -> inbound.read(new byte[1]));
            ZmuxErrorDetails details = ZmuxErrors.details(timeout);
            assertNotNull(details);
            assertEquals("read", details.operation());
            assertEquals(ZmuxErrorScope.STREAM, details.scope());
            assertTrue(details.timeout());

            inbound.clearReadDeadline();
            outbound.writeFinal("y".getBytes(StandardCharsets.UTF_8));
            assertEquals(1, inbound.read(buffer));
            assertEquals(-1, inbound.read(buffer));
            inbound.close();
            outbound.close();
        }
    }

    @Test
    void writeDeadlineTimesOutBlockedWriteAndCanBeCleared() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .settings(Settings.builder()
                        .initialMaxData(1L)
                        .initialMaxStreamDataBidiPeerOpened(1L)
                        .initialMaxStreamDataBidiLocallyOpened(1L)
                        .build())
                .build();
        try (SessionPair pair = SessionPair.open(config)) {
            ZmuxStream outbound = pair.client().openStream();
            outbound.write("x".getBytes(StandardCharsets.UTF_8));
            ZmuxStream inbound = pair.server().acceptStream(Duration.ofSeconds(1));

            outbound.setWriteTimeout(Duration.ofMillis(50));
            WriteTimeoutException timeout = assertThrows(
                    WriteTimeoutException.class,
                    () -> outbound.write("y".getBytes(StandardCharsets.UTF_8))
            );
            ZmuxErrorDetails details = ZmuxErrors.details(timeout);
            assertNotNull(details);
            assertEquals("write", details.operation());
            assertEquals(ZmuxErrorScope.STREAM, details.scope());
            assertTrue(details.timeout());

            outbound.clearWriteDeadline();
            AtomicReference<Throwable> writeError = new AtomicReference<>();
            CountDownLatch writeDone = new CountDownLatch(1);
            Thread writer = new Thread(() -> {
                try {
                    outbound.writeFinal("z".getBytes(StandardCharsets.UTF_8));
                } catch (Throwable error) {
                    writeError.set(error);
                } finally {
                    writeDone.countDown();
                }
            }, "api-surface-write-deadline");
            writer.start();

            byte[] buffer = new byte[2];
            assertEquals(1, inbound.read(buffer, 0, 1));
            assertTrue(writeDone.await(Duration.ofSeconds(1).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS));
            writer.join(Duration.ofSeconds(1).toMillis());
            assertNull(writeError.get());
            assertEquals(1, inbound.read(buffer, 1, 1));
            assertEquals(-1, inbound.read(new byte[1]));
            inbound.close();
            outbound.close();
        }
    }

    @Test
    void openUniAndSendWithTimeoutCarriesRemainingBudgetIntoFirstWrite() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .settings(Settings.builder()
                        .initialMaxData(1L)
                        .initialMaxStreamDataUni(1L)
                        .build())
                .build();
        try (SessionPair pair = SessionPair.open(config)) {
            WriteTimeoutException timeout = assertThrows(
                    WriteTimeoutException.class,
                    () -> pair.client().openUniAndSendWithTimeout(Duration.ofMillis(50), "xy".getBytes(StandardCharsets.UTF_8))
            );
            ZmuxErrorDetails details = ZmuxErrors.details(timeout);
            assertNotNull(details);
            assertEquals("write", details.operation());
            assertTrue(details.timeout());
        }
    }

    @Test
    void openAndSendWithTimeoutCarriesRemainingBudgetIntoFirstWrite() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .settings(Settings.builder()
                        .initialMaxData(1L)
                        .initialMaxStreamDataBidiPeerOpened(1L)
                        .initialMaxStreamDataBidiLocallyOpened(1L)
                        .build())
                .build();
        try (SessionPair pair = SessionPair.open(config)) {
            WriteTimeoutException timeout = assertThrows(
                    WriteTimeoutException.class,
                    () -> pair.client().openAndSendWithTimeout(Duration.ofMillis(50), "xy".getBytes(StandardCharsets.UTF_8))
            );
            ZmuxErrorDetails details = ZmuxErrors.details(timeout);
            assertNotNull(details);
            assertEquals("write", details.operation());
            assertTrue(details.timeout());
        }
    }

    @Test
    void writeDeadlineBlockedByEarlierProvisionalOpenCanTimeoutAndRetryWithoutBurningTheId() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            ZmuxNativeStream first = pair.client().openStream();
            ZmuxNativeStream second = pair.client().openStream();

            second.setWriteTimeout(Duration.ofMillis(50));
            WriteTimeoutException timeout = assertThrows(
                    WriteTimeoutException.class,
                    () -> second.write("b".getBytes(StandardCharsets.UTF_8))
            );
            ZmuxErrorDetails details = ZmuxErrors.details(timeout);
            assertNotNull(details);
            assertEquals("write", details.operation());
            assertTrue(details.timeout());
            assertEquals(0L, second.streamId(), "timed-out provisional write must not consume a stream ID");

            first.writeFinal("a".getBytes(StandardCharsets.UTF_8));
            assertTrue(first.streamId() != 0L, "head provisional write should commit a visible stream ID");

            second.clearWriteDeadline();
            second.writeFinal("b".getBytes(StandardCharsets.UTF_8));
            assertEquals(first.streamId() + 4L, second.streamId(), "later retry should reuse the next same-class stream ID");

            ZmuxStream firstInbound = pair.server().acceptStream(Duration.ofSeconds(5));
            ZmuxStream secondInbound = pair.server().acceptStream(Duration.ofSeconds(5));
            byte[] buffer = new byte[1];
            assertEquals(1, firstInbound.read(buffer));
            assertEquals('a', buffer[0]);
            assertEquals(-1, firstInbound.read(buffer));
            assertEquals(1, secondInbound.read(buffer));
            assertEquals('b', buffer[0]);
            assertEquals(-1, secondInbound.read(buffer));
            firstInbound.close();
            secondInbound.close();
            first.close();
            second.close();
        }
    }

    @Test
    void openAndSendWithTimeoutCarriesRemainingBudgetIntoProvisionalOpenTurnWait() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            pair.client().openStream();

            WriteTimeoutException timeout = assertThrows(
                    WriteTimeoutException.class,
                    () -> pair.client().openAndSendWithTimeout(Duration.ofMillis(50), "x".getBytes(StandardCharsets.UTF_8))
            );
            ZmuxErrorDetails details = ZmuxErrors.details(timeout);
            assertNotNull(details);
            assertEquals("write", details.operation());
            assertTrue(details.timeout());
        }
    }

    @Test
    void hugeWriteTimeoutDoesNotOverflowBlockedWriteDeadline() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .settings(Settings.builder()
                        .initialMaxData(1L)
                        .initialMaxStreamDataBidiPeerOpened(1L)
                        .initialMaxStreamDataBidiLocallyOpened(1L)
                        .build())
                .build();
        try (SessionPair pair = SessionPair.open(config)) {
            ZmuxStream outbound = pair.client().openStream();
            outbound.write("x".getBytes(StandardCharsets.UTF_8));
            ZmuxStream inbound = pair.server().acceptStream(Duration.ofSeconds(1));

            outbound.setWriteTimeout(Duration.ofSeconds(Long.MAX_VALUE));
            AtomicReference<Throwable> writeError = new AtomicReference<>();
            CountDownLatch writeDone = new CountDownLatch(1);
            Thread writer = new Thread(() -> {
                try {
                    outbound.writeFinal("y".getBytes(StandardCharsets.UTF_8));
                } catch (Throwable error) {
                    writeError.set(error);
                } finally {
                    writeDone.countDown();
                }
            }, "api-surface-huge-write-deadline");
            writer.start();

            Thread.sleep(100L);
            assertFalse(
                    writeDone.await(100L, java.util.concurrent.TimeUnit.MILLISECONDS),
                    "huge write deadline should not expire while flow-control is still blocking the write"
            );

            byte[] buffer = new byte[2];
            assertEquals(1, inbound.read(buffer, 0, 1));
            assertTrue(writeDone.await(Duration.ofSeconds(1).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS));
            writer.join(Duration.ofSeconds(1).toMillis());
            assertNull(writeError.get());
            assertEquals(1, inbound.read(buffer, 1, 1));
            assertEquals(-1, inbound.read(new byte[1]));
            inbound.close();
            outbound.close();
        }
    }

    @Test
    void structuredErrorLookupFindsNestedTypedErrors() {
        IOException wrapped = new IOException("outer", new ApplicationError(
                41L,
                "peer",
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.REMOTE,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        ));

        ZmuxErrorDetails details = ZmuxErrors.details(wrapped);
        ApplicationError applicationError = ZmuxErrors.applicationError(wrapped);

        assertNotNull(details);
        assertNotNull(applicationError);
        assertEquals(41L, details.code());
        assertEquals(41L, applicationError.code());
        assertEquals("peer", details.reason());
        assertEquals(ZmuxErrorDirection.BOTH, details.direction());
        assertTrue(ZmuxErrors.hasCode(wrapped));
        assertEquals(41L, ZmuxErrors.code(wrapped, -1L));
        assertNull(ZmuxErrors.code(wrapped), "unknown application codes should stay available through raw long helpers only");
        assertFalse(ZmuxErrors.isCode(wrapped, ErrorCode.PROTOCOL));
        assertSame(applicationError, ZmuxErrors.find(wrapped, ApplicationError.class));
        assertSame(details, ZmuxErrors.find(wrapped, ZmuxErrorDetails.class));
        assertEquals("", ZmuxErrors.operation(wrapped));
        assertEquals("peer", ZmuxErrors.reason(wrapped));
        assertEquals(ZmuxErrorScope.SESSION, ZmuxErrors.scope(wrapped));
        assertEquals(ZmuxErrorSource.REMOTE, ZmuxErrors.source(wrapped));
        assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, ZmuxErrors.terminationKind(wrapped));
        assertFalse(ZmuxErrors.timeout(wrapped));
        assertFalse(ZmuxErrors.interrupted(wrapped));
    }

    @Test
    void structuredErrorLookupFindsSuppressedTypedErrors() {
        IOException wrapped = new IOException("outer");
        ApplicationError suppressed = new ApplicationError(
                ErrorCode.PROTOCOL,
                "peer",
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.REMOTE,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        );
        wrapped.addSuppressed(suppressed);

        assertSame(suppressed, ZmuxErrors.applicationError(wrapped));
        assertSame(suppressed, ZmuxErrors.find(wrapped, ApplicationError.class));
        assertSame(suppressed, ZmuxErrors.details(wrapped));
        assertEquals("peer", ZmuxErrors.reason(wrapped));
        assertEquals(ErrorCode.PROTOCOL, ZmuxErrors.code(wrapped));
        assertTrue(ZmuxErrors.isCode(wrapped, ErrorCode.PROTOCOL));
    }

    @Test
    void typedErrorCodeHelpersRecognizeKnownStandardCodes() {
        IOException wrapped = new IOException("outer", new ApplicationError(
                ErrorCode.PROTOCOL.code(),
                "peer",
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.REMOTE,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        ));

        assertEquals(ErrorCode.PROTOCOL, ZmuxErrors.code(wrapped));
        assertTrue(ZmuxErrors.isCode(wrapped, ErrorCode.PROTOCOL));
        assertFalse(ZmuxErrors.isCode(wrapped, ErrorCode.INTERNAL));
    }

    @Test
    void capabilityErrorHelpersRecognizeAdapterUnsupportedVariants() {
        IOException wrapped = new IOException("outer", new PriorityUpdateUnavailableException());

        assertTrue(ZmuxErrors.adapterUnsupported(wrapped));
        assertTrue(ZmuxErrors.priorityUpdateUnavailable(wrapped));
        assertFalse(ZmuxErrors.adapterUnsupported(new IOException("plain")));
        assertFalse(ZmuxErrors.priorityUpdateUnavailable(new AdapterUnsupportedException("generic adapter limit")));
    }

    @Test
    void metadataAndKeepaliveHelpersRecognizeStructuredVariants() {
        IOException openInfo = new IOException("outer", new OpenInfoUnavailableException());
        IOException openMetadata = new IOException("outer", new OpenMetadataTooLargeException());
        IOException priorityUpdate = new IOException("outer", new PriorityUpdateTooLargeException());
        IOException keepalive = new IOException("outer", new ApplicationError(
                ErrorCode.IDLE_TIMEOUT,
                "zmux: keepalive timeout",
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.TIMEOUT
        ));
        IOException genericIdleTimeout = new IOException("outer", new ApplicationError(
                ErrorCode.IDLE_TIMEOUT,
                "peer idle timeout",
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.REMOTE,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.TIMEOUT
        ));

        assertTrue(ZmuxErrors.openInfoUnavailable(openInfo));
        assertTrue(ZmuxErrors.openMetadataTooLarge(openMetadata));
        assertTrue(ZmuxErrors.priorityUpdateTooLarge(priorityUpdate));
        assertTrue(ZmuxErrors.keepaliveTimeout(keepalive));

        assertFalse(ZmuxErrors.openInfoUnavailable(openMetadata));
        assertFalse(ZmuxErrors.openMetadataTooLarge(priorityUpdate));
        assertFalse(ZmuxErrors.priorityUpdateTooLarge(openInfo));
        assertFalse(ZmuxErrors.keepaliveTimeout(genericIdleTimeout));
    }

    @Test
    void streamSurfaceAndOpenHelpersRecognizeStructuredVariants() {
        IOException emptyMetadata = new IOException("outer", new EmptyMetadataUpdateException());
        IOException openLimited = new IOException("outer", new OpenLimitedException());
        IOException openExpired = new IOException("outer", new OpenExpiredException());
        IOException notReadable = new IOException("outer", new StreamNotReadableException());
        IOException notWritable = new IOException("outer", new StreamNotWritableException());
        IOException gracefulCloseTimeout = new IOException("outer", new GracefulCloseTimeoutException());
        IOException plain = new IOException("plain");

        assertTrue(ZmuxErrors.emptyMetadataUpdate(emptyMetadata));
        assertTrue(ZmuxErrors.openLimited(openLimited));
        assertTrue(ZmuxErrors.openExpired(openExpired));
        assertTrue(ZmuxErrors.streamNotReadable(notReadable));
        assertTrue(ZmuxErrors.streamNotWritable(notWritable));
        assertTrue(ZmuxErrors.gracefulCloseTimeout(gracefulCloseTimeout));

        assertFalse(ZmuxErrors.emptyMetadataUpdate(plain));
        assertFalse(ZmuxErrors.openLimited(openExpired));
        assertFalse(ZmuxErrors.openExpired(openLimited));
        assertFalse(ZmuxErrors.streamNotReadable(notWritable));
        assertFalse(ZmuxErrors.streamNotWritable(notReadable));
        assertFalse(ZmuxErrors.gracefulCloseTimeout(emptyMetadata));
    }

    @Test
    void closedStateHelpersRecognizeSessionAndDirectionalClosure() {
        IOException sessionClosed = new IOException("outer", new SessionClosedException(ZmuxErrorSource.REMOTE));
        IOException readClosed = new IOException("outer", new ReadClosedException(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.GRACEFUL));
        IOException writeClosed = new IOException("outer", new WriteClosedException(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.GRACEFUL));

        assertTrue(ZmuxErrors.sessionClosed(sessionClosed));
        assertFalse(ZmuxErrors.readClosed(sessionClosed));
        assertFalse(ZmuxErrors.writeClosed(sessionClosed));

        assertTrue(ZmuxErrors.readClosed(readClosed));
        assertFalse(ZmuxErrors.sessionClosed(readClosed));
        assertFalse(ZmuxErrors.writeClosed(readClosed));

        assertTrue(ZmuxErrors.writeClosed(writeClosed));
        assertFalse(ZmuxErrors.sessionClosed(writeClosed));
        assertFalse(ZmuxErrors.readClosed(writeClosed));
    }

    @Test
    void closedStateHelpersRecognizeSuppressedDirectionalClosure() {
        IOException aggregate = new IOException("aggregate");
        aggregate.addSuppressed(new ReadClosedException(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.GRACEFUL));
        aggregate.addSuppressed(new WriteClosedException(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.GRACEFUL));

        assertTrue(ZmuxErrors.readClosed(aggregate));
        assertTrue(ZmuxErrors.writeClosed(aggregate));
        assertFalse(ZmuxErrors.sessionClosed(aggregate));
    }

    @Test
    void genericErrorLookupReturnsNullWhenTypeIsAbsent() {
        IOException plain = new IOException("plain");

        assertNull(ZmuxErrors.find(plain, ApplicationError.class));
        assertNull(ZmuxErrors.applicationError(plain));
    }

    @Test
    void structuredErrorReasonFallsBackToTypedSentinelMessage() {
        SessionClosedException closed = new SessionClosedException(ZmuxErrorSource.REMOTE);

        assertEquals(SessionClosedException.MESSAGE, ZmuxErrors.reason(closed));
        assertEquals(ZmuxErrorScope.SESSION, ZmuxErrors.scope(closed));
        assertEquals(ZmuxErrorSource.REMOTE, ZmuxErrors.source(closed));
        assertEquals(ZmuxErrorDirection.BOTH, ZmuxErrors.direction(closed));
        assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, ZmuxErrors.terminationKind(closed));
    }

    @Test
    void defaultCloseHelpersMatchDirectionalAndBidiSemantics() throws Exception {
        AtomicReference<String> order = new AtomicReference<>("");
        ZmuxSendStream send = new ZmuxSendStream() {
            @Override
            public void write(byte[] src, int offset, int length) {
            }

            @Override
            public int writeFinal(byte[] src, int offset, int length) {
                return length;
            }

            @Override
            public void updateMetadata(MetadataUpdate update) {
            }

            @Override
            public void closeWrite() {
                order.set(order.get() + "send");
            }

            @Override
            public void cancelWrite(long code) {
            }

            @Override
            public void closeWithError(long code, String reason) {
            }

            @Override
            public void setWriteDeadline(Instant deadline) {
            }

            @Override
            public long streamId() {
                return 0L;
            }

            @Override
            public byte[] openInfo() {
                return new byte[0];
            }

            @Override
            public StreamMetadata metadata() {
                return StreamMetadata.empty();
            }

            @Override
            public SocketAddress localAddress() {
                return null;
            }

            @Override
            public SocketAddress remoteAddress() {
                return null;
            }
        };
        ZmuxRecvStream recv = new ZmuxRecvStream() {
            @Override
            public int read(byte[] dst, int offset, int length) {
                return -1;
            }

            @Override
            public void closeRead() {
                order.set(order.get() + "recv");
            }

            @Override
            public void cancelRead(long code) {
            }

            @Override
            public void closeWithError(long code, String reason) {
            }

            @Override
            public void setReadDeadline(Instant deadline) {
            }

            @Override
            public long streamId() {
                return 0L;
            }

            @Override
            public byte[] openInfo() {
                return new byte[0];
            }

            @Override
            public StreamMetadata metadata() {
                return StreamMetadata.empty();
            }

            @Override
            public SocketAddress localAddress() {
                return null;
            }

            @Override
            public SocketAddress remoteAddress() {
                return null;
            }
        };

        send.close();
        assertEquals("send", order.get());
        order.set("");

        recv.close();
        assertEquals("recv", order.get());
        order.set("");

        ZmuxStream bidi = new ZmuxStream() {
            @Override
            public int read(byte[] dst, int offset, int length) {
                return -1;
            }

            @Override
            public void write(byte[] src, int offset, int length) {
            }

            @Override
            public int writeFinal(byte[] src, int offset, int length) {
                return length;
            }

            @Override
            public void updateMetadata(MetadataUpdate update) {
            }

            @Override
            public void closeWrite() {
                order.set(order.get() + "send");
            }

            @Override
            public void cancelWrite(long code) {
            }

            @Override
            public void closeRead() {
                order.set(order.get() + "recv");
            }

            @Override
            public void cancelRead(long code) {
            }

            @Override
            public void closeWithError(long code, String reason) {
            }

            @Override
            public void setDeadline(Instant deadline) {
            }

            @Override
            public void setReadDeadline(Instant deadline) {
            }

            @Override
            public void setWriteDeadline(Instant deadline) {
            }

            @Override
            public long streamId() {
                return 0L;
            }

            @Override
            public byte[] openInfo() {
                return new byte[0];
            }

            @Override
            public StreamMetadata metadata() {
                return StreamMetadata.empty();
            }

            @Override
            public SocketAddress localAddress() {
                return null;
            }

            @Override
            public SocketAddress remoteAddress() {
                return null;
            }
        };

        bidi.close();
        assertEquals("sendrecv", order.get());
    }

    @Test
    void defaultBidiCloseAggregatesDirectionalCloseFailures() {
        IOException sendFailure = new IOException("send");
        IOException recvFailure = new IOException("recv");
        ZmuxStream bidi = new ZmuxStream() {
            @Override
            public int read(byte[] dst, int offset, int length) {
                return -1;
            }

            @Override
            public void write(byte[] src, int offset, int length) {
            }

            @Override
            public int writeFinal(byte[] src, int offset, int length) {
                return length;
            }

            @Override
            public void updateMetadata(MetadataUpdate update) {
            }

            @Override
            public void closeWrite() throws IOException {
                throw sendFailure;
            }

            @Override
            public void cancelWrite(long code) {
            }

            @Override
            public void closeRead() throws IOException {
                throw recvFailure;
            }

            @Override
            public void cancelRead(long code) {
            }

            @Override
            public void closeWithError(long code, String reason) {
            }

            @Override
            public void setDeadline(Instant deadline) {
            }

            @Override
            public void setReadDeadline(Instant deadline) {
            }

            @Override
            public void setWriteDeadline(Instant deadline) {
            }

            @Override
            public long streamId() {
                return 0L;
            }

            @Override
            public byte[] openInfo() {
                return new byte[0];
            }

            @Override
            public StreamMetadata metadata() {
                return StreamMetadata.empty();
            }

            @Override
            public SocketAddress localAddress() {
                return null;
            }

            @Override
            public SocketAddress remoteAddress() {
                return null;
            }
        };

        IOException error = assertThrows(IOException.class, bidi::close);
        assertSame(sendFailure, error);
        assertEquals(1, error.getSuppressed().length);
        assertSame(recvFailure, error.getSuppressed()[0]);
    }

    @Test
    void openTimeoutExceptionExposesStructuredTimeoutMetadata() {
        OpenTimeoutException timeout = new OpenTimeoutException();

        ZmuxErrorDetails details = ZmuxErrors.details(timeout);
        assertNotNull(details);
        assertEquals(OpenTimeoutException.MESSAGE, ZmuxErrors.reason(timeout));
        assertEquals("open", ZmuxErrors.operation(timeout));
        assertEquals("open", details.operation());
        assertEquals(ZmuxErrorScope.SESSION, details.scope());
        assertEquals(ZmuxErrorDirection.BOTH, details.direction());
        assertTrue(details.timeout());
        assertTrue(ZmuxErrors.timeout(timeout));
        assertFalse(ZmuxErrors.interrupted(timeout));
    }

    @Test
    void rawInterruptedExceptionIsRecognizedByErrorHelpers() {
        InterruptedException interrupted = new InterruptedException("stop");

        assertTrue(ZmuxErrors.interrupted(interrupted));
        assertFalse(ZmuxErrors.timeout(interrupted));
        assertEquals(ZmuxErrorScope.UNKNOWN, ZmuxErrors.scope(interrupted));
    }

    @Test
    void acceptInterruptionExposesStructuredInterruptedMetadata() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            AtomicReference<Throwable> error = new AtomicReference<>();
            CountDownLatch started = new CountDownLatch(1);
            Thread waiter = new Thread(() -> {
                started.countDown();
                try {
                    pair.client().acceptStream();
                } catch (Throwable throwable) {
                    error.set(throwable);
                }
            }, "zmux-accept-interrupt-test");

            waiter.start();
            assertTrue(started.await(Duration.ofSeconds(1).toMillis(), TimeUnit.MILLISECONDS));
            awaitBlockingState(waiter, Duration.ofSeconds(1));
            waiter.interrupt();
            waiter.join(Duration.ofSeconds(1).toMillis());

            assertFalse(waiter.isAlive(), "accept waiter should exit after interrupt");
            ZmuxInterruptedException interrupted = assertInstanceOf(ZmuxInterruptedException.class, error.get());
            assertStructuredInterrupted(interrupted, "accept");
        }
    }

    @Test
    void awaitTerminationInterruptionExposesStructuredInterruptedMetadata() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            AtomicReference<Throwable> error = new AtomicReference<>();
            CountDownLatch started = new CountDownLatch(1);
            Thread waiter = new Thread(() -> {
                started.countDown();
                try {
                    pair.client().awaitTermination(null);
                } catch (Throwable throwable) {
                    error.set(throwable);
                }
            }, "zmux-await-termination-interrupt-test");

            waiter.start();
            assertTrue(started.await(Duration.ofSeconds(1).toMillis(), TimeUnit.MILLISECONDS));
            awaitBlockingState(waiter, Duration.ofSeconds(1));
            waiter.interrupt();
            waiter.join(Duration.ofSeconds(1).toMillis());

            assertFalse(waiter.isAlive(), "awaitTermination waiter should exit after interrupt");
            ZmuxInterruptedException interrupted = assertInstanceOf(ZmuxInterruptedException.class, error.get());
            assertStructuredInterrupted(interrupted, "wait");
        }
    }

    @Test
    void awaitTerminationOrThrowSurfacesPeerCloseCause() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            pair.server().closeWithError(42L, "peer close");

            ApplicationError error = assertThrows(
                    ApplicationError.class,
                    () -> pair.client().awaitTerminationOrThrow(Duration.ofSeconds(2))
            );

            assertEquals(42L, error.code());
            assertEquals("peer close", error.reason());
            assertEquals(ZmuxErrorScope.SESSION, error.scope());
            assertEquals(ZmuxErrorSource.REMOTE, error.source());
            assertTrue(pair.client().terminationCause().isPresent());
            assertEquals(42L, ZmuxErrors.code(pair.client().terminationCause().get(), -1L));
        }
    }

    @Test
    void awaitTerminationOrThrowWithoutTimeoutSurfacesPeerCloseCause() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            pair.server().closeWithError(24L, "peer close");

            ApplicationError error = assertThrows(
                    ApplicationError.class,
                    () -> pair.client().awaitTerminationOrThrow()
            );

            assertEquals(24L, error.code());
            assertEquals("peer close", error.reason());
            assertEquals(ZmuxErrorScope.SESSION, error.scope());
            assertEquals(ZmuxErrorSource.REMOTE, error.source());
            assertTrue(pair.client().terminationCause().isPresent());
            assertEquals(24L, ZmuxErrors.code(pair.client().terminationCause().get(), -1L));
        }
    }

    @Test
    void awaitTerminationCauseUsesTypedTimeout() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            SessionWaitTimeoutException timeout = assertThrows(
                    SessionWaitTimeoutException.class,
                    () -> pair.client().awaitTerminationCause(Duration.ZERO)
            );

            assertEquals("wait", timeout.operation());
            assertEquals(ZmuxErrorScope.SESSION, timeout.scope());
            assertTrue(ZmuxErrors.timeout(timeout));
        }
    }

    @Test
    void defaultAwaitTerminationHelpersUseUnboundedWaitWhenTimeoutOmitted() throws Exception {
        RecordingDefaultSendStream stream = new RecordingDefaultSendStream();
        DefaultRecordingNativeSession session = new DefaultRecordingNativeSession(stream);
        ApplicationError cause = new ApplicationError(9L, "peer close");
        session.awaitTerminationResult = true;
        session.terminationCause = java.util.Optional.of(cause);

        assertSame(cause, session.awaitTerminationCause().orElseThrow(AssertionError::new));
        assertNull(session.lastAwaitTerminationTimeout, "no-arg cause helper must use an unbounded wait");

        session.lastAwaitTerminationTimeout = Duration.ofSeconds(1);
        ApplicationError thrown = assertThrows(ApplicationError.class, session::awaitTerminationOrThrow);
        assertSame(cause, thrown);
        assertNull(session.lastAwaitTerminationTimeout, "no-arg throw helper must use an unbounded wait");
    }

    @Test
    void defaultAwaitTerminationUsesUnboundedWaitWhenTimeoutOmitted() throws Exception {
        RecordingDefaultSendStream stream = new RecordingDefaultSendStream();
        DefaultRecordingNativeSession session = new DefaultRecordingNativeSession(stream);
        session.awaitTerminationResult = true;

        assertTrue(session.awaitTermination());
        assertNull(session.lastAwaitTerminationTimeout, "no-arg await helper must use an unbounded wait");
    }

    @Test
    void defaultSessionCloseWithStructuredThrowableUsesMappedCodeAndReason() throws Exception {
        RecordingDefaultSendStream stream = new RecordingDefaultSendStream();
        DefaultRecordingNativeSession session = new DefaultRecordingNativeSession(stream);
        IOException error = new IOException("outer", new ApplicationError(24L, "peer close"));

        session.closeWithError(error);

        assertEquals(1, session.closeWithErrorCalls);
        assertEquals(24L, session.lastCloseCode);
        assertEquals("peer close", session.lastCloseReason);
        assertEquals(0, session.closeCalls, "structured close helper must not degrade into graceful close");
    }

    @Test
    void defaultSessionCloseWithNullThrowableFallsBackToGracefulClose() throws Exception {
        RecordingDefaultSendStream stream = new RecordingDefaultSendStream();
        DefaultRecordingNativeSession session = new DefaultRecordingNativeSession(stream);

        session.closeWithError((Throwable) null);

        assertEquals(1, session.closeCalls);
        assertEquals(0, session.closeWithErrorCalls, "null helper must preserve graceful close semantics");
    }

    @Test
    void protocolPeerVisibleSemanticHelpersMatchCapabilityCarriageRules() {
        long openPriority = Protocol.CAPABILITY_OPEN_METADATA | Protocol.CAPABILITY_PRIORITY_HINTS;
        long updatePriority = Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS;
        long openGroup = Protocol.CAPABILITY_OPEN_METADATA | Protocol.CAPABILITY_STREAM_GROUPS;
        long updateGroup = Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_STREAM_GROUPS;

        assertEquals(Protocol.CAPABILITY_MULTILINK_BASIC_RETIRED, Protocol.CAPABILITY_MULTILINK_BASIC);
        assertEquals(2L, Protocol.EXT_ML_READY_RETIRED);
        assertEquals(6L, Protocol.EXT_ML_DRAIN_ACK_RETIRED);

        assertTrue(Protocol.hasPeerVisiblePrioritySemantics(openPriority));
        assertTrue(Protocol.hasPeerVisiblePrioritySemantics(updatePriority));
        assertFalse(Protocol.hasPeerVisiblePrioritySemantics(Protocol.CAPABILITY_PRIORITY_HINTS));

        assertTrue(Protocol.hasPeerVisibleGroupSemantics(openGroup));
        assertTrue(Protocol.hasPeerVisibleGroupSemantics(updateGroup));
        assertFalse(Protocol.hasPeerVisibleGroupSemantics(Protocol.CAPABILITY_STREAM_GROUPS));
    }

    @Test
    void publicCodecFacadeRoundTripsVarintsTlvsFramesAndPreface() throws Exception {
        byte[] varint = ZmuxCodec.encodeVarint(16_384L);
        DecodedVarint decoded = ZmuxCodec.parseVarint(varint);
        assertEquals(16_384L, decoded.value());
        assertEquals(varint.length, decoded.length());

        byte[] tlvBytes = ZmuxCodec.appendTlv(null, Protocol.METADATA_STREAM_PRIORITY, ZmuxCodec.encodeVarint(7L));
        Tlv tlv = ZmuxCodec.parseTlvs(tlvBytes).get(0);
        assertEquals(Protocol.METADATA_STREAM_PRIORITY, tlv.type());
        assertEquals(7L, ZmuxCodec.parseVarint(tlv.value()).value());

        Frame frame = new Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 4L, "ok".getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream encodedFrame = new ByteArrayOutputStream();
        ZmuxCodec.writeFrame(encodedFrame, frame, Settings.defaults().limits());
        ParsedFrame parsed = ZmuxCodec.parseFrame(encodedFrame.toByteArray(), Settings.defaults().limits());
        assertEquals(encodedFrame.size(), parsed.bytesRead());
        assertEquals(frame.type(), parsed.frame().type());
        assertEquals(frame.flags(), parsed.frame().flags());
        assertEquals(frame.streamId(), parsed.frame().streamId());
        assertEquals("ok", new String(parsed.frame().payload(), StandardCharsets.UTF_8));

        Preface preface = ZmuxConfig.defaults().withRole(Role.INITIATOR).localPreface();
        ByteArrayOutputStream encodedPreface = new ByteArrayOutputStream();
        ZmuxCodec.writePreface(encodedPreface, preface);
        assertEquals(preface, ZmuxCodec.parsePreface(encodedPreface.toByteArray()));
    }

    @Test
    void joinedDuplexConnectionCanPauseReplaceAndResumeHalves() throws Exception {
        ByteArrayOutputStream firstOutput = new ByteArrayOutputStream();
        JoinedDuplexConnection joined = new JoinedDuplexConnection(
                new ByteArrayInputStream("a".getBytes(StandardCharsets.UTF_8)),
                firstOutput
        );

        assertEquals('a', joined.input().read());
        JoinedDuplexConnection.PausedInput input = joined.pauseInput(Duration.ofSeconds(1));
        assertEquals(-1, input.current().read());
        input.set(new ByteArrayInputStream("b".getBytes(StandardCharsets.UTF_8)));
        input.resume();
        assertEquals('b', joined.input().read());

        joined.output().write("x".getBytes(StandardCharsets.UTF_8));
        JoinedDuplexConnection.PausedOutput output = joined.pauseOutput(Duration.ofSeconds(1));
        ByteArrayOutputStream secondOutput = new ByteArrayOutputStream();
        assertEquals(firstOutput, output.current());
        output.set(secondOutput);
        output.resume();
        joined.output().write("y".getBytes(StandardCharsets.UTF_8));

        assertEquals("x", firstOutput.toString(StandardCharsets.UTF_8.name()));
        assertEquals("y", secondOutput.toString(StandardCharsets.UTF_8.name()));
        joined.close();
    }

    @Test
    void joinedDuplexConnectionCanServeAsSessionTransport() throws Exception {
        ServerSocket listener = new ServerSocket(0);
        Socket clientSocket = new Socket("127.0.0.1", listener.getLocalPort());
        Socket serverSocket = listener.accept();
        listener.close();

        JoinedDuplexConnection clientTransport = new JoinedDuplexConnection(
                clientSocket.getInputStream(),
                clientSocket.getOutputStream(),
                clientSocket.getChannel(),
                clientSocket.getLocalSocketAddress(),
                clientSocket.getRemoteSocketAddress()
        );
        JoinedDuplexConnection serverTransport = new JoinedDuplexConnection(
                serverSocket.getInputStream(),
                serverSocket.getOutputStream(),
                serverSocket.getChannel(),
                serverSocket.getLocalSocketAddress(),
                serverSocket.getRemoteSocketAddress()
        );

        AtomicReference<ZmuxNativeSession> clientRef = new AtomicReference<>();
        AtomicReference<ZmuxNativeSession> serverRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        CountDownLatch established = new CountDownLatch(2);

        Thread clientThread = new Thread(() -> {
            try {
                clientRef.set(Zmux.client(clientTransport));
            } catch (Throwable error) {
                errorRef.compareAndSet(null, error);
            } finally {
                established.countDown();
            }
        }, "api-surface-joined-transport-client");
        clientThread.start();

        Thread serverThread = new Thread(() -> {
            try {
                serverRef.set(Zmux.server(serverTransport));
            } catch (Throwable error) {
                errorRef.compareAndSet(null, error);
            } finally {
                established.countDown();
            }
        }, "api-surface-joined-transport-server");
        serverThread.start();

        established.await();
        rethrow(errorRef.get());

        try (ZmuxNativeSession client = clientRef.get();
             ZmuxNativeSession server = serverRef.get();
             ZmuxNativeStream outbound = client.openStream()) {
            outbound.writeFinal("ping".getBytes(StandardCharsets.UTF_8));

            try (ZmuxNativeStream inbound = server.acceptStream(Duration.ofSeconds(2))) {
                assertEquals("ping", new String(inbound.readAllBytes(), StandardCharsets.UTF_8));
                inbound.writeFinal("pong".getBytes(StandardCharsets.UTF_8));
            }

            assertEquals("pong", new String(outbound.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void joinedConnectionBridgesOuterUniStreamsAsDuplexTransport() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            ZmuxSendStream clientSend = pair.client().openUniStream();
            ZmuxSendStream serverSend = pair.server().openUniStream();

            clientSend.write("hi".getBytes(StandardCharsets.UTF_8));
            serverSend.write("yo".getBytes(StandardCharsets.UTF_8));

            try (ZmuxRecvStream clientRecv = pair.client().acceptUniStream(Duration.ofSeconds(2));
                 ZmuxRecvStream serverRecv = pair.server().acceptUniStream(Duration.ofSeconds(2));
                 JoinedDuplexConnection clientConn = Zmux.join(clientRecv, clientSend);
                 JoinedDuplexConnection serverConn = Zmux.join(serverRecv, serverSend)) {
                byte[] buffer = new byte[8];

                int serverRead = serverConn.input().read(buffer);
                assertEquals("hi", new String(buffer, 0, serverRead, StandardCharsets.UTF_8));

                int clientRead = clientConn.input().read(buffer);
                assertEquals("yo", new String(buffer, 0, clientRead, StandardCharsets.UTF_8));

                clientConn.output().write("ping".getBytes(StandardCharsets.UTF_8));
                int serverSecondRead = serverConn.input().read(buffer);
                assertEquals("ping", new String(buffer, 0, serverSecondRead, StandardCharsets.UTF_8));

                serverConn.output().write("pong".getBytes(StandardCharsets.UTF_8));
                int clientSecondRead = clientConn.input().read(buffer);
                assertEquals("pong", new String(buffer, 0, clientSecondRead, StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void joinedConnectionCloseWriteProducesPeerEofOnUniRead() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            ZmuxSendStream clientSend = pair.client().openUniStream();

            try (JoinedDuplexConnection connection = Zmux.join((ZmuxRecvStream) null, clientSend)) {
                connection.closeWrite();
            }

            try (ZmuxRecvStream serverRecv = pair.server().acceptUniStream(Duration.ofSeconds(2))) {
                assertEquals(-1, serverRecv.read(new byte[1]));
            }
        }
    }

    @Test
    void joinedConnectionDeadlineViaOuterUniStreams() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            ZmuxSendStream clientSend = pair.client().openUniStream();
            ZmuxSendStream serverSend = pair.server().openUniStream();

            clientSend.write("a".getBytes(StandardCharsets.UTF_8));
            serverSend.write("b".getBytes(StandardCharsets.UTF_8));

            try (ZmuxRecvStream clientRecv = pair.client().acceptUniStream(Duration.ofSeconds(2));
                 JoinedDuplexConnection connection = Zmux.join(clientRecv, clientSend)) {
                byte[] buffer = new byte[1];
                assertEquals(1, connection.input().read(buffer));
                assertEquals("b", new String(buffer, StandardCharsets.UTF_8));

                connection.setReadDeadline(Instant.now().plusMillis(30));
                SocketTimeoutException timeout = assertThrows(
                        SocketTimeoutException.class,
                        () -> connection.input().read(new byte[1])
                );
                assertTrue(ZmuxErrors.timeout(timeout));
            }
        }
    }

    @Test
    void outerBidiStreamAdapterCanServeAsNestedSessionTransport() throws Exception {
        try (SessionPair outer = SessionPair.open()) {
            ZmuxNativeStream clientOuterStream = outer.client().openStream();
            AtomicReference<ZmuxNativeSession> clientRef = new AtomicReference<>();
            AtomicReference<Throwable> errorRef = new AtomicReference<>();
            CountDownLatch clientEstablished = new CountDownLatch(1);
            DuplexConnection clientTransport = ZmuxConnections.of(clientOuterStream);

            Thread clientThread = new Thread(() -> {
                try {
                    clientRef.set(Zmux.client(clientTransport));
                } catch (Throwable error) {
                    errorRef.compareAndSet(null, error);
                } finally {
                    clientEstablished.countDown();
                }
            }, "api-surface-nested-bidi-client");
            clientThread.start();

            ZmuxNativeStream serverOuterStream = outer.server().acceptStream(Duration.ofSeconds(2));
            DuplexConnection serverTransport = ZmuxConnections.of(serverOuterStream);
            ZmuxNativeSession server = null;
            try {
                server = Zmux.server(serverTransport);
            } catch (Throwable error) {
                errorRef.compareAndSet(null, error);
            }

            clientEstablished.await();
            rethrow(errorRef.get());

            try (ZmuxNativeSession client = clientRef.get();
                 ZmuxNativeSession serverSession = server;
                 ZmuxNativeStream outbound = client.openStream()) {
                outbound.writeFinal("inner-ping".getBytes(StandardCharsets.UTF_8));

                try (ZmuxNativeStream inbound = serverSession.acceptStream(Duration.ofSeconds(2))) {
                    assertEquals("inner-ping", new String(inbound.readAllBytes(), StandardCharsets.UTF_8));
                    inbound.writeFinal("inner-pong".getBytes(StandardCharsets.UTF_8));
                }

                assertEquals("inner-pong", new String(outbound.readAllBytes(), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void defaultWritevFinalRejectsNullPartsBeforeClosingWrite() {
        RecordingDefaultSendStream stream = new RecordingDefaultSendStream();

        NullPointerException error = assertThrows(
                NullPointerException.class,
                () -> stream.writevFinal((byte[][]) null)
        );

        assertEquals("parts", error.getMessage());
        assertEquals(0, stream.writeCalls, "null multipart array must not partially write");
        assertEquals(0, stream.writeFinalCalls, "null multipart array must not close write");
    }

    @Test
    void defaultSendHelpersRejectNullPayloadBeforeDelegating() {
        RecordingDefaultSendStream stream = new RecordingDefaultSendStream();

        NullPointerException writeError = assertThrows(NullPointerException.class, () -> stream.write((byte[]) null));
        NullPointerException finalError = assertThrows(NullPointerException.class, () -> stream.writeFinal((byte[]) null));

        assertEquals("src", writeError.getMessage());
        assertEquals("src", finalError.getMessage());
        assertEquals(0, stream.writeCalls, "null write payload must not delegate");
        assertEquals(0, stream.writeFinalCalls, "null final payload must not close write");
    }

    @Test
    void defaultSendHelperTreatsZeroLengthWriteAsLocalNoOp() throws Exception {
        RecordingDefaultSendStream stream = new RecordingDefaultSendStream();

        stream.write(new byte[0]);

        assertEquals(0, stream.writeCalls, "empty ordinary write must not delegate");
        assertEquals(0, stream.writeFinalCalls, "empty ordinary write must not close write");
    }

    @Test
    void defaultOpenUniAndSendWithTimeoutTreatsEmptyPayloadAsEmptyFinalWrite() throws Exception {
        RecordingDefaultSendStream stream = new RecordingDefaultSendStream();
        DefaultRecordingNativeSession session = new DefaultRecordingNativeSession(stream);

        ZmuxSendStream result = session.openUniAndSendWithTimeout(OpenOptions.empty(), Duration.ofMillis(50), new byte[0]);

        assertSame(stream, result);
        assertEquals(1, session.openUniStreamWithTimeoutCalls, "default helper must delegate through openUniStreamWithTimeout");
        assertEquals(0, stream.writeCalls, "empty final helper must not issue ordinary writes");
        assertEquals(1, stream.writeFinalCalls, "empty payload must still use writeFinal semantics");
        assertEquals(0, stream.closeWriteCalls, "empty payload must not degrade into closeWrite");
        assertEquals(0, stream.lastFinalLength);
        assertEquals(1, stream.writeDeadlineSetCalls, "bounded timeout must set a write deadline around the final write");
        assertEquals(1, stream.writeDeadlineClearCalls, "bounded timeout must clear the temporary write deadline");
        assertNotNull(stream.lastWriteDeadline);
    }

    @Test
    void nativeGoAwayHelperDefaultsToNoErrorPayload() throws Exception {
        RecordingDefaultSendStream stream = new RecordingDefaultSendStream();
        DefaultRecordingNativeSession session = new DefaultRecordingNativeSession(stream);

        session.goAway(4L, 8L);

        assertEquals(1, session.goAwayCalls);
        assertEquals(4L, session.lastGoAwayBidi);
        assertEquals(8L, session.lastGoAwayUni);
        assertEquals(ErrorCode.NO_ERROR.code(), session.lastGoAwayCode);
        assertEquals("", session.lastGoAwayReason);
    }

    @Test
    void nativeGoAwayEnumHelperDelegatesCodeValue() throws Exception {
        RecordingDefaultSendStream stream = new RecordingDefaultSendStream();
        DefaultRecordingNativeSession session = new DefaultRecordingNativeSession(stream);

        session.goAway(4L, 8L, ErrorCode.PROTOCOL, "bad frame");

        assertEquals(1, session.goAwayCalls);
        assertEquals(ErrorCode.PROTOCOL.code(), session.lastGoAwayCode);
        assertEquals("bad frame", session.lastGoAwayReason);
    }

    @Test
    void nativeGoAwayWithErrorHelpersDelegateCodeAndReason() throws Exception {
        RecordingDefaultSendStream stream = new RecordingDefaultSendStream();
        DefaultRecordingNativeSession session = new DefaultRecordingNativeSession(stream);

        session.goAwayWithError(5L, 9L, 77L, "custom");
        assertEquals(1, session.goAwayCalls);
        assertEquals(5L, session.lastGoAwayBidi);
        assertEquals(9L, session.lastGoAwayUni);
        assertEquals(77L, session.lastGoAwayCode);
        assertEquals("custom", session.lastGoAwayReason);

        session.goAwayWithError(6L, 10L, ErrorCode.PROTOCOL, "bad frame");
        assertEquals(2, session.goAwayCalls);
        assertEquals(6L, session.lastGoAwayBidi);
        assertEquals(10L, session.lastGoAwayUni);
        assertEquals(ErrorCode.PROTOCOL.code(), session.lastGoAwayCode);
        assertEquals("bad frame", session.lastGoAwayReason);
    }

    @Test
    void defaultDirectionalEnumHelpersDelegateCodeValues() throws Exception {
        RecordingDefaultSendStream send = new RecordingDefaultSendStream();
        RecordingDefaultRecvStream recv = new RecordingDefaultRecvStream();
        DefaultRecordingNativeSession session = new DefaultRecordingNativeSession(send);

        send.cancelWrite(ErrorCode.CANCELLED);
        send.closeWithError(ErrorCode.STREAM_CLOSED, "done");
        recv.cancelRead(ErrorCode.CANCELLED);
        recv.closeWithError(ErrorCode.STREAM_CLOSED, "done");
        session.closeWithError(ErrorCode.SESSION_CLOSING, "closing");

        assertEquals(ErrorCode.CANCELLED.code(), send.lastCancelWriteCode);
        assertEquals(ErrorCode.STREAM_CLOSED.code(), send.lastCloseWithErrorCode);
        assertEquals("done", send.lastCloseWithErrorReason);
        assertEquals(ErrorCode.CANCELLED.code(), recv.lastCancelReadCode);
        assertEquals(ErrorCode.STREAM_CLOSED.code(), recv.lastCloseWithErrorCode);
        assertEquals("done", recv.lastCloseWithErrorReason);
        assertEquals(1, session.closeWithErrorCalls);
        assertEquals(ErrorCode.SESSION_CLOSING.code(), session.lastCloseCode);
        assertEquals("closing", session.lastCloseReason);
    }

    @Test
    void nativePingHelpersUseUnboundedTimeoutAndPreservePayloadShape() throws Exception {
        RecordingDefaultSendStream stream = new RecordingDefaultSendStream();
        DefaultRecordingNativeSession session = new DefaultRecordingNativeSession(stream);
        session.pingResult = Duration.ofMillis(12);

        assertEquals(Duration.ofMillis(12), session.ping("ok".getBytes(StandardCharsets.UTF_8)));
        assertArrayEquals("ok".getBytes(StandardCharsets.UTF_8), session.lastPingPayload);
        assertNull(session.lastPingTimeout, "payload-only ping helper must use an unbounded wait");

        session.lastPingPayload = "sentinel".getBytes(StandardCharsets.UTF_8);
        session.lastPingTimeout = Duration.ofSeconds(1);
        assertEquals(Duration.ofMillis(12), session.ping());
        assertNull(session.lastPingPayload, "no-arg ping helper must preserve nil payload semantics");
        assertNull(session.lastPingTimeout, "no-arg ping helper must use an unbounded wait");
    }

    @Test
    void defaultRecvHelperRejectsNullPayloadBeforeDelegating() {
        RecordingDefaultRecvStream stream = new RecordingDefaultRecvStream();

        NullPointerException error = assertThrows(NullPointerException.class, () -> stream.read((byte[]) null));

        assertEquals("dst", error.getMessage());
        assertEquals(0, stream.readCalls, "null read target must not delegate");
    }

    @Test
    void defaultRecvHelperTreatsZeroLengthReadAsLocalNoOp() throws Exception {
        RecordingDefaultRecvStream stream = new RecordingDefaultRecvStream();

        int read = stream.read(new byte[0]);

        assertEquals(0, read);
        assertEquals(0, stream.readCalls, "empty read target must not delegate");
    }

    @Test
    void defaultReadAllBytesHelperDrainsStreamAndEnforcesLimit() throws Exception {
        RecordingDefaultRecvStream stream = new RecordingDefaultRecvStream("payload".getBytes(StandardCharsets.UTF_8));

        assertArrayEquals("payload".getBytes(StandardCharsets.UTF_8), stream.readAllBytes());
        assertEquals(2, stream.readCalls, "readAllBytes should stop after the first EOF");

        RecordingDefaultRecvStream limited = new RecordingDefaultRecvStream("abc".getBytes(StandardCharsets.UTF_8));
        ZmuxException tooLarge = assertThrows(ZmuxException.class, () -> limited.readAllBytes(2));
        assertEquals(ErrorCode.FRAME_SIZE.code(), tooLarge.code());
        assertEquals("readAllBytes", tooLarge.operation());
        assertEquals(ZmuxErrorScope.STREAM, tooLarge.scope());
        assertEquals(ZmuxErrorDirection.READ, tooLarge.direction());
    }

    @Test
    void defaultByteBufferHelpersAdvancePositionsAndDelegateWithoutExtraCopyForArrayBackedBuffers() throws Exception {
        RecordingDefaultSendStream send = new RecordingDefaultSendStream();
        ByteBuffer writeBuffer = ByteBuffer.wrap("abcd".getBytes(StandardCharsets.UTF_8));
        writeBuffer.position(1);
        writeBuffer.limit(3);

        assertEquals(2, send.write(writeBuffer));

        assertEquals(3, writeBuffer.position());
        assertEquals(1, send.writeCalls);
        assertArrayEquals("bc".getBytes(StandardCharsets.UTF_8), send.lastWriteBytes);

        RecordingDefaultRecvStream recv = new RecordingDefaultRecvStream("xy".getBytes(StandardCharsets.UTF_8));
        ByteBuffer readBuffer = ByteBuffer.allocate(4);
        readBuffer.position(1);

        assertEquals(2, recv.read(readBuffer));

        assertEquals(3, readBuffer.position());
        assertEquals((byte) 0, readBuffer.get(0));
        assertEquals((byte) 'x', readBuffer.get(1));
        assertEquals((byte) 'y', readBuffer.get(2));
    }

    @Test
    void defaultByteBufferHelpersSupportDirectBuffersWithoutChangingSemantics() throws Exception {
        RecordingDefaultSendStream send = new RecordingDefaultSendStream();
        ByteBuffer writeBuffer = ByteBuffer.allocateDirect(4);
        writeBuffer.put("abcd".getBytes(StandardCharsets.UTF_8));
        writeBuffer.flip();
        writeBuffer.position(1);
        writeBuffer.limit(3);

        assertEquals(2, send.write(writeBuffer));

        assertEquals(3, writeBuffer.position());
        assertEquals(1, send.writeCalls);
        assertArrayEquals("bc".getBytes(StandardCharsets.UTF_8), send.lastWriteBytes);

        RecordingDefaultSendStream finalSend = new RecordingDefaultSendStream();
        ByteBuffer finalBuffer = ByteBuffer.allocateDirect(5);
        finalBuffer.put("vwxyz".getBytes(StandardCharsets.UTF_8));
        finalBuffer.flip();
        finalBuffer.position(2);

        assertEquals(3, finalSend.writeFinal(finalBuffer));

        assertEquals(5, finalBuffer.position());
        assertEquals(0, finalSend.writeCalls);
        assertEquals(1, finalSend.writeFinalCalls);
        assertArrayEquals("xyz".getBytes(StandardCharsets.UTF_8), finalSend.lastFinalBytes);

        RecordingDefaultRecvStream recv = new RecordingDefaultRecvStream("xy".getBytes(StandardCharsets.UTF_8));
        ByteBuffer readBuffer = ByteBuffer.allocateDirect(4);
        readBuffer.position(1);

        assertEquals(2, recv.read(readBuffer));

        assertEquals(3, readBuffer.position());
        assertEquals((byte) 0, readBuffer.get(0));
        assertEquals((byte) 'x', readBuffer.get(1));
        assertEquals((byte) 'y', readBuffer.get(2));
    }

    @Test
    void defaultStreamViewsDelegateCloseToDirectionalClose() throws Exception {
        RecordingDefaultRecvStream recv = new RecordingDefaultRecvStream("q".getBytes(StandardCharsets.UTF_8));
        InputStream input = recv.asInputStream();
        assertEquals('q', input.read());
        assertEquals(-1, input.read());
        input.close();
        assertEquals(1, recv.closeReadCalls);

        RecordingDefaultSendStream send = new RecordingDefaultSendStream();
        OutputStream output = send.asOutputStream();
        output.write('z');
        output.close();

        assertEquals(1, send.writeCalls);
        assertArrayEquals(new byte[]{'z'}, send.lastWriteBytes);
        assertEquals(1, send.closeWriteCalls);
    }

    @Test
    void defaultBidiDeadlineHelpersHideNullClearing() throws Exception {
        RecordingDefaultBidiStream stream = new RecordingDefaultBidiStream();

        stream.setTimeout(Duration.ofMillis(25));
        assertNotNull(stream.lastDeadline);

        stream.clearDeadline();
        assertEquals(1, stream.deadlineSetCalls);
        assertEquals(1, stream.deadlineClearCalls);
        assertNull(stream.lastDeadline);
    }

    @Test
    void defaultWritevFinalRejectsNullPartBeforePartialWrite() {
        RecordingDefaultSendStream stream = new RecordingDefaultSendStream();

        NullPointerException error = assertThrows(
                NullPointerException.class,
                () -> stream.writevFinal("a".getBytes(StandardCharsets.UTF_8), null)
        );

        assertEquals("parts[1]", error.getMessage());
        assertEquals(0, stream.writeCalls, "null multipart element must be rejected before any write");
        assertEquals(0, stream.writeFinalCalls, "null multipart element must not close write");
    }

    @Test
    void defaultWritevFinalEmptyPartsStillClosesWriteWithEmptyFinalPayload() throws Exception {
        RecordingDefaultSendStream stream = new RecordingDefaultSendStream();

        int written = stream.writevFinal();

        assertEquals(0, written);
        assertEquals(0, stream.writeCalls);
        assertEquals(1, stream.writeFinalCalls);
        assertEquals(0, stream.lastFinalLength);
    }

    @Test
    void defaultWritevFinalSkipsEmptyNonFinalParts() throws Exception {
        RecordingDefaultSendStream stream = new RecordingDefaultSendStream();

        int written = stream.writevFinal(new byte[0], "x".getBytes(StandardCharsets.UTF_8), new byte[0]);

        assertEquals(1, written);
        assertEquals(0, stream.writeCalls);
        assertEquals(1, stream.writeFinalCalls);
        assertEquals(1, stream.lastFinalLength);
    }

    private static final class SessionPair implements AutoCloseable {
        private final ZmuxNativeSession client;
        private final ZmuxNativeSession server;

        private SessionPair(ZmuxNativeSession client, ZmuxNativeSession server) {
            this.client = client;
            this.server = server;
        }

        static SessionPair open() throws Exception {
            return open(ZmuxConfig.defaults());
        }

        static SessionPair open(ZmuxConfig config) throws Exception {
            return open(config, false);
        }

        static SessionPair openWithoutAddresses() throws Exception {
            return open(ZmuxConfig.defaults(), true);
        }

        private static SessionPair open(ZmuxConfig config, boolean omitAddresses) throws Exception {
            ServerSocket listener = new ServerSocket(0);
            Socket clientSocket = new Socket("127.0.0.1", listener.getLocalPort());
            Socket serverSocket = listener.accept();
            listener.close();

            AtomicReference<ZmuxNativeSession> clientRef = new AtomicReference<>();
            AtomicReference<ZmuxNativeSession> serverRef = new AtomicReference<>();
            AtomicReference<Throwable> errorRef = new AtomicReference<>();
            CountDownLatch established = new CountDownLatch(2);

            Thread clientThread = new Thread(() -> {
                try {
                    clientRef.set(
                            omitAddresses
                                    ? Zmux.client(clientSocket.getInputStream(), clientSocket.getOutputStream(), config)
                                    : Zmux.client(clientSocket, config)
                    );
                } catch (Throwable t) {
                    errorRef.compareAndSet(null, t);
                } finally {
                    established.countDown();
                }
            }, "api-surface-client-open");
            clientThread.start();

            Thread serverThread = new Thread(() -> {
                try {
                    serverRef.set(
                            omitAddresses
                                    ? Zmux.server(serverSocket.getInputStream(), serverSocket.getOutputStream(), config)
                                    : Zmux.server(serverSocket, config)
                    );
                } catch (Throwable t) {
                    errorRef.compareAndSet(null, t);
                } finally {
                    established.countDown();
                }
            }, "api-surface-server-open");
            serverThread.start();

            established.await();
            rethrow(errorRef.get());
            return new SessionPair(clientRef.get(), serverRef.get());
        }

        ZmuxNativeSession client() {
            return client;
        }

        ZmuxNativeSession server() {
            return server;
        }

        @Override
        public void close() throws Exception {
            IOException error = null;
            try {
                client.close();
            } catch (IOException e) {
                error = e;
            }
            try {
                server.close();
            } catch (IOException e) {
                if (error == null) {
                    error = e;
                }
            }
            if (error != null) {
                throw error;
            }
        }
    }

    private static final class RecordingDefaultSendStream implements ZmuxNativeSendStream {
        private int writeCalls;
        private int writeFinalCalls;
        private int closeWriteCalls;
        private int writeDeadlineSetCalls;
        private int writeDeadlineClearCalls;
        private long lastCancelWriteCode = Long.MIN_VALUE;
        private long lastCloseWithErrorCode = Long.MIN_VALUE;
        private int lastWriteLength = -1;
        private int lastFinalLength = -1;
        private byte[] lastWriteBytes = new byte[0];
        private byte[] lastFinalBytes = new byte[0];
        private String lastCloseWithErrorReason;
        private Instant lastWriteDeadline;

        @Override
        public void write(byte[] src, int offset, int length) {
            this.writeCalls++;
            this.lastWriteLength = length;
            this.lastWriteBytes = java.util.Arrays.copyOfRange(src, offset, offset + length);
        }

        @Override
        public int writeFinal(byte[] src, int offset, int length) {
            this.writeFinalCalls++;
            this.lastFinalLength = length;
            this.lastFinalBytes = java.util.Arrays.copyOfRange(src, offset, offset + length);
            return length;
        }

        @Override
        public void updateMetadata(MetadataUpdate update) {
        }

        @Override
        public void closeWrite() {
            this.closeWriteCalls++;
        }

        @Override
        public void cancelWrite(long code) {
            this.lastCancelWriteCode = code;
        }

        @Override
        public void closeWithError(long code, String reason) {
            this.lastCloseWithErrorCode = code;
            this.lastCloseWithErrorReason = reason;
        }

        @Override
        public void setWriteDeadline(Instant deadline) {
            if (deadline == null) {
                this.writeDeadlineClearCalls++;
            } else {
                this.writeDeadlineSetCalls++;
                this.lastWriteDeadline = deadline;
            }
        }

        @Override
        public void close() {
        }

        @Override
        public long streamId() {
            return 0L;
        }

        @Override
        public byte[] openInfo() {
            return new byte[0];
        }

        @Override
        public StreamMetadata metadata() {
            return StreamMetadata.empty();
        }

        @Override
        public SocketAddress localAddress() {
            return null;
        }

        @Override
        public SocketAddress remoteAddress() {
            return null;
        }

        @Override
        public boolean openedLocally() {
            return true;
        }

        @Override
        public boolean bidirectional() {
            return false;
        }

        @Override
        public boolean writeClosed() {
            return closeWriteCalls > 0 || writeFinalCalls > 0;
        }
    }

    private static final class RecordingDefaultBidiStream implements ZmuxNativeStream {
        private Instant lastDeadline;
        private int deadlineSetCalls;
        private int deadlineClearCalls;

        @Override
        public int read(byte[] dst, int offset, int length) {
            return -1;
        }

        @Override
        public void write(byte[] src, int offset, int length) {
        }

        @Override
        public int writeFinal(byte[] src, int offset, int length) {
            return length;
        }

        @Override
        public void updateMetadata(MetadataUpdate update) {
        }

        @Override
        public void closeRead() {
        }

        @Override
        public void cancelRead(long code) {
        }

        @Override
        public void closeWrite() {
        }

        @Override
        public void cancelWrite(long code) {
        }

        @Override
        public void closeWithError(long code, String reason) {
        }

        @Override
        public void setDeadline(Instant deadline) {
            lastDeadline = deadline;
            if (deadline == null) {
                deadlineClearCalls++;
            } else {
                deadlineSetCalls++;
            }
        }

        @Override
        public void setReadDeadline(Instant deadline) {
            setDeadline(deadline);
        }

        @Override
        public void setWriteDeadline(Instant deadline) {
            setDeadline(deadline);
        }

        @Override
        public void close() {
        }

        @Override
        public long streamId() {
            return 0L;
        }

        @Override
        public byte[] openInfo() {
            return new byte[0];
        }

        @Override
        public StreamMetadata metadata() {
            return StreamMetadata.empty();
        }

        @Override
        public SocketAddress localAddress() {
            return null;
        }

        @Override
        public SocketAddress remoteAddress() {
            return null;
        }

        @Override
        public boolean openedLocally() {
            return true;
        }

        @Override
        public boolean bidirectional() {
            return true;
        }

        @Override
        public boolean readClosed() {
            return false;
        }

        @Override
        public boolean writeClosed() {
            return false;
        }
    }

    private static final class DefaultRecordingNativeSession implements ZmuxNativeSession {
        private final RecordingDefaultSendStream stream;
        private final RecordingDefaultBidiStream bidiStream = new RecordingDefaultBidiStream();
        private int openStreamWithTimeoutCalls;
        private int openUniStreamWithTimeoutCalls;
        private int goAwayCalls;
        private int closeCalls;
        private int closeWithErrorCalls;
        private long lastGoAwayBidi;
        private long lastGoAwayUni;
        private long lastGoAwayCode;
        private long lastCloseCode;
        private String lastGoAwayReason;
        private String lastCloseReason;
        private byte[] lastPingPayload;
        private Duration lastPingTimeout;
        private Duration pingResult = Duration.ZERO;
        private OpenOptions lastOpenStreamOptions;
        private Duration lastOpenStreamTimeout;
        private OpenOptions lastOpenUniStreamOptions;
        private Duration lastOpenUniStreamTimeout;
        private Duration lastAwaitTerminationTimeout;
        private boolean awaitTerminationResult;
        private java.util.Optional<IOException> terminationCause = java.util.Optional.empty();

        private DefaultRecordingNativeSession(RecordingDefaultSendStream stream) {
            this.stream = stream;
        }

        @Override
        public ZmuxNativeSendStream openUniStreamWithTimeout(OpenOptions options, Duration timeout) {
            this.openUniStreamWithTimeoutCalls++;
            this.lastOpenUniStreamOptions = options;
            this.lastOpenUniStreamTimeout = timeout;
            return stream;
        }

        @Override
        public ZmuxNativeStream acceptStream() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ZmuxNativeStream acceptStream(Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ZmuxNativeRecvStream acceptUniStream() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ZmuxNativeRecvStream acceptUniStream(Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ZmuxNativeStream openStream() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ZmuxNativeStream openStream(OpenOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ZmuxNativeStream openStreamWithTimeout(Duration timeout) {
            return openStreamWithTimeout(OpenOptions.empty(), timeout);
        }

        @Override
        public ZmuxNativeStream openStreamWithTimeout(OpenOptions options, Duration timeout) {
            this.openStreamWithTimeoutCalls++;
            this.lastOpenStreamOptions = options;
            this.lastOpenStreamTimeout = timeout;
            return bidiStream;
        }

        @Override
        public ZmuxNativeSendStream openUniStream() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ZmuxNativeSendStream openUniStream(OpenOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ZmuxNativeSendStream openUniStreamWithTimeout(Duration timeout) {
            return openUniStreamWithTimeout(OpenOptions.empty(), timeout);
        }

        @Override
        public ZmuxNativeStream openAndSend(byte[] data) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ZmuxNativeStream openAndSend(OpenOptions options, byte[] data) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ZmuxNativeSendStream openUniAndSend(byte[] data) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ZmuxNativeSendStream openUniAndSend(OpenOptions options, byte[] data) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Duration ping(byte[] echo, Duration timeout) {
            this.lastPingPayload = echo;
            this.lastPingTimeout = timeout;
            return pingResult;
        }

        @Override
        public void goAway(long lastAcceptedBidi, long lastAcceptedUni, long code, String reason) {
            this.goAwayCalls++;
            this.lastGoAwayBidi = lastAcceptedBidi;
            this.lastGoAwayUni = lastAcceptedUni;
            this.lastGoAwayCode = code;
            this.lastGoAwayReason = reason;
        }

        @Override
        public ApplicationError peerGoAwayError() {
            return null;
        }

        @Override
        public ApplicationError peerCloseError() {
            return null;
        }

        @Override
        public Preface localPreface() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Preface peerPreface() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Negotiated negotiated() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void closeWithError(long code, String reason) {
            this.closeWithErrorCalls++;
            this.lastCloseCode = code;
            this.lastCloseReason = reason;
        }

        @Override
        public boolean awaitTermination(Duration timeout) {
            this.lastAwaitTerminationTimeout = timeout;
            return awaitTerminationResult;
        }

        @Override
        public java.util.Optional<IOException> terminationCause() {
            return terminationCause;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public SessionState state() {
            return SessionState.READY;
        }

        @Override
        public SessionStats stats() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            this.closeCalls++;
        }
    }

    private static final class RecordingDefaultRecvStream implements ZmuxRecvStream {
        private final byte[] source;
        private int readCalls;
        private int readOffset;
        private int closeReadCalls;
        private long lastCancelReadCode = Long.MIN_VALUE;
        private long lastCloseWithErrorCode = Long.MIN_VALUE;
        private String lastCloseWithErrorReason;

        private RecordingDefaultRecvStream() {
            this(new byte[0]);
        }

        private RecordingDefaultRecvStream(byte[] source) {
            this.source = source;
        }

        @Override
        public int read(byte[] dst, int offset, int length) {
            this.readCalls++;
            if (readOffset >= source.length) {
                return -1;
            }
            int read = Math.min(length, source.length - readOffset);
            System.arraycopy(source, readOffset, dst, offset, read);
            readOffset += read;
            return read;
        }

        @Override
        public void closeRead() {
            this.closeReadCalls++;
        }

        @Override
        public void cancelRead(long code) {
            this.lastCancelReadCode = code;
        }

        @Override
        public void closeWithError(long code, String reason) {
            this.lastCloseWithErrorCode = code;
            this.lastCloseWithErrorReason = reason;
        }

        @Override
        public void setReadDeadline(Instant deadline) {
        }

        @Override
        public void close() {
        }

        @Override
        public long streamId() {
            return 0L;
        }

        @Override
        public byte[] openInfo() {
            return new byte[0];
        }

        @Override
        public StreamMetadata metadata() {
            return StreamMetadata.empty();
        }

        @Override
        public SocketAddress localAddress() {
            return null;
        }

        @Override
        public SocketAddress remoteAddress() {
            return null;
        }
    }
}
