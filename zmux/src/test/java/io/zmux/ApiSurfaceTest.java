package io.zmux;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
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
    void openStreamExposesNativeBidiSurface() throws Exception {
        try (SessionPair pair = SessionPair.open()) {
            ZmuxStream stream = pair.client().openStream();

            assertTrue(stream instanceof ZmuxNativeStream, "bidi stream should expose native bidi state queries");
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

            ZmuxStream firstInbound = pair.server().acceptStream(Duration.ofSeconds(1));
            ZmuxStream secondInbound = pair.server().acceptStream(Duration.ofSeconds(1));
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

        assertNotNull(details);
        assertEquals(41L, details.code());
        assertEquals("peer", details.reason());
        assertEquals(ZmuxErrorDirection.BOTH, details.direction());
        assertTrue(ZmuxErrors.hasCode(wrapped));
        assertEquals(41L, ZmuxErrors.code(wrapped, -1L));
        assertEquals("", ZmuxErrors.operation(wrapped));
        assertEquals("peer", ZmuxErrors.reason(wrapped));
        assertEquals(ZmuxErrorScope.SESSION, ZmuxErrors.scope(wrapped));
        assertEquals(ZmuxErrorSource.REMOTE, ZmuxErrors.source(wrapped));
        assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, ZmuxErrors.terminationKind(wrapped));
        assertFalse(ZmuxErrors.timeout(wrapped));
        assertFalse(ZmuxErrors.interrupted(wrapped));
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
                    clientRef.set(Zmux.client(clientSocket, config));
                } catch (Throwable t) {
                    errorRef.compareAndSet(null, t);
                } finally {
                    established.countDown();
                }
            }, "api-surface-client-open");
            clientThread.start();

            Thread serverThread = new Thread(() -> {
                try {
                    serverRef.set(Zmux.server(serverSocket, config));
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

    private static final class RecordingDefaultSendStream implements ZmuxSendStream {
        private int writeCalls;
        private int writeFinalCalls;
        private int closeWriteCalls;
        private int lastWriteLength = -1;
        private int lastFinalLength = -1;
        private byte[] lastWriteBytes = new byte[0];
        private byte[] lastFinalBytes = new byte[0];

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
        }

        @Override
        public void closeWithError(long code, String reason) {
        }

        @Override
        public void setWriteDeadline(Instant deadline) {
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

    private static final class RecordingDefaultRecvStream implements ZmuxRecvStream {
        private final byte[] source;
        private int readCalls;
        private int readOffset;
        private int closeReadCalls;

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
        }

        @Override
        public void closeWithError(long code, String reason) {
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
