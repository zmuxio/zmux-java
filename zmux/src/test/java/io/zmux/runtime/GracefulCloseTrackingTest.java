package io.zmux.runtime;

import io.zmux.ErrorCode;
import io.zmux.Role;
import io.zmux.Settings;
import io.zmux.ZmuxException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Deque;

import static org.junit.jupiter.api.Assertions.*;

final class GracefulCloseTrackingTest {
    private static StreamRuntime createPeerOpenedBidi(SessionRuntime runtime) throws Exception {
        synchronized (runtime.lock()) {
            Field field = SessionRuntime.class.getDeclaredField("readerRuntime");
            field.setAccessible(true);
            Object readerRuntime = field.get(runtime);
            return (StreamRuntime) SessionRuntimeTestSupport.invokePrivate(
                    readerRuntime,
                    "createPeerOpenedStreamLocked",
                    new Class<?>[]{long.class},
                    SessionRuntime.firstPeerStreamId(Role.RESPONDER, true)
            );
        }
    }

    private static void drainDataQueue(SessionRuntime runtime, StreamRuntime stream) throws Exception {
        @SuppressWarnings("unchecked")
        Deque<Object> dataQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue");
        while (!dataQueue.isEmpty()) {
            Object outbound = SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "pollQueuedOutboundLocked",
                    new Class<?>[]{Deque.class},
                    dataQueue
            );
            if (outbound == null) {
                continue;
            }
            int dataBytes = SessionRuntimeTestSupport.outboundDataBytes(outbound);
            if (dataBytes > 0) {
                SessionRuntimeTestSupport.invokePrivate(
                        runtime,
                        "onDataFrameWrittenLocked",
                        new Class<?>[]{StreamRuntime.class, int.class},
                        stream,
                        dataBytes
                );
            }
            stream.onFrameWrittenLocked(SessionRuntimeTestSupport.outboundFrame(outbound), false);
        }
    }

    @Test
    void localOpenedTerminalTransitionDropsGracefulCloseBlockerCount() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "beginLocalOpenLocked",
                    new Class<?>[]{StreamRuntime.class},
                    stream
            );
            assertEquals(1L, SessionRuntimeTestSupport.getLongField(runtime, "gracefulCloseBlockingStreams"), "live local-opened stream should contribute one graceful-close blocker");
            assertTrue((Boolean) SessionRuntimeTestSupport.invokePrivate(runtime, "hasGracefulClosePendingWorkLocked", new Class<?>[0]), "live local-opened stream should keep graceful close pending");
        }

        stream.closeWithError(41L, "");

        synchronized (runtime.lock()) {
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "gracefulCloseBlockingStreams"), "fully terminal local-opened stream should release its graceful-close blocker");
            assertFalse((Boolean) SessionRuntimeTestSupport.invokePrivate(runtime, "hasGracefulClosePendingWorkLocked", new Class<?>[0]), "no live or provisional blockers should remain after terminal local abort");
        }
    }

    @Test
    void gracefulCloseReclaimsQueuedButNotPeerVisibleLocalOpener() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        SessionRuntimeTestSupport.queueWrite(stream, "queued".getBytes(StandardCharsets.UTF_8));

        synchronized (runtime.lock()) {
            assertTrue(stream.openedOnWire(), "test requires an opening frame already queued for write");
            assertFalse(stream.peerVisibleLocked(), "test requires the queued opener to remain not peer-visible");
            assertTrue(stream.unseenLocalTracked(), "queued local opener should remain in unseen-local tracking");

            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "reclaimGracefulCloseLocalStreamsLocked",
                    new Class<?>[0]
            );

            assertFalse(stream.unseenLocalTracked(), "graceful reclaim should remove queued openers from unseen-local tracking");
            assertTrue(stream.fullyTerminalLocked(), "graceful reclaim should refuse the queued local opener locally");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "gracefulCloseBlockingStreams"),
                    "reclaimed queued opener should release the graceful-close blocker");
            assertTrue(SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue").isEmpty(),
                    "reclaimed queued opener should release its queued DATA frame");
            assertFalse((Boolean) SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "hasGracefulClosePendingWorkLocked",
                    new Class<?>[0]
            ), "reclaimed queued opener should not keep graceful close pending");
        }
    }

    @Test
    void peerOpenedBidiQueuedSendWorkTracksGracefulCloseBlockerCount() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime);

        synchronized (runtime.lock()) {
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "gracefulCloseBlockingStreams"), "idle peer-opened bidi should not block graceful close before local send work exists");
            assertFalse((Boolean) SessionRuntimeTestSupport.invokePrivate(runtime, "hasGracefulClosePendingWorkLocked", new Class<?>[0]), "idle peer-opened bidi should not keep graceful close pending");
        }

        SessionRuntimeTestSupport.queueWrite(stream, "reply".getBytes(StandardCharsets.UTF_8));

        synchronized (runtime.lock()) {
            assertEquals(1L, SessionRuntimeTestSupport.getLongField(runtime, "gracefulCloseBlockingStreams"), "queued local send work on a peer-opened bidi should contribute one graceful-close blocker");
            assertTrue((Boolean) SessionRuntimeTestSupport.invokePrivate(runtime, "hasGracefulClosePendingWorkLocked", new Class<?>[0]), "queued local send work should keep graceful close pending");
        }

        stream.closeWrite();

        synchronized (runtime.lock()) {
            drainDataQueue(runtime, stream);
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "gracefulCloseBlockingStreams"), "peer-opened bidi should release its graceful-close blocker once queued DATA|FIN is fully written");
            assertFalse((Boolean) SessionRuntimeTestSupport.invokePrivate(runtime, "hasGracefulClosePendingWorkLocked", new Class<?>[0]), "after the send half drains there should be no graceful-close blockers left");
        }
    }

    @Test
    void peerStopSeenQueuedSendWorkStopsBlockingGracefulClose() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = createPeerOpenedBidi(runtime);

        SessionRuntimeTestSupport.queueWrite(stream, "reply".getBytes(StandardCharsets.UTF_8));

        synchronized (runtime.lock()) {
            assertEquals(1L, SessionRuntimeTestSupport.getLongField(runtime, "gracefulCloseBlockingStreams"), "queued local send work on a peer-opened bidi should initially block graceful close");
            assertTrue(stream.queuedDataBytesLocked() > 0L, "test setup should leave queued local send work in place");

            assertTrue(
                    stream.stopSendingFromPeerLocked(
                            ErrorCode.CANCELLED.code(),
                            "peer stop",
                            "peer stop".getBytes(StandardCharsets.UTF_8).length
                    ),
                    "first peer STOP_SENDING on an open send half should remain actionable"
            );

            assertFalse(stream.blocksGracefulSessionCloseLocked(), "peer STOP_SENDING should remove a peer-opened stream from graceful-close blockers");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "gracefulCloseBlockingStreams"), "peer STOP_SENDING should drop the graceful-close blocker count even while stale queued send work remains");
            assertFalse((Boolean) SessionRuntimeTestSupport.invokePrivate(runtime, "hasGracefulClosePendingWorkLocked", new Class<?>[0]), "peer STOP_SENDING should stop keeping graceful close pending");
        }
    }

    @Test
    void invalidLocalAbortCodeDoesNotLeaveStreamHalfTerminated() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "beginLocalOpenLocked",
                    new Class<?>[]{StreamRuntime.class},
                    stream
            );
        }

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> stream.closeWithError(-1L, "invalid"),
                "invalid local abort code should fail during payload encoding"
        );

        assertTrue(error.getMessage().contains("varint62 value out of range"), "invalid abort code should fail through the varint encoder");
        synchronized (runtime.lock()) {
            assertFalse(stream.fullyTerminalLocked(), "payload encoding failure must not leave the stream half-terminated");
        }

        stream.closeWithError(41L, "");
        synchronized (runtime.lock()) {
            assertTrue(stream.fullyTerminalLocked(), "stream should still be abortable after a prior local encoding failure");
        }
    }

    @Test
    void invalidFreshProvisionalAbortCodeDoesNotFailOrUntrackStream() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> stream.closeWithError(-1L, "invalid"),
                "fresh provisional abort should validate code before committing local failure"
        );

        assertTrue(error.getMessage().contains("varint62 value out of range"), "invalid abort code should fail through the varint encoder");
        synchronized (runtime.lock()) {
            assertEquals(1, runtime.stats().provisionals().bidi(), "failed fresh abort must keep the provisional slot");
            assertTrue(stream.provisionalTracked(), "failed fresh abort must keep the stream provisional");
            assertFalse(stream.fullyTerminalLocked(), "failed fresh abort must not terminate the stream locally");
        }

        stream.closeWithError(41L, "");
        synchronized (runtime.lock()) {
            assertEquals(0, runtime.stats().provisionals().bidi(), "valid retry should release the provisional slot");
            assertFalse(stream.provisionalTracked(), "valid retry should untrack the provisional stream");
            assertTrue(stream.fullyTerminalLocked(), "valid retry should abort the stream locally");
        }
    }

    @Test
    void invalidFreshProvisionalCancelWriteCodeDoesNotFailOrUntrackStream() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> stream.cancelWrite(-1L),
                "fresh provisional cancelWrite should validate code before committing local failure"
        );

        assertTrue(error.getMessage().contains("varint62 value out of range"), "invalid cancel code should fail through the varint encoder");
        synchronized (runtime.lock()) {
            assertEquals(1, runtime.stats().provisionals().bidi(), "failed fresh cancelWrite must keep the provisional slot");
            assertTrue(stream.provisionalTracked(), "failed fresh cancelWrite must keep the stream provisional");
            assertFalse(stream.fullyTerminalLocked(), "failed fresh cancelWrite must not terminate the stream locally");
        }

        stream.cancelWrite(41L);
        synchronized (runtime.lock()) {
            assertEquals(0, runtime.stats().provisionals().bidi(), "valid retry should release the provisional slot");
            assertFalse(stream.provisionalTracked(), "valid retry should untrack the provisional stream");
            assertTrue(stream.fullyTerminalLocked(), "valid retry should abort the stream locally");
        }
    }
}
