package io.zmux.runtime;

import io.zmux.ErrorCode;
import io.zmux.Role;
import io.zmux.Settings;
import io.zmux.ZmuxConfig;
import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Used-stream marker ranges stay partitioned by stream ID class and bounded by coarsening (STATE_MACHINE 8.1,
 * IMPLEMENTATION 3.4): interleaved classes never fragment each other, and exceeding the marker budget never fails or
 * stalls the session.
 */
final class UsedStreamMarkerRangeTest {
    private static final SessionTerminalBookkeeping.Tombstone GRACEFUL =
            new SessionTerminalBookkeeping.Tombstone(true, true, 0L, "", LateDataCause.NONE);
    private static final SessionTerminalBookkeeping.Tombstone ABORTED =
            new SessionTerminalBookkeeping.Tombstone(true, false, 8L, "", LateDataCause.ABORT);

    private static SessionTerminalBookkeeping bookkeeping(StubOwner owner) {
        return new SessionTerminalBookkeeping(owner, 64L, TimeUnit.SECONDS.toNanos(1L));
    }

    /** With a zero tombstone limit every tombstone is reaped straight into marker-only state. */
    private static void reap(SessionTerminalBookkeeping bookkeeping, long streamId, SessionTerminalBookkeeping.Tombstone tombstone) {
        bookkeeping.putTombstoneLocked(streamId, tombstone);
    }

    private static SessionTerminalBookkeeping.LateDataAction action(SessionTerminalBookkeeping bookkeeping, long streamId) {
        SessionTerminalBookkeeping.TerminalDataDisposition disposition = bookkeeping.terminalDataDispositionForLocked(streamId);
        assertNotNull(disposition, "used stream " + streamId + " must keep a marker");
        return disposition.action();
    }

    @Test
    void interleavedStreamClassesMergeWithinEachClass() {
        StubOwner owner = new StubOwner(1 << 20);
        SessionTerminalBookkeeping bookkeeping = bookkeeping(owner);

        for (int i = 1; i <= 10_000; i++) {
            reap(bookkeeping, 4L * i, GRACEFUL);
            reap(bookkeeping, 4L * i + 1L, GRACEFUL);
        }

        assertEquals(2, bookkeeping.markerOnlyRangeCountLocked(), "one range per stream ID class");
        for (int i = 1; i <= 10_000; i++) {
            assertEquals(SessionTerminalBookkeeping.LateDataAction.ABORT_CLOSED, action(bookkeeping, 4L * i));
            assertEquals(SessionTerminalBookkeeping.LateDataAction.ABORT_CLOSED, action(bookkeeping, 4L * i + 1L));
        }
        assertNull(bookkeeping.terminalDataDispositionForLocked(4L * 10_001), "unused IDs must not report a marker");
        assertEquals(0, owner.failures, "marker bookkeeping must never fail the session");
    }

    @Test
    void rangesOfAnotherClassDoNotHideMarkers() {
        StubOwner owner = new StubOwner(1 << 20);
        SessionTerminalBookkeeping bookkeeping = bookkeeping(owner);
        // Enter range mode with one merged class-3 range spanning the IDs that follow.
        for (int i = 0; i < 64; i++) {
            reap(bookkeeping, 3L + 4L * i, GRACEFUL);
        }

        reap(bookkeeping, 4L, ABORTED);
        reap(bookkeeping, 8L, ABORTED);
        reap(bookkeeping, 5L, GRACEFUL);

        assertEquals(SessionTerminalBookkeeping.LateDataAction.IGNORE, action(bookkeeping, 4L));
        assertEquals(SessionTerminalBookkeeping.LateDataAction.IGNORE, action(bookkeeping, 8L),
                "a range of another class starting inside [4..8] must not hide stream 8");
        assertEquals(SessionTerminalBookkeeping.LateDataAction.ABORT_CLOSED, action(bookkeeping, 5L));
        assertEquals(SessionTerminalBookkeeping.LateDataAction.ABORT_CLOSED, action(bookkeeping, 255L));
        assertEquals(3, bookkeeping.markerOnlyRangeCountLocked());
    }

    @Test
    void fragmentedDispositionsCoarsenWithinBudgetWithoutFailingSession() {
        int budget = 16;
        StubOwner owner = new StubOwner(budget);
        SessionTerminalBookkeeping bookkeeping = bookkeeping(owner);

        int streams = budget * 8;
        for (int i = 1; i <= streams; i++) {
            reap(bookkeeping, 4L * i, i % 2 == 0 ? GRACEFUL : ABORTED);
            reap(bookkeeping, 4L * i + 2L, GRACEFUL);
            assertTrue(bookkeeping.markerOnlyRetainedLocked() <= budget,
                    "marker-only state must stay within the budget: " + bookkeeping.markerOnlyRetainedLocked());
        }

        assertEquals(0, owner.failures, "exceeding the marker budget must not fail the session");
        for (int i = 1; i <= streams; i++) {
            action(bookkeeping, 4L * i);
            action(bookkeeping, 4L * i + 2L);
        }
        assertEquals(SessionTerminalBookkeeping.LateDataAction.IGNORE, action(bookkeeping, 8L),
                "the oldest markers are coarsened to ignore with session credit release");
        assertEquals(SessionTerminalBookkeeping.LateDataAction.ABORT_CLOSED, action(bookkeeping, 4L * streams),
                "the newest markers keep their own disposition");
        assertNull(bookkeeping.terminalDataDispositionForLocked(4L * (streams + 1)), "unused IDs must not report a marker");
    }

    @Test
    void coarsenedPrefixAbsorbsLaterReapsOfOlderStreams() {
        int budget = 8;
        StubOwner owner = new StubOwner(budget);
        SessionTerminalBookkeeping bookkeeping = bookkeeping(owner);
        // Stream 4 stays live while later streams of its class are reaped and coarsened.
        for (int i = 2; i <= budget * 4; i++) {
            reap(bookkeeping, 4L * i, i % 2 == 0 ? GRACEFUL : ABORTED);
        }
        int rangesBefore = bookkeeping.markerOnlyRangeCountLocked();

        reap(bookkeeping, 4L, GRACEFUL);

        assertEquals(rangesBefore, bookkeeping.markerOnlyRangeCountLocked(), "a reap inside the coarsened prefix must not add a range");
        assertEquals(SessionTerminalBookkeeping.LateDataAction.IGNORE, action(bookkeeping, 4L));
    }

    @Test
    void liveStreamInsideCoarsenedPrefixKeepsLiveHandling() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .tombstoneLimit(1)
                .markerOnlyUsedStreamLimit(2)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());
        Field readerField = SessionRuntime.class.getDeclaredField("readerRuntime");
        readerField.setAccessible(true);
        SessionReaderCoordinator reader = (SessionReaderCoordinator) readerField.get(runtime);
        Field bookkeepingField = SessionRuntime.class.getDeclaredField("terminalBookkeeping");
        bookkeepingField.setAccessible(true);
        SessionTerminalBookkeeping bookkeeping = (SessionTerminalBookkeeping) bookkeepingField.get(runtime);

        long liveId = SessionRuntime.firstPeerStreamId(Role.RESPONDER, true);
        StreamRuntime live;
        synchronized (runtime.lock()) {
            live = reader.createPeerOpenedStreamLocked(liveId);
            for (int i = 1; i <= 16; i++) {
                bookkeeping.putTombstoneLocked(liveId + 4L * i, i % 2 == 0 ? GRACEFUL : ABORTED);
            }
            assertNotNull(bookkeeping.terminalDataDispositionForLocked(liveId), "test requires a coarsened prefix over the live ID");
            assertNull(runtime.terminalDataDispositionForLocked(liveId), "a live stream has no terminal disposition");
            assertFalse(runtime.hasTerminalMarkerLocked(liveId), "a live stream has no terminal marker");
            assertFalse(live.recvStoppedOrTerminal(), "test requires an open receive half");
        }

        reader.handleResetFrame(new FrameCodec.Frame(
                FrameType.RESET,
                0,
                liveId,
                FrameCodec.buildErrorPayload(ErrorCode.CANCELLED.code(), "", Settings.defaults().maxControlPayloadBytes())
        ));

        synchronized (runtime.lock()) {
            assertTrue(live.recvStoppedOrTerminal(), "RESET on a live stream inside a coarsened prefix must still apply");
        }
    }

    private static final class StubOwner implements SessionTerminalBookkeeping.Owner {
        private final int markerBudget;
        private int failures;

        StubOwner(int markerBudget) {
            this.markerBudget = markerBudget;
        }

        @Override
        public int tombstoneLimitLocked() {
            return 0;
        }

        @Override
        public int markerOnlyUsedStreamHardCapLocked() {
            return this.markerBudget;
        }

        @Override
        public int hiddenControlStateHardCapLocked() {
            return 16;
        }

        @Override
        public long trackedSessionMemoryLocked() {
            return 0L;
        }

        @Override
        public long sessionMemoryHardCapLocked() {
            return Long.MAX_VALUE;
        }

        @Override
        public long retainedStateUnitLocked() {
            return 4096L;
        }

        @Override
        public boolean sessionMemoryWakeNeededLocked(long previousTracked) {
            return false;
        }

        @Override
        public void notifyStreamWriteWaitersLocked() {
        }

        @Override
        public boolean sessionTerminalLocked() {
            return false;
        }

        @Override
        public IOException sessionMemoryCapErrorLocked(String operation) {
            return null;
        }

        @Override
        public void failSession(IOException error) {
            this.failures++;
        }

        @Override
        public void failSessionAsync(IOException error) {
            this.failures++;
        }

        @Override
        public void releaseRetainedLateDataLocked(long bytes) {
        }
    }
}
