package io.zmux.internal;

import io.zmux.SessionStats;
import io.zmux.Settings;
import io.zmux.ZmuxConfig;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Deque;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class TombstoneMemoryPressureTest {
    @SuppressWarnings("unchecked")
    private static Map<Long, Object> tombstones(SessionRuntime runtime) throws Exception {
        Field field = terminalBookkeeping(runtime).getClass().getDeclaredField("tombstones");
        field.setAccessible(true);
        return (Map<Long, Object>) field.get(terminalBookkeeping(runtime));
    }

    @SuppressWarnings("unchecked")
    private static Deque<Long> tombstoneOrder(SessionRuntime runtime) throws Exception {
        Field field = terminalBookkeeping(runtime).getClass().getDeclaredField("tombstoneOrder");
        field.setAccessible(true);
        return (Deque<Long>) field.get(terminalBookkeeping(runtime));
    }

    @SuppressWarnings("unchecked")
    private static Deque<Long> hiddenTombstones(SessionRuntime runtime) throws Exception {
        Field field = terminalBookkeeping(runtime).getClass().getDeclaredField("hiddenTombstones");
        field.setAccessible(true);
        return (Deque<Long>) field.get(terminalBookkeeping(runtime));
    }

    @SuppressWarnings("unchecked")
    private static Map<Long, Object> markerOnly(SessionRuntime runtime) throws Exception {
        Field field = terminalBookkeeping(runtime).getClass().getDeclaredField("markerOnlyUsedStreams");
        field.setAccessible(true);
        return (Map<Long, Object>) field.get(terminalBookkeeping(runtime));
    }

    private static Object terminalBookkeeping(SessionRuntime runtime) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField("terminalBookkeeping");
        field.setAccessible(true);
        return field.get(runtime);
    }

    private static Object newTombstone(boolean hasReceiveHalf,
                                       boolean gracefulReceiveClosed,
                                       long terminalCode,
                                       String terminalReason,
                                       boolean hidden,
                                       long createdAtNanos) throws Exception {
        Class<?> tombstoneClass = Class.forName("io.zmux.internal.SessionTerminalBookkeeping$Tombstone");
        Constructor<?> constructor = tombstoneClass.getDeclaredConstructor(
                boolean.class,
                boolean.class,
                long.class,
                String.class,
                boolean.class,
                long.class
        );
        constructor.setAccessible(true);
        return constructor.newInstance(
                hasReceiveHalf,
                gracefulReceiveClosed,
                terminalCode,
                terminalReason,
                hidden,
                createdAtNanos
        );
    }

    private static void putTombstone(SessionRuntime runtime, long streamId, Object tombstone) throws Exception {
        SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "putTombstoneLocked",
                new Class<?>[]{long.class, tombstone.getClass()},
                streamId,
                tombstone
        );
    }

    private static void retainMarkerOnly(SessionRuntime runtime, long streamId, Object tombstone) throws Exception {
        SessionRuntimeTestSupport.invokePrivate(
                terminalBookkeeping(runtime),
                "retainMarkerOnlyUsedStreamLocked",
                new Class<?>[]{long.class, tombstone.getClass()},
                streamId,
                tombstone
        );
    }

    @Test
    void trackedMemoryPressureReapsOldestVisibleTombstoneToMarkerOnly() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .sessionMemoryCap(64L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());

        synchronized (runtime.lock()) {
            Map<Long, Object> tombstones = tombstones(runtime);
            Deque<Long> tombstoneOrder = tombstoneOrder(runtime);
            tombstones.put(2L, newTombstone(true, true, 0L, "", false, 0L));
            tombstones.put(6L, newTombstone(true, true, 0L, "", false, 0L));
            tombstoneOrder.addLast(2L);
            tombstoneOrder.addLast(6L);

            SessionRuntimeTestSupport.invokePrivate(runtime, "reapExcessTombstonesLocked", new Class<?>[0]);

            assertEquals(1, tombstones.size(), "tracked-memory pressure should retain only one visible tombstone");
            assertFalse(tombstones.containsKey(2L), "oldest visible tombstone should be reaped first");
            assertTrue(tombstones.containsKey(6L), "newest visible tombstone should remain after reaping");
            assertTrue(markerOnly(runtime).containsKey(2L), "reaped visible tombstone should become marker-only state");
            assertFalse(runtime.state().terminal(), "reaping visible tombstones under memory pressure must not fail the session");
        }
    }

    @Test
    void reapExcessTombstonesSkipsStaleOrderEntries() throws Exception {
        ZmuxConfig config = ZmuxConfig.builder()
                .tombstoneLimit(1)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(config, 0L, Settings.defaults());

        synchronized (runtime.lock()) {
            Map<Long, Object> tombstones = tombstones(runtime);
            Deque<Long> tombstoneOrder = tombstoneOrder(runtime);
            tombstones.put(4L, newTombstone(true, true, 0L, "", false, 0L));
            tombstones.put(8L, newTombstone(true, true, 0L, "", false, 0L));
            tombstoneOrder.addLast(2L);
            tombstoneOrder.addLast(4L);
            tombstoneOrder.addLast(8L);

            SessionRuntimeTestSupport.invokePrivate(runtime, "reapExcessTombstonesLocked", new Class<?>[0]);

            assertEquals(1, tombstones.size(), "stale order entries must not stop tombstone-limit reaping");
            assertFalse(tombstones.containsKey(4L), "oldest real tombstone should be reaped after stale entries");
            assertTrue(tombstones.containsKey(8L), "newest tombstone should remain after limit reaping");
            assertTrue(markerOnly(runtime).containsKey(4L), "reaped tombstone should leave marker-only state");
        }
    }

    @Test
    void putTombstoneLockedTracksHiddenQueueAcrossReplacement() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        Object hidden = newTombstone(false, false, 9L, "", true, System.nanoTime());
        Object visible = newTombstone(true, true, 0L, "", false, 0L);

        synchronized (runtime.lock()) {
            putTombstone(runtime, 4L, hidden);

            assertEquals(1, hiddenTombstones(runtime).size(), "hidden tombstone should be indexed once");
            assertTrue(hiddenTombstones(runtime).contains(4L), "hidden tombstone queue should contain the stream id");
            SessionStats hiddenStats = runtime.stats();
            assertEquals(
                    1L,
                    hiddenStats.pressure().retainedStateBreakdown().hiddenControl().count(),
                    "hidden tombstone should count as hidden retained state"
            );
            assertEquals(
                    0L,
                    hiddenStats.pressure().retainedStateBreakdown().visibleTombstones().count(),
                    "hidden tombstone must not inflate visible tombstone retained state"
            );

            putTombstone(runtime, 4L, visible);

            assertTrue(hiddenTombstones(runtime).isEmpty(), "replacing a hidden tombstone should remove hidden index state");
            SessionStats visibleStats = runtime.stats();
            assertEquals(
                    0L,
                    visibleStats.pressure().retainedStateBreakdown().hiddenControl().count(),
                    "visible replacement should no longer count as hidden retained state"
            );
            assertEquals(
                    1L,
                    visibleStats.pressure().retainedStateBreakdown().visibleTombstones().count(),
                    "visible replacement should count as one visible tombstone"
            );
        }
    }

    @Test
    void hiddenBudgetCleanupSkipsStaleHiddenTailEntries() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        synchronized (runtime.lock()) {
            int cap = runtime.hiddenControlStateHardCapLocked();
            assertTrue(cap > 0, "test requires a positive hidden-state hard cap");
            long nowNanos = System.nanoTime();

            for (int i = 0; i < cap; i++) {
                putTombstone(runtime, 4L + i * 4L, newTombstone(false, false, 9L, "", true, nowNanos + i));
            }
            hiddenTombstones(runtime).addLast(10_000L);

            putTombstone(runtime, 20_000L, newTombstone(true, true, 0L, "", false, 0L));

            assertEquals(cap, hiddenTombstones(runtime).size(), "stale hidden tail should be dropped without reaping live hidden state");
            assertFalse(hiddenTombstones(runtime).contains(10_000L), "stale hidden tail entry should be removed");
            assertEquals(
                    cap,
                    runtime.stats().pressure().retainedStateBreakdown().hiddenControl().count(),
                    "live hidden tombstones should remain at the hard cap"
            );
            assertTrue(tombstones(runtime).containsKey(20_000L), "visible tombstone used to trigger cleanup should remain");
        }
    }

    @Test
    void hiddenTombstoneHardCapShedsNewestHiddenState() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        synchronized (runtime.lock()) {
            int cap = runtime.hiddenControlStateHardCapLocked();
            assertTrue(cap > 0, "test requires a positive hidden-state hard cap");
            long firstId = 4L;
            long newestId = firstId + cap * 4L;
            long nowNanos = System.nanoTime();

            for (int i = 0; i <= cap; i++) {
                long streamId = firstId + i * 4L;
                putTombstone(runtime, streamId, newTombstone(false, false, 9L, "", true, nowNanos + i));
            }

            Map<Long, Object> tombstones = tombstones(runtime);
            assertEquals(cap, hiddenTombstones(runtime).size(), "hidden tombstones should be clamped to hard cap");
            assertEquals(cap, runtime.stats().pressure().retainedStateBreakdown().hiddenControl().count());
            assertTrue(tombstones.containsKey(firstId), "oldest hidden tombstone should be retained under hard-cap pressure");
            assertFalse(tombstones.containsKey(newestId), "newest hidden tombstone should be shed when hard cap is exceeded");
            assertTrue(markerOnly(runtime).containsKey(newestId), "shed hidden tombstone should leave marker-only state");
        }
    }

    @Test
    void expiredHiddenTombstoneReapsToMarkerOnlyState() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        synchronized (runtime.lock()) {
            long nowNanos = System.nanoTime();
            long oldCreatedAtNanos = nowNanos - java.util.concurrent.TimeUnit.SECONDS.toNanos(2L);
            putTombstone(runtime, 4L, newTombstone(false, false, 9L, "", true, oldCreatedAtNanos));
            Deque<Long> retainedOrder = tombstoneOrder(runtime);
            Deque<Long> retainedHidden = hiddenTombstones(runtime);

            assertEquals(1, hiddenTombstones(runtime).size(), "hidden tombstone should be retained before TTL reap");
            runtime.reapExpiredHiddenControlStateLocked(nowNanos);

            assertTrue(tombstones(runtime).isEmpty(), "expired hidden tombstone should be removed");
            assertTrue(hiddenTombstones(runtime).isEmpty(), "expired hidden tombstone should leave no hidden queue entry");
            assertNotSame(retainedOrder, tombstoneOrder(runtime), "empty tombstone order should release retained backing");
            assertNotSame(retainedHidden, hiddenTombstones(runtime), "empty hidden tombstone queue should release retained backing");
            assertTrue(markerOnly(runtime).containsKey(4L), "expired hidden tombstone should retain marker-only state");
        }
    }

    @Test
    void markerOnlyRangeCompactionReleasesEmptyMapBackingAndCountsStreams() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        Object tombstone = newTombstone(true, true, 0L, "", false, 0L);

        synchronized (runtime.lock()) {
            Map<Long, Object> retainedMap = markerOnly(runtime);
            for (int i = 0; i < 64; i++) {
                retainMarkerOnly(runtime, 4L + i * 4L, tombstone);
            }

            assertTrue(markerOnly(runtime).isEmpty(), "range compaction should remove migrated map entries");
            assertNotSame(retainedMap, markerOnly(runtime), "range compaction should release empty marker-only map backing");
            assertEquals(
                    1L,
                    runtime.stats().diagnostics().markerOnlyRangeCount(),
                    "sequential markers should merge into one range"
            );
            assertEquals(
                    64L,
                    runtime.stats().pressure().retainedStateBreakdown().markerOnly().count(),
                    "marker-only range retention should count streams, not ranges"
            );
        }
    }

    @Test
    void rangeModeMarkerUpdateDropsStaleMapEntry() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        Object graceful = newTombstone(true, true, 0L, "", false, 0L);
        Object abortive = newTombstone(false, false, 0L, "", false, 0L);

        synchronized (runtime.lock()) {
            for (int i = 0; i < 64; i++) {
                retainMarkerOnly(runtime, 4L + i * 4L, graceful);
            }
            Map<Long, Object> retainedMap = markerOnly(runtime);
            retainedMap.put(4L, new SessionTerminalBookkeeping.TerminalDataDisposition(
                    SessionTerminalBookkeeping.LateDataAction.ABORT_CLOSED,
                    LateDataCause.NONE
            ));

            retainMarkerOnly(runtime, 4L, abortive);

            assertFalse(markerOnly(runtime).containsKey(4L), "range-mode marker update should drop stale map entry");
            assertNotSame(retainedMap, markerOnly(runtime), "range-mode marker update should release empty map backing");
            assertEquals(
                    64L,
                    runtime.stats().pressure().retainedStateBreakdown().markerOnly().count(),
                    "stale map entry must not double-count a ranged marker"
            );
        }
    }
}
