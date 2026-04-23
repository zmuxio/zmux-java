package io.zmux.internal;

import io.zmux.SessionStats;
import io.zmux.Settings;
import io.zmux.ZmuxConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class SessionReasonStatsTest {
    @Test
    void reasonStatsBoundDistinctCodesAndCountOverflow() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                ZmuxConfig.builder().build(),
                0L,
                Settings.defaults()
        );
        int trackedCodes = SessionStats.ReasonStats.MAX_TRACKED_CODES;
        int overflow = 5;

        synchronized (runtime.lock()) {
            for (int i = 0; i < trackedCodes + overflow; i++) {
                runtime.noteResetReasonLocked(10_000L + i);
                runtime.noteAbortReasonLocked(20_000L + i);
            }
            runtime.noteResetReasonLocked(10_000L);
            runtime.noteAbortReasonLocked(20_000L);
        }

        SessionStats.ReasonStats reasons = runtime.stats().reasons();
        assertEquals(trackedCodes, reasons.reset().size());
        assertEquals(trackedCodes, reasons.abort().size());
        assertEquals(overflow, reasons.resetOverflow());
        assertEquals(overflow, reasons.abortOverflow());
        assertEquals(2L, reasons.reset().get(10_000L));
        assertEquals(2L, reasons.abort().get(20_000L));
    }
}
