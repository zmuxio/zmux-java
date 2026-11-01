package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Keep-alive (SPEC.md 6.4): at most one keep-alive request per period of inactivity after each activity. */
class KeepAliveOnceTest {
    @Test void aKeepAliveIsRequestedAtMostOnceAfterEachActivity() {
        int[] writes = {0};
        KeepAlive.Port port = new KeepAlive.Port() {
            @Override public void wantWrite() { writes[0]++; }
            @Override public void budgetExhausted() {}
            @Override public void pingMeasured(long rtt) {}
            @Override public void fenceReached() {}
        };
        long k = 10 * Time.SECOND;
        var ka = new KeepAlive(CoreConfig.builder().keepAlive(k).probeJitter(0).build(), port, null, 0);
        ka.start(0, 0);
        assertEquals(k, ka.deadline(true));
        ka.onTimeout(k, true);
        assertEquals(1, writes[0]);
        assertTrue(ka.wants());
        // The request expires unanswered and nothing else is active: no second keep-alive.
        for (long t = k; t < 100 * k; t += k) {
            ka.onTimeout(t, true);
            assertEquals(1, writes[0], "a second keep-alive without activity at " + t);
        }
        // Activity re-arms exactly one more.
        ka.activity(100 * k);
        assertEquals(101 * k, ka.deadline(true));
        ka.onTimeout(101 * k, true);
        ka.onTimeout(102 * k, true);
        assertEquals(2, writes[0]);
    }
}
