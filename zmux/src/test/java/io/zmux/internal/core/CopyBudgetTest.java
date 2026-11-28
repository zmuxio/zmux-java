package io.zmux.internal.core;

import io.zmux.DatagramCopyPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CopyBudgetTest {
    @Test void burstUsesConfiguredCapEvenWithoutPeerDatagrams() {
        var cfg = CoreConfig.builder();
        cfg.local().maxDatagramFrameSize = 103;
        var datagrams = new Session(cfg.build(), 0).datagrams();
        datagrams.peer(0, 65535, true);
        assertEquals(100, datagrams.budget.remaining(0));

        datagrams = new Session(cfg.build(), 0).datagrams();
        datagrams.peer(200, 65535, false);
        assertEquals(102, datagrams.budget.remaining(0));

        datagrams = new Session(cfg.build(), 0).datagrams();
        datagrams.peer(20, 65535, true);
        assertEquals(17, datagrams.budget.remaining(0));
    }

    @Test void chargesOutliveOriginalsByOneBucketWithAClockRelativeOrigin() {
        int rate = DatagramCopyPolicy.DEFAULT.extraPercent();
        assertEquals(20, rate);
        long origin = Time.SECOND / 2;
        var budget = new CopyBudget(rate, origin);
        budget.negotiateBurst(100);
        budget.original(origin, 500);
        for (int i = 0; i < 2; i++) {
            budget.original(origin, 100);
            assertTrue(budget.remaining(origin) >= 100);
            budget.copy(origin, 100);
        }
        long ten = origin + 10 * Time.SECOND;
        assertEquals(40, budget.remaining(ten - 1));
        budget.original(ten, 100);
        assertEquals(0, budget.remaining(ten));
        long eleven = origin + 11 * Time.SECOND;
        assertEquals(0, budget.remaining(eleven - 1));
        budget.original(eleven, 104);
        assertEquals(140, budget.remaining(eleven));
        budget.original(eleven, 1);
        assertEquals(141, budget.remaining(eleven));
    }
}
