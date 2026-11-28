package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * A deadline that passed before the timer pass ran stays pending: asking for the next
 * deadline in between must not drop it (the driver would arm no timer, and the pipe would
 * never become suspect).
 */
class PendingDeadlineTest {
    @Test
    void aPassedSuspicionDeadlineStaysPendingUntilThePass() {
        Session s = PipeTargetTest.session();
        Pipe p = PipeTargetTest.appendOut(s, true, Attach.OUT_ACTIVE, Attach.IN_OPEN);
        Poke.set(p.mp().link, "unacked", true);
        Poke.set(p.mp().link, "oldestAt", 0L);
        PathHealth health = s.mp().health;
        long late = health.tSuspect(p) + 1;
        long next = health.pipeTimers(late, false);
        assertEquals(health.tSuspect(p), next, "the elapsed suspicion deadline remains pending");
        assertEquals(Attach.OUT_ACTIVE, p.mp().health.out(), "asking changes nothing");
        health.pipeTimers(late, true);
        assertEquals(Attach.OUT_SUSPECT, p.mp().health.out());
    }

    @Test
    void aPassedJoinBackoffStaysPendingUntilThePass() {
        Session s = PipeTargetTest.session();
        Joins joins = s.mp().joins;
        Poke.set(joins, "backoffUntil", 10L);
        assertEquals(10, joins.backoffTimer(20, false));
        assertEquals(Time.NEVER, joins.backoffTimer(20, true));
        assertEquals(Time.NEVER, joins.backoffTimer(30, false));
    }
}
