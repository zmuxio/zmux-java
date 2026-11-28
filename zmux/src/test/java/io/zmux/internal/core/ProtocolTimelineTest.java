package io.zmux.internal.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ProtocolTimelineTest {
    @Test void movedSourcesReplaceTheirOldKeysAndPeekingMakesNoVisits() {
        var life = ProtocolTimeline.Reason.LIFE;
        var lane = ProtocolTimeline.Reason.LANE;
        var health = ProtocolTimeline.Reason.HEALTH;
        long[] at = {30, 20, 10};
        var timeline = new ProtocolTimeline(reason -> {
            if (reason == life) return at[0];
            if (reason == lane) return at[1];
            assertEquals(health, reason);
            return at[2];
        });
        long active = life.bit() | lane.bit() | health.bit();
        timeline.refresh(active);
        assertEquals(10, timeline.earliest());
        long visits = timeline.visits();
        for (int i = 0; i < 64; i++) assertEquals(10, timeline.earliest());
        assertEquals(visits, timeline.visits());

        at[2] = 40;
        timeline.changed(health.bit());
        timeline.refresh(active);
        assertEquals(visits + 1, timeline.visits());
        assertEquals(20, timeline.earliest());
        assertEquals(0, timeline.dueReasons(19));
        assertEquals(lane.bit(), timeline.dueReasons(20));

        at[0] = 20;
        timeline.changed(life.bit());
        timeline.refresh(active);
        assertEquals(life.bit() | lane.bit(), timeline.dueReasons(20));
        timeline.beginDispatch();
        assertEquals(0, timeline.recordDispatch(life.bit()));
        assertEquals(life.bit(), timeline.recordDispatch(life.bit() | lane.bit()));
        assertEquals(lane.bit(), timeline.dueReasons(20));

        at[1] = Time.NEVER;
        timeline.changed(active);
        timeline.refresh(active);
        assertEquals(40, timeline.earliest(), "the stalled instant stays out of the index");
        at[0] = 35;
        timeline.changed(life.bit());
        timeline.refresh(active);
        assertEquals(35, timeline.earliest(), "a different deadline rearms the source");
    }
}
