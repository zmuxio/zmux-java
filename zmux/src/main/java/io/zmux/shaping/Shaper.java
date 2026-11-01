package io.zmux.shaping;

import java.util.Set;

/**
 * Shaper customizes record sizes and timing. The session calls its methods one at a time on
 * its own thread; they must not block or call methods of the session.
 */
public interface Shaper {
    /** Called once, when the instance is installed. */
    Set<Capability> capabilities();

    /** Called before each outgoing record of the pipe ({@link Capability#PLAN}); plan is cleared. */
    void plan(RecordView view, RecordPlan plan);

    /** Receives a notification; reply is cleared. Unknown kinds should be ignored. */
    void onEvent(Event event, Reply reply);
}
