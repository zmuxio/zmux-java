package io.zmux.internal.core;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * The session's single output port: events, carrier actions and wake-ups produced by any
 * core component go here and nowhere else; the driver drains them once per round.
 */
public final class Outbox {
    private final ArrayDeque<Event> events = new ArrayDeque<>(2);
    private final ArrayList<Waiter> wakes = new ArrayList<>(4);
    private final ArrayList<Action> actions = new ArrayList<>(2);

    void event(Event e) {
        events.add(e);
    }

    void action(Action a) {
        actions.add(a);
    }

    /** Queues w once per round; null is ignored. */
    void wake(Waiter w) {
        if (w == null || w.queued) return;
        w.queued = true;
        wakes.add(w);
    }

    /** Whether wake-ups or carrier actions wait for the driver. */
    public boolean pending() {
        return !wakes.isEmpty() || !actions.isEmpty();
    }

    public Event pollEvent() {
        return events.poll();
    }

    public void takeActions(List<Action> dst) {
        dst.addAll(actions);
        actions.clear();
    }

    public void takeWakes(List<Waiter> dst) {
        for (Waiter w : wakes) w.queued = false;
        dst.addAll(wakes);
        wakes.clear();
    }
}
