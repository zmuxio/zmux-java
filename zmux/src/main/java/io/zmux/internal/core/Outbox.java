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
    /** Locator changes, apart from the events: the driver routes them before the round writes anything that names them. */
    private final ArrayDeque<Event> locators = new ArrayDeque<>(1);
    private final ArrayList<Waiter> wakes = new ArrayList<>(4);
    private final ArrayList<Action> actions = new ArrayList<>(2);

    void event(Event e) {
        if (e.kind() == Event.Kind.LOCATOR_ADDED || e.kind() == Event.Kind.LOCATOR_REMOVED) locators.add(e);
        else events.add(e);
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

    public Event lastEvent() { return events.peekLast(); }

    public Event pollEvent() {
        return events.poll();
    }

    /** The next Locator change, or null. */
    public Event pollLocator() {
        return locators.poll();
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
