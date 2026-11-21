package io.zmux.internal.loop;

import io.zmux.internal.core.Action;
import io.zmux.internal.core.Event;
import io.zmux.internal.core.PipeInfo;
import io.zmux.internal.core.Waiter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

/**
 * The session events the application has not read, on the loop: the keyed queue of the API
 * specification (Section 4.2). It holds at most one unread event per key and never drops one;
 * an event whose key has an unread one updates it as the key's rule says and moves it to the
 * tail, so events are read in the order of their keys' latest occurrences. The keys are those
 * of what the session holds (its pipes, tiers and carrier candidates) plus a fixed set of
 * session keys, so no count bounds the queue and none can overflow.
 *
 * <p>A retired pipe holds no key: its unread events leave with it, and the one
 * {@link SessionRuntime.RetiredPipes} summary (itself a key, moved to the tail at each
 * retirement) is the only report of retirement. Once {@link #freeze frozen} (the session is
 * CLOSED) it takes nothing new; what it holds is still read.
 */
final class EventQueue {
    /** How many of the last retired pipe IDs a summary names. */
    static final int SUMMARY_IDS = 4;

    /** The key of an event: its kind, and the pipe, direction and detail the kind's rule adds. */
    private record Key(Object kind, long pipe, int dir, Object detail) {}

    /** What an unread event of the same key becomes. */
    private enum Rule { KEEP, REPLACE }

    private static final Key SUMMARY = new Key("summary", -1, 0, null);
    private static final Key RESUME_UNATTENDED = new Key(Event.Kind.RESUME_UNATTENDED, -1, 0, null);
    /** The kinds keyed by pipe and direction, and by pipe alone: the keys a retirement removes. */
    private static final int[] DIRECTIONS = {Action.DIR_OUT, Action.DIR_IN};
    private static final Event.Kind[] PIPE_DIRECTED = {Event.Kind.PIPE_SUSPECT, Event.Kind.PIPE_RECOVERED, Event.Kind.PIPE_FAILED, Event.Kind.PIPE_CLOSED};
    private static final Event.Kind[] PIPE_UNDIRECTED = {Event.Kind.PIPE_ADDED, Event.Kind.PIPE_JOINED, Event.Kind.PIPE_QUALITY, Event.Kind.PIPE_PRIORITY};

    private final LinkedHashMap<Key, Object> pending = new LinkedHashMap<>();
    private final List<Waiter> waiters = new ArrayList<>(1);
    private SessionRuntime.RetiredPipes summary;
    /** The live pipes of which the application has read a PipeFailed or PipeClosed (bounded by the pipes the session holds). */
    private final java.util.Set<Long> terminalRead = new java.util.HashSet<>();
    private boolean frozen;

    /** Queues e (CLOSED and the internal kinds are not application events). */
    void add(Event e) {
        if (frozen) return;
        Event.Kind kind = e.kind();
        switch (kind) {
            case PIPE_STATE -> {
                // Only the retirement: the other states reach the application as the pipe kinds below.
                if (e.state() != PipeInfo.Out.RETIRED) return;
                retire(e.pipe(), e.flag());
            }
            case PIPE_ADDED, PIPE_JOINED -> put(new Key(kind, e.pipe(), 0, null), e, Rule.KEEP);
            case PIPE_FAILED, PIPE_CLOSED, PIPE_SUSPECT, PIPE_RECOVERED -> put(new Key(kind, e.pipe(), e.dir(), null), e,
                    kind == Event.Kind.PIPE_FAILED || kind == Event.Kind.PIPE_CLOSED ? Rule.KEEP : Rule.REPLACE);
            case PIPE_QUALITY, PIPE_PRIORITY -> put(new Key(kind, e.pipe(), 0, null), e, Rule.REPLACE);
            case TIER_SATURATION -> put(new Key(kind, -1, 0, e.code()), e, Rule.REPLACE);
            // Suspended and Resumed are two keys, so an interruption is never swallowed by the resumption after it.
            case CAPACITY_BELOW, CAPACITY_RESTORED, SUSPENDED, RESUME_UNATTENDED, GOAWAY_RECEIVED, EPOCH_COMMITTED, EPOCH_DELETED, DATAGRAM_PATH ->
                    put(new Key(kind, -1, 0, null), e, Rule.REPLACE);
            case RESUMED -> {
                // A resumption makes the unread "nothing will dial" report stale.
                pending.remove(RESUME_UNATTENDED);
                put(new Key(kind, -1, 0, null), e, Rule.REPLACE);
            }
            case JOIN_AUTH_FAILED -> {
                Key k = new Key(kind, -1, 0, e.reason());
                Event unread = (Event) pending.remove(k);
                put(k, unread == null ? e : unread.withCount(unread.count() + e.count()), Rule.REPLACE);
            }
            default -> { return; }
        }
        wakeFirst();
    }

    /**
     * Queues an event of the API above the core (such as a carrier selection report), returned
     * by {@link #poll} as it is; a later event with an equal key replaces an unread one.
     */
    void addApplication(Object key, Object event) {
        if (frozen) return;
        put(new Key("application", -2, 0, Objects.requireNonNull(key)), Objects.requireNonNull(event), Rule.REPLACE);
        wakeFirst();
    }

    /** Takes nothing new from now on (the session is CLOSED); what is queued is still read. */
    void freeze() {
        frozen = true;
    }

    /** Puts value at the tail under key; an unread value of the key is replaced (and moves) or, for KEEP, stays where it is. */
    private void put(Key key, Object value, Rule rule) {
        if (rule == Rule.KEEP && pending.containsKey(key)) return;
        pending.remove(key);
        pending.put(key, value);
    }

    /**
     * Retires a pipe: its keys leave the queue (a constant number of them). The summary counts
     * the pipe as failed (failed: the core's flag, either direction FAILED) or else closed only
     * when the application has read no terminal event of it, whether the events are still
     * queued or were never queued; once it read a PipeFailed or PipeClosed of the pipe it
     * counts in neither.
     */
    private void retire(long pipe, boolean failed) {
        boolean read = terminalRead.remove(pipe);
        for (Event.Kind kind : PIPE_DIRECTED) {
            for (int dir : DIRECTIONS) pending.remove(new Key(kind, pipe, dir, null));
        }
        for (Event.Kind kind : PIPE_UNDIRECTED) pending.remove(new Key(kind, pipe, 0, null));
        var s = summary == null ? new SessionRuntime.RetiredPipes(0, 0, 0, List.of()) : summary;
        List<Long> ids = new ArrayList<>(s.last());
        ids.add(pipe);
        if (ids.size() > SUMMARY_IDS) ids.removeFirst();
        summary = new SessionRuntime.RetiredPipes(s.retired() + 1, s.failed() + (!read && failed ? 1 : 0),
                s.closed() + (!read && !failed ? 1 : 0), List.copyOf(ids));
        pending.remove(SUMMARY);
        pending.put(SUMMARY, summary); // At the tail: the latest retirement is the summary's latest occurrence.
    }

    /** The oldest unread event (an Event, an application event or the RetiredPipes summary), or null with waiter registered. */
    @SuppressWarnings("ReferenceEquality") // SUMMARY is a sentinel key
    Object poll(Waiter waiter) {
        var it = pending.entrySet().iterator();
        if (!it.hasNext()) {
            if (!waiters.contains(waiter)) waiters.add(waiter);
            return null;
        }
        var first = it.next();
        it.remove();
        if (first.getKey() == SUMMARY) summary = null;
        else if (first.getValue() instanceof Event ev && ev.pipeTerminal()) terminalRead.add(ev.pipe());
        waiters.remove(waiter);
        if (!pending.isEmpty() && !waiters.isEmpty()) waiters.getFirst().wake();
        return first.getValue();
    }

    /** The number of unread events (one per key). */
    int size() {
        return pending.size();
    }

    private void wakeFirst() {
        if (!waiters.isEmpty()) waiters.getFirst().wake();
    }

    void cancel(Waiter waiter) {
        waiters.remove(waiter);
        if (!pending.isEmpty() && !waiters.isEmpty()) waiters.getFirst().wake();
    }

    /** The session ended: every waiter re-polls and then sees the end. */
    void wakeAll() {
        for (Waiter w : new ArrayList<>(waiters)) w.wake();
        waiters.clear();
    }
}
