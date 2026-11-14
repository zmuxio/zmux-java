package io.zmux.internal.loop;

import io.zmux.internal.core.Event;
import io.zmux.internal.core.PipeInfo;
import io.zmux.internal.core.Waiter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

/**
 * The session events the application has not read, on the
 * loop. The queue is bounded by its structure, not by a count, and drops nothing terminal:
 *
 * <ul>
 * <li>a pipe direction keeps each pipe event kind at most once, at the place of its latest
 *     occurrence, so a transient one (PipeSuspect) and the terminal one are both seen
 *     however often the pipe flaps;
 * <li>every other kind keeps only its latest value (per pipe or tier where it has one);
 * <li>only pipes that are not retired hold events: a retirement folds the pipe's unread
 *     events into the one {@link SessionRuntime.RetiredPipes} summary, the only report of
 *     retirement, which enters the queue at the first retirement after
 *     it was last read and accumulates until it is read.
 * </ul>
 */
final class EventQueue {
    /** How many of the last retired pipe IDs a summary names. */
    static final int SUMMARY_IDS = 4;

    private record Key(Event.Kind kind, long pipe, int dir, Object detail) {}

    private final LinkedHashMap<Key, Object> pending = new LinkedHashMap<>();
    private final List<Waiter> waiters = new ArrayList<>(1);
    private SessionRuntime.RetiredPipes summary;
    private static final Key SUMMARY = new Key(null, -1, 0, null);

    /** Queues e (CLOSED and the internal kinds are not application events). */
    void add(Event e) {
        Key k = switch (e.kind()) {
            // Only the retirement: the other states reach the application as the pipe kinds below.
            case PIPE_STATE -> e.state() == PipeInfo.Out.RETIRED ? new Key(e.kind(), e.pipe(), e.dir(), e.state()) : null;
            case PIPE_ADDED, PIPE_JOINED, PIPE_SUSPECT, PIPE_RECOVERED, PIPE_FAILED, PIPE_CLOSED -> new Key(e.kind(), e.pipe(), e.dir(), null);
            case PIPE_QUALITY, PIPE_PRIORITY -> new Key(e.kind(), e.pipe(), 0, null);
            case TIER_SATURATION -> new Key(e.kind(), -1, 0, e.code());
            case CAPACITY_BELOW, CAPACITY_RESTORED -> new Key(Event.Kind.CAPACITY_BELOW, -1, 0, null);
            case SUSPENDED, RESUMED -> new Key(Event.Kind.SUSPENDED, -1, 0, null);
            // RESUME_UNATTENDED has its own key: it follows SessionSuspended rather than replacing it; a resumption
            // makes it stale.
            case RESUME_UNATTENDED, GOAWAY_RECEIVED, EPOCH_COMMITTED, EPOCH_DELETED -> new Key(e.kind(), -1, 0, null);
            case JOIN_AUTH_FAILED -> new Key(e.kind(), -1, 0, e.reason());
            default -> null;
        };
        if (k == null) return;
        // The only PIPE_STATE with a key is the retirement.
        if (e.kind() == Event.Kind.PIPE_STATE) {
            fold(e.pipe());
        } else {
            if (e.kind() == Event.Kind.RESUMED) pending.remove(new Key(Event.Kind.RESUME_UNATTENDED, -1, 0, null));
            pending.remove(k);
            pending.put(k, e);
        }
        if (!waiters.isEmpty()) waiters.getFirst().wake();
    }

    /**
     * Queues an event of the API above the core (such as a carrier selection report), returned
     * by {@link #poll} as it is; a later event with an equal key replaces an unread one.
     */
    void addApplication(Object key, Object event) {
        Key k = new Key(null, -2, 0, Objects.requireNonNull(key));
        pending.remove(k);
        pending.put(k, Objects.requireNonNull(event));
        if (!waiters.isEmpty()) waiters.getFirst().wake();
    }

    /**
     * Folds a retired pipe's unread events into the summary. Only unread terminal states count,
     * by direction: an unread FAILED in either direction counts the pipe as failed, else an
     * unread end (outbound CLOSED, inbound ENDED) as closed; a direction whose terminal state was
     * already read counts for neither. Unread non-terminal events leave the queue uncounted:
     * {@code dropped} counts only events a queue limit discarded, and this queue has none.
     */
    private void fold(long pipe) {
        boolean failed = false, closed = false;
        for (Iterator<Key> it = pending.keySet().iterator(); it.hasNext(); ) {
            Key k = it.next();
            if (k.pipe() != pipe || k.kind() == null) continue;
            if (k.kind() == Event.Kind.PIPE_FAILED) failed = true;
            else if (k.kind() == Event.Kind.PIPE_CLOSED) closed = true;
            it.remove();
        }
        var s = summary == null ? new SessionRuntime.RetiredPipes(0, 0, 0, 0, List.of()) : summary;
        List<Long> ids = new ArrayList<>(s.last());
        ids.add(pipe);
        if (ids.size() > SUMMARY_IDS) ids.removeFirst();
        summary = new SessionRuntime.RetiredPipes(s.retired() + 1, s.failed() + (failed ? 1 : 0),
                s.closed() + (!failed && closed ? 1 : 0), s.dropped(), List.copyOf(ids));
        pending.put(SUMMARY, summary); // An unread summary keeps its place: that of the first retirement since the last read.
    }

    /** The oldest unread event (an Event or the RetiredPipes summary), or null with waiter registered. */
    @SuppressWarnings("ReferenceEquality") // SUMMARY is a sentinel key
    Object poll(Waiter waiter) {
        Iterator<java.util.Map.Entry<Key, Object>> it = pending.entrySet().iterator();
        if (!it.hasNext()) {
            if (!waiters.contains(waiter)) waiters.add(waiter);
            return null;
        }
        var first = it.next();
        it.remove();
        Key k = first.getKey();
        if (k == SUMMARY) summary = null;
        waiters.remove(waiter);
        if (!pending.isEmpty() && !waiters.isEmpty()) waiters.getFirst().wake();
        return first.getValue();
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
