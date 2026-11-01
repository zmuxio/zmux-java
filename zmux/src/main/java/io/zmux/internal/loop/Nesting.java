package io.zmux.internal.loop;

import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Process-local dependency graph of nested sessions. Only the cycle check and the edge
 * recording run under the lock, atomically; the session admission that follows runs outside
 * it and returns the edge when refused.
 */
final class Nesting {
    private static final Object LOCK = new Object();
    private static final IdentityHashMap<SessionRuntime, IdentityHashMap<SessionRuntime, Integer>> edges = new IdentityHashMap<>();
    private Nesting() {}

    private static boolean cyclic(SessionRuntime child, Object connection) {
        if (!(connection instanceof SessionRuntime parent)) return false;
        assert Thread.holdsLock(LOCK);
        var seen = new IdentityHashMap<SessionRuntime, Boolean>();
        var visit = new ArrayDeque<SessionRuntime>(); visit.add(parent);
        while (!visit.isEmpty()) {
            SessionRuntime current = visit.removeLast();
            if (current == child) return true;
            if (seen.put(current, Boolean.TRUE) != null) continue;
            var dependencies = edges.get(current);
            if (dependencies != null) visit.addAll(dependencies.keySet());
        }
        return false;
    }

    private static final Runnable NONE = () -> {};

    /** Records child's dependency on connection's session; null (nothing recorded) if it would form a cycle. */
    static Runnable tryAttach(SessionRuntime child, Object connection) {
        if (!(connection instanceof SessionRuntime parent)) return NONE;
        synchronized (LOCK) {
            if (cyclic(child, parent)) return null;
            edges.computeIfAbsent(child, ignored -> new IdentityHashMap<>()).merge(parent, 1, Integer::sum);
        }
        var released = new AtomicBoolean();
        return () -> {
            if (!released.compareAndSet(false, true)) return;
            synchronized (LOCK) {
                // The edge was recorded by this attach and is released once, so it is present.
                var dependencies = Objects.requireNonNull(edges.get(child));
                dependencies.computeIfPresent(parent, (ignored, refs) -> refs == 1 ? null : refs - 1);
                if (dependencies.isEmpty()) edges.remove(child);
            }
        };
    }
}
