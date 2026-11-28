package io.zmux.http3;

import io.zmux.internal.spi.CarrierBudget;
import io.zmux.internal.spi.Loops;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Future CONNECT inputs on one event loop. Capacity is reserved before the input is retained. */
final class WebTransportReorder {
    interface Input {
        void deliver(WebTransportSession session);
        void discard(boolean retired);
    }

    private final ScheduledExecutorService loop;
    private final LongSupplier clock;
    private final long timeout, limit;
    private final Map<Long, ArrayDeque<Entry>> waiting = new HashMap<>();
    private long count;

    WebTransportReorder(ScheduledExecutorService loop, LongSupplier clock, long timeout, long limit) {
        this.loop = loop;
        this.clock = clock;
        this.timeout = timeout;
        this.limit = limit;
    }

    private static final class Entry {
        Input input;
        final long deadline;
        Runnable release;
        ScheduledFuture<?> timer;

        Entry(Input input, long deadline, Runnable release) {
            this.input = input;
            this.deadline = deadline;
            this.release = release;
        }
    }

    Runnable hold(long id, long bytes, CarrierBudget budget, Object connection, Supplier<Input> retain) {
        if (count >= limit) return null;
        var release = budget.acquire(connection, 0, Math.addExact(bytes, CarrierBudget.DELIVERY_BYTES));
        if (release == null) return null;
        Entry entry;
        try {
            entry = new Entry(retain.get(), clock.getAsLong() + timeout, release);
        } catch (RuntimeException | Error failure) {
            release.run();
            throw failure;
        }
        waiting.computeIfAbsent(id, ignored -> new ArrayDeque<>()).addLast(entry);
        count++;
        Runnable remove = () -> {
            var queue = waiting.get(id);
            if (queue == null || !queue.remove(entry)) return;
            if (queue.isEmpty()) waiting.remove(id);
            finish(entry, null, false);
        };
        entry.timer = Loops.schedule(loop, remove, timeout, TimeUnit.NANOSECONDS, remove);
        return remove;
    }

    void resolve(long id, WebTransportSession session) {
        var queue = waiting.remove(id);
        if (queue == null) return;
        for (var entry : queue) finish(entry, session, session == null);
    }

    void close() {
        for (long id : List.copyOf(waiting.keySet())) resolve(id, null);
    }

    private void finish(Entry entry, WebTransportSession session, boolean retired) {
        count--;
        Loops.cancel(entry.timer);
        try {
            if (session == null || clock.getAsLong() - entry.deadline >= 0) entry.input.discard(retired);
            else entry.input.deliver(session);
        } finally {
            entry.release.run();
            entry.input = null;
            entry.release = null;
            entry.timer = null;
        }
    }
}
