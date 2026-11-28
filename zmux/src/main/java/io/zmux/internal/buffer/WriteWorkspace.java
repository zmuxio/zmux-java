package io.zmux.internal.buffer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** One prepaid write workspace. Retained carrier views postpone reuse and physical release. */
public final class WriteWorkspace {
    private static final int CLOSED = Integer.MIN_VALUE;
    private static final int RETURNING = 1 << 30;
    private final AtomicInteger holds = new AtomicInteger();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private volatile Runnable writable;

    public boolean available() { return holds.get() == 0; }

    public void onWritable(Runnable writable) { this.writable = writable; }

    public boolean tryTake() { return holds.compareAndSet(0, 1); }

    /** Acquires a user's reference while the workspace's batch is valid. */
    public Runnable retain(Runnable release) {
        for (int state = holds.get(); ; state = holds.get()) {
            int count = state & ~CLOSED;
            if (count == 0 || count >= RETURNING - 1) throw new IllegalStateException("write workspace has no retainable batch");
            if (holds.compareAndSet(state, state + 1)) break;
        }
        var open = new AtomicBoolean(true);
        return () -> {
            if (open.compareAndSet(true, false)) giveBack(release, true);
        };
    }

    /** Returns one reference. Cleanup precedes publishing a reusable or drained workspace. */
    public void giveBack(Runnable cleanup, boolean external) {
        for (int state = holds.get(); ; state = holds.get()) {
            int count = state & ~CLOSED;
            if (count == 0 || count >= RETURNING) throw new IllegalStateException("write workspace already returned");
            int next = count == 1 ? (state & CLOSED) | RETURNING : state - 1;
            if (!holds.compareAndSet(state, next)) continue;
            if (count != 1) return;
            try {
                if (cleanup != null) cleanup.run();
            } finally {
                int before = holds.getAndUpdate(current -> current & CLOSED);
                if ((before & CLOSED) != 0) drained.complete(null);
                else if (external) {
                    Runnable ready = writable;
                    if (ready != null) ready.run();
                }
            }
            return;
        }
    }

    public CompletionStage<Void> closeAfter(CompletionStage<Void> physicalClose) {
        physicalClose.whenComplete((v, error) -> {
            if ((holds.getAndUpdate(current -> current | CLOSED) & ~CLOSED) == 0) drained.complete(null);
        });
        return drained;
    }
}
