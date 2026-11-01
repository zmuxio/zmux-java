package io.zmux.internal.loop;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Whether a session took a carrier ({@link Transport#whenTaken}): decided once, by the first of
 * {@link Transport#taken} (true) and the carrier's close (false), and completed off the loop,
 * as a carrier's close is. It belongs to the carrier, so it outlives the transports that move it.
 */
public final class TakenSignal {
    private final CompletableFuture<Boolean> taken = new CompletableFuture<>();
    private final AtomicBoolean decided = new AtomicBoolean();

    /** The first call decides. */
    public void decide(boolean v) {
        if (decided.compareAndSet(false, true)) Callbacks.EXECUTOR.execute(() -> taken.complete(v));
    }

    /** Decides as next (a transport the carrier moved to) is decided; false when there is none. */
    public void follow(Transport next) {
        if (next == null) { decide(false); return; }
        next.whenTaken().whenComplete((v, e) -> decide(Boolean.TRUE.equals(v)));
    }

    public CompletionStage<Boolean> stage() { return taken.minimalCompletionStage(); }
}
