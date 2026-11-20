package io.zmux.adapter;

import java.util.concurrent.CompletionStage;

/**
 * The step of an HTTP upgrade adapter that completes the upgrade (answers it with 101) once the listener's admission
 * accepted its head or deferred the decision to the carrier ({@link ListenerAdapter#admitFirst}).
 */
@FunctionalInterface
public interface Upgrade {
    /**
     * Completes the upgrade, so that the carrier's first message may follow; called at most once, on a zmux event
     * loop: it must not block. The stage completes once the carrier is open; a stage that fails (or a null one, or
     * a throw) is a failed upgrade, and the adapter closes what it upgraded. While the decision is deferred, zmux
     * waits for it within what is left of the peek timeout and while the listener takes carriers; when it stops
     * waiting it cancels the stage ({@code toCompletableFuture().cancel(false)}) and closes the carrier.
     */
    CompletionStage<?> open();
}
