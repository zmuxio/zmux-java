package io.zmux;

import java.util.IdentityHashMap;
import java.util.Objects;

/** A reusable cancellation signal with removable registrations. Callbacks run outside its lock. */
public final class Cancellation implements AutoCloseable {
    @FunctionalInterface public interface Registration extends AutoCloseable { @Override void close(); }
    /** Keyed by a fresh token per registration, so one callback registered twice is two registrations. */
    private final IdentityHashMap<Object, Runnable> listeners = new IdentityHashMap<>();
    private boolean cancelled;

    /** A signal not yet cancelled. */
    public Cancellation() {}

    /** Registers a nonblocking callback, or invokes it immediately when already cancelled. */
    public Registration onCancel(Runnable callback) {
        Objects.requireNonNull(callback);
        Object token = new Object();
        synchronized (this) {
            if (!cancelled) { listeners.put(token, callback); return () -> { synchronized (this) { listeners.remove(token); } }; }
        }
        callback.run(); return () -> {};
    }
    public synchronized boolean isCancelled() { return cancelled; }
    /** Cancels once; all callbacks are notified even when one throws. */
    public void cancel() {
        Runnable[] pending;
        synchronized (this) {
            if (cancelled) return;
            cancelled = true; pending = listeners.values().toArray(Runnable[]::new); listeners.clear();
        }
        Throwable failure = null;
        for (Runnable callback : pending) {
            try { callback.run(); }
            catch (RuntimeException | Error error) { if (failure == null) failure = error; else if (failure != error) failure.addSuppressed(error); }
        }
        if (failure instanceof RuntimeException error) throw error;
        if (failure instanceof Error error) throw error;
    }
    @Override public void close() { cancel(); }
}
