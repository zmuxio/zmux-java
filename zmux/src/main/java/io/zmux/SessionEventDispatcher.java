package io.zmux;


import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;

final class SessionEventDispatcher {
    private final Object lock;
    private final ZmuxEventHandler handler;
    private final Deque<ZmuxEvent> pendingEvents = new ArrayDeque<>();
    private boolean sessionClosedEventQueued;
    private volatile boolean pendingEventsAvailable;
    private Thread emittingThread;

    SessionEventDispatcher(Object lock, ZmuxEventHandler handler) {
        this.lock = lock;
        this.handler = handler;
    }

    void emitPendingEvents() {
        if (this.handler == null) {
            return;
        }
        while (this.hasPendingEvents()) {
            if (!this.tryClaimEmitter()) {
                return;
            }
            try {
                this.drainEvents();
            } finally {
                this.releaseEmitter();
            }
        }
    }

    private boolean hasPendingEvents() {
        return this.pendingEventsAvailable;
    }

    private boolean tryClaimEmitter() {
        synchronized (this) {
            if (this.emittingThread != null) {
                return false;
            }
            this.emittingThread = Thread.currentThread();
            return true;
        }
    }

    private void releaseEmitter() {
        synchronized (this) {
            this.emittingThread = null;
        }
    }

    private void drainEvents() {
        while (true) {
            ZmuxEvent event;
            synchronized (this.lock) {
                event = this.pendingEvents.pollFirst();
                this.pendingEventsAvailable = !this.pendingEvents.isEmpty();
            }
            if (event == null) {
                return;
            }
            try {
                this.handler.onEvent(event);
            } catch (RuntimeException ignored) {
                // User handler failures are best-effort; JVM Errors must still escape.
            }
        }
    }

    void enqueueSessionClosedEventLocked(SessionState state, IOException error) {
        if (this.handler == null || this.sessionClosedEventQueued) {
            return;
        }
        this.sessionClosedEventQueued = true;
        this.pendingEvents.addLast(new ZmuxEvent(
                ZmuxEventType.SESSION_CLOSED,
                state,
                0L,
                null,
                false,
                false,
                Instant.now(),
                error,
                false
        ));
        this.pendingEventsAvailable = true;
    }

    void enqueueStreamEventLocked(SessionState state, StreamRuntime streamRuntime, ZmuxEventType type, IOException error) {
        ZmuxEvent event = this.takeStreamEventLocked(state, streamRuntime, type, error);
        if (event != null) {
            this.pendingEvents.addLast(event);
            this.pendingEventsAvailable = true;
        }
    }

    private ZmuxEvent takeStreamEventLocked(SessionState state,
                                            StreamRuntime streamRuntime,
                                            ZmuxEventType type,
                                            IOException error) {
        if (this.handler == null || streamRuntime == null) {
            return null;
        }

        switch (type) {
            case STREAM_OPENED:
                if (!streamRuntime.openedLocally()
                        || !streamRuntime.peerVisibleLocked()
                        || streamRuntime.openedEventSentLocked()) {
                    return null;
                }
                streamRuntime.markOpenedEventSentLocked();
                break;
            case STREAM_ACCEPTED:
                if (!streamRuntime.acceptedLocked() || streamRuntime.acceptedEventSentLocked()) {
                    return null;
                }
                streamRuntime.markAcceptedEventSentLocked();
                break;
            default:
                return null;
        }

        ZmuxStreamInfo stream = streamRuntime.eventStreamSnapshotLocked();
        return new ZmuxEvent(
                type,
                state,
                streamRuntime.streamIdInternal(),
                stream,
                streamRuntime.openedLocally(),
                streamRuntime.bidirectional(),
                Instant.now(),
                error,
                streamRuntime.applicationVisible()
        );
    }
}
