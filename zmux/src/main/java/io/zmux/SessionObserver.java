package io.zmux;

/** Ordered asynchronous notifications. A slow observer loses events, never the terminal notification. */
public interface SessionObserver {
    default void established() {}
    default void event(SessionEvent event) {}
    default void dropped(long count) {}
    default void closed(SessionError error) {}
}
