package io.zmux;

/**
 * The event hook of a session ({@link ZmuxConfig.Builder#tracer}): ordered asynchronous
 * notifications of the session's events. A slow tracer loses events (reported by
 * {@link #dropped}), never a terminal one or the end of the session.
 */
public interface Tracer {
    default void established() {}
    default void event(SessionEvent event) {}
    default void dropped(long count) {}
    default void closed(SessionError error) {}
}
