package io.zmux;

/**
 * The event hook of a session ({@link ZmuxConfig.Builder#tracer(Tracer,int)}): ordered asynchronous
 * notifications of the session's events, each naming the session ({@link ZmuxSession#id()}), so one tracer can
 * serve many sessions. Calls for one session are serial; calls for different sessions can run concurrently.
 * A slow tracer loses events of its session once that session's trace backlog (the capacity given with the
 * tracer) is full; {@link #dropped} reports how many, before the next event delivered. Terminal events and the
 * end of the session are never lost.
 */
public interface Tracer {
    /** The session was established. */
    default void established(SessionId session) {}
    /** An event of the session. */
    default void event(SessionId session, SessionEvent event) {}
    /** count events of the session were lost because its trace backlog was full. */
    default void dropped(SessionId session, long count) {}
    /** The session ended with error. */
    default void closed(SessionId session, SessionError error) {}
}
