package io.zmux;

/**
 * The event hook of a session ({@link ZmuxConfig.Builder#tracer(Tracer)}): ordered asynchronous
 * notifications of the session's events, each naming the session ({@link ZmuxSession#id()}), so one tracer can
 * serve many sessions. Calls for one session are serial; calls for different sessions can run concurrently.
 *
 * <p>The tracer observes: it sees each occurrence of an event, before the event queue of
 * {@link SessionEvent} merges it (a {@link SessionEvent.JoinAuthFailed} with count 1, one
 * {@link SessionEvent.PipesRetired} per retired pipe), without taking it from the stream. The session
 * never waits for its tracer: each session has a backlog of 256 events, fixed, and an event that finds
 * it full is dropped and counted, never one already queued. An application that must not lose events reads
 * {@link ZmuxSession#nextEvent}, which never drops. {@link #established} is the first call and {@link #closed} the
 * last, nothing follows it and nothing that happens after the session ended is traced; {@link #established},
 * {@link #dropped} and {@link #closed} take no room from the 256 and are never dropped. {@link #dropped} is
 * called where the lost events stood: before the next event the backlog accepts, or before {@code closed},
 * whichever comes first.
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
