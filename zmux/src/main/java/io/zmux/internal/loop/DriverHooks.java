package io.zmux.internal.loop;

import io.zmux.helper.Limiter;
import io.zmux.internal.core.Event;
import io.zmux.SessionId;
import java.util.function.LongPredicate;

/** Immutable driver callbacks; the core receives only the corresponding feature flags. */
public record DriverHooks(TraceSink tracer, LongPredicate allowWindowGrowth,
                          Limiter datagramSend, Limiter datagramReceive, PickerBridge.Picker picker, Object scheduler, JoinPolicy joinPolicy,
                          io.zmux.carrier.HandshakeShaper handshakeShaper, System.Logger logger, io.zmux.ClockDomain clockDomain, io.zmux.AccountingPolicy accounting) {
    public DriverHooks(TraceSink tracer, LongPredicate allowWindowGrowth, Limiter datagramSend, Limiter datagramReceive,
                       PickerBridge.Picker picker, Object scheduler, JoinPolicy joinPolicy,
                       io.zmux.carrier.HandshakeShaper handshakeShaper, System.Logger logger) {
        this(tracer, allowWindowGrowth, datagramSend, datagramReceive, picker, scheduler, joinPolicy, handshakeShaper,
                logger, io.zmux.ClockDomain.system(), io.zmux.AccountingPolicy.OFF);
    }

    public DriverHooks withClock(io.zmux.ClockDomain clock) {
        return new DriverHooks(tracer, allowWindowGrowth, datagramSend, datagramReceive, picker, scheduler, joinPolicy,
                handshakeShaper, logger, clock, accounting);
    }
    /**
     * The tracer as the loop drives it. {@link #view} runs on the loop, the other calls on the session's serial
     * hook context, in order.
     */
    public interface TraceSink {
        /** The application's form of a core event, or null when the tracer is told nothing of it. */
        io.zmux.SessionEvent view(Event e);

        void established(SessionId session);

        void event(SessionId session, io.zmux.SessionEvent event);

        /** count events of the session were lost to its full backlog, where they stood. */
        void dropped(SessionId session, long count);

        void closed(SessionId session, io.zmux.internal.core.SessionError error);
    }


    /** The acceptor's checks of a verified Hello; unset, only Sealed passes. */
    public interface JoinPolicy {
        JoinPolicy DEFAULT = new JoinPolicy() { };

        // Internal packages are exported only to this project's modules (qualified export), so these signatures name types that stay unexported.
        //noinspection ClassEscapesItsScope
        default boolean allow(Transport c, io.zmux.internal.join.Mode mode, Object peer) { return mode == io.zmux.internal.join.Mode.SEALED; }

        default io.zmux.AuthorizeJoin initialAuthorizer() { return null; }

        default Throwable publicError(io.zmux.internal.core.CoreException terminal) { return terminal; }

        /** The record mode of a carrier the local endpoint joins, when its options name none. */
        //noinspection ClassEscapesItsScope
        default io.zmux.internal.join.Mode choose(Transport c) { return io.zmux.internal.join.Mode.SEALED; }
    }

    /** Logs a warning through the configured logger, if any; a failing logger is ignored. */
    public void warn(String message) {
        log(System.Logger.Level.WARNING, message, null);
    }

    /** Logs through the configured logger, if any; a failing logger is ignored. */
    public void log(System.Logger.Level level, String message, Throwable error) {
        log(logger, level, message, error);
    }

    /** Logs through {@code logger}, if any; a failing logger is ignored. */
    public static void log(System.Logger logger, System.Logger.Level level, String message, Throwable error) {
        if (logger == null) return;
        try {
            if (error == null) logger.log(level, message);
            else logger.log(level, message, error);
        } catch (RuntimeException ignored) {
            // A logger is no event source: its failure changes nothing.
        }
    }

    public static final DriverHooks NONE = new DriverHooks(null, null, null, null, null, null, JoinPolicy.DEFAULT, null, null);
}
