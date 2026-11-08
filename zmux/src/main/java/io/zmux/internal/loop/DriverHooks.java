package io.zmux.internal.loop;

import io.zmux.helper.Limiter;
import io.zmux.internal.core.Event;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;

/** Immutable driver callbacks; the core receives only the corresponding feature flags. */
public record DriverHooks(Consumer<Event> tracer, LongConsumer dropped, int tracerQueue, LongPredicate allowWindowGrowth,
                          Limiter datagramSend, Limiter datagramReceive, PickerBridge.Picker picker, JoinPolicy joinPolicy,
                          io.zmux.carrier.HandshakeShaper handshakeShaper, System.Logger logger, Redial redial) {
    /** The session's redial hook and its backoff bounds (nanoseconds), for the public layer. */
    public record Redial(Object hook, long minBackoff, long maxBackoff) {}

    /** The acceptor's checks of a verified Hello; unset, only Sealed passes. */
    public interface JoinPolicy {
        JoinPolicy DEFAULT = new JoinPolicy() { };

        // Internal packages are exported only to this project's modules (qualified export), so these signatures name types that stay unexported.
        //noinspection ClassEscapesItsScope
        default boolean allow(Transport c, io.zmux.internal.join.Mode mode, Object peer) { return mode == io.zmux.internal.join.Mode.SEALED; }

        default boolean authorize(Object host, long pipe) { return true; }

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

    public static final DriverHooks NONE = new DriverHooks(null, null, 256, null, null, null, null, JoinPolicy.DEFAULT, null, null, null);
}
