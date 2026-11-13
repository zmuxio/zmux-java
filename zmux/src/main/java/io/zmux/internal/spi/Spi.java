package io.zmux.internal.spi;

import io.zmux.ZmuxFuture;
import io.zmux.ZmuxSession;
import io.zmux.carrier.PipeIo;
import java.util.concurrent.CompletionStage;

/**
 * The entry points the carrier modules (zmux-http, zmux-quic) need beyond the public API,
 * exported to them only. io.zmux installs the implementation when its classes initialise.
 */
public final class Spi {
    /** What io.zmux implements. */
    public interface Access {
        void handleLane(ZmuxSession session, PipeIo io);

        <T> ZmuxFuture<T> future(CompletionStage<T> stage, Runnable cancel);
    }

    private static volatile Access access;

    private Spi() {}

    /** Installs io.zmux's implementation; once. */
    public static void install(Access a) {
        if (access == null) access = a;
    }

    private static Access access() {
        Access a = access;
        if (a == null) {
            try {
                Class.forName("io.zmux.ZmuxFuture", true, Spi.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
            a = access;
        }
        return a;
    }

    /**
     * Hands a lane carrier (a further stream of the connection that one of session's pipes runs
     * on) to the session, which reads its lane header and attaches it to its pipe.
     */
    public static void handleLane(ZmuxSession session, PipeIo io) { access().handleLane(session, io); }

    /** A future that completes as stage does; cancelling it runs cancel (null: nothing). */
    public static <T> ZmuxFuture<T> future(CompletionStage<T> stage, Runnable cancel) { return access().future(stage, cancel); }
}
