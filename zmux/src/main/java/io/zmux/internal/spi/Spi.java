package io.zmux.internal.spi;

import io.zmux.ZmuxFuture;
import io.zmux.helper.Permit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * The entry points the carrier modules (zmux-http, zmux-quic) need beyond the public API,
 * exported to them only. io.zmux installs the implementation when its classes initialise.
 */
public final class Spi {
    /** What io.zmux implements. */
    public interface Access {

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
     * Gives a pending grant up: cancelled while it waits, its permit closed once granted, so the bytes go back either way.
     * The one place the abandon of a {@link Permit#acquire} future is written.
     */
    public static void abandon(CompletableFuture<Permit> grant) {
        if (!grant.cancel(true) && grant.isDone() && !grant.isCompletedExceptionally()) grant.join().close();
    }

    /** A future that completes as stage does; cancelling it runs cancel (null: nothing). */
    public static <T> ZmuxFuture<T> future(CompletionStage<T> stage, Runnable cancel) { return access().future(stage, cancel); }
}
