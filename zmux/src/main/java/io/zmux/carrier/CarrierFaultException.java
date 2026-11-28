package io.zmux.carrier;

import java.io.IOException;
import java.util.Objects;

/** Identifies one explicitly shared physical carrier failure across affected attachments. */
public final class CarrierFaultException extends IOException {
    /** Create one identity per failure and share it only among notifications of that failure. */
    // Episodes distinguish separate failures on the same physical connection by identity.
    @SuppressWarnings("ClassCanBeRecord")
    public static final class Episode {
        private final Object connection;

        public Episode(Object connection) { this.connection = Objects.requireNonNull(connection); }

        public Object connection() { return connection; }
    }

    private final Episode episode;

    public CarrierFaultException(Episode episode, IOException cause) {
        super(cause);
        this.episode = Objects.requireNonNull(episode);
    }

    public Episode episode() { return episode; }

    public static Episode episodeOf(Throwable failure) {
        for (var cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof CarrierFaultException fault) return fault.episode;
        }
        return null;
    }
}
