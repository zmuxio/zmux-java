package io.zmux;

import java.util.Objects;

/** An application's reason for closing a session: its error code and a human-readable reason. */
public record ApplicationError(long code, String reason) {
    /** Code 0, no reason: the ordinary end. */
    public static final ApplicationError NONE = new ApplicationError(0, "");

    public ApplicationError {
        Objects.requireNonNull(reason);
        if (code < 0) throw new IllegalArgumentException("negative code");
    }
}
