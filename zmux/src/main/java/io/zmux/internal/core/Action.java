package io.zmux.internal.core;

/** A carrier command for the driver. */
public record Action(Kind kind, long pipe, long lane, int dir, Throwable cause) {

    public Action(Kind kind, long pipe, long lane, int dir) {
        this(kind, pipe, lane, dir, null);
    }

    // A diagnostic cause does not change the carrier command's identity.
    @Override public boolean equals(Object other) {
        return other instanceof Action action && kind == action.kind && pipe == action.pipe
                && lane == action.lane && dir == action.dir;
    }

    @Override public int hashCode() {
        int hash = java.util.Objects.hashCode(kind);
        hash = 31 * hash + Long.hashCode(pipe);
        hash = 31 * hash + Long.hashCode(lane);
        return 31 * hash + Integer.hashCode(dir);
    }

    /** Carrier commands. */
    public enum Kind {
        /** Gracefully end the write side of an outbound lane. */
        SHUTDOWN,
        /** Abort both halves (failure). */
        ABORT,
        /** Close a carrier direction gracefully (after linger). */
        CLOSE,
        /** Open a new outbound lane on the pipe's connection. */
        OPEN_LANE,
        /** Reset an unconfirmed outbound lane (refused). */
        RESET_LANE
    }

    public static final int DIR_IN = 1;
    public static final int DIR_OUT = 2;
    public static final int DIR_BIDI = 3;
}
