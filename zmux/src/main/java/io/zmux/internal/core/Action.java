package io.zmux.internal.core;

/** A carrier command for the driver. */
public record Action(Kind kind, long pipe, long lane, int dir) {

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
