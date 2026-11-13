package io.zmux;

/** The outcome of a {@link ReplayFilter#check}. */
public enum ReplayVerdict {
    /** The key is new within the window and is now recorded. */
    FRESH,
    /** The key was recorded before: a replay. */
    SEEN,
    /** The stamp lies outside the window. */
    OUTSIDE,
    /** The filter holds its maximum of keys. */
    FULL
}
