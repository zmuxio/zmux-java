package io.zmux;

/**
 * A stream's scheduling priority (EXTENSIONS.md priorities): urgency 0 (first) to 7, and
 * whether streams of the same urgency share the pipe round-robin.
 */
public record Priority(int urgency, boolean incremental) {
    /** Urgency 3, not incremental. */
    public static final Priority DEFAULT = new Priority(3, false);

    public Priority {
        if (urgency < 0 || urgency > 7) throw new IllegalArgumentException("urgency 0 to 7");
    }
}
