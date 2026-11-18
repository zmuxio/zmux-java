package io.zmux;

/**
 * A stream's scheduling priority: urgency 0 (first) to 7, and
 * whether streams of the same urgency share the pipe round-robin.
 */
public record Priority(int urgency, boolean incremental) {
    /** The local default of a new sending part: urgency 3, incremental. */
    public static final Priority LOCAL_DEFAULT = new Priority(3, true);
    /**
     * What an absent PRIORITY signal means on the wire (EXTENSIONS.md Section 5): urgency 3, not incremental. It is
     * not the local default.
     */
    public static final Priority WIRE_DEFAULT = new Priority(3, false);

    public Priority {
        if (urgency < 0 || urgency > 7) throw new IllegalArgumentException("urgency 0 to 7");
    }
}
