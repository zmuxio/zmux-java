package io.zmux;

/**
 * The identifier of a pipe: unique for the life of its session, 0 for the bootstrap pipe. A pipe ID
 * is an unsigned 62-bit value; it is its own type, never mixed with stream IDs, epochs or counts,
 * and offers no arithmetic.
 *
 * @param value the ID, from 0 to 2<sup>62</sup> - 1
 */
public record PipeId(long value) {
    /** The bootstrap pipe's ID. */
    public static final PipeId BOOTSTRAP = new PipeId(0);

    public PipeId {
        if (value < 0 || value > (1L << 62) - 1) throw new IllegalArgumentException("pipe ID " + value + " outside 0 to 2^62 - 1");
    }

    /** The ID of value; the bootstrap pipe's shared instance for 0. */
    public static PipeId of(long value) { return value == 0 ? BOOTSTRAP : new PipeId(value); }

    @Override public String toString() { return Long.toString(value); }
}
