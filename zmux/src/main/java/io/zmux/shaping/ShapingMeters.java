package io.zmux.shaping;

/**
 * A session's readings, all unsigned. {@code -1L} is 2^64 - 1, the largest value.
 *
 * @param wireLeft what the wire account allows injections now; the largest value when it is not set
 * @param stallLeft reserved; always the largest value
 * @param wireSpent all wire bytes shaping added, saturating
 * @param stallSpentMicros the waits applied, in microseconds
 * @param sentBytes the bytes this endpoint sent of its own: no padding, injections or copies
 */
public record ShapingMeters(long wireLeft, long stallLeft, long wireSpent, long stallSpentMicros, long sentBytes) {
    /** What a shaper without {@link Capability#BUDGET} is given. */
    public static final ShapingMeters ZERO = new ShapingMeters(0, 0, 0, 0, 0);

    /** {@link #wireSpent} in parts per million of {@link #sentBytes}. */
    public long wirePpm() { return wirePpm(wireSpent, sentBytes); }

    /** s bytes in parts per million of e, both unsigned; the largest value when e is 0. Past 2^44 both lose their low 20 bits. */
    public static long wirePpm(long s, long e) {
        if (((s | e) >>> 44) != 0) {
            s >>>= 20;
            e >>>= 20;
        }
        return e == 0 ? -1L : Long.divideUnsigned(s * 1_000_000, e);
    }
}
