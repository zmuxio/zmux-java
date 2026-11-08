package io.zmux.shaping;

/**
 * The next outgoing record.
 *
 * @param pipe the pipe ID
 * @param lane the lane
 * @param seq records of the pipe planned before this one
 * @param limit the largest frames length, bytes
 * @param writeStart the record is the first of a carrier write (every record of a message carrier)
 * @param nowNanos the session clock, nanoseconds
 * @param meters the session's readings with {@link Capability#BUDGET}, otherwise {@link ShapingMeters#ZERO}
 */
public record RecordView(long pipe, long lane, long seq, int limit, boolean writeStart, long nowNanos, ShapingMeters meters) {
    /** A view without readings. */
    public RecordView(long pipe, long lane, long seq, int limit, boolean writeStart, long nowNanos) {
        this(pipe, lane, seq, limit, writeStart, nowNanos, ShapingMeters.ZERO);
    }
}
