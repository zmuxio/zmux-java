package io.zmux.shaping;

/**
 * The decision for one record. Lengths are of the unit on the carrier: the session counts the record's size field where
 * the framing has one and the pipe's write overhead on the first record of a write, and takes a length no record can have
 * to the next one. Values beyond limits are clamped; negative values count as 0.
 */
public final class RecordPlan {
    /**
     * The largest record a TLS carrier takes, in bytes of frames, and the default max_record_size.
     */
    public static final int MAX_RECORD_LENGTH = 16382;
    /** The smallest record a {@link #maxLength} leaves, in frames bytes: a STREAM frame header and data fit. */
    public static final int MIN_MAX_LENGTH = 64;

    private int maxLength;
    private final Effects effects = new Effects();
    private final UnitPlan unit = new UnitPlan();
    private boolean flush, stop;

    /** A cleared plan. */
    public RecordPlan() {}

    /** Requests a maximum encoded length. The session reports an unsupported capability for record splitting. */
    public RecordPlan maxLength(int bytes) { maxLength = bytes; return this; }
    /** The length padded up to; 0: none. */
    public RecordPlan padTo(int bytes) { unit.padTo(bytes); return this; }
    /** The length rounded up to a multiple; 0: none. */
    public RecordPlan padMultiple(int bytes) { unit.padMultiple(bytes); return this; }
    /** Bytes added after padTo and padMultiple; 0: none. */
    public RecordPlan pad(int bytes) { unit.pad(bytes); return this; }
    /** Requests a carrier write boundary. The session reports an unsupported capability for this request. */
    public RecordPlan flush(boolean on) { flush = on; return this; }
    /** Stream data of the pipe waits this long, nanoseconds; 0: none. */
    public RecordPlan delayNanos(long nanos) { unit.delayNanos(nanos); return this; }
    /** Shaping of the pipe ends after this record. */
    public RecordPlan stop(boolean on) { stop = on; return this; }
    /** Applied when the plan is made. */
    public Effects effects() { return effects; }

    public int maxLength() { return maxLength; }
    public int padTo() { return unit.padTo(); }
    public int padMultiple() { return unit.padMultiple(); }
    public int pad() { return unit.pad(); }
    public boolean isFlush() { return flush; }
    public long delayNanos() { return unit.delayNanos(); }
    public boolean isStop() { return stop; }

    /** Resets every field. */
    public void clear() {
        maxLength = 0;
        flush = stop = false;
        unit.clear();
        effects.clear();
    }
}
