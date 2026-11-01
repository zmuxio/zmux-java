package io.zmux.shaping;

/** The decision for one record. Values beyond limits are clamped; negative values count as 0. */
public final class RecordPlan {
    /**
     * The largest record a TLS carrier takes, in bytes of frames, and the default max_record_size
     * (SPEC.md Section 3.3); also the PADDING a ratio limit allows from the start of a session.
     */
    public static final int MAX_RECORD_LENGTH = 16382;
    /** The smallest {@link #maxLength} honoured, in bytes: a STREAM frame header and data fit. */
    public static final int MIN_MAX_LENGTH = 64;

    private int maxLength, padTo, padMultiple, pad;
    private boolean flush, stop;
    private long delayNanos;
    private final Reply reply = new Reply();

    /** A cleared plan. */
    public RecordPlan() {}

    /** Largest frames length, bytes, at least {@link #MIN_MAX_LENGTH}; 0: the view's limit. */
    public RecordPlan maxLength(int bytes) { maxLength = bytes; return this; }
    /** Frames length padded up to, bytes; 0: none. */
    public RecordPlan padTo(int bytes) { padTo = bytes; return this; }
    /** Frames length rounded up to a multiple, bytes; 0: none. */
    public RecordPlan padMultiple(int bytes) { padMultiple = bytes; return this; }
    /** PADDING bytes appended after padTo and padMultiple; 0: none. */
    public RecordPlan pad(int bytes) { pad = bytes; return this; }
    /** The record ends its carrier write. */
    public RecordPlan flush(boolean on) { flush = on; return this; }
    /** Stream data of the pipe waits this long, nanoseconds; 0: none. */
    public RecordPlan delayNanos(long nanos) { delayNanos = nanos; return this; }
    /** Shaping of the pipe ends after this record. */
    public RecordPlan stop(boolean on) { stop = on; return this; }
    /** Applied as an event reply when the plan is made. */
    public Reply reply() { return reply; }

    public int maxLength() { return maxLength; }
    public int padTo() { return padTo; }
    public int padMultiple() { return padMultiple; }
    public int pad() { return pad; }
    public boolean flush() { return flush; }
    public long delayNanos() { return delayNanos; }
    public boolean stop() { return stop; }

    /** Resets every field. */
    public void clear() {
        maxLength = padTo = padMultiple = pad = 0;
        flush = stop = false;
        delayNanos = 0;
        reply.clear();
    }
}
