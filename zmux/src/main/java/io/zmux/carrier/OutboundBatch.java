package io.zmux.carrier;

import java.nio.ByteBuffer;

/**
 * One write of records to a {@link PipeWriter}: read-only segment views, the record
 * boundaries and one handle that keeps the payload past the write. With
 * {@link Framing#MESSAGES} each record is one message; with {@link Framing#BYTE_STREAM}
 * the segments are written in order and the boundaries are informational.
 *
 * <p>The batch and its views are valid until the write's completion is reported; a writer
 * that keeps payload past that (because its completion means "accepted", not "released")
 * calls {@link #retain()} before completing and closes the handle when it is done.
 */
public interface OutboundBatch {
    /** Bytes in all segments. */
    int bytes();

    int segmentCount();

    /** Segment i as a read-only view whose position is independent of the batch. */
    ByteBuffer segment(int i);

    /**
     * Segments [0, {@link #segmentCount()}) as views, for a gathering write: in reuse when it
     * has room (entries past the count are left as they are), else in a new array.
     */
    ByteBuffer[] segments(ByteBuffer[] reuse);

    /** The number of records (messages). */
    int messageCount();

    /** The segment after record m: record m is segments [messageEnd(m - 1), messageEnd(m)), from 0 for m = 0. */
    int messageEnd(int m);

    /**
     * Keeps the views of this batch valid after the completion, until the returned handle
     * is closed; one handle, however many segments. May be called more than once.
     */
    Retained retain();

    /** Keeps a batch's payload alive; closing it (once is enough, more is harmless) lets it go. */
    interface Retained extends AutoCloseable {
        @Override
        void close();
    }

    /**
     * A batch of the given buffers, one record (message) each, for writes outside zmux (a
     * fallback target writing its carrier). The buffers stay the caller's and are not
     * modified; {@link #retain()} copies them once and later views come from the copies.
     */
    static OutboundBatch of(ByteBuffer... messages) {
        return new Buffers(messages);
    }
}
