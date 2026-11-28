package io.zmux.internal.core;

/**
 * A retained frame of a multipath session: what to do when the
 * record carrying it is acknowledged or lost.
 *
 * @param id stream ID, or pipe ID for PIPE_STATUS and PIPE_ABANDON
 * @param val PRIORITY_UPDATE or PIPE_STATUS sequence number; for a prefix repair, the source pipe ID
 *     plus one when the original stream was credit-blocked, or -1 otherwise. For fresh STREAM bytes, -2.
 */
record FrameDesc(Kind kind, boolean bidi, boolean fin, long id, long off, long n, long val) {
    enum Kind {
        STREAM, RESET, STOP, MAX_DATA, MAX_STREAM_DATA, MAX_STREAMS, DATA_BLOCKED, STREAM_DATA_BLOCKED,
        STREAMS_BLOCKED, GOAWAY, PRIORITY, STATUS, ABANDON
    }

    static FrameDesc of(Kind k) {
        return new FrameDesc(k, false, false, 0, 0, 0, 0);
    }

    static FrameDesc id(Kind k, long id) {
        return new FrameDesc(k, false, false, id, 0, 0, 0);
    }

    static FrameDesc prefixRepair(long id, long off, long n, Pipe penalizedSource) {
        long source = penalizedSource == null ? -1 : penalizedSource.id + 1;
        return new FrameDesc(Kind.STREAM, false, false, id, off, n, source);
    }

    boolean prefixRepair() { return kind == Kind.STREAM && (val == -1 || val > 0); }

    boolean originalStream() { return kind == Kind.STREAM && val == -2; }

    static FrameDesc originalStream(long id, long off, long n, boolean fin) {
        return new FrameDesc(Kind.STREAM, false, fin, id, off, n, -2);
    }

    static FrameDesc stream(long id, long off, long n, boolean fin) {
        return new FrameDesc(Kind.STREAM, false, fin, id, off, n, 0);
    }
}
