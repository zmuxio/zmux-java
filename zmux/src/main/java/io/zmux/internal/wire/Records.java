package io.zmux.internal.wire;

import io.zmux.carrier.VarInt;

import java.nio.ByteBuffer;

/** Record framing (SPEC.md Section 3) and the first-record check (SPEC.md Section 6.2). */
public final class Records {
    /** The first 8 bytes after the Size of a bootstrap pipe's first record. */
    public static final byte[] TRANSPORT_PARAMETERS_PREFIX = {(byte) 0xff, 0x51, 0x53, 0x30, 0x0d, 0x0a, 0x0d, 0x0a};
    /** The smallest Size of a first record. */
    public static final int MIN_FIRST_RECORD = 9;

    private Records() {}

    /** Length of the shortest Size field for a record of {@code size} bytes of frames. */
    public static int sizeFieldLength(int size) {
        return VarInt.length(size);
    }

    /**
     * Reads the Size of the record at {@code pos}: returns -1 when the Size field is
     * incomplete. Throws FRAME_ENCODING_ERROR for Size 0 or above {@code maxRecordSize}
     * (SPEC.md Section 3.2), before the body is read.
     */
    public static long size(ByteBuffer b, int pos, int limit, long maxRecordSize) throws WireException {
        long s = VarInt.get(b, pos, limit);
        if (s < 0) return -1;
        if (s == 0) throw WireException.frame("record Size 0");
        if (s > maxRecordSize) {
            throw WireException.withDetails(TransportError.FRAME_ENCODING_ERROR, "record above max_record_size",
                    "length", s, "limit", maxRecordSize);
        }
        return s;
    }

    /** Result of {@link FirstRecordCheck#feed}. */
    public enum Verdict { NEED_MORE, TRANSPORT_PARAMETERS, PIPE_JOIN, PIPE_LANE, REJECT }

    /**
     * Incremental check of a new carrier's first record, byte by byte (SPEC.md
     * Section 6.2): QX_TRANSPORT_PARAMETERS after a Size of at least 9, and, when
     * allowed, PIPE_JOIN after a Size of at least 26 and PIPE_LANE after a Size of
     * at least 22 (MULTIPATH.md Section 5.5); any Size above the local limit and
     * any other prefix is rejected as soon as it is seen.
     */
    public static final class FirstRecordCheck {
        private static final byte[][] PREFIX = {TRANSPORT_PARAMETERS_PREFIX,
                {(byte) 0x9a, 0x24, (byte) 0xde, 0x36}, {(byte) 0xba, 0x05, (byte) 0xa1, (byte) 0xc9}};
        private static final int[] MIN_SIZE = {MIN_FIRST_RECORD, 26, 22};
        private static final Verdict[] VERDICT = {Verdict.TRANSPORT_PARAMETERS, Verdict.PIPE_JOIN, Verdict.PIPE_LANE};

        private final long maxRecordSize;
        private long sizeAcc;
        private int alive;
        private int sizeLen;
        private int seen;
        private long size = -1;
        private int matched;

        /** A check of a first record; allowJoin accepts PIPE_JOIN, allowLane PIPE_LANE. */
        public FirstRecordCheck(long maxRecordSize, boolean allowJoin, boolean allowLane) {
            this.maxRecordSize = maxRecordSize;
            this.alive = 1 | (allowJoin ? 2 : 0) | (allowLane ? 4 : 0);
        }

        /** Feeds bytes [pos, limit) and returns the verdict once it is known. */
        public Verdict feed(ByteBuffer b, int pos, int limit) {
            for (int i = pos; i < limit; i++) {
                Verdict v = feed(b.get(i));
                if (v != Verdict.NEED_MORE) return v;
            }
            return Verdict.NEED_MORE;
        }

        /** Number of bytes of the first record's Size and prefix consumed so far. */
        public int consumed() {
            return seen + matched;
        }

        private Verdict feed(byte c) {
            if (size < 0) {
                if (seen == 0) {
                    sizeLen = 1 << ((c & 0xff) >>> 6);
                    sizeAcc = c & 0x3f;
                } else {
                    sizeAcc = sizeAcc << 8 | (c & 0xff);
                }
                if (++seen < sizeLen) return Verdict.NEED_MORE;
                size = sizeAcc;
                if (size > maxRecordSize) return Verdict.REJECT;
                for (int k = 0; k < 3; k++) if (size < MIN_SIZE[k]) alive &= ~(1 << k);
                return alive == 0 ? Verdict.REJECT : Verdict.NEED_MORE;
            }
            for (int k = 0; k < 3; k++) {
                if ((alive & (1 << k)) != 0 && PREFIX[k][matched] != c) alive &= ~(1 << k);
            }
            matched++;
            if (alive == 0) return Verdict.REJECT;
            for (int k = 0; k < 3; k++) {
                if ((alive & (1 << k)) != 0 && matched == PREFIX[k].length) return VERDICT[k];
            }
            return Verdict.NEED_MORE;
        }
    }
}
