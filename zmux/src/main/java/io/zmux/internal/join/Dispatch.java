package io.zmux.internal.join;

import io.zmux.internal.wire.VarInt;

/** What the first bytes of a new carrier begin. */
public final class Dispatch {
    /** The classes of first bytes. */
    public enum Kind {
        ESTABLISH,
        JOIN,
        RESERVED,
        RECORD,
        OTHER,
        INCOMPLETE
    }

    /** A class and, for {@link Kind#INCOMPLETE}, the total length needed. */
    public record Result(Kind kind, int need) {}

    private static final int MIN_FIRST_RECORD = 9;
    private static final int LEAD_A = 0xff;
    private static final int LEAD_B = 0xba;

    private Dispatch() {}

    /** Classifies {@code len} bytes of {@code b} at {@code off}; {@code maxRecord} is the largest record size. */
    public static Result classify(byte[] b, int off, int len, Framing f, long maxRecord) {
        if (len == 0) {
            return f == Framing.STREAM ? new Result(Kind.INCOMPLETE, 1) : other();
        }
        int first = b[off] & 0xff;
        switch (first) {
            case 1, 3, 4, 5, 8 -> {
                return new Result(Kind.ESTABLISH, 0);
            }
            case 2 -> {
                return new Result(Kind.JOIN, 0);
            }
            case 6, 7 -> {
                return new Result(Kind.RESERVED, 0);
            }
            default -> {}
        }
        if (f == Framing.MESSAGE) {
            return isLead(first) ? new Result(Kind.RECORD, 0) : other();
        }
        int n = VarInt.lengthOf(b[off]);
        if (n == 8) return other();
        if (n == 1 && first < MIN_FIRST_RECORD) return other();
        if (len < n) return new Result(Kind.INCOMPLETE, n + 1);
        long size = Tlv.readVarint(b, off, n);
        if (size < MIN_FIRST_RECORD || size > maxRecord) return other();
        if (len < n + 1) return new Result(Kind.INCOMPLETE, n + 1);
        return isLead(b[off + n] & 0xff) ? new Result(Kind.RECORD, 0) : other();
    }

    private static boolean isLead(int b) {
        return b == LEAD_A || b == LEAD_B;
    }

    private static Result other() {
        return new Result(Kind.OTHER, 0);
    }
}
