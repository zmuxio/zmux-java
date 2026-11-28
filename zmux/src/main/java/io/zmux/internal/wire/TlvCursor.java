package io.zmux.internal.wire;

import io.zmux.internal.buffer.ReceiveSpans;
import java.nio.ByteBuffer;

/** Bounded TLV traversal. The enclosing protocol assigns meaning and reports errors. */
final class TlvCursor {
    private final ByteBuffer input;
    private final ReceiveSpans spans;
    private final int end;
    private int next;
    private long type;
    private int offset, length;

    TlvCursor(ByteBuffer input, ReceiveSpans spans, int start, int end) {
        this.input = input;
        this.spans = spans;
        this.next = start;
        this.end = end;
    }

    boolean hasNext() { return next < end; }

    /** Positions the cursor on the next value, or returns false for a truncated item. */
    boolean advance() {
        long id = varint(next, end);
        if (id < 0) return false;
        int lengthAt = next + VarInt.lengthOf(get(next));
        long size = varint(lengthAt, end);
        if (size < 0) return false;
        int valueAt = lengthAt + VarInt.lengthOf(get(lengthAt));
        if (size > end - valueAt) return false;
        type = id;
        offset = valueAt;
        length = (int) size;
        next = valueAt + length;
        return true;
    }

    long type() { return type; }
    int offset() { return offset; }
    int length() { return length; }
    byte get(int pos) { return ReceiveSpans.get(input, spans, pos); }
    long varint(int pos, int bound) { return VarInt.get(input, spans, pos, bound); }
    void copy(int pos, byte[] dst) { ReceiveSpans.copy(input, spans, pos, dst, 0, dst.length); }

    long getLong(int pos) {
        if (spans == null) return input.getLong(pos);
        long value = 0;
        for (int i = 0; i < Long.BYTES; i++) value = (value << 8) | (spans.get(pos + i) & 0xffL);
        return value;
    }
}
