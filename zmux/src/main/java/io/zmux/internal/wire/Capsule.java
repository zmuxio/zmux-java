package io.zmux.internal.wire;

import java.nio.ByteBuffer;

/**
 * Capsules of a packet stream (EXTENSIONS.md Section 12, RFC 9297 Section 3.2): Type (i),
 * Length (i), Value. The one capsule codec of the library: packet streams and the packet relay
 * both parse with {@link Field} and encode with {@link #putDatagramHeader}.
 */
public final class Capsule {
    /** DATAGRAM (RFC 9297 Section 3.5): Value is Context ID (i) followed by the payload. */
    public static final long DATAGRAM = 0x00;
    /** The longest DATAGRAM header: Type, Length and Context ID at their longest encodings. */
    public static final int MAX_DATAGRAM_HEADER = 1 + 8 + 8;

    private Capsule() {}

    /** The bytes {@link #putDatagramHeader} writes. */
    public static int datagramHeaderLength(long context, int payload) {
        int value = VarInt.length(context) + payload;
        return 1 + VarInt.length(value) + VarInt.length(context);
    }

    /** Writes Type 0x00, Length and Context ID of a DATAGRAM capsule whose payload follows. */
    public static void putDatagramHeader(ByteBuffer b, long context, int payload) {
        b.put((byte) DATAGRAM);
        VarInt.put(b, VarInt.length(context) + (long) payload);
        VarInt.put(b, context);
    }

    /** {@link #putDatagramHeader} into a, at i; returns the next index. */
    public static int putDatagramHeader(byte[] a, int i, long context, int payload) {
        a[i++] = (byte) DATAGRAM;
        i = VarInt.put(a, i, VarInt.length(context) + (long) payload);
        return VarInt.put(a, i, context);
    }

    /** A variable-length integer read one byte at a time, across buffer boundaries. Not thread-safe. */
    public static final class Field {
        private long value;
        private int need;
        private int length;

        /** Consumes bytes of b up to the end of the integer; true when it is complete, then {@link #value()} holds it. */
        public boolean feed(ByteBuffer b) {
            while (b.hasRemaining()) {
                int x = b.get() & 0xff;
                if (need == 0) { length = need = 1 << (x >>> 6); value = x & 0x3f; }
                else value = (value << 8) | x;
                if (--need == 0) return true;
            }
            return false;
        }

        /** Whether no byte of the next integer has been read. */
        public boolean idle() { return need == 0; }
        /** The completed integer. */
        public long value() { return value; }
        /** The completed integer's encoded length. */
        public int length() { return length; }
    }

    /** A capsule header, Type then Length, read across buffer boundaries. Not thread-safe. */
    public static final class Header {
        private final Field field = new Field();
        private boolean typed;
        private long type;

        /** Consumes header bytes of b; true when the header is complete, then {@link #type()} and {@link #length()} hold it and the next call starts a new header. */
        public boolean feed(ByteBuffer b) {
            if (!typed) {
                if (!field.feed(b)) return false;
                type = field.value(); typed = true;
            }
            if (!field.feed(b)) return false;
            typed = false;
            return true;
        }

        /** Whether no byte of the next header has been read: the reader is at a capsule boundary. */
        public boolean idle() { return !typed && field.idle(); }
        public long type() { return type; }
        public long length() { return field.value(); }
    }
}
