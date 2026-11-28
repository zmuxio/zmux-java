package io.zmux.internal.wire;

import java.nio.ByteBuffer;

/**
 * Capsules of a packet stream (EXTENSIONS.md Section 12, RFC 9297 Section 3.2): Type (i),
 * Length (i), Value. The one capsule codec of the library: packet streams and the packet relay
 * both parse with {@link Cursor} and encode with {@link #putDatagramHeader}.
 */
public final class Capsule {
    /** DATAGRAM (RFC 9297 Section 3.5): Value is Context ID (i) followed by the payload. */
    public static final long DATAGRAM = 0x00;
    /** The longest DATAGRAM header: Type, Length and Context ID at their longest encodings. */
    public static final int MAX_DATAGRAM_HEADER = 1 + 8 + 8;

    /** Default payload bound for a DATAGRAM capsule. */
    public static final int DEFAULT_MAX_DATAGRAM_PAYLOAD = 65535;
    /** Default opaque Value bound. */
    public static final int MAX_OPAQUE_VALUE = 65535;
    /** The longest received header, also allowing non-minimal Type encodings. */
    public static final int MAX_HEADER = 24;

    private Capsule() {}

    /** Encodes the header for a modified capsule. */
    public static ByteBuffer encodeHeader(long type, long context, int length) {
        ByteBuffer result = ByteBuffer.allocate(headerLength(type, context, length));
        if (type == DATAGRAM) putDatagramHeader(result, context, length);
        else {
            VarInt.put(result, type);
            VarInt.put(result, length);
        }
        return result.flip();
    }

    public static int headerLength(long type, long context, int length) {
        return type == DATAGRAM ? datagramHeaderLength(context, length) : VarInt.length(type) + VarInt.length(length);
    }

    /** The bytes {@link #putDatagramHeader} writes. */
    public static int datagramHeaderLength(long context, int payload) {
        long value = VarInt.length(context) + (long) payload;
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

    /** Incremental bounded Type, Length, Context and Value cursor. Not thread-safe. */
    public static final class Cursor {
        private final Header prefix = new Header();
        private final Field context = new Field();
        private final ByteBuffer saved;
        private final int maxDatagram, maxOpaque;
        private boolean lengthKnown, ready;
        private int remaining;

        public Cursor(int maxDatagram, int maxOpaque, boolean preserveHeader) {
            this.maxDatagram = maxDatagram;
            this.maxOpaque = maxOpaque;
            saved = preserveHeader ? ByteBuffer.allocate(MAX_HEADER) : null;
        }

        public boolean atBoundary() { return !lengthKnown && prefix.idle(); }
        public boolean headerReady() { return ready; }
        public long type() { return prefix.type(); }
        public long context() { return type() == DATAGRAM ? context.value() : -1; }
        public int remaining() { return remaining; }
        public int headerSize() { return saved.position(); }
        public ByteBuffer header() { return saved.asReadOnlyBuffer().flip(); }
        public int headerNeed() { return Math.max(1, lengthKnown ? context.need : prefix.field.need); }

        /** Reads an available header prefix. Returns false on a malformed field or an oversized Value. */
        public boolean readHeader(ByteBuffer source) {
            int from = source.position();
            try {
                if (!lengthKnown) {
                    if (!prefix.feed(source)) return true;
                    long length = prefix.length();
                    boolean datagram = prefix.type() == DATAGRAM;
                    if (datagram ? length == 0 || length > maxDatagram + 8L : length > maxOpaque) return false;
                    remaining = (int) length;
                    lengthKnown = true;
                    if (!datagram) { ready = true; return true; }
                }
                int end = source.limit();
                int beforeContext = source.position();
                source.limit(beforeContext + Math.min(remaining, source.remaining()));
                try { ready = context.feed(source); }
                finally { source.limit(end); }
                remaining -= source.position() - beforeContext;
                return ready ? remaining <= maxDatagram : context.need <= remaining;
            } finally {
                if (saved != null) saved.put(source.slice(from, source.position() - from));
            }
        }

        public void readValue(int size) { remaining -= size; }

        public void clear() {
            prefix.typed = false;
            prefix.field.need = 0;
            context.need = 0;
            lengthKnown = false;
            ready = false;
            remaining = 0;
            if (saved != null) saved.clear();
        }
    }
}
