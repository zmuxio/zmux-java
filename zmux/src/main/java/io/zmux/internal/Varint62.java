package io.zmux.internal;

import io.zmux.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

public final class Varint62 {
    private Varint62() {
    }

    public static int length(long value) throws ZmuxException {
        if (value < 0 || value > Protocol.MAX_VARINT62) {
            throw error("varint length", "varint62 value out of range", null);
        }
        if (value <= 63L) {
            return 1;
        }
        if (value <= 16_383L) {
            return 2;
        }
        if (value <= 1_073_741_823L) {
            return 4;
        }
        return 8;
    }

    public static void write(OutputStream output, long value) throws IOException {
        int length = length(value);
        switch (length) {
            case 1:
                output.write((int) value);
                break;
            case 2:
                output.write((int) (((value >>> 8) & 0x3f) | 0x40));
                output.write((int) value);
                break;
            case 4:
                output.write((int) (((value >>> 24) & 0x3f) | 0x80));
                output.write((int) (value >>> 16));
                output.write((int) (value >>> 8));
                output.write((int) value);
                break;
            case 8:
                output.write((int) (((value >>> 56) & 0x3f) | 0xc0));
                output.write((int) (value >>> 48));
                output.write((int) (value >>> 40));
                output.write((int) (value >>> 32));
                output.write((int) (value >>> 24));
                output.write((int) (value >>> 16));
                output.write((int) (value >>> 8));
                output.write((int) value);
                break;
            default:
                throw new IllegalStateException("unsupported varint length " + length);
        }
    }

    public static byte[] encode(long value) throws IOException {
        byte[] encoded = new byte[length(value)];
        write(encoded, 0, value);
        return encoded;
    }

    public static int write(byte[] target, int offset, long value) throws IOException {
        int length = length(value);
        RangeChecks.checkFromIndexSize(offset, length, target.length);
        switch (length) {
            case 1:
                target[offset] = (byte) value;
                break;
            case 2:
                target[offset] = (byte) (((value >>> 8) & 0x3f) | 0x40);
                target[offset + 1] = (byte) value;
                break;
            case 4:
                target[offset] = (byte) (((value >>> 24) & 0x3f) | 0x80);
                target[offset + 1] = (byte) (value >>> 16);
                target[offset + 2] = (byte) (value >>> 8);
                target[offset + 3] = (byte) value;
                break;
            case 8:
                target[offset] = (byte) (((value >>> 56) & 0x3f) | 0xc0);
                target[offset + 1] = (byte) (value >>> 48);
                target[offset + 2] = (byte) (value >>> 40);
                target[offset + 3] = (byte) (value >>> 32);
                target[offset + 4] = (byte) (value >>> 24);
                target[offset + 5] = (byte) (value >>> 16);
                target[offset + 6] = (byte) (value >>> 8);
                target[offset + 7] = (byte) value;
                break;
            default:
                throw new IllegalStateException("unsupported varint length " + length);
        }
        return length;
    }

    public static Decoded decode(byte[] source, int offset) throws ZmuxException {
        Objects.requireNonNull(source, "source");
        return decode(source, offset, source.length);
    }

    static Decoded decode(byte[] source, int offset, int limit) throws ZmuxException {
        Objects.requireNonNull(source, "source");
        if (offset < 0) {
            throw new IndexOutOfBoundsException("offset < 0");
        }
        if (offset > source.length) {
            throw new IndexOutOfBoundsException("offset > source.length");
        }
        if (limit < offset || limit > source.length) {
            throw new IndexOutOfBoundsException("limit out of bounds");
        }
        if (offset >= limit) {
            throw error("parse varint62", "truncated varint62", null);
        }
        int first = source[offset] & 0xff;
        int prefix = first >>> 6;
        int length = 1 << prefix;
        if (length > limit - offset) {
            throw error("parse varint62", "truncated varint62", null);
        }
        long value;
        switch (length) {
            case 1:
                value = first & 0x3fL;
                break;
            case 2:
                value = ((first & 0x3fL) << 8) | (source[offset + 1] & 0xffL);
                break;
            case 4:
                value = ((first & 0x3fL) << 24)
                        | ((source[offset + 1] & 0xffL) << 16)
                        | ((source[offset + 2] & 0xffL) << 8)
                        | (source[offset + 3] & 0xffL);
                break;
            case 8:
                value = decodeEightByteValue(
                        first,
                        source[offset + 1],
                        source[offset + 2],
                        source[offset + 3],
                        source[offset + 4],
                        source[offset + 5],
                        source[offset + 6],
                        source[offset + 7]
                );
                break;
            default:
                throw error("parse varint62", "truncated varint62", null);
        }
        if (length(value) != length) {
            throw error("parse varint62", "non-canonical varint62", null);
        }
        return new Decoded(value, length);
    }

    public static Decoded read(InputStream input) throws IOException {
        int first = input.read();
        if (first < 0) {
            throw error("read varint62", "truncated varint62", null);
        }
        int prefix = first >>> 6;
        int length = 1 << prefix;
        long value;
        switch (length) {
            case 1:
                value = first & 0x3fL;
                break;
            case 2:
                value = ((first & 0x3fL) << 8) | readRequiredByte(input);
                break;
            case 4:
                value = ((first & 0x3fL) << 24)
                        | ((long) readRequiredByte(input) << 16)
                        | ((long) readRequiredByte(input) << 8)
                        | readRequiredByte(input);
                break;
            case 8:
                value = decodeEightByteValue(
                        first,
                        readRequiredByte(input),
                        readRequiredByte(input),
                        readRequiredByte(input),
                        readRequiredByte(input),
                        readRequiredByte(input),
                        readRequiredByte(input),
                        readRequiredByte(input)
                );
                break;
            default:
                throw error("read varint62", "truncated varint62", null);
        }
        if (length(value) != length) {
            throw error("read varint62", "non-canonical varint62", null);
        }
        return new Decoded(value, length);
    }

    static Decoded read(FrameCodec.Decoder input) throws IOException {
        int first = input.readByte();
        int prefix = first >>> 6;
        int length = 1 << prefix;
        long value;
        switch (length) {
            case 1:
                value = first & 0x3fL;
                break;
            case 2:
                value = ((first & 0x3fL) << 8) | input.readByte();
                break;
            case 4:
                value = ((first & 0x3fL) << 24)
                        | ((long) input.readByte() << 16)
                        | ((long) input.readByte() << 8)
                        | input.readByte();
                break;
            case 8:
                value = decodeEightByteValue(
                        first,
                        input.readByte(),
                        input.readByte(),
                        input.readByte(),
                        input.readByte(),
                        input.readByte(),
                        input.readByte(),
                        input.readByte()
                );
                break;
            default:
                throw error("read varint62", "truncated varint62", null);
        }
        if (length(value) != length) {
            throw error("read varint62", "non-canonical varint62", null);
        }
        return new Decoded(value, length);
    }

    private static int readRequiredByte(InputStream input) throws IOException {
        int next = input.read();
        if (next < 0) {
            throw error("read varint62", "truncated varint62", null);
        }
        return next & 0xff;
    }

    static long decodeEightByteValue(int first, int b1, int b2, int b3, int b4, int b5, int b6, int b7) {
        return ((first & 0x3fL) << 56)
                | ((b1 & 0xffL) << 48)
                | ((b2 & 0xffL) << 40)
                | ((b3 & 0xffL) << 32)
                | ((b4 & 0xffL) << 24)
                | ((b5 & 0xffL) << 16)
                | ((b6 & 0xffL) << 8)
                | (b7 & 0xffL);
    }

    static boolean isTruncatedVarint(IOException error) {
        return error instanceof ZmuxException && "truncated varint62".equals(error.getMessage());
    }

    private static ZmuxException error(String operation, String message, Throwable cause) {
        return new ZmuxException(
                ErrorCode.PROTOCOL.code(),
                operation,
                message,
                cause,
                ZmuxErrorScope.SESSION,
                varintErrorSource(operation),
                varintErrorDirection(operation),
                ZmuxTerminationKind.UNKNOWN
        );
    }

    private static ZmuxErrorSource varintErrorSource(String operation) {
        if (operation == null || operation.isEmpty()) {
            return ZmuxErrorSource.UNKNOWN;
        }
        if (operation.startsWith("read ") || operation.startsWith("parse ")) {
            return ZmuxErrorSource.REMOTE;
        }
        if (operation.startsWith("varint length")) {
            return ZmuxErrorSource.LOCAL;
        }
        return ZmuxErrorSource.UNKNOWN;
    }

    private static ZmuxErrorDirection varintErrorDirection(String operation) {
        if (operation == null || operation.isEmpty()) {
            return ZmuxErrorDirection.BOTH;
        }
        if (operation.startsWith("read ") || operation.startsWith("parse ")) {
            return ZmuxErrorDirection.READ;
        }
        if (operation.startsWith("varint length")) {
            return ZmuxErrorDirection.WRITE;
        }
        return ZmuxErrorDirection.BOTH;
    }

    public static final class Decoded {
        private final long value;
        private final int length;

        public Decoded(long value, int length) {
            this.value = value;
            this.length = length;
        }

        public long value() {
            return value;
        }

        public int length() {
            return length;
        }
    }
}
