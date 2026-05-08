package io.zmux.protocol;

import java.util.Arrays;
import java.util.Objects;

public final class Tlv {
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final long type;
    private final byte[] value;

    public Tlv(long type, byte[] value) {
        if (type < 0L || type > Protocol.MAX_VARINT62) {
            throw new IllegalArgumentException("zmux tlv type must be within varint62 range");
        }
        this.type = type;
        this.value = value == null || value.length == 0 ? EMPTY_BYTES : Arrays.copyOf(value, value.length);
    }

    public Tlv(long type, byte[] value, int offset, int length) {
        if (type < 0L || type > Protocol.MAX_VARINT62) {
            throw new IllegalArgumentException("zmux tlv type must be within varint62 range");
        }
        Objects.requireNonNull(value, "value");
        if ((offset | length) < 0 || length > value.length - offset) {
            throw new IndexOutOfBoundsException(
                    "range [" + offset + ", " + offset + " + " + length + ") out of bounds for length " + value.length
            );
        }
        this.type = type;
        this.value = length == 0 ? EMPTY_BYTES : Arrays.copyOfRange(value, offset, offset + length);
    }

    public long type() {
        return type;
    }

    public byte[] value() {
        return value.length == 0 ? EMPTY_BYTES : Arrays.copyOf(value, value.length);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Tlv)) {
            return false;
        }
        Tlv that = (Tlv) other;
        return type == that.type && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return 31 * Long.hashCode(type) + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "Tlv[type=" + type + ", valueLength=" + value.length + "]";
    }
}
