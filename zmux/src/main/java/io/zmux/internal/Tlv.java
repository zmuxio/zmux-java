package io.zmux.internal;

import java.util.Arrays;

public final class Tlv {
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final long type;
    private final byte[] value;

    public Tlv(long type, byte[] value) {
        this.type = type;
        this.value = normalizeValue(value);
    }

    private static byte[] normalizeValue(byte[] value) {
        if (value == null || value.length == 0) {
            return EMPTY_BYTES;
        }
        return Arrays.copyOf(value, value.length);
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
