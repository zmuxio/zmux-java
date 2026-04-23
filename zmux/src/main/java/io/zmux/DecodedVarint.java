package io.zmux;

public final class DecodedVarint {
    private final long value;
    private final int length;

    public DecodedVarint(long value, int length) {
        if (value < 0L || value > Protocol.MAX_VARINT62) {
            throw new IllegalArgumentException("zmux decoded varint value must be within varint62 range");
        }
        if (length != 1 && length != 2 && length != 4 && length != 8) {
            throw new IllegalArgumentException("zmux decoded varint length must be 1, 2, 4, or 8");
        }
        this.value = value;
        this.length = length;
    }

    public long value() {
        return value;
    }

    public int length() {
        return length;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DecodedVarint)) {
            return false;
        }
        DecodedVarint that = (DecodedVarint) other;
        return value == that.value && length == that.length;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(value);
        return 31 * result + Integer.hashCode(length);
    }

    @Override
    public String toString() {
        return "DecodedVarint[value=" + value + ", length=" + length + "]";
    }
}
