package io.zmux.seal;

import java.util.Arrays;
import java.util.Objects;

/**
 * An opaque credential; its type selects how it is verified. The bytes are copied in and out, compared by content,
 * and never shown in {@link #toString()}.
 */
public record Credential(long type, byte[] data) {
    public Credential {
        data = Objects.requireNonNull(data, "data").clone();
    }

    @Override public byte[] data() { return data.clone(); }

    /** The bytes without a copy, for the encoder. */
    byte[] raw() { return data; }

    @Override
    public boolean equals(Object o) {
        return o instanceof Credential c && type == c.type && Arrays.equals(data, c.data);
    }

    @Override
    public int hashCode() { return Long.hashCode(type) * 31 + Arrays.hashCode(data); }

    @Override
    public String toString() { return "Credential[type=" + type + ", " + data.length + " bytes]"; }
}
