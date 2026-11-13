package io.zmux.seal;

/**
 * Whom a verdict names: a type of the host's and its bytes. It does not change when the peer's key does. The bytes are
 * copied in and out and compared by content.
 */
public record Principal(int type, byte[] bytes) {
    /** A node named by the KeyId of its pinned static key (KeyAlg 1), as {@link PinAnchor} names it. */
    public static final int NODE_KEY = 1;

    public Principal {
        bytes = bytes.clone();
    }

    @Override public byte[] bytes() { return bytes.clone(); }

    @Override
    public boolean equals(Object o) {
        return o instanceof Principal p && type == p.type && java.util.Arrays.equals(bytes, p.bytes);
    }

    @Override
    public int hashCode() { return type * 31 + java.util.Arrays.hashCode(bytes); }

    @Override
    public String toString() { return "Principal[type=" + type + ", " + java.util.HexFormat.of().formatHex(bytes) + "]"; }
}
