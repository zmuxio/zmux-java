package io.zmux.seal;

/** Whom a verdict names: a type of the host's and its bytes. It does not change when the peer's key does. */
public record Principal(int type, byte[] bytes) {
    /** A node named by the KeyId of its pinned static key (KeyAlg 1), as {@link PinAnchor} names it. */
    public static final int NODE_KEY = 1;

    public Principal {
        bytes = bytes.clone();
    }

    @Override public byte[] bytes() { return bytes.clone(); }
}
