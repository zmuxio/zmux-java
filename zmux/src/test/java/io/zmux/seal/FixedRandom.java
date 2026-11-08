package io.zmux.seal;

import java.security.SecureRandom;

/** Gives fixed bytes, once: the randomness of a vector. More than it holds is a test failure. */
public final class FixedRandom extends SecureRandom {
    private static final long serialVersionUID = 1L;
    private final byte[] bytes;
    private int at;

    public FixedRandom(byte[] bytes) {
        super(new byte[0]);
        this.bytes = bytes.clone();
    }

    @Override
    public void nextBytes(byte[] out) {
        if (out.length > bytes.length - at) throw new IllegalStateException("fixed randomness exhausted");
        System.arraycopy(bytes, at, out, 0, out.length);
        at += out.length;
    }
}
