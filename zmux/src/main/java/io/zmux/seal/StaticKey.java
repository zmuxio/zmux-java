package io.zmux.seal;

import io.zmux.internal.kex.X25519;

import static io.zmux.seal.SecureCarrierException.Kind.INVALID_CONFIG;

import java.security.PrivateKey;

/**
 * An X25519 key pair: a node's or client's static key, or an ephemeral one inside a handshake.
 *
 * <p>{@link #close()} zeroes the private scalar this object holds and destroys the JDK key built from it where the
 * provider supports that (the JDK's own X25519 keys keep an internal copy that only the garbage collector
 * reclaims). A closed key can no longer take part in a handshake or give its scalar; its public key stays
 * readable. Close a key once no configuration or session that holds it is used any more. {@link #toString()}
 * shows the public key only.
 */
public final class StaticKey implements AutoCloseable {
    /** The length of a public key and of a shared secret. */
    public static final int KEY_LEN = 32;


    private final PrivateKey priv;
    private final byte[] scalar;
    private final byte[] pub;
    private volatile boolean closed;

    private StaticKey(PrivateKey priv, byte[] scalar, byte[] pub) {
        this.priv = priv;
        this.scalar = scalar;
        this.pub = pub;
    }

    /**
     * A new random key, from the operating system's source, which can fail (early boot, a sandbox without it) and
     * has no fallback.
     *
     * @throws SecureCarrierException {@code RANDOM_FAILED} when that source fails, as a handshake reports it
     */
    public static StaticKey generate() throws SecureCarrierException {
        byte[] scalar = X25519.generate(null);
        try {
            return fromScalar(scalar);
        } finally {
            java.util.Arrays.fill(scalar, (byte) 0);
        }
    }

    /** The key of a 32-byte private scalar; another length is an {@link IllegalArgumentException}. */
    public static StaticKey fromScalar(byte[] scalar) throws SecureCarrierException {
        if (scalar == null || scalar.length != KEY_LEN) throw new IllegalArgumentException("static key scalar must be 32 bytes");
        PrivateKey priv = X25519.privateKey(scalar);
        return new StaticKey(priv, scalar.clone(), X25519.publicKey(priv));
    }

    /** The raw public key. */
    public byte[] publicKey() {
        return pub.clone();
    }

    /**
     * The raw private scalar, a copy the caller should zero after use.
     *
     * @throws IllegalStateException once the key is closed
     */
    public byte[] scalar() {
        if (closed) throw new IllegalStateException("static key closed");
        return scalar.clone();
    }

    /** Whether {@link #close()} ran. */
    public boolean isClosed() {
        return closed;
    }

    /** Zeroes the private scalar and destroys the private key where the provider allows; idempotent. */
    @Override
    public void close() {
        if (closed) return;
        closed = true;
        java.util.Arrays.fill(scalar, (byte) 0);
        try {
            priv.destroy();
        } catch (javax.security.auth.DestroyFailedException | RuntimeException notSupported) {
            // The JDK's X25519 keys do not support destroy(): their copy is left to the garbage collector.
        }
    }

    /** The public key only: the scalar never appears in a string. */
    @Override
    public String toString() {
        return "StaticKey[" + java.util.HexFormat.of().formatHex(pub) + "]";
    }

    byte[] publicRaw() {
        return pub;
    }

    /** X25519 with a peer's raw key. A key that gives an all-zero output is KEX; a closed key is invalid_config. */
    byte[] dh(byte[] peer) throws SecureCarrierException {
        if (closed) throw new SecureCarrierException(INVALID_CONFIG, "static key closed");
        return X25519.dh(priv, peer);
    }
}
