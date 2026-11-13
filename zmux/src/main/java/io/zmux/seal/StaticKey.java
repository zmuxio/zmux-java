package io.zmux.seal;

import io.zmux.internal.kex.X25519;

import static io.zmux.seal.SecureCarrierException.Code.INVALID_CONFIG;

import java.security.PrivateKey;

/** An X25519 key pair: a node's or client's static key, or an ephemeral one inside a handshake. */
public final class StaticKey {
    /** The length of a public key and of a shared secret. */
    public static final int KEY_LEN = 32;


    private final PrivateKey priv;
    private final byte[] scalar;
    private final byte[] pub;

    private StaticKey(PrivateKey priv, byte[] scalar, byte[] pub) {
        this.priv = priv;
        this.scalar = scalar;
        this.pub = pub;
    }

    /**
     * A new random key, from the operating system's source.
     *
     * @throws java.io.UncheckedIOException wrapping {@code random_failed} when that source fails
     */
    public static StaticKey generate() {
        try {
            return random();
        } catch (SecureCarrierException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** A new random key; a failure of the source is {@code random_failed}, as a handshake reports it. */
    static StaticKey random() throws SecureCarrierException {
        return fromScalar(X25519.generate(null));
    }

    /** The key of a 32-byte private scalar; another length is {@code invalid_config}. */
    public static StaticKey fromScalar(byte[] scalar) throws SecureCarrierException {
        if (scalar == null || scalar.length != KEY_LEN) throw new SecureCarrierException(INVALID_CONFIG, "static key");
        PrivateKey priv = X25519.privateKey(scalar);
        return new StaticKey(priv, scalar.clone(), X25519.publicKey(priv));
    }

    /** The raw public key. */
    public byte[] publicKey() {
        return pub.clone();
    }

    /** The raw private scalar. */
    public byte[] scalar() {
        return scalar.clone();
    }

    /** The public key only: the scalar never appears in a string. */
    @Override
    public String toString() {
        return "StaticKey[" + java.util.HexFormat.of().formatHex(pub) + "]";
    }

    byte[] publicRaw() {
        return pub;
    }

    /** X25519 with a peer's raw key. A key that gives an all-zero output is KEX. */
    byte[] dh(byte[] peer) throws SecureCarrierException {
        return X25519.dh(priv, peer);
    }
}
