package io.zmux.seal;

import io.zmux.internal.kex.X25519;

import static io.zmux.carrier.SecureCarrierException.Code.INVALID_CONFIG;

import io.zmux.carrier.SecureCarrierException;
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

    /** A new random key, from the operating system's source. */
    public static StaticKey generate() throws SecureCarrierException {
        return of(X25519.generate(null));
    }

    /** The key of a 32-byte private scalar. */
    public static StaticKey of(byte[] scalar) throws SecureCarrierException {
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

    byte[] publicRaw() {
        return pub;
    }

    /** X25519 with a peer's raw key. A key that gives an all-zero output is KEX. */
    byte[] dh(byte[] peer) throws SecureCarrierException {
        return X25519.dh(priv, peer);
    }
}
