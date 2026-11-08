package io.zmux.internal.kex;

import static io.zmux.carrier.SecureCarrierException.Code.INVALID_CONFIG;
import static io.zmux.carrier.SecureCarrierException.Code.KEX;
import static io.zmux.carrier.SecureCarrierException.Code.RANDOM_FAILED;

import io.zmux.carrier.SecureCarrierException;
import io.zmux.internal.random.OsRandom;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.interfaces.XECPrivateKey;
import java.security.spec.NamedParameterSpec;
import java.security.spec.XECPrivateKeySpec;
import java.security.spec.XECPublicKeySpec;
import javax.crypto.KeyAgreement;

/** X25519 of RFC 7748 on the JDK: the one implementation of Establish's static and ephemeral keys and of the Groups. */
public final class X25519 {
    /** The length of a key and of a shared secret. */
    public static final int LEN = 32;

    private static final byte[] BASE_POINT = new byte[LEN];
    private static final byte[] ZERO = new byte[LEN];

    static {
        BASE_POINT[0] = 9;
    }

    private X25519() {}

    /** A new private scalar; {@code random} null is the operating system's source ({@link OsRandom}). */
    public static byte[] generate(SecureRandom random) throws SecureCarrierException {
        try {
            KeyPairGenerator g = KeyPairGenerator.getInstance("X25519");
            g.initialize(NamedParameterSpec.X25519, random != null ? random : OsRandom.get());
            return ((XECPrivateKey) g.generateKeyPair().getPrivate()).getScalar().orElseThrow();
        } catch (GeneralSecurityException | RuntimeException e) {
            throw new SecureCarrierException(RANDOM_FAILED, "key generation", e); // no fallback
        }
    }

    /** The JDK key of a 32-byte scalar. */
    public static PrivateKey privateKey(byte[] scalar) throws SecureCarrierException {
        if (scalar == null || scalar.length != LEN) throw new SecureCarrierException(INVALID_CONFIG, "X25519 key");
        try {
            return KeyFactory.getInstance("X25519").generatePrivate(new XECPrivateKeySpec(NamedParameterSpec.X25519, scalar));
        } catch (GeneralSecurityException e) {
            throw new SecureCarrierException(INVALID_CONFIG, "X25519 key", e);
        }
    }

    /** The public key of a private key. */
    public static byte[] publicKey(PrivateKey priv) throws SecureCarrierException {
        try {
            return agree(priv, BASE_POINT);
        } catch (GeneralSecurityException e) {
            throw new SecureCarrierException(INVALID_CONFIG, "X25519 key", e);
        }
    }

    /** X25519 with a peer's raw key. An all-zero output, compared in constant time, is KEX. */
    public static byte[] dh(PrivateKey priv, byte[] peer) throws SecureCarrierException {
        try {
            byte[] out = agree(priv, peer);
            if (MessageDigest.isEqual(out, ZERO)) throw new SecureCarrierException(KEX);
            return out;
        } catch (InvalidKeyException e) {
            throw new SecureCarrierException(KEX); // how the JDK reports a small-order point
        } catch (GeneralSecurityException | RuntimeException e) {
            throw new SecureCarrierException(KEX, "key agreement", e);
        }
    }

    private static byte[] agree(PrivateKey priv, byte[] peer) throws GeneralSecurityException {
        KeyAgreement ka = KeyAgreement.getInstance("X25519");
        ka.init(priv);
        ka.doPhase(publicOf(peer), true);
        return ka.generateSecret();
    }

    /** RFC 7748: little-endian, the top bit of the last byte ignored. The JDK reduces u modulo p. */
    private static PublicKey publicOf(byte[] raw) throws GeneralSecurityException {
        if (raw == null || raw.length != LEN) throw new InvalidKeyException("X25519 key length");
        byte[] be = new byte[LEN];
        for (int i = 0; i < LEN; i++) be[i] = raw[LEN - 1 - i];
        be[0] &= 0x7f;
        return KeyFactory.getInstance("X25519").generatePublic(new XECPublicKeySpec(NamedParameterSpec.X25519, new BigInteger(1, be)));
    }
}
