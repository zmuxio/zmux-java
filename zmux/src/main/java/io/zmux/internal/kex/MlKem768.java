package io.zmux.internal.kex;

import static io.zmux.seal.SecureCarrierException.Code.KEX;
import static io.zmux.seal.SecureCarrierException.Code.RANDOM_FAILED;

import io.zmux.seal.SecureCarrierException;
import io.zmux.internal.random.OsRandom;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.NamedParameterSpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import javax.crypto.KEM;
import javax.crypto.SecretKey;

/**
 * ML-KEM-768 of FIPS 203 on the JDK's implementation, as the KEM of the hybrid Profiles (Noise HFS). The initiator sends
 * an encapsulation key, the responder a ciphertext, and both mix the shared secret. A KEM is not contributory: the
 * encapsulating end alone decides the secret.
 */
public final class MlKem768 {
    /** The KEM's name in a Noise protocol name. */
    public static final String NAME = "MLKEM768";
    public static final int ENCAPSULATION_KEY_LEN = 1184;
    public static final int CIPHERTEXT_LEN = 1088;
    public static final int SECRET_LEN = 32;

    private static final String ALGORITHM = "ML-KEM-768";
    /** The fixed DER head of the X.509 SubjectPublicKeyInfo of an ML-KEM-768 key: the raw key follows it. */
    private static final byte[] SPKI_PREFIX = {
        0x30, (byte) 0x82, 0x04, (byte) 0xb2, 0x30, 0x0b, 0x06, 0x09, 0x60, (byte) 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x04,
        0x02, 0x03, (byte) 0x82, 0x04, (byte) 0xa1, 0x00
    };
    /** The bytes of the 3 x 256 coefficients of t, 12 bits each; the 32 bytes of rho follow. */
    private static final int T_LEN = 3 * 384;
    private static final int Q = 3329;

    private MlKem768() {}

    /** A key pair for one handshake: its private part decapsulates once. */
    public record DecapsulationKey(PrivateKey key, byte[] encapsulationKey) {}

    /** A ciphertext and the shared secret it carries. */
    public record Encapsulation(byte[] ciphertext, byte[] secret) {}

    /** A new key pair; {@code random} is the source of the seed (d, z), null for the operating system's ({@link OsRandom}). */
    public static DecapsulationKey generate(SecureRandom random) throws SecureCarrierException {
        SecureRandom r = random != null ? random : OsRandom.get();
        try {
            KeyPairGenerator g = KeyPairGenerator.getInstance(ALGORITHM);
            g.initialize(NamedParameterSpec.ML_KEM_768, r);
            KeyPair kp = g.generateKeyPair();
            byte[] spki = kp.getPublic().getEncoded();
            if (spki.length != SPKI_PREFIX.length + ENCAPSULATION_KEY_LEN
                    || !Arrays.equals(spki, 0, SPKI_PREFIX.length, SPKI_PREFIX, 0, SPKI_PREFIX.length)) {
                throw new GeneralSecurityException("unexpected ML-KEM-768 public key encoding");
            }
            return new DecapsulationKey(kp.getPrivate(), Arrays.copyOfRange(spki, SPKI_PREFIX.length, spki.length));
        } catch (GeneralSecurityException | RuntimeException e) {
            throw new SecureCarrierException(RANDOM_FAILED, "ML-KEM key generation", e); // no fallback
        }
    }

    /**
     * The encapsulation key check of FIPS 203 Section 7.2, the one place a received key is judged: its length, and every
     * 12-bit coefficient below q. The key is public, so the check need not take constant time.
     */
    public static void check(byte[] ek) throws SecureCarrierException {
        if (ek == null || ek.length != ENCAPSULATION_KEY_LEN) throw new SecureCarrierException(KEX, "encapsulation key length");
        for (int i = 0; i < T_LEN; i += 3) {
            int b0 = ek[i] & 0xff, b1 = ek[i + 1] & 0xff, b2 = ek[i + 2] & 0xff;
            if ((b0 | (b1 & 0x0f) << 8) >= Q || (b1 >> 4 | b2 << 4) >= Q) {
                throw new SecureCarrierException(KEX, "encapsulation key coefficient out of range");
            }
        }
    }

    /** Checks a received key and encapsulates to it; {@code random} is the source of m, null for the operating system's ({@link OsRandom}). */
    public static Encapsulation encapsulate(byte[] ek, SecureRandom random) throws SecureCarrierException {
        check(ek);
        SecureRandom r = random != null ? random : OsRandom.get();
        KEM.Encapsulated e;
        try {
            byte[] spki = Arrays.copyOf(SPKI_PREFIX, SPKI_PREFIX.length + ENCAPSULATION_KEY_LEN);
            System.arraycopy(ek, 0, spki, SPKI_PREFIX.length, ENCAPSULATION_KEY_LEN);
            PublicKey pk = KeyFactory.getInstance(ALGORITHM).generatePublic(new X509EncodedKeySpec(spki));
            KEM kem = KEM.getInstance("ML-KEM");
            e = kem.newEncapsulator(pk, null, r).encapsulate();
        } catch (GeneralSecurityException | RuntimeException x) {
            throw new SecureCarrierException(KEX, "encapsulation", x); // the library's own key check, after this one
        }
        return new Encapsulation(e.encapsulation(), secret(e.key()));
    }

    /**
     * The shared secret of a ciphertext. A wrong ciphertext gives a pseudorandom secret (implicit rejection) and the AEAD
     * that follows fails, so nothing here judges it.
     */
    public static byte[] decapsulate(PrivateKey dk, byte[] ct) throws SecureCarrierException {
        try {
            return secret(KEM.getInstance("ML-KEM").newDecapsulator(dk).decapsulate(ct));
        } catch (GeneralSecurityException | RuntimeException x) {
            throw new SecureCarrierException(SecureCarrierException.Code.MALFORMED, "ciphertext", x);
        }
    }

    private static byte[] secret(SecretKey k) throws SecureCarrierException {
        byte[] s = k.getEncoded();
        if (s == null || s.length != SECRET_LEN) throw new SecureCarrierException(SecureCarrierException.Code.MALFORMED, "shared secret");
        return s;
    }
}
