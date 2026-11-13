package io.zmux.seal;

import static io.zmux.seal.SecureCarrierException.Code.INVALID_CONFIG;
import static io.zmux.seal.SecureCarrierException.Code.RANDOM_FAILED;

import java.io.Serial;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.SecureRandom;

/** A proof key held in this process: a {@link Signer} of ML-DSA-44. Immutable and thread-safe. */
public final class SigningKey implements Signer {
    private final KeyAlg alg;
    private final PrivateKey priv;
    private final byte[] pub;

    private SigningKey(KeyAlg alg, KeyPair kp) {
        this.alg = alg;
        this.priv = kp.getPrivate();
        this.pub = MlDsa44.raw(kp.getPublic());
    }

    /**
     * A new key of alg from the operating system's entropy.
     *
     * @throws IllegalArgumentException when alg does not sign proofs
     * @throws java.io.UncheckedIOException wrapping {@code random_failed} when key generation fails
     */
    public static SigningKey generate(KeyAlg alg) {
        if (alg != KeyAlg.ML_DSA_44) throw new IllegalArgumentException("KeyAlg " + alg + " does not sign proofs");
        try {
            return new SigningKey(alg, MlDsa44.generate(new SecureRandom()));
        } catch (GeneralSecurityException e) {
            throw new java.io.UncheckedIOException(new SecureCarrierException(RANDOM_FAILED, "ML-DSA key generation", e));
        }
    }

    /** The key of alg from its seed: ξ of FIPS 204 KeyGen, 32 bytes for ML-DSA-44. */
    public static SigningKey fromSeed(KeyAlg alg, byte[] seed) throws SecureCarrierException {
        check(alg);
        if (seed == null || seed.length != MlDsa44.SEED_LEN) throw new SecureCarrierException(INVALID_CONFIG, "a seed of " + MlDsa44.SEED_LEN + " bytes");
        try {
            return new SigningKey(alg, MlDsa44.generate(new Once(seed)));
        } catch (GeneralSecurityException e) {
            throw new SecureCarrierException(INVALID_CONFIG, "ML-DSA key from a seed", e);
        }
    }

    private static void check(KeyAlg alg) throws SecureCarrierException {
        if (alg != KeyAlg.ML_DSA_44) throw new SecureCarrierException(INVALID_CONFIG, "KeyAlg " + alg + " does not sign proofs");
    }

    @Override public KeyAlg alg() { return alg; }

    @Override public byte[] publicKey() { return pub.clone(); }

    /** The public key's algorithm only: the private key never appears in a string. */
    @Override
    public String toString() {
        return "SigningKey[" + alg + "]";
    }

    /** The KeyId of the key, for a verdict or a pin to name. */
    public byte[] keyId() { return KeyId.of(alg, pub); }

    @Override
    public byte[] signProof(byte[] m) throws SecureCarrierException {
        return sign(m, new SecureRandom());
    }

    /** The deterministic signature, rnd of zeros: for vectors only. */
    byte[] signDeterministic(byte[] m) throws SecureCarrierException {
        return sign(m, new Once(new byte[32]));
    }

    private byte[] sign(byte[] m, SecureRandom random) throws SecureCarrierException {
        try {
            return MlDsa44.sign(priv, m, random);
        } catch (GeneralSecurityException e) {
            throw new SecureCarrierException(RANDOM_FAILED, "ML-DSA signature", e);
        }
    }

    /** Gives fixed bytes once: the seed of a key, or the zero rnd of a deterministic signature. */
    private static final class Once extends SecureRandom {
        @Serial
        private static final long serialVersionUID = 1L;
        private final byte[] bytes;
        private boolean used;

        Once(byte[] bytes) {
            super(new byte[0]);
            this.bytes = bytes.clone();
        }

        @Override
        public synchronized void nextBytes(byte[] out) {
            if (used || out.length != bytes.length) throw new IllegalStateException("fixed randomness of " + bytes.length + " bytes, asked for " + out.length);
            used = true;
            System.arraycopy(bytes, 0, out, 0, out.length);
            java.util.Arrays.fill(bytes, (byte) 0);
        }

        // SecureRandom is Serializable, but these bytes are a key's seed or a signature's randomness: never written out.
        @Serial
        private void writeObject(java.io.ObjectOutputStream out) throws java.io.NotSerializableException {
            throw new java.io.NotSerializableException(Once.class.getName());
        }

        @Serial
        private void readObject(java.io.ObjectInputStream in) throws java.io.NotSerializableException {
            throw new java.io.NotSerializableException(Once.class.getName());
        }
    }
}
