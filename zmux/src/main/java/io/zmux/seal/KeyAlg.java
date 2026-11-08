package io.zmux.seal;

/**
 * The algorithms of long-term public keys: the length of a key's
 * canonical encoding and of a signature, whether the algorithm resists a quantum adversary,
 * and its use. A new algorithm is a new constant with its {@link Use} and, for a proof key,
 * its verification in {@link #verify}.
 */
public enum KeyAlg {
    X25519(1, 32, 0, false, Use.STATIC_KEY),
    ML_DSA_44(3, MlDsa44.PUBLIC_KEY_LEN, MlDsa44.SIGNATURE_LEN, true, Use.PROOF_KEY);

    /** What a key of the algorithm is for. */
    public enum Use {
        /** The static key of Establish. */
        STATIC_KEY,
        /** The key of a transcript signature (a proof). */
        PROOF_KEY
    }

    private final long id;
    private final int keyLength, signatureLength;
    private final boolean pq;
    private final Use use;

    KeyAlg(long id, int keyLength, int signatureLength, boolean pq, Use use) {
        this.id = id;
        this.keyLength = keyLength;
        this.signatureLength = signatureLength;
        this.pq = pq;
        this.use = use;
    }

    public long id() { return id; }

    public int keyLength() { return keyLength; }

    /** The length of a signature; 0 for an algorithm that does not sign. */
    public int signatureLength() { return signatureLength; }

    public boolean pq() { return pq; }

    public Use use() { return use; }

    /** The algorithm of this id, or null for one this implementation does not know. */
    public static KeyAlg of(long id) {
        for (KeyAlg a : values()) if (a.id == id) return a;
        return null;
    }

    /** Whether sig is a valid signature by key over message; false for a key or signature of the wrong length. */
    boolean verify(byte[] key, byte[] message, byte[] sig) {
        if (use != Use.PROOF_KEY || key.length != keyLength || sig.length != signatureLength) return false;
        return switch (this) {
            case ML_DSA_44 -> MlDsa44.verify(key, message, sig);
            default -> false;
        };
    }
}
