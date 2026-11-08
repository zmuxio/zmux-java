package io.zmux.seal;

/**
 * Makes the transcript signature (proof) of a handshake message,
 * once per handshake. It may live in an HSM or another process; it runs on the handshake's
 * thread, under its timeout. Signatures are hedged. In Profiles 4 and 5 anyone can ask a
 * server for a proof, so a server's signer limits its own rate.
 */
public interface Signer {
    /** The KeyAlg of the signing key, one of {@link KeyAlg.Use#PROOF_KEY}. */
    KeyAlg alg();

    /** The canonical encoding of the public key. */
    byte[] publicKey();

    /**
     * The signature over {@code m}, of {@link KeyAlg#signatureLength()} bytes. An exception fails the
     * handshake.
     */
    byte[] signProof(byte[] m) throws Exception;
}
