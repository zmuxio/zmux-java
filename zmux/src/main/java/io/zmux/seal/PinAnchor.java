package io.zmux.seal;

import java.security.MessageDigest;

/**
 * Trusts the one peer whose static key has this KeyId (KeyAlg 1), and reads no credential. With the KeyId of
 * a proof key, the peer must also sign the transcript with that key. Immutable.
 */
public final class PinAnchor implements Anchor {
    private final byte[] keyId;
    private final byte[] proofKeyId;

    public PinAnchor(byte[] keyId) {
        this(keyId, null);
    }

    /** {@code proofKeyId} may be null for a pin of the static key alone. */
    public PinAnchor(byte[] keyId, byte[] proofKeyId) {
        if (keyId == null || keyId.length != KeyId.LEN) throw new IllegalArgumentException("a pin is a KeyId of " + KeyId.LEN + " bytes");
        if (proofKeyId != null && proofKeyId.length != KeyId.LEN) throw new IllegalArgumentException("a proof key is a KeyId of " + KeyId.LEN + " bytes");
        this.keyId = keyId.clone();
        this.proofKeyId = proofKeyId == null ? null : proofKeyId.clone();
    }

    /** Whether the pin names a proof key; a pin without one can never give PQ authentication. */
    public boolean isProofKeyPinned() { return proofKeyId != null; }

    @Override
    public VerifyVerdict verify(VerifyInput in) throws VerifyRejectedException {
        byte[] peer = in.peerStatic();
        // A PSK-only profile shows no static key, and there is nothing to pin: the peer is rejected.
        if (peer == null) throw new VerifyRejectedException("no static key to pin");
        byte[] id = KeyId.staticKeyId(peer);
        if (!MessageDigest.isEqual(id, keyId)) throw new VerifyRejectedException("key not pinned");
        // The principal is the pin, not the key bytes: one name for the node however its key is encoded.
        return new VerifyVerdict(new Principal(Principal.NODE_KEY, id), proofKeyId, null);
    }

    /** The rule of requirePqAuth: an end that requires it accepts no pin of an X25519 key alone. */
    static void checkPqAuth(Anchor a, boolean requirePqAuth) throws io.zmux.seal.SecureCarrierException {
        if (requirePqAuth && a instanceof PinAnchor p && !p.isProofKeyPinned()) {
            throw new io.zmux.seal.SecureCarrierException(io.zmux.seal.SecureCarrierException.Kind.INVALID_CONFIG, "requirePqAuth with a pin without a proof key");
        }
    }
}
