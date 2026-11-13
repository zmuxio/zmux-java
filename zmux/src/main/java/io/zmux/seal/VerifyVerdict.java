package io.zmux.seal;

/**
 * An {@link Anchor}'s acceptance of a peer.
 *
 * @param principal whom the peer is
 * @param proofKeyId the KeyId of the key whose transcript signature the peer's message must carry, or
 *     null: a verdict that names one fails the handshake unless such a signature verifies
 * @param value the Anchor's own result for the host, handed back unread ({@link Peer#verdict()}); may be null
 */
public record VerifyVerdict(Principal principal, byte[] proofKeyId, Object value) {
    public VerifyVerdict {
        java.util.Objects.requireNonNull(principal, "principal");
        if (proofKeyId != null && proofKeyId.length != KeyId.LEN) throw new IllegalArgumentException("a proof key is a KeyId of " + KeyId.LEN + " bytes");
        proofKeyId = proofKeyId == null ? null : proofKeyId.clone();
    }

    /** A verdict without a proof key or a value. */
    public VerifyVerdict(Principal principal) {
        this(principal, null, null);
    }

    @Override public byte[] proofKeyId() { return proofKeyId == null ? null : proofKeyId.clone(); }

    @Override
    public boolean equals(Object o) {
        return o instanceof VerifyVerdict v && principal.equals(v.principal) && java.util.Arrays.equals(proofKeyId, v.proofKeyId)
                && java.util.Objects.equals(value, v.value);
    }

    @Override
    public int hashCode() { return (principal.hashCode() * 31 + java.util.Arrays.hashCode(proofKeyId)) * 31 + java.util.Objects.hashCode(value); }

    @Override
    public String toString() {
        return "VerifyVerdict[" + principal + ", proofKeyId=" + (proofKeyId == null ? "none" : java.util.HexFormat.of().formatHex(proofKeyId))
                + ", value=" + value + "]";
    }
}
