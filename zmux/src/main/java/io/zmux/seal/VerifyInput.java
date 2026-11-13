package io.zmux.seal;

/**
 * What an {@link Anchor} judges.
 *
 * @param role the role expected of the peer, {@link io.zmux.Role#CLIENT} or {@link io.zmux.Role#SERVER}: SERVER on a client, CLIENT on a server
 * @param profile the handshake's Profile
 * @param peerStatic the peer's static X25519 key; null when not yet known (a server's message 2)
 * @param pskIdentity the Handle in Profiles 1 and 3; null in Profiles 4 and 5
 * @param credential the credential the peer's message carries; null when it carries none
 * @param peerAppData the application data the peer has sent so far, in message order
 * @param localContext the host's label of this end (a Listener's or a client's), handed through unread; may be null
 */
public record VerifyInput(io.zmux.Role role, Profile profile, byte[] peerStatic, byte[] pskIdentity, Credential credential,
                          java.util.List<AppData> peerAppData, Object localContext) {
    public VerifyInput {
        peerStatic = peerStatic == null ? null : peerStatic.clone();
        peerAppData = java.util.List.copyOf(peerAppData);
        pskIdentity = pskIdentity == null ? null : pskIdentity.clone();
    }

    @Override public byte[] peerStatic() { return peerStatic == null ? null : peerStatic.clone(); }

    @Override public byte[] pskIdentity() { return pskIdentity == null ? null : pskIdentity.clone(); }

    @Override
    public boolean equals(Object o) {
        return o instanceof VerifyInput v && role == v.role && profile == v.profile && java.util.Arrays.equals(peerStatic, v.peerStatic)
                && java.util.Arrays.equals(pskIdentity, v.pskIdentity) && java.util.Objects.equals(credential, v.credential)
                && peerAppData.equals(v.peerAppData) && java.util.Objects.equals(localContext, v.localContext);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(role, profile, credential, peerAppData, localContext) * 961
                + java.util.Arrays.hashCode(peerStatic) * 31 + java.util.Arrays.hashCode(pskIdentity);
    }

    @Override
    public String toString() {
        return "VerifyInput[role=" + role + ", profile=" + profile + ", peerStatic=" + (peerStatic == null ? "none" : java.util.HexFormat.of().formatHex(peerStatic))
                + ", pskIdentity=" + (pskIdentity == null ? "none" : pskIdentity.length + " bytes") + ", credential=" + credential
                + ", peerAppData=" + peerAppData + ", localContext=" + localContext + "]";
    }
}
