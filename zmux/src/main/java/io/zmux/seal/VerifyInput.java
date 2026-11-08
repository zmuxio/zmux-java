package io.zmux.seal;

/**
 * What an {@link Anchor} judges.
 *
 * @param role the role expected of the peer: {@link Role#NODE} on a client, {@link Role#CLIENT} on a server
 * @param profile the handshake's Profile
 * @param peerStatic the peer's static X25519 key; null when not yet known (a server's message 2)
 * @param pskIdentity the Handle in Profiles 1 and 3; null in Profiles 4 and 5
 * @param credential the credential the peer's message carries; null when it carries none
 * @param peerAppData the application data the peer has sent so far, in message order
 * @param localContext the host's label of this end (a Listener's or a client's), handed through unread; may be null
 */
public record VerifyInput(Role role, Profile profile, byte[] peerStatic, byte[] pskIdentity, Credential credential,
                          java.util.List<AppData> peerAppData, Object localContext) {
    /** The role of the peer. */
    public enum Role { NODE, CLIENT }

    public VerifyInput {
        peerStatic = peerStatic == null ? null : peerStatic.clone();
        peerAppData = java.util.List.copyOf(peerAppData);
        pskIdentity = pskIdentity == null ? null : pskIdentity.clone();
    }

    @Override public byte[] peerStatic() { return peerStatic == null ? null : peerStatic.clone(); }

    @Override public byte[] pskIdentity() { return pskIdentity == null ? null : pskIdentity.clone(); }
}
