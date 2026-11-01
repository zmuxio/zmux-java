package io.zmux;

/** Application-verified endpoint identities and an optional best-effort route rejoin. */
public record DuplicateOptions(byte[] localIdentity, byte[] peerIdentity, PipePool rejoin, Cancellation cancellation) {
    public DuplicateOptions {
        localIdentity = localIdentity == null ? new byte[0] : localIdentity.clone();
        peerIdentity = peerIdentity == null ? new byte[0] : peerIdentity.clone();
        if ((localIdentity.length == 0) != (peerIdentity.length == 0))
            throw new IllegalArgumentException("supply both endpoint identities or neither");
    }
    @Override public byte[] localIdentity() { return localIdentity.clone(); }
    @Override public byte[] peerIdentity() { return peerIdentity.clone(); }
    public static DuplicateOptions defaults() { return new DuplicateOptions(null, null, null, null); }
    public DuplicateOptions withIdentities(byte[] local, byte[] peer) { return new DuplicateOptions(local, peer, rejoin, cancellation); }
    public DuplicateOptions withRejoin(PipePool route) { return new DuplicateOptions(localIdentity, peerIdentity, route, cancellation); }
    public DuplicateOptions withCancellation(Cancellation value) { return new DuplicateOptions(localIdentity, peerIdentity, rejoin, value); }
}
