package io.zmux;

/** Application-verified endpoint identities and an optional best-effort route rejoin. The identities are copied in and out and compared by content. */
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

    @Override
    public boolean equals(Object o) {
        return o instanceof DuplicateOptions d && java.util.Arrays.equals(localIdentity, d.localIdentity) && java.util.Arrays.equals(peerIdentity, d.peerIdentity)
                && java.util.Objects.equals(rejoin, d.rejoin) && java.util.Objects.equals(cancellation, d.cancellation);
    }

    @Override
    public int hashCode() {
        return (java.util.Arrays.hashCode(localIdentity) * 31 + java.util.Arrays.hashCode(peerIdentity)) * 31 + java.util.Objects.hash(rejoin, cancellation);
    }

    @Override
    public String toString() {
        return "DuplicateOptions[identities of " + localIdentity.length + " and " + peerIdentity.length + " bytes, rejoin=" + rejoin + ", cancellation=" + (cancellation != null) + "]";
    }
}
