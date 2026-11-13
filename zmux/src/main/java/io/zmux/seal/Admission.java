package io.zmux.seal;

/**
 * The gate's record of one handshake, for {@link Gate#commit}; unrelated to the carrier admission of a listener
 * ({@link io.zmux.Admission}).
 *
 * @param handle the Handle; empty on a Profile without a pre-shared key
 * @param peer the client as the handshake established it: its static key, credential, verdict, application
 *     data and the two PQ facts; its value is null until the commit returns one
 */
public record Admission(byte[] handle, Peer peer) {
    public Admission {
        handle = handle == null ? new byte[0] : handle.clone();
        java.util.Objects.requireNonNull(peer, "peer");
    }

    @Override public byte[] handle() { return handle.clone(); }

    @Override
    public boolean equals(Object o) {
        return o instanceof Admission a && java.util.Arrays.equals(handle, a.handle) && peer.equals(a.peer);
    }

    @Override
    public int hashCode() { return java.util.Arrays.hashCode(handle) * 31 + peer.hashCode(); }

    @Override
    public String toString() { return "Admission[handle of " + handle.length + " bytes, " + peer + "]"; }
}
