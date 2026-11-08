package io.zmux;

/** Who protects a carrier's records, from strongest to weakest. */
public enum RecordMode {
    /** Records are protected with the keys of the handshake. */
    SEALED,
    /** Records are left to the carrier, whose exporter is bound into the handshake. */
    BOUND,
    /** Records are left to the deployment; both ends configure it explicitly. */
    TRUSTED,
    /** A first pipe without the secure carrier. No Join has this mode. */
    UNMANAGED
}
