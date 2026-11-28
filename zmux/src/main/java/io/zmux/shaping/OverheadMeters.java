package io.zmux.shaping;

/** Direction-wide readings at the carrier handoff. Byte counts are unsigned and saturating. */
public record OverheadMeters(Precision precision, Window policy, Window local, long measuredBytes,
                             long handshakeReservedBytes, long handshakePaddingBytes, long handshakeUnresolvedBytes) {
    /** Whether current overhead can be accounted for exactly. */
    public enum Precision { EXACT, UNAVAILABLE }

    /** The allowance and charges for one active measurement window. */
    public record Window(ProfileLimits limits, long baselineBytes, long allowanceBytes, long consumedBytes,
                         long reservedBytes, boolean historyComplete) {}
}
