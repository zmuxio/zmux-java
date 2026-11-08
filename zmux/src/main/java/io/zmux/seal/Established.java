package io.zmux.seal;

import io.zmux.carrier.SecureCarrierException;
import io.zmux.internal.join.Mode;
import io.zmux.internal.join.Opener;
import io.zmux.internal.join.Result;
import io.zmux.internal.join.Sealer;

/** What a finished Establish leaves. */
final class Established {
    private final Profile profile;
    private final Result result;
    private final byte[] peerStatic;
    private final byte[] firstBytes;

    Established(Profile profile, Result result, byte[] peerStatic, byte[] firstBytes) {
        this.profile = profile;
        this.result = result;
        this.peerStatic = peerStatic;
        this.firstBytes = firstBytes;
    }

    public Profile profile() {
        return profile;
    }

    public Mode mode() {
        return result.mode();
    }

    /** The record protection of the sending direction; null unless the Mode is Sealed. */
    public Sealer sealer() {
        return result.sealer();
    }

    /** The record protection of the receiving direction; null unless the Mode is Sealed. */
    public Opener opener() {
        return result.opener();
    }

    /** The peer's static key. */
    public byte[] peerStatic() {
        return peerStatic.clone();
    }

    /** The carrier first bytes of the peer, handed to zmux before any later bytes; null when none. */
    public byte[] firstBytes() {
        return firstBytes;
    }

    public byte[] exporterSecret() {
        return result.exporterSecret();
    }

    /** Derives keying material from this handshake, in every Mode. */
    public byte[] export(String label, byte[] context, int n) throws SecureCarrierException {
        return result.export(label, context, n);
    }
}
