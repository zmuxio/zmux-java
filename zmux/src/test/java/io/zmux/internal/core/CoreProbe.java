package io.zmux.internal.core;

/** Reads core state for tests outside this package. */
public final class CoreProbe {
    private CoreProbe() {}

    /** The stream credit the peer granted this session for its own streams of one type. */
    public static long peerStreamCredit(Session s, boolean bidi) {
        return s.streams().localLimit(bidi ? 0 : 1);
    }
}
