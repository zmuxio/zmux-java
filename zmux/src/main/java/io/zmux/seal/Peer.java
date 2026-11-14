package io.zmux.seal;

import io.zmux.RecordMode;
import java.util.List;

/**
 * What an Establish learned of the other end. On a client it is {@link Seal#peerOf}; on a server it goes to
 * {@link Gate#commit} in an {@link CommitRequest} and, with the commit's value, is the session's host value
 * ({@link io.zmux.ZmuxSession#admitted()}).
 *
 * @param staticKey the peer's static key, copied in and out
 * @param credential the credential the peer's message carried; null when it carried none
 * @param verdict the anchor's verdict, its principal and value; null when no anchor judged the peer
 * @param value {@link Gate#commit}'s result, server only; null otherwise, and in the {@link CommitRequest} itself
 * @param profile the handshake's Profile
 * @param mode the handshake's record mode
 * @param appData the application data the peer sent, in message order (from a client messages 1 and 3,
 *     from a server message 2)
 * @param pqConfidential whether the handshake and its records resist a passive quantum adversary, the column of its Profile
 * @param pqAuthenticated whether the peer is authenticated against an active quantum adversary: its verdict named a PQ
 *     proof key and the proof verified
 */
public record Peer(byte[] staticKey, Credential credential, VerifyVerdict verdict, Object value, Profile profile, RecordMode mode,
                   List<AppData> appData, boolean pqConfidential, boolean pqAuthenticated) {
    public Peer {
        staticKey = staticKey == null ? null : staticKey.clone();
        appData = List.copyOf(appData);
    }

    @Override public byte[] staticKey() { return staticKey == null ? null : staticKey.clone(); }

    @Override
    public boolean equals(Object o) {
        return o instanceof Peer p && java.util.Arrays.equals(staticKey, p.staticKey) && java.util.Objects.equals(credential, p.credential)
                && java.util.Objects.equals(verdict, p.verdict) && java.util.Objects.equals(value, p.value) && profile == p.profile
                && mode == p.mode && appData.equals(p.appData) && pqConfidential == p.pqConfidential && pqAuthenticated == p.pqAuthenticated;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(credential, verdict, value, profile, mode, appData, pqConfidential, pqAuthenticated) * 31
                + java.util.Arrays.hashCode(staticKey);
    }

    @Override
    public String toString() {
        return "Peer[staticKey=" + (staticKey == null ? "none" : java.util.HexFormat.of().formatHex(staticKey)) + ", credential=" + credential
                + ", verdict=" + verdict + ", value=" + value + ", profile=" + profile + ", mode=" + mode + ", appData=" + appData
                + ", pqConfidential=" + pqConfidential + ", pqAuthenticated=" + pqAuthenticated + "]";
    }

    /** This peer with the commit's value. */
    Peer withValue(Object v) {
        return new Peer(staticKey, credential, verdict, v, profile, mode, appData, pqConfidential, pqAuthenticated);
    }
}
