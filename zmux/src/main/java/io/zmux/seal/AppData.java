package io.zmux.seal;

import java.util.List;

/**
 * Application handshake data of one message: opaque bytes, with the message that
 * carried them and what that message protects. The bytes are copied in and out, compared by content, and not shown in
 * {@link #toString()}.
 *
 * @param message 1, 2 or 3
 * @param data the bytes, never empty
 * @param forwardSecret whether the data is encrypted under keys with forward secrecy. Message 1 (Profiles 1
 *     and 3 only) is under the pre-shared key alone: an eavesdropper who later learns the key reads it, and
 *     it can be replayed like 0-RTT data
 * @param authenticated whether the peer's verification has passed for the data: never for message 1, and for
 *     messages 2 and 3 once the anchor judged the peer. Data of message 3 is not to be acted on with side
 *     effects before {@link Gate#commit}
 */
public record AppData(int message, byte[] data, boolean forwardSecret, boolean authenticated) {
    public AppData {
        data = data.clone();
    }

    @Override public byte[] data() { return data.clone(); }

    @Override
    public boolean equals(Object o) {
        return o instanceof AppData a && message == a.message && forwardSecret == a.forwardSecret && authenticated == a.authenticated
                && java.util.Arrays.equals(data, a.data);
    }

    @Override
    public int hashCode() { return java.util.Objects.hash(message, forwardSecret, authenticated) * 31 + java.util.Arrays.hashCode(data); }

    @Override
    public String toString() {
        return "AppData[message=" + message + ", " + data.length + " bytes, forwardSecret=" + forwardSecret + ", authenticated=" + authenticated + "]";
    }

    /** The data of message msg as received, before any verification; empty without data. */
    static List<AppData> of(int msg, byte[] b) {
        return b == null || b.length == 0 ? List.of() : List.of(new AppData(msg, b, msg > 1, false));
    }

    /** The entries once the peer's verification has passed: those of messages 2 and 3 are authenticated. */
    static List<AppData> verified(List<AppData> in) {
        return in.stream().map(a -> new AppData(a.message, a.data, a.forwardSecret, a.message > 1)).toList();
    }

    /** a followed by b. */
    static List<AppData> concat(List<AppData> a, List<AppData> b) {
        if (a.isEmpty()) return b;
        if (b.isEmpty()) return a;
        var out = new java.util.ArrayList<>(a);
        out.addAll(b);
        return List.copyOf(out);
    }
}
