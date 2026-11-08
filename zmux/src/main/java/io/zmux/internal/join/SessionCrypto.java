package io.zmux.internal.join;

import static io.zmux.carrier.SecureCarrierException.Code.INVALID_CONFIG;

import io.zmux.carrier.SecureCarrierException;
import io.zmux.carrier.VarInt;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/**
 * One end's Join Suites and Groups, the value of {@code zmux_session_crypto}
 * (zmux_session_crypto): {@code SuiteSet ‖ GroupSet}, each a count and its
 * values, ascending, shortest, without zero, holding Suite 1 and Group 2. Immutable.
 */
public final class SessionCrypto {
    private final long[] suites, groups;

    private SessionCrypto(long[] suites, long[] groups) {
        this.suites = suites;
        this.groups = groups;
    }

    /** This implementation's sets: every Suite and every Group. */
    public static SessionCrypto supported() {
        return new SessionCrypto(Suite.supported(), Group.supported());
    }

    /** Checked sets, as given: INVALID_CONFIG when they break the rules. */
    public static SessionCrypto of(long[] suites, long[] groups) throws SecureCarrierException {
        check(suites, Suite.AES.id(), "suite set");
        check(groups, Group.X25519_MLKEM768.id(), "group set");
        return new SessionCrypto(suites.clone(), groups.clone());
    }

    private static void check(long[] set, long required, String what) throws SecureCarrierException {
        boolean has = false;
        for (int i = 0; i < set.length; i++) {
            if (set[i] <= 0 || set[i] > VarInt.MAX || i > 0 && set[i] <= set[i - 1]) {
                throw new SecureCarrierException(INVALID_CONFIG, what + " not ascending, with zero or a duplicate");
            }
            has |= set[i] == required;
        }
        if (!has) throw new SecureCarrierException(INVALID_CONFIG, what + " without " + required);
    }

    /** Parses a whole value: INVALID_CONFIG for any rule it breaks, a non-shortest varint or bytes after it. */
    public static SessionCrypto decode(byte[] b) throws SecureCarrierException {
        int[] at = {0};
        long[] suites = set(b, at), groups = set(b, at);
        if (at[0] != b.length) throw new SecureCarrierException(INVALID_CONFIG, "bytes after the group set");
        return of(suites, groups);
    }

    private static long[] set(byte[] b, int[] at) throws SecureCarrierException {
        long n = varint(b, at);
        if (n > b.length - at[0]) throw new SecureCarrierException(INVALID_CONFIG, "set longer than the value");
        long[] v = new long[(int) n];
        for (int i = 0; i < n; i++) v[i] = varint(b, at);
        return v;
    }

    private static long varint(byte[] b, int[] at) throws SecureCarrierException {
        if (at[0] >= b.length) throw new SecureCarrierException(INVALID_CONFIG, "value ends early");
        int len = VarInt.lengthOf(b[at[0]]);
        if (at[0] + len > b.length) throw new SecureCarrierException(INVALID_CONFIG, "value ends early");
        long v = b[at[0]] & 0x3f;
        for (int i = 1; i < len; i++) v = v << 8 | (b[at[0] + i] & 0xff);
        if (VarInt.length(v) != len) throw new SecureCarrierException(INVALID_CONFIG, "non-shortest varint");
        at[0] += len;
        return v;
    }

    public long[] suites() { return suites.clone(); }

    public long[] groups() { return groups.clone(); }

    /** Whether the set holds this Group. */
    public boolean hasGroup(long id) {
        for (long g : groups) if (g == id) return true;
        return false;
    }

    /** The value's encoding. */
    public byte[] encode() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        put(out, suites);
        put(out, groups);
        return out.toByteArray();
    }

    private static void put(ByteArrayOutputStream out, long[] set) {
        Tlv.putVarint(out, set.length);
        for (long v : set) Tlv.putVarint(out, v);
    }

    /** {@code session_crypto}: the value of the session initiator, then that of the responder. */
    public static byte[] session(SessionCrypto initiator, SessionCrypto responder) {
        return Kdf.concat(initiator.encode(), responder.encode());
    }

    /** The values in both sets, ascending. */
    public static long[] intersect(long[] a, long[] b) {
        return Arrays.stream(a).filter(x -> Arrays.stream(b).anyMatch(y -> y == x)).toArray();
    }

    @Override public boolean equals(Object o) {
        return o instanceof SessionCrypto c && Arrays.equals(suites, c.suites) && Arrays.equals(groups, c.groups);
    }

    @Override public int hashCode() { return 31 * Arrays.hashCode(suites) + Arrays.hashCode(groups); }

    @Override public String toString() { return "suites " + Arrays.toString(suites) + " groups " + Arrays.toString(groups); }
}
