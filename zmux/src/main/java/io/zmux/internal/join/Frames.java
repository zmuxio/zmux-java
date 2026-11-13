package io.zmux.internal.join;

import io.zmux.seal.SecureCarrierException;
import static io.zmux.seal.SecureCarrierException.Code.MALFORMED;
import static io.zmux.seal.SecureCarrierException.Code.TOO_LARGE;

import java.util.Arrays;

/** Unit boundaries on a carrier, shared by Establish and Join. */
public final class Frames {
    /** The first byte of a Reply. */
    public static final int REPLY_TYPE = 0x03;

    private static final int LEN_PREFIX = 2;

    /** A cut unit and the bytes it took. */
    public record Unit(byte[] unit, int consumed) {}

    /** A cut Reply and the bytes it took. */
    public record CutReply(byte[] r, byte[] ciphertext, int consumed) {}

    private Frames() {}

    /** What a unit of n bytes takes on a carrier of this framing. */
    public static int unitLength(Framing f, int n) { return (f == Framing.STREAM ? LEN_PREFIX : 0) + n; }

    /** Appends a unit's framing: a two-byte length on a byte stream, nothing on a message carrier. */
    public static byte[] frame(Framing f, byte[] unit) throws SecureCarrierException {
        if (unit.length > Sealer.MAX_MESSAGE) throw new SecureCarrierException(TOO_LARGE);
        if (f == Framing.MESSAGE) return unit;
        byte[] out = new byte[LEN_PREFIX + unit.length];
        out[0] = (byte) (unit.length >>> 8);
        out[1] = (byte) unit.length;
        System.arraycopy(unit, 0, out, LEN_PREFIX, unit.length);
        return out;
    }

    /** Cuts a Reply of Establish, a Finish or a sealed record; on a message carrier the input is the whole unit. */
    public static Unit cut(byte[] b, int off, int len, Framing f) throws SecureCarrierException {
        if (f == Framing.MESSAGE) {
            if (len > Sealer.MAX_MESSAGE) throw new SecureCarrierException(TOO_LARGE);
            return new Unit(Arrays.copyOfRange(b, off, off + len), len);
        }
        if (len < LEN_PREFIX) throw SecureCarrierException.incomplete(LEN_PREFIX);
        int n = LEN_PREFIX + be16(b, off);
        if (len < n) throw SecureCarrierException.incomplete(n);
        return new Unit(Arrays.copyOfRange(b, off + LEN_PREFIX, off + n), n);
    }

    /** Cuts a Join Reply. */
    public static CutReply cutReply(byte[] b, int off, int len, Framing f, Suite s) throws SecureCarrierException {
        if (len == 0) throw shortInput(f, 1);
        if ((b[off] & 0xff) != REPLY_TYPE) throw new SecureCarrierException(MALFORMED, "reply type");
        int n = 1 + s.nonceLen();
        if (len < n) throw shortInput(f, n);
        byte[] r = Arrays.copyOfRange(b, off + 1, off + n);
        if (f == Framing.MESSAGE) {
            if (len - n > Sealer.MAX_MESSAGE) throw new SecureCarrierException(TOO_LARGE);
            return new CutReply(r, Arrays.copyOfRange(b, off + n, off + len), len);
        }
        if (len < n + LEN_PREFIX) throw SecureCarrierException.incomplete(n + LEN_PREFIX);
        int cl = be16(b, off + n);
        n += LEN_PREFIX;
        if (len < n + cl) throw SecureCarrierException.incomplete(n + cl);
        return new CutReply(r, Arrays.copyOfRange(b, off + n, off + n + cl), n + cl);
    }

    /** A header that ends early: incomplete on a byte stream, malformed on a message carrier. */
    public static SecureCarrierException shortInput(Framing f, int need) {
        return f == Framing.MESSAGE ? new SecureCarrierException(MALFORMED, "header cut") : SecureCarrierException.incomplete(need);
    }

    static int be16(byte[] b, int off) {
        return (b[off] & 0xff) << 8 | (b[off + 1] & 0xff);
    }
}
