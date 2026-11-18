package io.zmux.internal.join;

import io.zmux.seal.SecureCarrierException;
import static io.zmux.seal.SecureCarrierException.Kind.BAD_FLAGS;
import static io.zmux.seal.SecureCarrierException.Kind.BAD_HANDLE;
import static io.zmux.seal.SecureCarrierException.Kind.MALFORMED;
import static io.zmux.seal.SecureCarrierException.Kind.TOO_LARGE;
import static io.zmux.seal.SecureCarrierException.Kind.TRAILING_BYTES;
import static io.zmux.seal.SecureCarrierException.Kind.UNKNOWN_PROFILE;
import static io.zmux.seal.SecureCarrierException.Kind.UNKNOWN_SUITE;

import io.zmux.internal.wire.VarInt;
import java.util.Arrays;

/** A parsed Join Hello; its fields are copies of the input. */
public final class Hello {
    /** The first byte of a Join Hello. */
    public static final int PROFILE_JOIN = 2;

    private static final int LEN_PREFIX = 2;
    /** The bits of ModeByte between the Mode and the key phase, which are zero. */
    private static final int MODE_RESERVED = 0x7c;

    /** The ModeByte of a Mode and a key phase. */
    public static int modeByte(Mode m, int phase) {
        return m.wire() | (phase & 1) << 7;
    }

    private final Suite suite;
    private final Mode mode;
    private final int phase;
    private final byte[] locator;
    private final byte[] r;
    private final byte[] ciphertext;
    private final byte[] clear;
    private final int consumed;

    private Hello(Suite suite, Mode mode, int phase, byte[] locator, byte[] r, byte[] ciphertext, byte[] clear, int consumed) {
        this.suite = suite;
        this.mode = mode;
        this.phase = phase;
        this.locator = locator;
        this.r = r;
        this.ciphertext = ciphertext;
        this.clear = clear;
        this.consumed = consumed;
    }

    public Suite suite() {
        return suite;
    }

    public Mode mode() {
        return mode;
    }

    /** The key phase, e mod 2 of the epoch of the joiner's key. */
    public int phase() {
        return phase;
    }

    public byte[] locator() {
        return locator;
    }

    public byte[] r() {
        return r;
    }

    public byte[] ciphertext() {
        return ciphertext;
    }

    /** The bytes the Hello took. */
    public int consumed() {
        return consumed;
    }

    byte[] clear() {
        return clear;
    }

    /**
     * Parses the Hello at the start of the input, in wire order: the first failure is the result.
     * Later bytes are left to the caller. On a message carrier the input is the whole unit.
     */
    public static Hello parse(byte[] b, int off, int len, Framing f) throws SecureCarrierException {
        Hello head = parseHead(b, off, len, f);
        int n = head.consumed;
        Suite s = head.suite;
        Mode mode = head.mode;
        int phase = head.phase;
        byte[] locator = head.locator, r = head.r, clear = head.clear;
        if (f == Framing.MESSAGE) {
            if (len - n > Sealer.MAX_MESSAGE) throw new SecureCarrierException(TOO_LARGE);
            return new Hello(s, mode, phase, locator, r, Arrays.copyOfRange(b, off + n, off + len), clear, len);
        }
        if (len < n + LEN_PREFIX) throw SecureCarrierException.incomplete(n + LEN_PREFIX);
        int cl = Frames.be16(b, off + n);
        n += LEN_PREFIX;
        if (len < n + cl) throw SecureCarrierException.incomplete(n + cl);
        return new Hello(s, mode, phase, locator, r, Arrays.copyOfRange(b, off + n, off + n + cl), clear, n + cl);
    }

    /**
     * Parses the clear head of a Hello, through R, at the start of the input; ciphertext is null
     * and {@link #consumed()} is the head's length. With {@link SecureCarrierException.Kind#INCOMPLETE}
     * the exception names the smallest total length that can make progress.
     */
    public static Hello parseHead(byte[] b, int off, int len, Framing f) throws SecureCarrierException {
        if (len == 0) throw Frames.shortInput(f, 1);
        if ((b[off] & 0xff) != PROFILE_JOIN) throw new SecureCarrierException(UNKNOWN_PROFILE);
        if (len < 2) throw Frames.shortInput(f, 2);
        int m = VarInt.lengthOf(b[off + 1]);
        if (len < 1 + m) throw Frames.shortInput(f, 1 + m);
        long id = Tlv.readVarint(b, off + 1, m);
        if (VarInt.length(id) != m) throw new SecureCarrierException(MALFORMED, "suite encoding");
        Suite s = Suite.lookup(id);
        if (s == null) throw new SecureCarrierException(UNKNOWN_SUITE);
        int n = 1 + m;
        if (len < n + 1) throw Frames.shortInput(f, n + 1);
        int modeByte = b[off + n] & 0xff;
        Mode mode = (modeByte & MODE_RESERVED) != 0 ? null : Mode.ofWire(modeByte & 0x03);
        if (mode == null) throw new SecureCarrierException(BAD_FLAGS);
        int phase = modeByte >>> 7;
        n++;
        if (len < n + 1) throw Frames.shortInput(f, n + 1);
        int l = b[off + n] & 0xff;
        n++;
        if (l == 0) throw new SecureCarrierException(BAD_HANDLE, "locator");
        if (len < n + l + s.nonceLen()) throw Frames.shortInput(f, n + l + s.nonceLen());
        byte[] locator = Arrays.copyOfRange(b, off + n, off + n + l);
        n += l;
        byte[] r = Arrays.copyOfRange(b, off + n, off + n + s.nonceLen());
        n += s.nonceLen();
        byte[] clear = Arrays.copyOfRange(b, off, off + n);
        return new Hello(s, mode, phase, locator, r, null, clear, n);
    }

    /** {@link #parse} for an acceptor's buffer: a joiner sends nothing after a Hello until it has the Reply. */
    public static Hello parseExact(byte[] b, int off, int len, Framing f) throws SecureCarrierException {
        Hello h = parse(b, off, len, f);
        if (h.consumed < len) throw new SecureCarrierException(TRAILING_BYTES);
        return h;
    }
}
