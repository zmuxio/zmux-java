package io.zmux.seal;

import static io.zmux.seal.SecureCarrierException.Kind.BAD_FLAGS;
import static io.zmux.seal.SecureCarrierException.Kind.MALFORMED;
import static io.zmux.seal.SecureCarrierException.Kind.TOO_LARGE;
import static io.zmux.seal.SecureCarrierException.Kind.TRAILING_BYTES;
import static io.zmux.seal.SecureCarrierException.Kind.UNKNOWN_PROFILE;

import io.zmux.internal.wire.VarInt;
import io.zmux.internal.join.Framing;
import io.zmux.internal.join.Frames;
import io.zmux.internal.join.Kdf;
import io.zmux.internal.join.Mode;
import io.zmux.internal.join.Sealer;
import io.zmux.internal.join.Tlv;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/** The first unit of an Establish; its fields are copies of the input. */
final class EstablishHello {
    private static final int LEN_PREFIX = 2;
    private static final int FLAGS_MODE = 0x03;

    private final Profile profile;
    private final Mode mode;
    private final byte[] handle;
    private final byte[] msg1;
    private final int consumed;

    private EstablishHello(Profile profile, Mode mode, byte[] handle, byte[] msg1, int consumed) {
        this.profile = profile;
        this.mode = mode;
        this.handle = handle;
        this.msg1 = msg1;
        this.consumed = consumed;
    }

    public Profile profile() {
        return profile;
    }

    public Mode mode() {
        return mode;
    }

    /** The Handle, empty for a Profile without one. */
    public byte[] handle() {
        return handle;
    }

    /** The Noise message 1. */
    public byte[] message() {
        return msg1;
    }

    /** The bytes the Hello took. */
    public int consumed() {
        return consumed;
    }

    /**
     * Parses the Hello at the start of the input, in wire order: Profile, Flags, Handle, then the
     * unit; the first failure is the result. On a message carrier the input is the whole unit.
     */
    public static EstablishHello parse(byte[] b, int off, int len, Framing f) throws SecureCarrierException {
        if (len == 0) throw Frames.shortInput(f, 1);
        int first = b[off] & 0xff;
        if (first == 0 || first > Profile.ESCAPE) throw new SecureCarrierException(UNKNOWN_PROFILE);
        long value = first;
        int n = 1;
        if (first == Profile.ESCAPE) {
            if (len < 2) throw Frames.shortInput(f, 2);
            int m = VarInt.lengthOf(b[off + 1]);
            if (len < 1 + m) throw Frames.shortInput(f, 1 + m);
            value = VarInt.getKnownWidth(b, off + 1, m);
            if (VarInt.length(value) != m) throw new SecureCarrierException(MALFORMED, "escape encoding");
            if (value <= Profile.ESCAPE) throw new SecureCarrierException(UNKNOWN_PROFILE);
            n = 1 + m;
        }
        Profile p = Profile.of(value);
        if (p == null) throw new SecureCarrierException(UNKNOWN_PROFILE);
        if (len < n + 1) throw Frames.shortInput(f, n + 1);
        int flags = b[off + n] & 0xff;
        n++;
        Mode mode = (flags & ~FLAGS_MODE) != 0 ? null : Mode.ofWire(flags);
        if (mode == null) throw new SecureCarrierException(BAD_FLAGS);
        byte[] handle = new byte[0];
        if (p.isPskBased()) {
            if (len < n + Handshake.HANDLE_LEN) throw Frames.shortInput(f, n + Handshake.HANDLE_LEN);
            handle = Arrays.copyOfRange(b, off + n, off + n + Handshake.HANDLE_LEN);
            n += Handshake.HANDLE_LEN;
        }
        if (f == Framing.MESSAGE) {
            if (len - n > Sealer.MAX_MESSAGE) throw new SecureCarrierException(TOO_LARGE);
            return new EstablishHello(p, mode, handle, Arrays.copyOfRange(b, off + n, off + len), len);
        }
        if (len < n + LEN_PREFIX) throw SecureCarrierException.incomplete(n + LEN_PREFIX);
        int l = (b[off + n] & 0xff) << 8 | (b[off + n + 1] & 0xff);
        n += LEN_PREFIX;
        if (len < n + l) throw SecureCarrierException.incomplete(n + l);
        return new EstablishHello(p, mode, handle, Arrays.copyOfRange(b, off + n, off + n + l), n + l);
    }

    /** {@link #parse} for a server's buffer: a client sends nothing after a Hello until it has the Reply. */
    public static EstablishHello parseExact(byte[] b, int off, int len, Framing f) throws SecureCarrierException {
        EstablishHello h = parse(b, off, len, f);
        if (h.consumed < len) throw new SecureCarrierException(TRAILING_BYTES);
        return h;
    }

    /** The bytes before the framed first message. */
    static int headerLength(Profile p) {
        int n = (p.value() >= Profile.ESCAPE ? 1 + VarInt.length(p.value()) : 1) + 1;
        return p.isPskBased() ? n + Handshake.HANDLE_LEN : n;
    }

    static byte[] encode(Profile p, Mode m, byte[] handle, byte[] msg1, Framing f) throws SecureCarrierException {
        ByteArrayOutputStream head = new ByteArrayOutputStream();
        if (p.value() >= Profile.ESCAPE) {
            head.write(Profile.ESCAPE);
            Tlv.putVarint(head, p.value());
        } else {
            head.write((int) p.value());
        }
        head.write(m.wire());
        if (p.isPskBased()) head.writeBytes(handle);
        return Kdf.concat(head.toByteArray(), Frames.frame(f, msg1));
    }
}
