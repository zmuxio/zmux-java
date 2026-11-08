package io.zmux.internal.join;

import io.zmux.carrier.SecureCarrierException;
import static io.zmux.carrier.SecureCarrierException.Code.AUTH;
import static io.zmux.carrier.SecureCarrierException.Code.BAD_CONFIRM;
import static io.zmux.carrier.SecureCarrierException.Code.INCOMPLETE;
import static io.zmux.carrier.SecureCarrierException.Code.MALFORMED;
import static io.zmux.carrier.SecureCarrierException.Code.TOO_LARGE;

import java.security.MessageDigest;
import java.util.Arrays;

/** The acceptor's state machine: Unconfirmed, then Confirmed. It is not thread-safe. */
public final class Acceptor {
    private enum State {
        OPENED,
        UNCONFIRMED,
        CONFIRMED,
        FAILED
    }

    private final Schedule c;
    private final byte[] helloCt;
    private final HelloPayload payload;
    private final KeyShare share;
    private KeyShare.Response exchanged;
    private State state = State.OPENED;
    private SecureCarrierException failure;
    private Result result;

    private Acceptor(Schedule c, byte[] helloCt, HelloPayload payload, KeyShare share) {
        this.share = share;
        this.c = c;
        this.helloCt = helloCt;
        this.payload = payload;
    }

    /**
     * Opens a parsed Hello with the key of the joiner's role and the local carrier's Binding, if
     * any. A Hello in Bound without a Binding is AUTH, unopened; otherwise any disagreement fails as the AEAD does.
     */
    public static Acceptor accept(Hello h, Framing f, byte[] psk, byte[] binding) throws SecureCarrierException {
        return accept(h, f, psk, binding, null);
    }

    /**
     * {@link #accept(Hello, Framing, byte[], byte[])} for a session whose usable Groups are
     * usable (null: every known one). The key share's value is checked with the payload.
     */
    public static Acceptor accept(Hello h, Framing f, byte[] psk, byte[] binding, long[] usable) throws SecureCarrierException {
        if (h.mode() == Mode.BOUND && (binding == null || binding.length == 0)) throw new SecureCarrierException(AUTH);
        Schedule c = new Schedule(h.suite(), h.mode(), f, psk);
        c.ri = h.r();
        c.begin(h.clear(), binding);
        byte[] plain = c.open1(c.k0, h.ciphertext());
        Schedule.ParsedHello p = Schedule.parseHello(plain, usable);
        return new Acceptor(c, h.ciphertext(), p.payload(), p.share());
    }

    /** The joiner's key share. */
    public KeyShare joinerShare() {
        return share;
    }

    /**
     * The acceptor's side of the key exchange, once the Pipe ID's bit is set: a refused share
     * is KEX and permanent. {@link #writeReply} performs it when it has not run.
     */
    public void exchange() throws SecureCarrierException {
        exchange(null, null);
    }

    /** {@link #exchange()} with a given X25519 scalar and ML-KEM randomness, for vectors only. */
    void exchange(byte[] x, java.security.SecureRandom kem) throws SecureCarrierException {
        if (state == State.FAILED) throw failure;
        if (exchanged != null) return;
        try {
            exchanged = KeyShare.respond(share, x, kem);
        } catch (SecureCarrierException e) {
            throw fail(e);
        }
    }

    /** The content of the Hello. */
    public HelloPayload payload() {
        return payload;
    }

    private SecureCarrierException fail(SecureCarrierException e) {
        failure = e;
        state = State.FAILED;
        return e;
    }

    /** The Reply, with a fresh R. */
    public byte[] writeReply(ReplyPayload p) throws SecureCarrierException {
        exchange();
        byte[] plain = Schedule.encodeReply(p, exchanged.share());
        Schedule.Entropy e;
        try {
            e = Schedule.fresh(c.suite);
        } catch (SecureCarrierException x) {
            throw fail(x);
        }
        return writeReplyRaw(plain, e);
    }

    /** The Reply's length on the carrier, for a payload without padding. */
    public int replyLength(ReplyPayload p) throws SecureCarrierException {
        return replyLength(c.suite.nonceLen(), c.framing, replyCiphertext(p));
    }

    /** The length of a Reply on the carrier from its parts, without the carrier's write overhead. */
    public static int replyLength(int rLen, Framing f, int ciphertext) {
        return 1 + rLen + Frames.unitLength(f, ciphertext);
    }

    /** The ciphertext of a Reply with this payload. */
    public int replyCiphertext(ReplyPayload p) throws SecureCarrierException {
        return Schedule.encodeReply(p, new KeyShare(share.group(), new byte[share.group().shareLength(false)])).length + Suite.TAG_LEN;
    }

    /** With the encoded payload and entropy as they are, for vectors only. */
    byte[] writeReplyRaw(byte[] plain, Schedule.Entropy e) throws SecureCarrierException {
        if (state == State.FAILED) throw failure;
        if (state != State.OPENED) throw new IllegalStateException("join is in state " + state);
        exchange();
        byte[] r = c.nonceR(e, new byte[] {(byte) Frames.REPLY_TYPE}, plain);
        c.afterHello(helloCt, r);
        byte[] ct = c.seal1(c.k1, plain);
        if (ct.length > Sealer.MAX_MESSAGE) throw fail(new SecureCarrierException(TOO_LARGE));
        c.afterReply(ct, exchanged.secrets());
        result = c.result(false);
        state = State.UNCONFIRMED;
        return Kdf.concat(new byte[] {(byte) Frames.REPLY_TYPE}, r, Frames.frame(c.framing, ct));
    }

    /**
     * Takes the bytes that begin with the confirmation and returns the bytes it took. INCOMPLETE
     * leaves the state as it was; any other failure is permanent. Not called in Bound.
     */
    public int readConfirm(byte[] b, int off, int len) throws SecureCarrierException {
        if (state == State.FAILED) throw failure;
        if (state != State.UNCONFIRMED || c.mode == Mode.BOUND) {
            throw new IllegalStateException("no confirmation is expected");
        }
        try {
            int n = c.mode == Mode.TRUSTED ? readTag(b, off, len) : readRecord(b, off, len);
            state = State.CONFIRMED;
            return n;
        } catch (SecureCarrierException e) {
            if (e.code() == INCOMPLETE) throw e;
            throw fail(e);
        }
    }

    private int readTag(byte[] b, int off, int len) throws SecureCarrierException {
        int l = c.suite.hashLen(), at = off, unit = len, used = len;
        if (c.framing == Framing.STREAM) {
            if (len < 2) throw SecureCarrierException.incomplete(2);
            unit = (b[off] & 0xff) << 8 | (b[off + 1] & 0xff);
            at = off + 2;
            used = 2 + unit;
        }
        if (unit < l) throw new SecureCarrierException(MALFORMED, "confirmation length");
        if (len < used) throw SecureCarrierException.incomplete(used);
        if (!MessageDigest.isEqual(Arrays.copyOfRange(b, at, at + l), c.confirm)) throw new SecureCarrierException(AUTH);
        requireZeros(b, at + l, at + unit);
        return used;
    }

    private int readRecord(byte[] b, int off, int len) throws SecureCarrierException {
        Frames.Unit u = Frames.cut(b, off, len, c.framing);
        byte[] plain = result.opener().open(u.unit(), 0, u.unit().length);
        requireZeros(plain, 0, plain.length);
        return u.consumed();
    }

    private static void requireZeros(byte[] b, int from, int to) throws SecureCarrierException {
        int any = 0;
        for (int i = from; i < to; i++) any |= b[i];
        if (any != 0) throw new SecureCarrierException(BAD_CONFIRM);
    }

    /** Whether the joiner has proven the Reply was read; in Bound, whether the Hello has passed. */
    public boolean confirmed() {
        return state == State.CONFIRMED || (c.mode == Mode.BOUND && state != State.FAILED);
    }

    /** The result, once Confirmed and the Reply is written. */
    public Result finish() throws SecureCarrierException {
        if (state == State.FAILED) throw failure;
        if (!confirmed() || result == null) throw new IllegalStateException("not confirmed");
        return result;
    }

    Schedule schedule() {
        return c;
    }
}
