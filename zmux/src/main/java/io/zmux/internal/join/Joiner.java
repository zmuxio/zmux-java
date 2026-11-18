package io.zmux.internal.join;

import io.zmux.seal.SecureCarrierException;
import static io.zmux.seal.SecureCarrierException.Kind.INVALID_CONFIG;
import static io.zmux.seal.SecureCarrierException.Kind.TOO_LARGE;

import io.zmux.internal.wire.VarInt;
import java.io.ByteArrayOutputStream;

/** The joiner's state machine: Pending, HelloSent, Proven, Active. It is not thread-safe. */
public final class Joiner {
    private enum State {
        NEW,
        HELLO_SENT,
        PROVEN,
        DONE,
        FAILED
    }

    private final Schedule c;
    private final byte[] locator;
    private final byte[] binding;
    private final Schedule.Entropy fixed;
    private final KeyShare.Initiator kex;
    private final int phase;
    private byte[] helloCt;
    private State state = State.NEW;
    private SecureCarrierException failure;
    private Result result;
    private int replyConsumed;

    private Joiner(Schedule c, byte[] locator, byte[] binding, Schedule.Entropy fixed, KeyShare.Initiator kex, int phase) {
        this.kex = kex;
        this.phase = phase;
        this.c = c;
        this.locator = locator;
        this.binding = binding;
        this.fixed = fixed;
    }

    /** Starts a Join of Group 1 in epoch phase 0, for tests. */
    public static Joiner create(Suite s, Mode m, Framing f, byte[] locator, byte[] psk, byte[] binding) throws SecureCarrierException {
        return create(s, m, f, locator, psk, binding, Group.X25519, 0);
    }

    /**
     * Starts a Join with the key of the local role in the session, of the epoch whose key phase
     * is phase, exchanging keys in group; {@code binding} is present exactly in Bound.
     */
    public static Joiner create(Suite s, Mode m, Framing f, byte[] locator, byte[] psk, byte[] binding, Group group, int phase)
            throws SecureCarrierException {
        return start(s, m, f, locator, psk, binding, null, group, phase, null, null);
    }

    /** With given entropy and key exchange randomness, for vectors only. */
    static Joiner createFixed(Suite s, Mode m, Framing f, byte[] locator, byte[] psk, byte[] binding, Schedule.Entropy e,
                              Group group, int phase, byte[] x, java.security.SecureRandom kem) throws SecureCarrierException {
        return start(s, m, f, locator, psk, binding, e, group, phase, x, kem);
    }

    private static Joiner start(Suite s, Mode m, Framing f, byte[] locator, byte[] psk, byte[] binding, Schedule.Entropy e,
                                Group group, int phase, byte[] x, java.security.SecureRandom kem) throws SecureCarrierException {
        Schedule c = new Schedule(s, m, f, psk);
        int bl = binding == null ? 0 : binding.length;
        if ((m == Mode.BOUND && bl != Kdf.BINDING_LEN) || (m != Mode.BOUND && bl != 0)) {
            throw new SecureCarrierException(INVALID_CONFIG, "binding");
        }
        if (locator == null || locator.length == 0 || locator.length > 255) {
            throw new SecureCarrierException(INVALID_CONFIG, "locator");
        }
        if (group == null || (phase & ~1) != 0) throw new SecureCarrierException(INVALID_CONFIG, "group or phase");
        return new Joiner(c, locator.clone(), binding, e, KeyShare.initiate(group, x, kem), phase);
    }

    /** The Group of this Join. */
    public Group group() {
        return kex.group();
    }

    private SecureCarrierException fail(SecureCarrierException e) {
        failure = e;
        state = State.FAILED;
        return e;
    }

    private void expect(State s) throws SecureCarrierException {
        if (state == State.FAILED) throw failure;
        if (state != s) throw new IllegalStateException("join is in state " + state);
    }

    /** The Hello, with a fresh R. */
    public byte[] writeHello(HelloPayload p) throws SecureCarrierException {
        return writeHelloRaw(Schedule.encodeHello(p, kex.share()));
    }

    /**
     * A Hello whose key share item is share instead of this Join's own (none when null), for
     * conformance tests of an acceptor: the Join cannot then complete.
     */
    public byte[] writeHelloWithShare(HelloPayload p, KeyShare share) throws SecureCarrierException {
        return writeHelloRaw(Schedule.encodeHello(p, share));
    }

    /** The length of a Hello with this payload on the carrier, without the carrier's write overhead. */
    public int helloLength(HelloPayload p) throws SecureCarrierException {
        return helloLength(c.suite.id(), locator.length, c.suite.nonceLen(), c.framing, helloCiphertext(p));
    }

    /** The length of a Hello on the carrier from its parts, without the carrier's write overhead. */
    public static int helloLength(long suite, int locatorLen, int rLen, Framing f, int ciphertext) {
        return 1 + VarInt.length(suite) + 1 + 1 + locatorLen + rLen + Frames.unitLength(f, ciphertext);
    }

    /** The ciphertext of a Hello with this payload. */
    public int helloCiphertext(HelloPayload p) throws SecureCarrierException {
        return Schedule.encodeHello(p, kex.share()).length + Suite.TAG_LEN;
    }

    /** With the encoded payload as it is, for vectors only. */
    byte[] writeHelloRaw(byte[] plain) throws SecureCarrierException {
        expect(State.NEW);
        ByteArrayOutputStream head = new ByteArrayOutputStream();
        head.write(Hello.PROFILE_JOIN);
        Tlv.putVarint(head, c.suite.id());
        head.write(Hello.modeByte(c.mode, phase));
        head.write(locator.length);
        head.writeBytes(locator);
        Schedule.Entropy e;
        try {
            e = fixed != null ? fixed : Schedule.fresh(c.suite);
        } catch (SecureCarrierException x) {
            throw fail(x);
        }
        c.ri = c.nonceR(e, head.toByteArray(), plain);
        head.writeBytes(c.ri);
        byte[] clear = head.toByteArray();
        c.begin(clear, binding);
        byte[] ct = c.seal1(c.k0, plain);
        if (clear.length + Frames.unitLength(c.framing, ct.length) > Sealer.MESSAGE_FLOOR) throw fail(new SecureCarrierException(TOO_LARGE));
        helloCt = ct;
        state = State.HELLO_SENT;
        return Kdf.concat(clear, Frames.frame(c.framing, ct));
    }

    /**
     * Takes the bytes that begin with the Reply. INCOMPLETE leaves the state as it was, with the
     * total length in {@code need()}; any other failure is permanent. A refusing decision is not a failure.
     */
    public ReplyPayload readReply(byte[] b, int off, int len) throws SecureCarrierException {
        expect(State.HELLO_SENT);
        Frames.CutReply cut;
        try {
            cut = Frames.cutReply(b, off, len, c.framing, c.suite);
        } catch (SecureCarrierException e) {
            if (e.kind() == SecureCarrierException.Kind.INCOMPLETE) throw e;
            throw fail(e);
        }
        try {
            c.afterHello(helloCt, cut.r());
            byte[] plain = c.open1(c.k1, cut.ciphertext());
            Schedule.ParsedReply parsed = Schedule.parseReply(plain, kex.group());
            ReplyPayload p = parsed.payload();
            // Only now, after the Reply's AEAD and payload: a refused share is KEX.
            c.afterReply(cut.ciphertext(), kex.finish(parsed.share().share()));
            result = c.result(true);
            replyConsumed = cut.consumed();
            state = State.PROVEN;
            return p;
        } catch (SecureCarrierException e) {
            throw fail(e);
        }
    }

    /** The bytes the Reply took. */
    public int replyConsumed() {
        return replyConsumed;
    }

    /** The length of the confirmation without zero bytes on the carrier, without the carrier's write overhead. */
    public int confirmLength() {
        return switch (c.mode) {
            case SEALED -> Frames.unitLength(c.framing, Suite.TAG_LEN);
            case TRUSTED -> Frames.unitLength(c.framing, c.suite.hashLen());
            case BOUND -> 0;
        };
    }

    /** The zero bytes the confirmation can hold. */
    public int confirmRoom() {
        return switch (c.mode) {
            case SEALED -> Sealer.MAX_MESSAGE - Suite.TAG_LEN;
            case TRUSTED -> Sealer.MAX_MESSAGE - c.suite.hashLen();
            case BOUND -> 0;
        };
    }

    /** The confirmation: a sealed record of zeros zero bytes in Sealed, the tag and the zero bytes in Trusted, nothing in Bound. */
    public byte[] writeConfirm(int zeros) throws SecureCarrierException {
        expect(State.PROVEN);
        state = State.DONE;
        return switch (c.mode) {
            case SEALED -> {
                try {
                    yield Frames.frame(c.framing, result.sealer().seal(new byte[zeros], 0, zeros));
                } catch (SecureCarrierException e) {
                    throw fail(e);
                }
            }
            case TRUSTED -> Frames.frame(c.framing, java.util.Arrays.copyOf(c.confirm, c.confirm.length + zeros));
            case BOUND -> new byte[0];
        };
    }

    /** The result, after {@link #writeConfirm}. */
    public Result finish() throws SecureCarrierException {
        expect(State.DONE);
        return result;
    }

    Schedule schedule() {
        return c;
    }
}
