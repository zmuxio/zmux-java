package io.zmux.internal.join;

import io.zmux.seal.SecureCarrierException;
import static io.zmux.seal.SecureCarrierException.Code.AUTH;
import static io.zmux.seal.SecureCarrierException.Code.BAD_FIELD;
import static io.zmux.seal.SecureCarrierException.Code.INVALID_CONFIG;
import static io.zmux.seal.SecureCarrierException.Code.MALFORMED;
import static io.zmux.seal.SecureCarrierException.Code.MISSING_FIELD;
import static io.zmux.seal.SecureCarrierException.Code.TLV_TRUNCATED;
import static java.nio.charset.StandardCharsets.US_ASCII;

import io.zmux.internal.wire.VarInt;
import io.zmux.internal.random.OsRandom;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/** The key schedule of one Join, and the payload codec both ends share. */
final class Schedule {
    private static final int T_PADDING = 0;
    // Type 4 is unassigned: an unknown even type, skipped.
    private static final int T_PIPE = 5;
    private static final int T_FRAMES = 6;
    private static final int T_DECISION = 7;
    private static final int T_KEY_SHARE = 9;

    private static final byte[] NONCE_LABEL = "zmux join nonce".getBytes(US_ASCII);
    private static final byte[] CONFIRM_LABEL = "confirm".getBytes(US_ASCII);
    private static final AtomicLong ATTEMPTS = new AtomicLong();
    /** R's entropy: the operating system's source, read anew for every Join ({@link OsRandom}). */
    static final String SOURCE = OsRandom.SOURCE;

    /** What R takes besides its message. */
    record Entropy(long attempt, byte[] random) {}

    final Suite suite;
    final Mode mode;
    final Framing framing;
    final byte[] psk;
    byte[] ri;
    byte[] rr;
    byte[] th;
    byte[] th2;
    byte[] th3;
    byte[] k0;
    byte[] k1;
    byte[] hs;
    byte[] i2r;
    byte[] r2i;
    byte[] macKey;
    byte[] exporter;
    byte[] confirm;

    Schedule(Suite suite, Mode mode, Framing framing, byte[] psk) throws SecureCarrierException {
        if (suite == null || mode == null) throw new SecureCarrierException(INVALID_CONFIG, "suite or mode");
        if (psk == null || psk.length < suite.hashLen()) throw new SecureCarrierException(INVALID_CONFIG, "join key");
        this.suite = suite;
        this.mode = mode;
        this.framing = framing;
        this.psk = psk;
    }

    static Entropy fresh(Suite s) throws SecureCarrierException {
        byte[] random = new byte[s.nonceLen()];
        OsRandom.fill(random); // random_failed, with no fallback
        return new Entropy(ATTEMPTS.incrementAndGet(), random);
    }

    byte[] nonceR(Entropy e, byte[] head, byte[] payload) {
        byte[] attempt = ByteBuffer.allocate(Long.BYTES).putLong(e.attempt()).array();
        byte[] full = Kdf.hmac(psk, NONCE_LABEL, attempt, e.random(), Kdf.hash(head, payload));
        return java.util.Arrays.copyOf(full, suite.nonceLen());
    }

    void begin(byte[] clear, byte[] binding) {
        th = Kdf.hash(clear, binding);
        k0 = Kdf.expandLabel(psk, "hello", th, suite.keyLen());
    }

    void afterHello(byte[] helloCt, byte[] r) {
        rr = r;
        th2 = Kdf.hash(th, helloCt, r);
        k1 = Kdf.expandLabel(psk, "reply", th2, suite.keyLen());
    }

    /** After the Reply, with the Group's shared secrets in order. */
    void afterReply(byte[] replyCt, byte[][] ss) {
        th3 = Kdf.hash(th2, replyCt);
        hs = Kdf.combine(psk, ss);
        i2r = Kdf.expandLabel(hs, "i2r", th3, suite.keyLen());
        r2i = Kdf.expandLabel(hs, "r2i", th3, suite.keyLen());
        macKey = Kdf.expandLabel(hs, "mac", th3, suite.hashLen());
        exporter = Kdf.expandLabel(hs, "exporter", th3, suite.hashLen());
        confirm = Kdf.hmac(macKey, CONFIRM_LABEL, th3);
    }

    byte[] seal1(byte[] key, byte[] plain) {
        Aead a = new Aead(suite);
        a.setKey(key);
        return a.encrypt(0, null, plain, 0, plain.length);
    }

    byte[] open1(byte[] key, byte[] ct) throws SecureCarrierException {
        if (ct.length < Suite.TAG_LEN) throw new SecureCarrierException(MALFORMED, "ciphertext under a tag");
        Aead a = new Aead(suite);
        a.setKey(key);
        try {
            return a.decrypt(0, null, ct, 0, ct.length);
        } catch (SecureCarrierException e) {
            throw new SecureCarrierException(AUTH);
        }
    }

    Result result(boolean joiner) {
        Sealer sealer = null;
        Opener opener = null;
        if (mode == Mode.SEALED) {
            sealer = Sealer.create(suite, joiner ? i2r : r2i, 0);
            opener = Opener.create(suite, joiner ? r2i : i2r, 0);
        }
        return new Result(mode, suite, sealer, opener, exporter);
    }

    static byte[] encodeHello(HelloPayload p, KeyShare share) throws SecureCarrierException {
        JoinPipe pipe = p.pipe();
        if (pipe == null
                || pipe.direction() < 0
                || pipe.direction() > 2
                || outOfRange(pipe.id())
                || outOfRange(pipe.fateGroup())
                || outOfRange(pipe.priority())) {
            throw new SecureCarrierException(INVALID_CONFIG, "pipe fields");
        }
        ByteArrayOutputStream v = new ByteArrayOutputStream();
        Tlv.putVarint(v, pipe.id());
        v.write(pipe.direction());
        Tlv.putVarint(v, pipe.fateGroup());
        Tlv.putVarint(v, pipe.priority());
        v.write(pipe.standby() ? 1 : 0);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        putPadding(out, p.padding());
        Tlv.putItem(out, T_PIPE, v.toByteArray());
        if (p.firstBatch() != null && p.firstBatch().length > 0) Tlv.putItem(out, T_FRAMES, p.firstBatch());
        if (share != null) Tlv.putItem(out, T_KEY_SHARE, share.encode());
        return out.toByteArray();
    }

    static byte[] encodeReply(ReplyPayload p, KeyShare share) throws SecureCarrierException {
        if (p.decision() < 0 || p.decision() > 1) throw new SecureCarrierException(INVALID_CONFIG, "decision");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        putPadding(out, p.padding());
        if (p.frames() != null && p.frames().length > 0) Tlv.putItem(out, T_FRAMES, p.frames());
        Tlv.putItem(out, T_DECISION, new byte[] {(byte) p.decision()});
        Tlv.putItem(out, T_KEY_SHARE, share.encode());
        return out.toByteArray();
    }

    private static boolean outOfRange(long v) {
        return v < 0 || v > VarInt.MAX;
    }

    private static void putPadding(ByteArrayOutputStream out, int padding) {
        if (padding != 0) Tlv.putItem(out, T_PADDING, new byte[Math.max(padding, 0)]);
    }

    private static JoinPipe decodePipe(byte[] v) throws SecureCarrierException {
        int i = 0;
        long id = Tlv.minimalVarint(v, i, v.length);
        i += VarInt.length(id);
        if (i >= v.length) throw new SecureCarrierException(TLV_TRUNCATED);
        int direction = v[i++] & 0xff;
        if (direction > 2) throw new SecureCarrierException(BAD_FIELD, "direction");
        long fate = Tlv.minimalVarint(v, i, v.length);
        i += VarInt.length(fate);
        long priority = Tlv.minimalVarint(v, i, v.length);
        i += VarInt.length(priority);
        if (v.length - i != 1) throw new SecureCarrierException(TLV_TRUNCATED);
        int standby = v[i] & 0xff;
        if (standby > 1) throw new SecureCarrierException(BAD_FIELD, "standby");
        return new JoinPipe(id, direction, fate, priority, standby == 1);
    }

    /** A Hello's payload and its key share. */
    record ParsedHello(HelloPayload payload, KeyShare share) {}

    /**
     * The key share item's value: {@code Group(varint) ‖ Share}, of a Group in usable (any
     * known one when null) and of the length of the sender's role; BAD_FIELD otherwise.
     */
    static KeyShare decodeShare(byte[] v, boolean initiator, long[] usable) throws SecureCarrierException {
        long id = Tlv.minimalVarint(v, 0, v.length);
        Group g = Group.of(id);
        if (g == null || usable != null && java.util.Arrays.stream(usable).noneMatch(u -> u == id)) {
            throw new SecureCarrierException(BAD_FIELD, "key share group");
        }
        int at = VarInt.length(id);
        if (v.length - at != g.shareLength(initiator)) throw new SecureCarrierException(BAD_FIELD, "key share length");
        return new KeyShare(g, java.util.Arrays.copyOfRange(v, at, v.length));
    }

    static ParsedHello parseHello(byte[] plain, long[] usable) throws SecureCarrierException {
        KeyShare[] share = new KeyShare[1];
        JoinPipe[] pipe = new JoinPipe[1];
        byte[][] batch = new byte[1][];
        int[] padding = new int[1];
        Tlv.parse(plain, (type, val) -> {
            switch ((int) Math.min(type, Integer.MAX_VALUE)) {
                case T_PADDING -> padding[0] = val.length;
                case T_PIPE -> pipe[0] = decodePipe(val);
                case T_FRAMES -> batch[0] = val;
                case T_KEY_SHARE -> share[0] = decodeShare(val, true, usable);
                default -> {
                    return false;
                }
            }
            return true;
        });
        if (pipe[0] == null) throw new SecureCarrierException(MISSING_FIELD, "join pipe");
        if (share[0] == null) throw new SecureCarrierException(MISSING_FIELD, "key share");
        return new ParsedHello(new HelloPayload(pipe[0], batch[0], padding[0]), share[0]);
    }

    /** A Reply's payload and the responder's share of the Hello's Group. */
    record ParsedReply(ReplyPayload payload, KeyShare share) {}

    static ParsedReply parseReply(byte[] plain, Group group) throws SecureCarrierException {
        KeyShare[] share = new KeyShare[1];
        int[] decision = {-1};
        byte[][] frames = new byte[1][];
        int[] padding = new int[1];
        Tlv.parse(plain, (type, val) -> {
            switch ((int) Math.min(type, Integer.MAX_VALUE)) {
                case T_PADDING -> padding[0] = val.length;
                case T_FRAMES -> frames[0] = val;
                case T_DECISION -> {
                    if (val.length != 1 || (val[0] & 0xff) > 1) throw new SecureCarrierException(BAD_FIELD, "decision");
                    decision[0] = val[0];
                }
                case T_KEY_SHARE -> {
                    KeyShare k = decodeShare(val, false, null);
                    if (k.group() != group) throw new SecureCarrierException(BAD_FIELD, "key share group");
                    share[0] = k;
                }
                default -> {
                    return false;
                }
            }
            return true;
        });
        if (decision[0] < 0) throw new SecureCarrierException(MISSING_FIELD, "join decision");
        if (share[0] == null) throw new SecureCarrierException(MISSING_FIELD, "key share");
        return new ParsedReply(new ReplyPayload(decision[0], frames[0], padding[0]), share[0]);
    }
}
