package io.zmux.seal;

import io.zmux.internal.kex.MlKem768;

import static io.zmux.seal.SecureCarrierException.Kind.INVALID_CONFIG;
import static io.zmux.seal.SecureCarrierException.Kind.MALFORMED;
import static io.zmux.seal.SecureCarrierException.Kind.PEER_IS_SELF;
import static io.zmux.seal.SecureCarrierException.Kind.TOO_LARGE;
import static java.nio.charset.StandardCharsets.US_ASCII;

import io.zmux.internal.join.Kdf;
import io.zmux.internal.join.Mode;
import io.zmux.internal.join.Opener;
import io.zmux.internal.join.Result;
import io.zmux.internal.join.Sealer;
import io.zmux.internal.join.Suite;
import io.zmux.internal.join.Tlv;
import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;

/** The Noise state machine of one handshake, driven by its Profile. Not thread-safe. */
final class Handshake {
    /** Where ephemeral keys come from; only vectors fix it. */
    @FunctionalInterface
    interface EphemeralSource {
        StaticKey next() throws SecureCarrierException;
    }

    static final int PSK_LEN = 32;
    /** The length of a Handle. */
    static final int HANDLE_LEN = 16;

    private enum Token {
        PSK,
        E,
        S,
        EE,
        ES,
        SE,
        /** A fresh KEM encapsulation key (Noise HFS). */
        E1,
        /** Encapsulates to the peer's key and mixes the shared secret. */
        EKEM1
    }

    // The KEM ciphertext follows ee, so the responder's static key, its credentials and everything after them are under
    // keys that hold the KEM secret.
    private static final Token[][] XXHFS = {
        {Token.E, Token.E1}, {Token.E, Token.EE, Token.EKEM1, Token.S, Token.ES}, {Token.S, Token.SE}
    };
    private static final Token[][] XXHFS_PSK0 = {
        {Token.PSK, Token.E, Token.E1}, {Token.E, Token.EE, Token.EKEM1, Token.S, Token.ES}, {Token.S, Token.SE}
    };
    // Application data goes in message 1 of a Profile with a pre-shared key, and in messages 2 and 3; the msg2 of XX
    // does not authenticate the client, so it takes no first bytes of a carrier.
    private static final int KNOWN_MSG1_PSK = EstablishPayload.KNOWN_APP_DATA;
    private static final int KNOWN_MSG2 = EstablishPayload.KNOWN_CREDENTIAL | EstablishPayload.KNOWN_APP_DATA | EstablishPayload.KNOWN_PROOF;
    private static final int KNOWN_MSG2_PSK = KNOWN_MSG2 | EstablishPayload.KNOWN_FIRST_BYTES;
    private static final int KNOWN_MSG3 = KNOWN_MSG2 | EstablishPayload.KNOWN_FIRST_BYTES;

    /** Signs M of the message being written. */
    @FunctionalInterface
    interface ProofSigner {
        byte[] sign(byte[] m) throws SecureCarrierException;

        /** A host's signer; its failure is local, INVALID_CONFIG with the failure as the cause. */
        static ProofSigner of(Signer s) {
            return m -> {
                try {
                    return s.signProof(m);
                } catch (SecureCarrierException e) {
                    throw e;
                } catch (Exception | LinkageError e) {
                    throw new SecureCarrierException(INVALID_CONFIG, "the proof signer failed", e);
                }
            };
        }
    }

    final SymmetricState ss;
    private final Profile profile;
    private final Mode mode;
    private final boolean initiator;
    private final boolean noBinding;
    private final byte[] psk;
    private final StaticKey s;
    private final EphemeralSource source;
    private final Token[][] msgs;
    /** The source of the KEM's randomness, null for the operating system's (OsRandom); only vectors fix it. */
    private final SecureRandom kemRandom;
    private StaticKey e;
    private byte[] rs;
    private byte[] re;
    /** This initiator's KEM key, and the encapsulation key a responder received; each lives for one token pair. */
    private MlKem768.DecapsulationKey e1;
    private byte[] re1;
    private int idx;
    /** h after the last token of the latest message, before its payload: h_p, which a proof signs. */
    private byte[] hp;
    /** The bound on the application data of a received message. */
    private long maxAppData = ClientConfig.DEFAULT_MAX_APP_DATA;
    private SecureCarrierException failure;
    private boolean finished;

    Handshake(Profile p, Mode m, boolean initiator, byte[] handle, byte[] psk, byte[] binding, StaticKey s, EphemeralSource source)
            throws SecureCarrierException {
        this(p, m, initiator, handle, psk, binding, s, source, null);
    }

    /** With a given source of the KEM's randomness: the seed (d, z) of a key pair, or the m of an encapsulation. */
    Handshake(Profile p, Mode m, boolean initiator, byte[] handle, byte[] psk, byte[] binding, StaticKey s, EphemeralSource source,
              SecureRandom kemRandom) throws SecureCarrierException {
        if (p == null) throw new SecureCarrierException(SecureCarrierException.Kind.UNKNOWN_PROFILE);
        if (m == null) throw new SecureCarrierException(INVALID_CONFIG, "mode");
        int hl = handle == null ? 0 : handle.length;
        if (p.isPskBased() ? hl != HANDLE_LEN : hl != 0) throw new SecureCarrierException(INVALID_CONFIG, "handle");
        int pl = psk == null ? 0 : psk.length;
        if (p.isPskBased() ? pl != PSK_LEN : pl != 0) throw new SecureCarrierException(INVALID_CONFIG, "pre-shared key");
        int bl = binding == null ? 0 : binding.length;
        // Only a sender judges its Binding here: a responder fails as AUTH when it reads.
        if (initiator && (m == Mode.BOUND ? bl != Kdf.BINDING_LEN : bl != 0)) {
            throw new SecureCarrierException(INVALID_CONFIG, "binding");
        }
        if (s == null) throw new SecureCarrierException(INVALID_CONFIG, "static key");
        this.profile = p;
        this.mode = m;
        this.initiator = initiator;
        this.psk = psk;
        this.s = s;
        this.source = source;
        this.kemRandom = kemRandom;
        this.msgs = pattern(p);
        this.ss = new SymmetricState(p.noiseName(), p.suite());
        ss.mixHash(prologue(p, m, handle, binding));
        this.noBinding = !initiator && m == Mode.BOUND && bl == 0;
    }

    private static Token[][] pattern(Profile p) {
        return p.isPskBased() ? XXHFS_PSK0 : XXHFS;
    }

    static byte[] prologue(Profile p, Mode m, byte[] handle, byte[] binding) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.writeBytes("zmux-seal".getBytes(US_ASCII));
        Tlv.putVarint(b, p.value());
        b.write(m.wire());
        if (handle != null) b.writeBytes(handle);
        if (binding != null) b.writeBytes(binding);
        return b.toByteArray();
    }

    /** Bounds the application data of a received message, in bytes; {@link Long#MAX_VALUE}: no bound beyond the message. */
    Handshake maxAppData(long limit) {
        maxAppData = limit;
        return this;
    }

    private int known(int message) {
        return switch (message) {
            case 0 -> profile.isPskBased() ? KNOWN_MSG1_PSK : 0;
            case 1 -> profile.isPskBased() ? KNOWN_MSG2_PSK : KNOWN_MSG2;
            default -> KNOWN_MSG3;
        };
    }

    private void ready(boolean writing) throws SecureCarrierException {
        if (failure != null) throw failure;
        if (idx >= msgs.length || (idx % 2 == 0) != (initiator == writing)) {
            throw new IllegalStateException("handshake is not at this message");
        }
    }

    private SecureCarrierException fail(SecureCarrierException x) {
        failure = x;
        dropKem();
        return x;
    }

    /** Gives up the KEM state: a decapsulation key serves one handshake and one ciphertext. */
    private void dropKem() {
        e1 = null;
        re1 = null;
    }

    /** The next Noise message, without framing. */
    byte[] writeMessage(EstablishPayload p) throws SecureCarrierException {
        return writeMessage(p, null);
    }

    /**
     * The next Noise message, without framing. With a signer, p's proof is {@link Proof#placeholder}: its KeyAlg and
     * key, and a signature of zeros that gives the message its length; the signature is made over M from h_p,
     * after the message's last token, and must have the KeyAlg's length.
     */
    byte[] writeMessage(EstablishPayload p, ProofSigner signer) throws SecureCarrierException {
        return writeMessage(p, signer, 0);
    }

    /**
     * {@link #writeMessage(EstablishPayload, ProofSigner)} for a message that goes out with wire more bytes around it
     * (framing, and the Hello's header): the size check of the application data counts them, so it refuses exactly what
     * {@link #checkWritten} refuses once the message is written, and before this handshake's state moved on.
     */
    byte[] writeMessage(EstablishPayload p, ProofSigner signer, int wire) throws SecureCarrierException {
        ready(true);
        // Without a signer a proof goes as it is: only vectors and tests write one so.
        if (signer != null && p.proof() == null) throw new IllegalArgumentException("a signer without its proof's placeholder");
        byte[] plain;
        try {
            plain = p.encode(known(idx));
            // What is sent is bounded only by the message.
            if (p.appData() != null && p.appData().length > 0 && wire + minMessage() + plain.length > Sealer.MESSAGE_FLOOR) {
                throw new SecureCarrierException(TOO_LARGE, "application data of " + p.appData().length + " bytes does not fit the message");
            }
        } catch (SecureCarrierException x) {
            throw fail(x);
        }
        if (signer == null) return writeRaw(hp -> plain);
        int message = idx;
        return writeRaw(hp -> {
            byte[] sig = signer.sign(Proof.message(message, hp));
            if (sig == null || sig.length != p.proof().sig().length) throw new SecureCarrierException(INVALID_CONFIG, "a proof signature of the wrong length");
            return p.withProof(p.proof().withSig(sig)).encode(known(message));
        });
    }

    /** With the encoded payload as it is. */
    byte[] writeRaw(byte[] plain) throws SecureCarrierException {
        return writeRaw(hp -> plain);
    }

    /** The payload of the message being written, from its h_p. */
    @FunctionalInterface
    private interface Plain {
        byte[] of(byte[] hp) throws SecureCarrierException;
    }

    private byte[] writeRaw(Plain payload) throws SecureCarrierException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            for (Token t : msgs[idx]) {
                switch (t) {
                    case E -> {
                        e = source.next();
                        byte[] pub = e.publicRaw();
                        out.writeBytes(pub);
                        ss.mixHash(pub);
                        if (profile.isPskBased()) ss.mixKey(pub);
                    }
                    case S -> out.writeBytes(ss.encryptAndHash(s.publicRaw()));
                    case PSK -> ss.mixKeyAndHash(psk);
                    case E1 -> {
                        // New for every handshake, a retry included; never cached.
                        e1 = MlKem768.generate(kemRandom);
                        out.writeBytes(ss.encryptAndHash(e1.encapsulationKey()));
                    }
                    case EKEM1 -> {
                        // The received key is judged here, before this end sends anything.
                        byte[] ek = re1;
                        re1 = null;
                        MlKem768.Encapsulation enc = MlKem768.encapsulate(ek, kemRandom);
                        out.writeBytes(ss.encryptAndHash(enc.ciphertext()));
                        ss.mixKey(enc.secret());
                        Arrays.fill(enc.secret(), (byte) 0);
                    }
                    default -> mixDh(t);
                }
            }
            hp = ss.h.clone();
            out.writeBytes(ss.encryptAndHash(payload.of(hp)));
        } catch (SecureCarrierException x) {
            throw fail(x);
        }
        if (out.size() > Sealer.MAX_MESSAGE) throw fail(new SecureCarrierException(TOO_LARGE));
        idx++;
        return out.toByteArray();
    }

    /** A message as written, framing included, beyond the handshake message floor: too_large, before it is written. */
    byte[] checkWritten(byte[] written) throws SecureCarrierException {
        if (written.length > Sealer.MESSAGE_FLOOR) throw fail(new SecureCarrierException(TOO_LARGE, "a handshake message of " + written.length + " bytes"));
        return written;
    }

    /** Takes the next Noise message, without framing, and returns its validated payload. */
    EstablishPayload readMessage(byte[] msg) throws SecureCarrierException {
        ready(false);
        // A first message in Bound and no Binding here: refused, unread.
        if (noBinding && idx == 0) throw fail(new SecureCarrierException(SecureCarrierException.Kind.AUTH));
        int known = known(idx);
        try {
            // Application data in a message 1 without a key is refused, not skipped.
            int message = idx;
            boolean refuseAppData = message == 0 && !profile.isPskBased();
            EstablishPayload p = EstablishPayload.parse(readRaw(msg), known, maxAppData, refuseAppData);
            // A proof read carries the M it signs.
            return p.proof() == null ? p : p.withProof(p.proof().withMessage(Proof.message(message, hp)));
        } catch (SecureCarrierException x) {
            throw fail(x);
        }
    }

    private byte[] readRaw(byte[] msg) throws SecureCarrierException {
        if (msg.length > Sealer.MAX_MESSAGE) throw new SecureCarrierException(TOO_LARGE);
        // A short message is malformed before anything else happens to it.
        if (msg.length < minMessage()) throw new SecureCarrierException(MALFORMED, "message shorter than its pattern");
        int tag = profile.suite().tagLen();
        int at = 0;
        for (Token t : msgs[idx]) {
            switch (t) {
                case E -> {
                    re = Arrays.copyOfRange(msg, at, at + StaticKey.KEY_LEN);
                    at += StaticKey.KEY_LEN;
                    ss.mixHash(re);
                    if (profile.isPskBased()) ss.mixKey(re);
                }
                case S -> {
                    int n = StaticKey.KEY_LEN + (ss.cs.hasKey() ? tag : 0);
                    rs = ss.decryptAndHash(Arrays.copyOfRange(msg, at, at + n));
                    at += n;
                    if (MessageDigest.isEqual(rs, s.publicRaw())) throw new SecureCarrierException(PEER_IS_SELF);
                }
                case E1 -> {
                    int n = MlKem768.ENCAPSULATION_KEY_LEN + (ss.cs.hasKey() ? tag : 0);
                    // Judged at EKEM1, when this end writes.
                    re1 = ss.decryptAndHash(Arrays.copyOfRange(msg, at, at + n));
                    at += n;
                }
                case EKEM1 -> {
                    int n = MlKem768.CIPHERTEXT_LEN + (ss.cs.hasKey() ? tag : 0);
                    byte[] ct = ss.decryptAndHash(Arrays.copyOfRange(msg, at, at + n));
                    at += n;
                    MlKem768.DecapsulationKey dk = e1;
                    e1 = null;
                    byte[] secret = MlKem768.decapsulate(dk.key(), ct);
                    ss.mixKey(secret);
                    Arrays.fill(secret, (byte) 0);
                }
                case PSK -> ss.mixKeyAndHash(psk);
                default -> mixDh(t);
            }
        }
        hp = ss.h.clone();
        byte[] plain = ss.decryptAndHash(Arrays.copyOfRange(msg, at, msg.length));
        idx++;
        return plain;
    }

    /** The shortest valid form of the message about to be read. */
    private int minMessage() {
        return minLength(profile, idx);
    }

    /**
     * The shortest valid form of message {@code message} (0 to 2) of a Profile, its payload empty: each public key, KEM
     * encapsulation key and KEM ciphertext, a tag for each sent while there is a key, and the payload's tag.
     */
    static int minLength(Profile p, int message) {
        Token[][] msgs = pattern(p);
        int tag = p.suite().tagLen();
        boolean keyed = false;
        int n = 0;
        for (int m = 0; m <= message; m++) {
            n = 0;
            for (Token t : msgs[m]) {
                switch (t) {
                    case E -> {
                        n += StaticKey.KEY_LEN;
                        keyed |= p.isPskBased();
                    }
                    case S -> n += StaticKey.KEY_LEN + (keyed ? tag : 0);
                    case E1 -> n += MlKem768.ENCAPSULATION_KEY_LEN + (keyed ? tag : 0);
                    case EKEM1 -> {
                        n += MlKem768.CIPHERTEXT_LEN + (keyed ? tag : 0);
                        keyed = true;
                    }
                    case PSK, EE, ES, SE -> keyed = true;
                }
            }
            if (keyed) n += tag;
        }
        return n;
    }

    private void mixDh(Token t) throws SecureCarrierException {
        byte[] out = switch (t) {
            case EE -> e.dh(re);
            case ES -> initiator ? e.dh(rs) : s.dh(re);
            case SE -> initiator ? s.dh(re) : e.dh(rs);
            default -> throw new IllegalStateException();
        };
        ss.mixKey(out);
    }

    /** The static key the peer has shown so far, or null. */
    byte[] peerStatic() {
        return rs;
    }

    boolean complete() {
        return failure == null && idx == msgs.length;
    }

    /** Ends the handshake; it can succeed once. */
    Result finish() throws SecureCarrierException {
        if (failure != null) throw failure;
        if (!complete() || finished) throw new IllegalStateException("handshake is not complete");
        finished = true;
        dropKem();
        Suite suite = profile.suite();
        byte[] exporter = Kdf.expandLabel(ss.ck, "zmux-seal exporter", ss.h, suite.hashLen());
        Sealer sealer = null;
        Opener opener = null;
        if (mode == Mode.SEALED) {
            byte[][] split = ss.split();
            sealer = Sealer.create(suite, split[initiator ? 0 : 1], 0);
            opener = Opener.create(suite, split[initiator ? 1 : 0], 0);
        }
        return new Result(mode, suite, sealer, opener, exporter);
    }
}
