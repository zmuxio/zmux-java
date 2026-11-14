package io.zmux.seal;

import static io.zmux.seal.SecureCarrierException.Code.INVALID_CONFIG;

import io.zmux.internal.join.Framing;
import io.zmux.internal.join.Frames;
import io.zmux.internal.join.Mode;
import java.util.List;

/**
 * The client's side of Establish without IO: {@link #writeHello}, {@link #readReply}, then
 * {@link #verify} or {@link #trustPskOnly}, {@link #writeFinish}, {@link #finish}. Nothing can be
 * written after the Reply until the server is judged. Not thread-safe.
 */
final class EstablishClient {
    /** What message 2 shows. Carrier first bytes are not here: they wait until the server is judged. */
    public record Reply(byte[] peerStatic, Credential credential, List<AppData> appData, int consumed) {}

    private final Handshake hs;
    private final Profile profile;
    private final Mode mode;
    private final Framing framing;
    private final byte[] handle;
    private Credential credential;
    /** The proof of message 2, or null. */
    private Proof proof;
    private List<AppData> peerAppData = List.of();
    /** What is known of the server once judged, or trusted for its pre-shared key. */
    private VerifyInput known;
    private boolean replied;
    private byte[] peerFirstBytes;
    private Verification.Judged judgedPeer;
    private boolean judged;

    private EstablishClient(Handshake hs, Profile p, Mode m, Framing f, byte[] handle) {
        this.hs = hs;
        this.profile = p;
        this.mode = m;
        this.framing = f;
        this.handle = handle;
    }

    /**
     * Starts a client. {@code handle} and {@code psk} are present exactly for a Profile with a
     * pre-shared key, and {@code binding} exactly in Bound; anything else is INVALID_CONFIG.
     */
    public static EstablishClient create(Profile p, Mode m, Framing f, byte[] handle, byte[] psk, byte[] binding, StaticKey key)
            throws SecureCarrierException {
        return create(p, m, f, handle, psk, binding, key, StaticKey::random);
    }

    /** Bounds the application data of message 2, in bytes; {@link Long#MAX_VALUE}: none. */
    EstablishClient maxAppData(long n) {
        hs.maxAppData(n);
        return this;
    }

    /** With a given source of ephemeral keys, for vectors only. */
    static EstablishClient create(
            Profile p, Mode m, Framing f, byte[] handle, byte[] psk, byte[] binding, StaticKey key, Handshake.EphemeralSource source)
            throws SecureCarrierException {
        Handshake hs = new Handshake(p, m, true, handle, psk, binding, key, source);
        return new EstablishClient(hs, p, m, f, handle);
    }

    /** The Hello carrying p; its padding item, when the field is not zero, is random unless the message has a key. */
    byte[] writeHello(EstablishPayload p) throws SecureCarrierException {
        byte[] msg = hs.writeMessage(p.withPadding(p.padding(), !profile.isPskBased()));
        return EstablishHello.encode(profile, mode, handle, msg, framing);
    }

    /** Takes the bytes that begin with the Reply. INCOMPLETE leaves the state as it was. */
    public Reply readReply(byte[] b, int off, int len) throws SecureCarrierException {
        Frames.Unit u = Frames.cut(b, off, len, framing);
        EstablishPayload p = hs.readMessage(u.unit());
        credential = p.credential();
        peerAppData = AppData.of(2, p.appData());
        proof = p.proof();
        replied = true;
        peerFirstBytes = p.firstBytes();
        return new Reply(hs.peerStatic().clone(), credential, peerAppData, u.consumed());
    }

    /**
     * Judges the server: the anchor is asked once, with the credential of message 2 or none.
     * {@code localContext} is handed to it unread.
     */
    public Verification.Judged verify(Anchor anchor, Object localContext, boolean requirePqAuth)
            throws CredentialsFailedException, VerifierUnavailableException {
        if (!replied) throw new IllegalStateException("no Reply yet");
        VerifyInput in = input(localContext);
        judgedPeer = Verification.judge(anchor, in, proof, requirePqAuth);
        known = in;
        judged = true;
        return judgedPeer;
    }

    /** Accepts any server that knows the pre-shared key: an explicit bootstrap, instead of {@link #verify}. */
    public void trustPskOnly(Object localContext) throws SecureCarrierException {
        if (!replied) throw new IllegalStateException("no Reply yet");
        if (!profile.isPskBased()) throw new SecureCarrierException(INVALID_CONFIG, "no pre-shared key");
        known = input(localContext);
        judged = true;
    }

    private VerifyInput input(Object localContext) {
        return new VerifyInput(io.zmux.Role.SERVER, profile, hs.peerStatic(), profile.isPskBased() ? handle : null, credential, peerAppData, localContext);
    }

    /** What is known of the server when message 3 is written: what the anchor judged, and its verdict. */
    public AppDataSource.Known known() {
        if (!judged) throw new IllegalStateException("the server has not been judged");
        return new AppDataSource.Known(profile, known, judgedPeer == null ? null : judgedPeer.verdict());
    }

    /** The server's application data; authenticated once {@link #verify} passed. */
    public List<AppData> peerAppData() {
        return judgedPeer == null ? peerAppData : AppData.verified(peerAppData);
    }

    /** What {@link #verify} accepted, or null. */
    public Verification.Judged judged() {
        return judgedPeer;
    }

    /**
     * Message 3 carrying p, framed. With a signer, p carries {@link Proof#placeholder} of it and the
     * signature over the transcript is made here.
     */
    byte[] writeFinish(EstablishPayload p, Signer signer) throws SecureCarrierException {
        if (!judged) throw new IllegalStateException("the server has not been judged");
        byte[] msg = hs.writeMessage(p, signer == null ? null : Handshake.ProofSigner.of(signer));
        return Frames.frame(framing, msg);
    }

    /** The result, after {@link #writeFinish}. */
    public Established finish() throws SecureCarrierException {
        return new Established(profile, hs.finish(), hs.peerStatic().clone(), peerFirstBytes);
    }
}
