package io.zmux.seal;

import io.zmux.internal.join.Framing;
import io.zmux.internal.join.Frames;
import io.zmux.carrier.SecureCarrierException;
import java.util.List;

/**
 * The server's side of Establish without IO: {@link #create} from a parsed Hello and the key its
 * Handle resolved to, {@link #readHello}, {@link #writeReply}, {@link #readFinish}, {@link #finish}.
 * No DH runs and nothing is written before message 1 authenticates. Not thread-safe.
 */
final class EstablishServer {
    /** What message 3 shows; appData is the client's of messages 1 and 3. */
    public record Finish(byte[] peerStatic, Credential credential, List<AppData> appData, int consumed) {}

    private final Handshake hs;
    private final EstablishHello hello;
    private final Framing framing;
    private byte[] peerFirstBytes;
    /** The client's application data of message 1. */
    private List<AppData> early = List.of();
    /** The proof of message 3, or null. */
    private Proof proof;

    private EstablishServer(Handshake hs, EstablishHello hello, Framing f) {
        this.hs = hs;
        this.hello = hello;
        this.framing = f;
    }

    /**
     * Starts a server. {@code psk} is the key {@link Gate#resolve} found. {@code binding} is what
     * the local carrier has: it is not judged against the Mode, any disagreement being AUTH.
     */
    public static EstablishServer create(EstablishHello h, Framing f, byte[] psk, byte[] binding, StaticKey key)
            throws SecureCarrierException {
        return create(h, f, psk, binding, key, StaticKey::generate);
    }

    /** With a given source of ephemeral keys, for vectors only. */
    static EstablishServer create(EstablishHello h, Framing f, byte[] psk, byte[] binding, StaticKey key, Handshake.EphemeralSource source)
            throws SecureCarrierException {
        Handshake hs = new Handshake(h.profile(), h.mode(), false, h.handle(), psk, binding, key, source);
        return new EstablishServer(hs, h, f);
    }

    /** Bounds the application data of messages 1 and 3; 0 is the default. */
    public EstablishServer maxAppData(int n) {
        hs.maxAppData(n);
        return this;
    }

    /** Reads message 1. */
    public void readHello() throws SecureCarrierException {
        early = AppData.of(1, hs.readMessage(hello.message()).appData());
    }

    /** What is known of the client before message 2: its PSK identity and its data of message 1. */
    public AppDataSource.Known known(Object localContext) {
        Profile p = hello.profile();
        return new AppDataSource.Known(p, new VerifyInput(VerifyInput.Role.CLIENT, p, null, p.hasPsk() ? hello.handle() : null, null, early, localContext), null);
    }

    /**
     * Message 2 carrying p, framed; first bytes only on a Profile with a pre-shared key. With a signer, p carries {@link Proof#placeholder} of it and the
     * signature over the transcript is made here.
     */
    byte[] writeReply(EstablishPayload p, Signer signer) throws SecureCarrierException {
        byte[] msg = hs.writeMessage(p, signer == null ? null : Handshake.ProofSigner.of(signer));
        return Frames.frame(framing, msg);
    }

    /** Takes the bytes that begin with message 3. INCOMPLETE leaves the state as it was. */
    public Finish readFinish(byte[] b, int off, int len) throws SecureCarrierException {
        Frames.Unit u = Frames.cut(b, off, len, framing);
        EstablishPayload p = hs.readMessage(u.unit());
        peerFirstBytes = p.firstBytes();
        proof = p.proof();
        return new Finish(hs.peerStatic().clone(), p.credential(), AppData.concat(early, AppData.of(3, p.appData())), u.consumed());
    }

    /**
     * Judges the client after message 3: the anchor is asked once, with the credential of
     * message 3 or none, then the proof of message 3 against its verdict.
     */
    public Verification.Judged verify(Anchor anchor, Finish finish, Object localContext, boolean requirePqAuth)
            throws CredentialsFailedException, VerifierUnavailableException {
        return Verification.judge(anchor, new VerifyInput(VerifyInput.Role.CLIENT, hello.profile(), finish.peerStatic(),
                hello.profile().hasPsk() ? hello.handle() : null, finish.credential(), finish.appData(), localContext), proof, requirePqAuth);
    }

    /** The result, after {@link #readFinish}. */
    public Established finish() throws SecureCarrierException {
        return new Established(hello.profile(), hs.finish(), hs.peerStatic().clone(), peerFirstBytes);
    }
}
