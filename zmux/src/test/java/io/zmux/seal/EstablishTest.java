package io.zmux.seal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.zmux.internal.join.Framing;
import io.zmux.internal.join.Mode;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The public client and server driven against each other with real randomness. */
class EstablishTest {
    private static final byte[] PSK = new byte[32];
    private static final byte[] HANDLE = new byte[16];

    private record Pair(EstablishClient client, EstablishServer server) {}

    private static Pair hello(Profile p, Mode m, Framing f, StaticKey cKey, StaticKey sKey) throws Exception {
        byte[] handle = p.isPskBased() ? HANDLE : null;
        byte[] psk = p.isPskBased() ? PSK : null;
        byte[] binding = m == Mode.BOUND ? new byte[32] : null;
        EstablishClient c = EstablishClient.create(p, m, f, handle, psk, binding, cKey);
        byte[] w = c.writeHello(EstablishPayload.padding(3, false));
        EstablishHello h = EstablishHello.parseExact(w, 0, w.length, f);
        EstablishServer s = EstablishServer.create(h, f, psk, binding, sKey);
        s.readHello();
        return new Pair(c, s);
    }

    @Test
    void everyProfileAndMode() throws Exception {
        StaticKey cKey = StaticKey.generate();
        StaticKey sKey = StaticKey.generate();
        Anchor pin = new PinAnchor(KeyId.of(KeyAlg.X25519, sKey.publicKey()));
        for (Profile p : Profile.values()) {
            for (Mode m : Mode.values()) {
                for (Framing f : Framing.values()) {
                    Pair x = hello(p, m, f, cKey, sKey);
                    byte[] first = p.isPskBased() ? new byte[] {9} : null;
                    byte[] reply = x.server().writeReply(new EstablishPayload(null, null, first, 0), null);
                    EstablishClient.Reply r = x.client().readReply(reply, 0, reply.length);
                    assertEquals(reply.length, r.consumed());
                    assertArrayEquals(sKey.publicKey(), r.peerStatic());
                    assertThrows(IllegalStateException.class, () -> x.client().writeFinish(new EstablishPayload(null, null, null, 0), null));
                    x.client().verify(pin, null, false);
                    byte[] finish = x.client().writeFinish(new EstablishPayload(null, null, new byte[] {7}, 0), null);
                    EstablishServer.Finish fin = x.server().readFinish(finish, 0, finish.length);
                    assertArrayEquals(cKey.publicKey(), fin.peerStatic());
                    Established a = x.client().finish();
                    Established b = x.server().finish();
                    assertEquals(m, a.mode());
                    assertArrayEquals(first, a.firstBytes());
                    assertArrayEquals(new byte[] {7}, b.firstBytes());
                    assertArrayEquals(a.export("x", null, 32), b.export("x", new byte[0], 32));
                    if (m == Mode.SEALED) {
                        byte[] ct = a.sealer().seal(new byte[] {5}, 0, 1);
                        assertArrayEquals(new byte[] {5}, b.opener().open(ct, 0, ct.length));
                    } else {
                        assertNull(a.sealer());
                        assertNull(b.opener());
                    }
                }
            }
        }
    }

    /** The Anchor's input as each end gives it, recorded. */
    private static final class Recording implements Anchor {
        VerifyInput seen;
        int calls;

        @Override public VerifyVerdict verify(VerifyInput in) {
            seen = in;
            calls++;
            return new VerifyVerdict(new Principal(Principal.NODE_KEY, in.peerStatic()));
        }
    }

    @Test
    void eachEndAsksItsAnchorOnceWithTheWholeInput() throws Exception {
        StaticKey cKey = StaticKey.generate(), sKey = StaticKey.generate();
        for (Profile p : Profile.values()) {
            Pair x = hello(p, Mode.SEALED, Framing.STREAM, cKey, sKey);
            Credential node = new Credential(7, new byte[] {1, 2}), client = new Credential(1L << 30, new byte[0]);
            byte[] reply = x.server().writeReply(new EstablishPayload(node, null, null, 0), null);
            assertEquals(node.type(), x.client().readReply(reply, 0, reply.length).credential().type());
            Recording c = new Recording();
            x.client().verify(c, "client context", false);
            assertEquals(1, c.calls);
            assertEquals(io.zmux.Role.SERVER, c.seen.role());
            assertEquals(p, c.seen.profile());
            assertArrayEquals(sKey.publicKey(), c.seen.peerStatic());
            assertArrayEquals(p.isPskBased() ? HANDLE : null, c.seen.pskIdentity());
            assertEquals(7, c.seen.credential().type());
            assertArrayEquals(new byte[] {1, 2}, c.seen.credential().data());
            assertEquals("client context", c.seen.localContext());
            byte[] finish = x.client().writeFinish(new EstablishPayload(client, null, null, 0), null);
            EstablishServer.Finish fin = x.server().readFinish(finish, 0, finish.length);
            Recording s = new Recording();
            x.server().verify(s, fin, "listener", false);
            assertEquals(io.zmux.Role.CLIENT, s.seen.role());
            assertArrayEquals(cKey.publicKey(), s.seen.peerStatic());
            assertEquals(1L << 30, s.seen.credential().type());
            assertEquals(0, s.seen.credential().data().length);
            assertEquals("listener", s.seen.localContext());
        }
    }

    private static Pair replied(StaticKey sKey, Credential cred) throws Exception {
        Pair x = hello(Profile.XXHFS_PSK0_AES, Mode.SEALED, Framing.STREAM, StaticKey.generate(), sKey);
        byte[] reply = x.server().writeReply(new EstablishPayload(cred, null, null, 0), null);
        x.client().readReply(reply, 0, reply.length);
        return x;
    }

    @Test
    void noCredentialIsOneCallWithNone() throws Exception {
        Recording a = new Recording();
        replied(StaticKey.generate(), null).client().verify(a, null, false);
        assertEquals(1, a.calls);
        assertNull(a.seen.credential());
    }

    @Test
    void aRejectionAndAnUnavailableVerifierAreTold() throws Exception {
        StaticKey sKey = StaticKey.generate();
        assertThrows(CredentialsFailedException.class, () -> replied(sKey, null).client().verify(in -> {
            throw new CredentialsFailedException("no");
        }, null, false));
        // An error of the verifier, a panic, is a rejection too: only VerifierUnavailableException is temporary.
        var panic = assertThrows(CredentialsFailedException.class, () -> replied(sKey, null).client().verify(in -> {
            throw new AssertionError("panic");
        }, null, false));
        assertInstanceOf(AssertionError.class, panic.getCause());
        assertThrows(VerifierUnavailableException.class, () -> replied(sKey, null).client().verify(in -> {
            throw new VerifierUnavailableException("store down", null);
        }, null, false));
        var bug = assertThrows(CredentialsFailedException.class, () -> replied(sKey, null).client().verify(in -> {
            throw new IllegalStateException("bug");
        }, null, false));
        assertInstanceOf(IllegalStateException.class, bug.getCause());
        assertThrows(CredentialsFailedException.class, () -> replied(sKey, null).client().verify(in -> null, null, false));
        assertThrows(CredentialsFailedException.class, () -> replied(StaticKey.generate(), null).client()
                .verify(new PinAnchor(KeyId.of(KeyAlg.X25519, sKey.publicKey())), null, false));
        // No static key to pin: a rejection, not a failure of the verifier.
        var none = new VerifyInput(io.zmux.Role.SERVER, Profile.of(1), null, null, null, List.of(), null);
        assertThrows(CredentialsFailedException.class, () -> new PinAnchor(KeyId.of(KeyAlg.X25519, sKey.publicKey())).verify(none));
    }

    @Test
    void theVerdictValueComesBackAndAProofKeyIsRequired() throws Exception {
        StaticKey sKey = StaticKey.generate();
        Object mine = new Object();
        var j = replied(sKey, null).client().verify(in -> new VerifyVerdict(new Principal(5, new byte[] {1}), null, mine), null, false);
        assertEquals(mine, j.verdict().value());
        // A Reply without a proof: a verdict that names a proof key fails the handshake.
        assertThrows(CredentialsFailedException.class, () -> replied(sKey, null).client()
                .verify(in -> new VerifyVerdict(new Principal(5, new byte[0]), new byte[KeyId.LEN], null), null, false));
        assertThrows(IllegalArgumentException.class, () -> new VerifyVerdict(new Principal(5, new byte[0]), new byte[3], null));
    }

    @Test
    void requiredPqAuthenticationFailsWithoutAProof() throws Exception {
        StaticKey sKey = StaticKey.generate();
        var j = replied(sKey, null).client().verify(new PinAnchor(KeyId.of(KeyAlg.X25519, sKey.publicKey())), null, false);
        assertEquals(false, j.pqAuthenticated());
        assertEquals(Principal.NODE_KEY, j.verdict().principal().type());
        assertArrayEquals(KeyId.of(KeyAlg.X25519, sKey.publicKey()), j.verdict().principal().bytes());
        assertThrows(CredentialsFailedException.class, () -> replied(sKey, null).client()
                .verify(new PinAnchor(KeyId.of(KeyAlg.X25519, sKey.publicKey())), null, true));
    }

    @Test
    void applicationDataItems() throws Exception {
        int k = EstablishPayload.KNOWN_APP_DATA;
        assertArrayEquals(new byte[] {5}, EstablishPayload.parse(new byte[] {2, 1, 5}, k, 1, false).appData());
        // Above the local bound, a second item, and any item in an unencrypted message 1.
        assertEquals(SecureCarrierException.Code.TOO_LARGE, assertThrows(SecureCarrierException.class,
                () -> EstablishPayload.parse(new byte[] {2, 2, 5, 5}, k, 1, false)).code());
        assertEquals(SecureCarrierException.Code.TLV_ORDER, assertThrows(SecureCarrierException.class,
                () -> EstablishPayload.parse(new byte[] {2, 1, 5, 2, 1, 6}, k, 256, false)).code());
        assertEquals(SecureCarrierException.Code.MALFORMED, assertThrows(SecureCarrierException.class,
                () -> EstablishPayload.parse(new byte[] {2, 0}, 0, 256, true)).code());
        // Unknown in a message that does not know it: an even type, skipped.
        assertNull(EstablishPayload.parse(new byte[] {2, 1, 5}, 0, 256, false).appData());
        // CredType 0 is no credential.
        assertEquals(SecureCarrierException.Code.TLV_CREDENTIALS, assertThrows(SecureCarrierException.class,
                () -> EstablishPayload.parse(new byte[] {1, 2, 0, 9}, EstablishPayload.KNOWN_CREDENTIAL, 256, false)).code());
    }

    private static final SigningKey SERVER_PROOF = SigningKey.generate(KeyAlg.ML_DSA_44), CLIENT_PROOF = SigningKey.generate(KeyAlg.ML_DSA_44);

    /** A verdict that names proofKey. */
    private static Anchor naming(byte[] proofKey) {
        return in -> new VerifyVerdict(new Principal(Principal.NODE_KEY, in.peerStatic()), proofKey, null);
    }

    /** Messages 1 and 2, message 2 signed by signer, or with p as it is. */
    private static Pair signedReply(Profile p, Signer signer, Proof raw) throws Exception {
        Pair x = hello(p, Mode.SEALED, Framing.STREAM, StaticKey.generate(), StaticKey.generate());
        EstablishPayload pl = new EstablishPayload(null, null, null, 0);
        byte[] reply = signer != null ? x.server().writeReply(pl.withProof(Proof.placeholder(signer)), signer)
                : x.server().writeReply(pl.withProof(raw), null) ;
        x.client().readReply(reply, 0, reply.length);
        return x;
    }

    @Test
    void bothEndsProveAndArePqAuthenticated() throws Exception {
        for (Profile p : Profile.values()) {
            Pair x = signedReply(p, SERVER_PROOF, null);
            var server = x.client().verify(naming(SERVER_PROOF.keyId()), null, true);
            assertTrue(server.pqAuthenticated());
            byte[] finish = x.client().writeFinish(new EstablishPayload(null, null, null, 0).withProof(Proof.placeholder(CLIENT_PROOF)), CLIENT_PROOF);
            EstablishServer.Finish fin = x.server().readFinish(finish, 0, finish.length);
            assertTrue(x.server().verify(naming(CLIENT_PROOF.keyId()), fin, null, true).pqAuthenticated());
            // A verdict without a proof key does not look at the proof: no PQ authentication.
            assertEquals(false, x.server().verify(naming(null), fin, null, false).pqAuthenticated());
            assertNotNull(x.client().finish());
        }
    }

    @Test
    void aProofTheVerdictRequiresMustVerify() throws Exception {
        Profile p = Profile.XXHFS_PSK0_AES;
        // No proof, another key, a key of a KeyAlg that is no proof key's, a wrong signature.
        assertThrows(CredentialsFailedException.class, () -> signedReply(p, null, null).client().verify(naming(SERVER_PROOF.keyId()), null, false));
        assertThrows(CredentialsFailedException.class, () -> signedReply(p, CLIENT_PROOF, null).client().verify(naming(SERVER_PROOF.keyId()), null, false));
        byte[] x25519 = new byte[32];
        Proof wrongAlg = new Proof(KeyAlg.X25519.id(), x25519, new byte[KeyAlg.ML_DSA_44.signatureLength()], null);
        assertThrows(CredentialsFailedException.class, () -> signedReply(p, null, wrongAlg).client().verify(naming(KeyId.of(KeyAlg.X25519, x25519)), null, false));
        Proof unsigned = Proof.placeholder(SERVER_PROOF);
        assertThrows(CredentialsFailedException.class, () -> signedReply(p, null, unsigned).client().verify(naming(SERVER_PROOF.keyId()), null, false));
        // PQ authentication required, and a verdict without a proof key.
        assertThrows(CredentialsFailedException.class, () -> signedReply(p, SERVER_PROOF, null).client().verify(naming(null), null, true));
        // A signer with a placeholder of another length is the host's mistake.
        Pair y = hello(p, Mode.SEALED, Framing.STREAM, StaticKey.generate(), StaticKey.generate());
        Signer shortSig = new Signer() {
            @Override public KeyAlg alg() { return KeyAlg.ML_DSA_44; }
            @Override public byte[] publicKey() { return SERVER_PROOF.publicKey(); }
            @Override public byte[] signProof(byte[] m) { return new byte[1]; }
        };
        assertEquals(SecureCarrierException.Code.INVALID_CONFIG, assertThrows(SecureCarrierException.class,
                () -> y.server().writeReply(new EstablishPayload(null, null, null, 0).withProof(Proof.placeholder(shortSig)), shortSig)).code());
    }

    @Test
    void theProofStructure() throws Exception {
        Proof pr = Proof.placeholder(SERVER_PROOF);
        Proof back = Proof.parse(pr.encode());
        assertEquals(3, back.alg());
        assertArrayEquals(SERVER_PROOF.publicKey(), back.pub());
        assertEquals(KeyAlg.ML_DSA_44.signatureLength(), back.sig().length);
        assertEquals(SecureCarrierException.Code.TLV_NON_MINIMAL, assertThrows(SecureCarrierException.class, () -> Proof.parse(new byte[] {0x40, 3, 0})).code());
        assertEquals(SecureCarrierException.Code.TLV_NON_MINIMAL, assertThrows(SecureCarrierException.class, () -> Proof.parse(new byte[] {3, 0x40, 0})).code());
        assertEquals(SecureCarrierException.Code.TLV_TRUNCATED, assertThrows(SecureCarrierException.class, () -> Proof.parse(new byte[] {3, 5, 1, 2})).code());
        assertEquals(SecureCarrierException.Code.TLV_TRUNCATED, assertThrows(SecureCarrierException.class, () -> Proof.parse(new byte[0])).code());
        assertArrayEquals("zmux-seal proof r\0h".getBytes(java.nio.charset.StandardCharsets.US_ASCII), Proof.message(1, new byte[] {'h'}));
        assertArrayEquals("zmux-seal proof i\0h".getBytes(java.nio.charset.StandardCharsets.US_ASCII), Proof.message(2, new byte[] {'h'}));
    }

    @Test
    void requirePqAuthRefusesAPinWithoutAProofKey() throws Exception {
        var pin = new PinAnchor(new byte[KeyId.LEN]);
        assertThrows(SecureCarrierException.class, () -> PinAnchor.checkPqAuth(pin, true));
        PinAnchor.checkPqAuth(pin, false);
        PinAnchor.checkPqAuth(new PinAnchor(new byte[KeyId.LEN], new byte[KeyId.LEN]), true);
        Gate gate = new Gate() {
            @Override public byte[] resolve(byte[] h, java.net.SocketAddress src) { return null; }
            @Override public Credential credential(byte[] h, Profile p, java.net.SocketAddress src) { return null; }
            @Override public Object commit(io.zmux.seal.CommitRequest a, java.net.SocketAddress src) { return null; }
        };
        var cfg = ServerConfig.builder(gate, StaticKey.generate()).build();
        assertThrows(IllegalArgumentException.class, () -> cfg.toBuilder().anchor(pin).requirePqAuth(true).build());
        assertNotNull(cfg.toBuilder().anchor(new PinAnchor(new byte[KeyId.LEN], new byte[KeyId.LEN])).requirePqAuth(true).build());
    }

    @Test
    void aMessageCarriesOneCredential() throws Exception {
        // Types strictly increase: a second credential item is out of order.
        byte[] two = {1, 2, 7, 1, 1, 2, 7, 2};
        assertEquals(SecureCarrierException.Code.TLV_ORDER, assertThrows(SecureCarrierException.class,
                () -> EstablishPayload.parse(two, EstablishPayload.KNOWN_CREDENTIAL, 256, false)).code());
        assertEquals(SecureCarrierException.Code.TLV_CREDENTIALS, assertThrows(SecureCarrierException.class,
                () -> EstablishPayload.parse(new byte[] {1, 0}, EstablishPayload.KNOWN_CREDENTIAL, 256, false)).code());
        assertEquals(SecureCarrierException.Code.TLV_NON_MINIMAL, assertThrows(SecureCarrierException.class,
                () -> EstablishPayload.parse(new byte[] {1, 2, 0x40, 7}, EstablishPayload.KNOWN_CREDENTIAL, 256, false)).code());
        Credential c = EstablishPayload.parse(new byte[] {1, 3, 7, 9, 9}, EstablishPayload.KNOWN_CREDENTIAL, 256, false).credential();
        assertEquals(7, c.type());
        assertArrayEquals(new byte[] {9, 9}, c.data());
        assertThrows(SecureCarrierException.class, () -> EstablishPayload.encode(new Credential(Anchor.CRED_TYPE_NONE, new byte[1])));
    }

    @Test
    void serverConfigIsValidatedWhenMade() throws Exception {
        Gate gate = new Gate() {
            @Override public byte[] resolve(byte[] h, java.net.SocketAddress src) { return null; }
            @Override public Credential credential(byte[] h, Profile p, java.net.SocketAddress src) { return null; }
            @Override public Object commit(io.zmux.seal.CommitRequest a, java.net.SocketAddress src) { return null; }
        };
        StaticKey key = StaticKey.generate();
        assertThrows(IllegalArgumentException.class, () -> ServerConfig.builder(null, key).build());
        assertThrows(IllegalArgumentException.class, () -> ServerConfig.builder(gate, null).build());
        ServerConfig ok = ServerConfig.builder(gate, key).build();
        assertEquals(ServerConfig.DEFAULT_PROFILES, ok.acceptedProfiles());
        assertTrue(ok.acceptedProfiles().stream().allMatch(Profile::isPskBased));
        assertThrows(IllegalArgumentException.class, () -> ok.toBuilder().acceptedProfiles(Profile.XXHFS_AES, Profile.of(6)).build());
        assertEquals(List.of(Profile.XXHFS_AES), ok.toBuilder().acceptedProfiles(Profile.XXHFS_AES).build().acceptedProfiles());
        assertThrows(IllegalArgumentException.class, () -> ok.toBuilder().anchor(null).requirePqAuth(true).build());
        assertEquals(256, ok.maxAppData().getAsInt());
        assertTrue(ok.toBuilder().noMaxAppData().build().maxAppData().isEmpty());
        assertThrows(IllegalArgumentException.class, () -> ok.toBuilder().maxAppData(-1));
    }

    @Test
    void clientConfigSettingsThatAreNotValues() {
        ClientConfig d = ClientConfig.builder().build();
        assertEquals(256, d.maxAppData().getAsInt());
        assertEquals(java.time.Duration.ofSeconds(10), d.handshakeTimeout().orElseThrow());
        ClientConfig none = ClientConfig.builder().noMaxAppData().noHandshakeTimeout().build();
        assertTrue(none.maxAppData().isEmpty());
        assertTrue(none.handshakeTimeout().isEmpty());
        assertEquals(0, ClientConfig.builder().maxAppData(0).build().maxAppData().getAsInt());
        assertThrows(IllegalArgumentException.class, () -> ClientConfig.builder().maxAppData(-1));
        assertThrows(IllegalArgumentException.class, () -> ClientConfig.builder().handshakeTimeout(java.time.Duration.ZERO));
    }

    @Test
    void byteFieldsAreCopiedComparedByContentAndKeysStayHidden() throws Exception {
        byte[] psk = new byte[32];
        java.util.Arrays.fill(psk, (byte) 0x5a);
        HandlePsk h = new HandlePsk(new byte[] {1, 2}, psk);
        psk[0] = 0;
        assertEquals(0x5a, h.psk()[0]);
        h.psk()[1] = 0;
        assertEquals(0x5a, h.psk()[1]);
        assertEquals(h, new HandlePsk(new byte[] {1, 2}, h.psk()));
        assertFalse(h.toString().contains("5a5a"));
        assertEquals(new Credential(3, new byte[] {9}), new Credential(3, new byte[] {9}));
        assertEquals(new Principal(1, new byte[] {7}).hashCode(), new Principal(1, new byte[] {7}).hashCode());
        StaticKey k = StaticKey.generate();
        assertFalse(k.toString().contains(HexFormat.of().formatHex(k.scalar())));
    }

    @Test
    void pskOnlyNeedsAPsk() throws Exception {
        StaticKey sKey = StaticKey.generate();
        Pair x = hello(Profile.XXHFS_AES, Mode.SEALED, Framing.STREAM, StaticKey.generate(), sKey);
        byte[] reply = x.server().writeReply(new EstablishPayload(null, null, null, 0), null);
        x.client().readReply(reply, 0, reply.length);
        assertEquals(SecureCarrierException.Code.INVALID_CONFIG, assertThrows(SecureCarrierException.class, () -> x.client().trustPskOnly(null)).code());
    }

    @Test
    void x25519FollowsRfc7748() throws Exception {
        // RFC 7748 Section 5.2, first vector; its u has the high bit set, which is ignored.
        HexFormat h = HexFormat.of();
        StaticKey k = StaticKey.fromScalar(h.parseHex("a546e36bf0527c9d3b16154b82465edd62144c0ac1fc5a18506a2244ba449ac4"));
        byte[] out = k.dh(h.parseHex("e6db6867583030db3594c1a424b15f7c726624ec26b3353b10a903a6d0ab1c4c"));
        assertEquals("c3da55379de9c6908e94ea4df28d084f32eccf03491c71f754b4075577a28552", h.formatHex(out));
        // A key above 2^255-19 is reduced: the encoding of p + 9 means u = 9.
        byte[] pPlus9 = h.parseHex("f6ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f");
        assertArrayEquals(k.publicKey(), k.dh(pPlus9));
    }

    @Test
    void incompleteIsControlFlowWithoutAStackTrace() {
        SecureCarrierException more = SecureCarrierException.incomplete(7);
        assertEquals(0, more.getStackTrace().length);
        assertEquals(7, more.need());
        assertInstanceOf(io.zmux.ZmuxException.class, more);
        assertTrue(new SecureCarrierException(SecureCarrierException.Code.AUTH).getStackTrace().length > 0);
    }
}
