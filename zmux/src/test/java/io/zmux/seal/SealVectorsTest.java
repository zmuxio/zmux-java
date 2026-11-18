package io.zmux.seal;

import static io.zmux.internal.join.VectorFiles.errorClass;
import static io.zmux.internal.join.VectorFiles.framing;
import static io.zmux.internal.join.VectorFiles.hex;
import static io.zmux.internal.join.VectorFiles.hexOrNull;
import static io.zmux.internal.join.VectorFiles.items;
import static io.zmux.internal.join.VectorFiles.longs;
import static io.zmux.internal.join.VectorFiles.num;
import static io.zmux.internal.join.VectorFiles.str;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.zmux.internal.join.Framing;
import io.zmux.internal.join.Frames;
import io.zmux.internal.join.Hello;
import io.zmux.internal.join.Joiner;
import io.zmux.internal.join.Kdf;
import io.zmux.internal.join.SessionCrypto;
import io.zmux.internal.join.Mode;
import io.zmux.internal.join.Result;
import io.zmux.internal.join.Suite;
import io.zmux.internal.join.VectorFiles;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** handshake, negative, hello, config, credential and derive, and the Establish list of receiver. */
class SealVectorsTest {
    private static void count(String file, int n, int expected) {
        System.out.println("vectors " + file + ": " + n);
        assertEquals(expected, n, file);
    }

    private static Handshake side(Map<String, Object> v, boolean initiator, Profile p, Mode m, byte[] handle) throws SecureCarrierException {
        String who = initiator ? "initiator" : "responder";
        StaticKey eph = StaticKey.fromScalar(hex(v, who + "_ephemeral"));
        // The KEM's randomness: the initiator's key pair seed (d, z), the responder's encapsulation m.
        byte[] kem = hexOrNull(v, initiator ? "initiator_kem_seed" : "responder_encaps_m");
        Handshake hs = new Handshake(p, m, initiator, handle, hexOrNull(v, "psk"), hexOrNull(v, "binding"),
                StaticKey.fromScalar(hex(v, who + "_static")), () -> eph, kem == null ? null : new FixedRandom(kem));
        hs.maxAppData(ClientConfig.appDataLimit(configured(v).maxAppDataBound()));
        return hs;
    }

    /** The vector's local bound on received application data: positive bytes, 0 (or absent) the default 256, negative none. */
    static int maxAppData(Map<String, Object> v) {
        Object n = v.get("max_app_data");
        return n == null ? 0 : Math.toIntExact(((Number) n).longValue());
    }

    /** The configuration a host writes for the vector's bound: nothing for the default, noMaxAppData() for none, else the value. */
    static ClientConfig configured(Map<String, Object> v) {
        int n = maxAppData(v);
        ClientConfig.Builder b = ClientConfig.builder();
        if (n < 0) b.noMaxAppData();
        else if (n > 0) b.maxAppData(n);
        return b.build();
    }

    private static Handshake side(Map<String, Object> v, boolean initiator) throws SecureCarrierException {
        return side(v, initiator, Profile.of(num(v, "profile")), Mode.ofWire((int) num(v, "mode")), hexOrNull(v, "handle"));
    }

    @Test
    void handshake() throws Exception {
        List<Map<String, Object>> all = VectorFiles.list("handshake.json");
        assertEquals(18, handshakes(all));
        count("handshake.json", all.size(), 35);
        assertEquals(java.util.Set.of(-1, 0, 65466), all.stream().filter(v -> v.containsKey("max_app_data")).map(SealVectorsTest::maxAppData)
                .collect(java.util.stream.Collectors.toSet()), "every kind of bound is exercised");
        for (var v : all) {
            int n = maxAppData(v);
            java.util.OptionalInt bound = configured(v).maxAppData();
            if (n < 0) assertTrue(bound.isEmpty(), "noMaxAppData()");
            else assertEquals(n == 0 ? 256 : n, bound.getAsInt(), "the default or the value");
        }
    }

    private static final int ALL_TYPES = EstablishPayload.KNOWN_CREDENTIAL | EstablishPayload.KNOWN_FIRST_BYTES
            | EstablishPayload.KNOWN_APP_DATA | EstablishPayload.KNOWN_PROOF;

    /** The signer of a vector: the key from its seed, signing deterministically. */
    private static Signer deterministic(byte[] seed) throws SecureCarrierException {
        SigningKey k = SigningKey.fromSeed(KeyAlg.ML_DSA_44, seed);
        return new Signer() {
            @Override public KeyAlg alg() { return k.alg(); }
            @Override public byte[] publicKey() { return k.publicKey(); }
            @Override public byte[] signProof(byte[] m) throws SecureCarrierException { return k.signDeterministic(m); }
        };
    }

    /** A verifier as a vector's verify_server or verify_client describes it. */
    private static Anchor anchor(Map<String, Object> want) {
        byte[] pin = hexOrNull(want, "pin_id");
        if (pin != null) return new PinAnchor(pin, hexOrNull(want, "pin_proof_id"));
        byte[] proofKey = hexOrNull(want, "proof_key");
        return in -> new VerifyVerdict(new Principal(Principal.NODE_KEY, in.peerStatic()), proofKey, null);
    }

    /** Judges the peer of message got as a vector's verify_server or verify_client says, and checks the PQ fact. */
    private static void judged(Map<String, Object> want, Handshake rd, Profile p, byte[] handle, EstablishPayload got, boolean server, String name)
            throws Exception {
        if (want == null) return;
        VerifyInput in = new VerifyInput(server ? io.zmux.Role.SERVER : io.zmux.Role.CLIENT, p, rd.peerStatic(), handle, got.credential(), List.of(), null);
        var j = Verification.judge(anchor(want), in, got.proof(), Boolean.TRUE.equals(want.get("require_pq_auth")));
        assertEquals(want.get("pq_authenticated"), j.pqAuthenticated(), name);
    }

    /** Runs the handshake vectors; returns how many carry a node credential. */
    private static int handshakes(List<Map<String, Object>> all) throws Exception {
        int credentialed = 0;
        for (Map<String, Object> v : all) {
            String name = str(v, "name");
            Profile p = Profile.of(num(v, "profile"));
            Mode m = Mode.ofWire((int) num(v, "mode"));
            assertEquals(str(v, "noise_name"), p.noiseName(), name);
            byte[] handle = hexOrNull(v, "handle");
            assertArrayEquals(hex(v, "prologue"), Handshake.prologue(p, m, handle, hexOrNull(v, "binding")), name);
            Handshake i = side(v, true);
            Handshake r = side(v, false);
            List<Map<String, Object>> msgs = items(v, "messages");
            assertEquals(3, msgs.size(), name);
            for (int idx = 0; idx < msgs.size(); idx++) {
                Map<String, Object> msg = msgs.get(idx);
                Handshake w = idx % 2 == 0 ? i : r;
                Handshake rd = idx % 2 == 0 ? r : i;
                assertEquals(idx % 2 == 0 ? "initiator" : "responder", msg.get("sender"), name);
                byte[] payload = hex(msg, "payload");
                byte[] seed = hexOrNull(msg, "proof_seed");
                byte[] noise;
                if (seed != null) {
                    // The proof is made here, through the signer, from h_p of this state.
                    EstablishPayload pl = EstablishPayload.parse(payload, ALL_TYPES, Long.MAX_VALUE, false);
                    Signer signer = deterministic(seed);
                    noise = w.writeMessage(pl.withProof(Proof.placeholder(signer)), Handshake.ProofSigner.of(signer));
                } else {
                    noise = w.writeRaw(payload);
                }
                assertArrayEquals(hex(msg, "noise"), noise, name + " message " + idx);
                assertArrayEquals(hex(msg, "h"), w.ss.h, name);
                assertArrayEquals(hex(msg, "ck"), w.ss.ck, name);
                EstablishPayload got = rd.readMessage(noise);
                assertArrayEquals(hex(msg, "h"), rd.ss.h, name);
                @SuppressWarnings("unchecked") // parsed JSON: the vector file's schema fixes the type
                Map<String, Object> cred = (Map<String, Object>) msg.get("credential");
                if (cred != null) {
                    assertEquals(num(cred, "type"), got.credential().type(), name);
                    assertArrayEquals(hex(cred, "data"), got.credential().data(), name);
                    credentialed++;
                } else {
                    assertEquals(null, got.credential(), name);
                }
                byte[] app = hexOrNull(msg, "app_data");
                // An empty item is no data.
                assertArrayEquals(app, got.appData() == null || got.appData().length == 0 ? null : got.appData(), name);
                if (seed != null) {
                    assertArrayEquals(hex(msg, "proof_message"), got.proof().message(), name);
                    assertArrayEquals(Proof.message(idx, hex(msg, "h_p")), got.proof().message(), name);
                    assertArrayEquals(hex(msg, "proof"), got.proof().encode(), name);
                } else {
                    assertNull(got.proof(), name);
                }
                @SuppressWarnings("unchecked") // parsed JSON: the vector file's schema fixes the type
                Map<String, Object> want = (Map<String, Object>) v.get(idx == 1 ? "verify_server" : idx == 2 ? "verify_client" : "none");
                judged(want, rd, p, p.isPskBased() ? handle : null, got, idx == 1, name);
                for (Framing f : Framing.values()) {
                    byte[] wire = idx == 0 ? EstablishHello.encode(p, m, handle == null ? new byte[0] : handle, noise, f) : Frames.frame(f, noise);
                    assertArrayEquals(hex(msg, f == Framing.STREAM ? "wire_stream" : "wire_message"), wire, name);
                    if (idx == 0) {
                        EstablishHello h = EstablishHello.parseExact(wire, 0, wire.length, f);
                        assertEquals(p, h.profile(), name);
                        assertEquals(m, h.mode(), name);
                        assertArrayEquals(noise, h.message(), name);
                    }
                }
            }
            assertArrayEquals(hex(v, "handshake_hash"), i.ss.h, name);
            assertArrayEquals(hex(v, "chaining_key"), i.ss.ck, name);
            byte[][] split = i.ss.split();
            assertArrayEquals(hex(v, "split_c1"), split[0], name);
            assertArrayEquals(hex(v, "split_c2"), split[1], name);
            Result ri = i.finish();
            Result rr = r.finish();
            assertArrayEquals(hex(v, "exporter_secret"), ri.exporterSecret(), name);
            @SuppressWarnings("unchecked") // parsed JSON: the vector file's schema fixes the type
            Map<String, Object> ex = (Map<String, Object>) v.get("exporter");
            for (Result res : List.of(ri, rr)) {
                assertArrayEquals(hex(ex, "output"), res.export(str(ex, "label"), hex(ex, "context"), (int) num(ex, "length")), name);
            }
            boolean sealed = Boolean.TRUE.equals(v.get("records_use_split"));
            assertEquals(sealed, m == Mode.SEALED, name);
            assertEquals(sealed, ri.sealer() != null, name);
            if (sealed) {
                byte[] pt = {1, 2, 3};
                byte[] a = ri.sealer().seal(pt, 0, 3);
                assertArrayEquals(pt, rr.opener().open(a, 0, a.length), name);
                assertArrayEquals(a, io.zmux.internal.join.Sealer.create(p.suite(), split[0], 0).seal(pt, 0, 3), name);
                byte[] b = rr.sealer().seal(pt, 0, 3);
                assertArrayEquals(b, io.zmux.internal.join.Sealer.create(p.suite(), split[1], 0).seal(pt, 0, 3), name);
                assertArrayEquals(pt, ri.opener().open(b, 0, b.length), name);
            }
        }
        return credentialed;
    }

    /** Runs the steps; returns the class of the first failure, or "", and asserts it is at the last step. */
    private static String run(Map<String, Object> v) throws SecureCarrierException {
        Framing f = framing(v);
        Handshake hs = "initiator".equals(v.get("side")) ? side(v, true) : null;
        EstablishPayload last = null;
        List<Map<String, Object>> steps = items(v, "steps");
        for (int i = 0; i < steps.size(); i++) {
            Map<String, Object> s = steps.get(i);
            byte[] wire = hex(s, "wire");
            try {
                switch (str(s, "op")) {
                    case "hello" -> {
                        EstablishHello h = EstablishHello.parseExact(wire, 0, wire.length, f);
                        byte[] handle = h.handle().length == 0 ? null : h.handle();
                        hs = side(v, false, h.profile(), h.mode(), handle);
                        last = hs.readMessage(h.message());
                    }
                    case "read" -> last = hs.readMessage(Frames.cut(wire, 0, wire.length, f).unit());
                    case "write" -> {
                        Object n = s.get("app_data_len");
                        hs.writeMessage(n == null ? EstablishPayload.padding((int) num(s, "padding"), false)
                                : new EstablishPayload(null, new byte[((Number) n).intValue()], null, 0));
                    }
                    case "verify" -> {
                        byte[] proofKey = hexOrNull(s, "proof_key");
                        try {
                            Verification.judge(in -> new VerifyVerdict(new Principal(Principal.NODE_KEY, new byte[0]), proofKey, null),
                                    new VerifyInput(io.zmux.Role.SERVER, Profile.of(num(v, "profile")), hs.peerStatic(), null, last.credential(), List.of(), null),
                                    last.proof(), Boolean.TRUE.equals(s.get("require_pq_auth")));
                        } catch (CredentialsFailedException | VerifyUnavailableException e) {
                            assertEquals(steps.size() - 1, i, str(v, "name") + " failed early: " + e.getMessage());
                            return "credentials_failed";
                        }
                    }
                    case "force_n" -> hs.ss.cs.setN(-1L);
                    default -> throw new AssertionError("op " + s);
                }
            } catch (SecureCarrierException e) {
                assertEquals(steps.size() - 1, i, str(v, "name") + " failed early: " + e.getMessage());
                return errorClass(e);
            }
        }
        return "";
    }

    @Test
    void negative() throws Exception {
        List<Map<String, Object>> all = VectorFiles.list("negative.json");
        for (Map<String, Object> v : all) {
            assertEquals(str(v, "error"), run(v), str(v, "name"));
        }
        count("negative.json", all.size(), 153);
    }

    @Test
    void receiverEstablish() throws Exception {
        List<Map<String, Object>> all = items(VectorFiles.object("receiver.json"), "establish");
        for (Map<String, Object> v : all) {
            assertEquals("", run(v), str(v, "name"));
        }
        count("receiver.json establish", all.size(), 12);
    }

    @Test
    void hello() throws Exception {
        List<Map<String, Object>> all = VectorFiles.list("hello.json");
        for (Map<String, Object> v : all) {
            String name = str(v, "name");
            byte[] head = hex(v, "header");
            byte[] b = Arrays.copyOf(head, head.length + (int) num(v, "fill"));
            Arrays.fill(b, head.length, b.length, (byte) 0x5a);
            Framing f = framing(v);
            String error = str(v, "error");
            try {
                int n = switch (str(v, "kind")) {
                    case "join" -> Hello.parse(b, 0, b.length, f).consumed();
                    case "reply" -> Frames.cutReply(b, 0, b.length, f, Suite.AES).consumed();
                    case "frame" -> Frames.cut(b, 0, b.length, f).consumed();
                    default -> EstablishHello.parse(b, 0, b.length, f).consumed();
                };
                assertEquals("", error, name);
                assertEquals((int) num(v, "consumed"), n, name);
            } catch (SecureCarrierException e) {
                assertEquals(error, errorClass(e), name);
                assertEquals((int) num(v, "need"), e.need(), name);
            }
        }
        count("hello.json", all.size(), 54);
    }

    private static EstablishPayload withType(int type) {
        return switch (type) {
            case 1 -> new EstablishPayload(new Credential(1, new byte[0]), null, null, 0);
            case 2 -> new EstablishPayload(null, new byte[1], null, 0);
            case 3 -> new EstablishPayload(null, null, new byte[1], 0);
            case 8 -> new EstablishPayload(null, null, null, 0).withProof(new Proof(KeyAlg.ML_DSA_44.id(), new byte[KeyAlg.ML_DSA_44.keyLength()],
                    new byte[KeyAlg.ML_DSA_44.signatureLength()], null));
            default -> EstablishPayload.padding(0, false);
        };
    }

    @Test
    void config() throws Exception {
        List<Map<String, Object>> all = VectorFiles.list("config.json");
        for (Map<String, Object> v : all) {
            String name = str(v, "name");
            Mode m = Mode.ofWire((int) num(v, "mode"));
            SecureCarrierException e = assertThrows(SecureCarrierException.class, () -> {
                if ("session_crypto".equals(v.get("kind"))) {
                    set(v, "crypto");
                } else if ("pin".equals(v.get("kind"))) {
                    // The pin of an X25519 key alone, at an end that requires PQ authentication.
                    PinAnchor.checkPqAuth(new PinAnchor(new byte[KeyId.LEN]), Boolean.TRUE.equals(v.get("require_pq_auth")));
                } else if ("join".equals(v.get("kind"))) {
                    Joiner.create(Suite.lookup(num(v, "suite")), m, Framing.STREAM, hex(v, "handle"), hexOrNull(v, "psk"), hexOrNull(v, "binding"));
                } else {
                    Profile p = Profile.of(num(v, "profile"));
                    StaticKey key = StaticKey.fromScalar(hex(v, "static"));
                    // The public entrance judges the same parameters as the state machine.
                    EstablishClient.create(p, m, Framing.STREAM, hexOrNull(v, "handle"), hexOrNull(v, "psk"), hexOrNull(v, "binding"), key);
                    Handshake hs = new Handshake(p, m, true, hexOrNull(v, "handle"), hexOrNull(v, "psk"), hexOrNull(v, "binding"), key, StaticKey::generate);
                    Object n = v.get("app_data_len");
                    hs.writeMessage(n != null ? new EstablishPayload(null, new byte[((Number) n).intValue()], null, 0) : withType((int) num(v, "write_type")));
                }
            }, name);
            assertEquals(str(v, "error"), errorClass(e), name);
        }
        count("config.json", all.size(), 33);
    }

    @Test
    void derive() throws Exception {
        Map<String, Object> d = VectorFiles.object("derive.json");
        int n = 0;
        for (Map<String, Object> v : items(d, "join_psk")) {
            String name = str(v, "name");
            byte[] crypto = crypto(v);
            byte[] secret = hex(v, "epoch_secret");
            long suite = num(v, "suite");
            int len = (int) num(v, "hash_len");
            assertArrayEquals(hex(v, "join_psk_i"), Kdf.joinPsk(suite, secret, true, crypto, len), name);
            assertArrayEquals(hex(v, "join_psk_r"), Kdf.joinPsk(suite, secret, false, crypto, len), name);
            n++;
        }
        for (Map<String, Object> v : items(d, "session_crypto")) {
            String name = str(v, "name");
            String error = str(v, "error");
            if (v.containsKey("value")) {
                byte[] value = hex(v, "value");
                assertEquals(error, errorClass(assertThrows(SecureCarrierException.class, () -> SessionCrypto.decode(value))), name);
            } else if (!error.isEmpty()) {
                assertEquals(error, errorClass(assertThrows(SecureCarrierException.class, () -> crypto(v))), name);
            } else {
                assertArrayEquals(hex(v, "encoding"), crypto(v), name);
                byte[] enc = hex(v, "encoding");
                SessionCrypto i = set(v, "crypto_i");
                assertEquals(i, SessionCrypto.decode(java.util.Arrays.copyOf(enc, i.encode().length)), name);
            }
            n++;
        }
        for (Map<String, Object> v : items(d, "key_id")) {
            KeyAlg alg = KeyAlg.of(num(v, "key_alg"));
            assertArrayEquals(hex(v, "key_id"), KeyId.of(alg, hex(v, "key")), alg.toString());
            n++;
        }
        for (Map<String, Object> v : items(d, "combine")) {
            String name = str(v, "name");
            assertEquals("SHA256", str(v, "hash"), name);
            byte[][] ss = ((List<?>) v.get("ss")).stream().map(x -> HexFormat.of().parseHex((String) x)).toArray(byte[][]::new);
            assertArrayEquals(hex(v, "output"), Kdf.combine(hex(v, "salt"), ss), name);
            n++;
        }
        for (Map<String, Object> v : items(d, "epoch")) {
            String name = str(v, "name");
            byte[][] ss = ((List<?>) v.get("ss")).stream().map(x -> HexFormat.of().parseHex((String) x)).toArray(byte[][]::new);
            assertArrayEquals(hex(v, "next_epoch_secret"), Kdf.nextEpochSecret(hex(v, "epoch_secret"), num(v, "epoch"), crypto(v),
                    hex(v, "offer_body"), hex(v, "answer_body"), ss), name);
            n++;
        }
        @SuppressWarnings("unchecked") // parsed JSON: the vector file's schema fixes the type
        Map<String, Object> es = (Map<String, Object>) d.get("exporter_secret");
        assertArrayEquals(hex(es, "exporter_secret"), Kdf.expandLabel(hex(es, "ck"), "zmux-seal exporter", hex(es, "h"), 32));
        n++;
        for (Map<String, Object> v : items(d, "bound_exporter")) {
            String name = str(v, "name");
            byte[] up = hex(v, "up");
            byte[] down = hex(v, "down");
            String error = str(v, "error");
            if (!error.isEmpty()) {
                assertEquals(error, errorClass(assertThrows(SecureCarrierException.class, () -> Kdf.binding(up, down))), name);
            } else {
                assertArrayEquals(hex(v, "output"), Kdf.binding(up, down), name);
            }
            n++;
        }
        for (Map<String, Object> v : items(d, "handle_psk")) {
            String name = str(v, "name");
            byte[] secret = hex(v, "secret");
            byte[] domain = hex(v, "domain");
            byte[] salt = hexOrNull(v, "salt");
            String error = str(v, "error");
            if (!error.isEmpty()) {
                assertEquals(error, errorClass(assertThrows(SecureCarrierException.class, () -> HandlePsk.derive(secret, domain, salt))), name);
            } else {
                HandlePsk h = HandlePsk.derive(secret, domain, salt);
                assertArrayEquals(hex(v, "handle"), h.handle(), name);
                assertArrayEquals(hex(v, "psk"), h.psk(), name);
            }
            n++;
        }
        for (Map<String, Object> v : items(d, "session")) {
            Suite s = Profile.of(num(v, "profile")).suite();
            byte[] exp = Kdf.export(s, hex(v, "exporter_secret"), Kdf.EXPORTER_SESSION, null, 32);
            assertArrayEquals(hex(v, "session_export"), exp);
            byte[] secret = Kdf.sessionSecret(hex(v, "init_nonce"), hex(v, "resp_nonce"), exp, new byte[0]);
            assertArrayEquals(hex(v, "session_secret"), secret);
            for (Map<String, Object> j : items(v, "join_psk")) {
                assertArrayEquals(secret, hex(j, "epoch_secret"));
                byte[] crypto = crypto(j);
                long js = num(j, "suite");
                assertArrayEquals(hex(j, "join_psk_i"), Kdf.joinPsk(js, secret, true, crypto, (int) num(j, "hash_len")));
                assertArrayEquals(hex(j, "join_psk_r"), Kdf.joinPsk(js, secret, false, crypto, (int) num(j, "hash_len")));
            }
            n++;
        }
        count("derive.json", n, 9 + 11 + 2 + 4 + 3 + 1 + 3 + 6 + 2);
    }

    /** {@code session_crypto} from a vector's crypto_i and crypto_r. */
    private static byte[] crypto(Map<String, Object> v) throws SecureCarrierException {
        return SessionCrypto.session(set(v, "crypto_i"), set(v, "crypto_r"));
    }

    @SuppressWarnings("unchecked") // parsed JSON: the vector file's schema fixes the type
    private static SessionCrypto set(Map<String, Object> v, String key) throws SecureCarrierException {
        Map<String, Object> c = (Map<String, Object>) v.get(key);
        return SessionCrypto.of(longs(c, "suites"), longs(c, "groups"));
    }
}
