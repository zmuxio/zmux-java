package io.zmux.internal.join;

import io.zmux.carrier.SecureCarrierException;
import static io.zmux.internal.join.VectorFiles.errorClass;
import static io.zmux.internal.join.VectorFiles.fill;
import static io.zmux.internal.join.VectorFiles.framing;
import static io.zmux.internal.join.VectorFiles.hex;
import static io.zmux.internal.join.VectorFiles.hexOrNull;
import static io.zmux.internal.join.VectorFiles.items;
import static io.zmux.internal.join.VectorFiles.longs;
import static io.zmux.internal.join.VectorFiles.num;
import static io.zmux.internal.join.VectorFiles.str;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** record, exporter, dispatch, join and join_negative, and the Join list of receiver. */
class JoinVectorsTest {
    private static void count(String file, int n, int expected) {
        System.out.println("vectors " + file + ": " + n);
        assertEquals(expected, n, file);
    }

    private static Suite cipher(String name) {
        return "AESGCM".equals(name) ? Suite.AES : Suite.CHACHA;
    }

    private static byte[] plaintext(Map<String, Object> v) {
        @SuppressWarnings("unchecked")
        Map<String, Object> f = (Map<String, Object>) v.get("plaintext_fill");
        return f != null ? fill((int) num(f, "byte"), (int) num(f, "length")) : hex(v, "plaintext");
    }

    @Test
    void record() throws Exception {
        List<Map<String, Object>> all = VectorFiles.list("record.json");
        for (Map<String, Object> v : all) {
            String name = str(v, "name");
            Suite s = cipher(str(v, "cipher"));
            long n = num(v, "n");
            byte[] key = hex(v, "key");
            byte[] plain = plaintext(v);
            String error = str(v, "error");
            if (!error.isEmpty()) {
                if (v.containsKey("ciphertext")) {
                    byte[] ct = hex(v, "ciphertext");
                    Opener o = Opener.create(s, key, n);
                    assertEquals(error, errorClass(assertThrows(SecureCarrierException.class, () -> o.open(ct, 0, ct.length))), name);
                    assertThrows(SecureCarrierException.class, () -> o.open(ct, 0, ct.length), name + " permanent");
                } else {
                    Sealer x = Sealer.create(s, key, n);
                    assertEquals(error, errorClass(assertThrows(SecureCarrierException.class, () -> x.seal(plain, 0, plain.length))), name);
                    if (error.equals("too_large")) assertEquals(n, x.state().n(), name + " consumed a nonce");
                }
                continue;
            }
            Sealer x = Sealer.create(s, key, n);
            byte[] ct = x.seal(plain, 0, plain.length);
            if (v.containsKey("ciphertext_sha256")) {
                assertArrayEquals(hex(v, "ciphertext_sha256"), Kdf.hash(ct), name);
            } else {
                assertArrayEquals(hex(v, "ciphertext"), ct, name);
            }
            assertArrayEquals(hex(v, "key_after"), x.state().key(), name);
            assertEquals(num(v, "n_after"), x.state().n(), name);
            Opener o = Opener.create(s, key, n);
            assertArrayEquals(plain, o.open(ct, 0, ct.length), name);
            assertArrayEquals(hex(v, "key_after"), o.state().key(), name);

            // The zero-copy forms give the same bytes, also in place.
            ByteBuffer buf = ByteBuffer.allocateDirect(plain.length + x.overhead());
            buf.put(plain).flip();
            ByteBuffer out = buf.duplicate().limit(buf.capacity());
            Sealer direct = Sealer.create(s, key, n);
            assertEquals(ct.length, direct.seal(buf, out), name);
            byte[] got = new byte[ct.length];
            out.flip().get(got);
            assertArrayEquals(ct, got, name);
            ByteBuffer opened = ByteBuffer.allocate(plain.length);
            assertEquals(plain.length, Opener.create(s, key, n).open(ByteBuffer.wrap(ct), opened), name);
            assertArrayEquals(plain, opened.array(), name);
        }
        count("record.json", all.size(), 30);
    }

    @Test
    void exporter() throws Exception {
        List<Map<String, Object>> all = VectorFiles.list("exporter.json");
        for (Map<String, Object> v : all) {
            String name = str(v, "name");
            String label = v.containsKey("label_fill")
                    ? new String(fill('l', (int) num(v, "label_fill")), StandardCharsets.US_ASCII)
                    : str(v, "label");
            byte[] ctx = v.containsKey("context_fill") ? new byte[(int) num(v, "context_fill")] : hex(v, "context");
            byte[] secret = hex(v, "exporter_secret");
            int n = (int) num(v, "length");
            String error = str(v, "error");
            if (!error.isEmpty()) {
                assertEquals(error, errorClass(assertThrows(SecureCarrierException.class, () -> Kdf.export(Suite.AES, secret, label, ctx, n))), name);
                continue;
            }
            byte[] out = Kdf.export(Suite.AES, secret, label, ctx, n);
            if (v.containsKey("output_sha256")) {
                assertArrayEquals(hex(v, "output_sha256"), Kdf.hash(out), name);
            } else {
                assertArrayEquals(hex(v, "output"), out, name);
            }
            if (ctx.length == 0) assertArrayEquals(out, Kdf.export(Suite.AES, secret, label, null, n), name);
        }
        count("exporter.json", all.size(), 14);
    }

    @Test
    void dispatch() throws Exception {
        List<Map<String, Object>> all = VectorFiles.list("dispatch.json");
        for (Map<String, Object> v : all) {
            byte[] b = hex(v, "bytes");
            Dispatch.Result r = Dispatch.classify(b, 0, b.length, framing(v), 1L << 30);
            String id = str(v, "framing") + " " + str(v, "bytes");
            assertEquals(str(v, "class").toUpperCase(java.util.Locale.ROOT), r.kind().name(), id);
            assertEquals((int) num(v, "need"), r.need(), id);
            assertEquals(Boolean.TRUE.equals(v.get("record")), r.kind() == Dispatch.Kind.RECORD, id);
            boolean helloByte = b.length > 0 && b[0] >= 1 && b[0] <= 8;
            assertEquals(Boolean.TRUE.equals(v.get("hello")), helloByte, id);
        }
        count("dispatch.json", all.size(), 36);
    }

    private record Case(Map<String, Object> v) {
        Suite suite() {
            return Suite.lookup(num(v, "suite"));
        }

        Mode mode() {
            return Mode.ofWire((int) num(v, "mode"));
        }

        Group group() {
            return Group.of(num(v, "group"));
        }

        int phase() {
            return (int) num(v, "mode_byte") >>> 7;
        }

        java.security.SecureRandom kem(String key) {
            return v.containsKey(key) ? new io.zmux.seal.FixedRandom(hex(v, key)) : null;
        }

        Joiner joiner(Framing f) throws SecureCarrierException {
            return Joiner.createFixed(suite(), mode(), f, hex(v, "locator"), hex(v, "join_psk"), hexOrNull(v, "binding"),
                    new Schedule.Entropy(num(v, "attempt"), hex(v, "random_i")), group(), phase(), hex(v, "x_i"), kem("kem_seed"));
        }

        /** The groups both ends declare. */
        @SuppressWarnings("unchecked")
        long[] usable() {
            long[] i = longs((Map<String, Object>) v.get("crypto_i"), "groups"), r = longs((Map<String, Object>) v.get("crypto_r"), "groups");
            return SessionCrypto.intersect(i, r);
        }

        /** Opens a Hello as an acceptor of the epochs of accepted_phases (phase 0 when absent). */
        Acceptor accept(Hello h, Framing f) throws SecureCarrierException {
            boolean ok = true;
            Object ph = v.get("accepted_phases");
            if (ph instanceof String b64) {
                // The generator writes the list of phases as a base64 byte string.
                byte[] b = java.util.Base64.getDecoder().decode(b64);
                ok = false;
                for (byte x : b) ok |= x == h.phase();
            } else if (ph instanceof List<?> l) {
                ok = l.stream().anyMatch(x -> ((Number) x).intValue() == h.phase());
            }
            return Acceptor.accept(h, f, ok ? hex(v, "join_psk") : new byte[32], binding(), usable());
        }

        void exchange(Acceptor a) throws SecureCarrierException {
            a.exchange(hex(v, "x_r"), kem("encaps_m"));
        }

        Schedule.Entropy entropyR() {
            return new Schedule.Entropy(num(v, "attempt_r"), hex(v, "random_r"));
        }

        byte[] binding() {
            return hexOrNull(v, "binding");
        }
    }

    private static void assertSchedule(Map<String, Object> v, Schedule s, String name) {
        assertArrayEquals(hex(v, "r_i"), s.ri, name);
        assertArrayEquals(hex(v, "r_r"), s.rr, name);
        assertArrayEquals(hex(v, "th"), s.th, name);
        assertArrayEquals(hex(v, "th2"), s.th2, name);
        assertArrayEquals(hex(v, "th3"), s.th3, name);
        assertArrayEquals(hex(v, "k0"), s.k0, name);
        assertArrayEquals(hex(v, "k1"), s.k1, name);
        assertArrayEquals(hex(v, "hs"), s.hs, name);
        assertArrayEquals(hex(v, "key_i2r"), s.i2r, name);
        assertArrayEquals(hex(v, "key_r2i"), s.r2i, name);
        assertArrayEquals(hex(v, "mac_key"), s.macKey, name);
        assertArrayEquals(hex(v, "exporter_secret"), s.exporter, name);
        assertArrayEquals(hex(v, "confirm_tag"), s.confirm, name);
    }

    @Test
    @SuppressWarnings("unchecked")
    void join() throws Exception {
        List<Map<String, Object>> all = VectorFiles.list("join.json");
        for (Map<String, Object> v : all) {
            String name = str(v, "name");
            Case c = new Case(v);
            byte[] secret = hex(v, "epoch_secret");
            Map<String, Object> ci = (Map<String, Object>) v.get("crypto_i"), cr = (Map<String, Object>) v.get("crypto_r");
            byte[] crypto = SessionCrypto.session(SessionCrypto.of(longs(ci, "suites"), longs(ci, "groups")),
                    SessionCrypto.of(longs(cr, "suites"), longs(cr, "groups")));
            assertArrayEquals(hex(v, "join_psk"), Kdf.joinPsk(c.suite(), secret, "i".equals(v.get("role")), crypto), name);
            Map<String, Object> pipe = (Map<String, Object>) v.get("pipe");
            JoinPipe want = new JoinPipe(num(pipe, "ID"), (int) num(pipe, "Direction"), num(pipe, "FateGroup"), num(pipe, "Priority"),
                    Boolean.TRUE.equals(pipe.get("Standby")));
            for (Framing f : Framing.values()) {
                String sfx = f == Framing.STREAM ? "_stream" : "_message";
                Joiner j = c.joiner(f);
                byte[] hello = j.writeHelloRaw(hex(v, "hello_payload"));
                assertArrayEquals(hex(v, "hello" + sfx), hello, name);
                Hello h = Hello.parseExact(hello, 0, hello.length, f);
                assertEquals(hello.length, h.consumed(), name);
                assertArrayEquals(hex(v, "locator"), h.locator(), name);
                assertArrayEquals(hex(v, "r_i"), h.r(), name);
                assertEquals(c.phase(), h.phase(), name);
                Acceptor a = c.accept(h, f);
                assertEquals(want, a.payload().pipe(), name);
                assertArrayEquals(hex(v, "share_i"), a.joinerShare().share(), name);
                assertArrayEquals(hexOrNull(v, "first_batch"), a.payload().firstBatch(), name);
                // An unassigned item is skipped, so the payload re-encodes without it.
                if (onlyAssigned(hex(v, "hello_payload"))) assertArrayEquals(hex(v, "hello_payload"), Schedule.encodeHello(a.payload(), a.joinerShare()), name);
                c.exchange(a);
                byte[] reply = a.writeReplyRaw(hex(v, "reply_payload"), c.entropyR());
                assertArrayEquals(hex(v, "reply" + sfx), reply, name);
                ReplyPayload rp = j.readReply(reply, 0, reply.length);
                assertEquals(reply.length, j.replyConsumed(), name);
                assertArrayEquals(hexOrNull(v, "return_batch"), rp.frames(), name);
                if (onlyAssigned(hex(v, "reply_payload"))) {
                    KeyShare rs = new KeyShare(c.group(), hex(v, "share_r"));
                    assertArrayEquals(hex(v, "reply_payload"), Schedule.encodeReply(rp, rs), name);
                }
                byte[] confirm = j.writeConfirm((int) num(v, "confirm_padding"));
                assertArrayEquals(hex(v, "confirm" + sfx), confirm, name);
                if (c.mode() == Mode.BOUND) {
                    assertTrue(a.confirmed(), name);
                } else {
                    assertFalse(a.confirmed(), name);
                    assertEquals(confirm.length, a.readConfirm(confirm, 0, confirm.length), name);
                    assertTrue(a.confirmed(), name);
                }
                assertSchedule(v, j.schedule(), name);
                assertSchedule(v, a.schedule(), name);
                Result rj = j.finish();
                Result ra = a.finish();
                Map<String, Object> ex = (Map<String, Object>) v.get("export");
                byte[] out = hex(ex, "output");
                assertArrayEquals(out, rj.export(str(ex, "label"), hex(ex, "context"), (int) num(ex, "length")), name);
                assertArrayEquals(out, ra.export(str(ex, "label"), hex(ex, "context"), (int) num(ex, "length")), name);
                if (c.mode() == Mode.SEALED) {
                    byte[] up = rj.sealer().seal(new byte[] {1, 2}, 0, 2);
                    assertArrayEquals(new byte[] {1, 2}, ra.opener().open(up, 0, up.length), name);
                    byte[] down = ra.sealer().seal(new byte[] {3}, 0, 1);
                    assertArrayEquals(new byte[] {3}, rj.opener().open(down, 0, down.length), name);
                } else {
                    assertEquals(null, rj.sealer(), name);
                    assertEquals(null, ra.opener(), name);
                }
            }
        }
        count("join.json", all.size(), 104);
    }

    private static byte[] padded(Map<String, Object> step, byte[] payload) {
        int n = (int) num(step, "padding");
        if (n == 0) return payload;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Tlv.putItem(out, 0, new byte[n]);
        out.writeBytes(payload);
        return out.toByteArray();
    }

    /** Runs the steps; returns the class of the first failure, or "", and asserts it is at the last step. */
    private static String run(Map<String, Object> v) {
        Case c = new Case(v);
        Framing f = framing(v);
        Joiner j = null;
        Acceptor a = null;
        List<Map<String, Object>> steps = items(v, "steps");
        for (int i = 0; i < steps.size(); i++) {
            Map<String, Object> s = steps.get(i);
            byte[] wire = hex(s, "wire");
            try {
                if (i == 0 && "joiner".equals(v.get("side"))) j = c.joiner(f);
                switch (str(s, "op")) {
                    case "hello" -> a = c.accept(Hello.parseExact(wire, 0, wire.length, f), f);
                    case "write_hello" -> j.writeHelloRaw(padded(s, hex(v, "hello_payload")));
                    case "reply" -> j.readReply(wire, 0, wire.length);
                    case "write_reply" -> {
                        c.exchange(a);
                        a.writeReplyRaw(padded(s, hex(v, "reply_payload")), c.entropyR());
                    }
                    case "confirm" -> a.readConfirm(wire, 0, wire.length);
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
    void joinNegative() throws Exception {
        List<Map<String, Object>> all = VectorFiles.list("join_negative.json");
        for (Map<String, Object> v : all) {
            assertEquals(str(v, "error"), run(v), str(v, "name"));
        }
        count("join_negative.json", all.size(), 86);
    }

    @Test
    void receiverJoin() throws Exception {
        List<Map<String, Object>> all = items(VectorFiles.object("receiver.json"), "join");
        for (Map<String, Object> v : all) {
            assertEquals("", run(v), str(v, "name"));
        }
        count("receiver.json join", all.size(), 4);
    }

    @Test
    void incompleteKeepsState() throws Exception {
        byte[] psk = new byte[32];
        Joiner j = Joiner.create(Suite.AES, Mode.TRUSTED, Framing.STREAM, new byte[] {1}, psk, null);
        byte[] hello = j.writeHello(new HelloPayload(new JoinPipe(1, 2, 0, 0, false), null, 0));
        Acceptor a = Acceptor.accept(Hello.parseExact(hello, 0, hello.length, Framing.STREAM), Framing.STREAM, psk, null);
        byte[] reply = a.writeReply(new ReplyPayload(1, null, 0));
        SecureCarrierException e = assertThrows(SecureCarrierException.class, () -> j.readReply(reply, 0, reply.length - 1));
        assertEquals(SecureCarrierException.Code.INCOMPLETE, e.code());
        assertEquals(reply.length, e.need());
        assertEquals(1, j.readReply(reply, 0, reply.length).decision());
        byte[] tag = j.writeConfirm(0);
        SecureCarrierException e2 = assertThrows(SecureCarrierException.class, () -> a.readConfirm(tag, 0, 5));
        assertEquals(34, e2.need());
        assertEquals(34, a.readConfirm(tag, 0, 34));
        assertTrue(a.confirmed());
    }

    @Test
    void suiteTable() {
        assertArrayEquals(new long[] {1, 2}, Suite.supported());
        assertEquals(null, Suite.lookup(3));
        assertEquals(Suite.AES, Suite.preferred(new long[] {1}));
        assertEquals(Suite.AES.id(), Suite.lookup(1).id());
        assertEquals(Suite.aesHardware() ? Suite.AES : Suite.CHACHA, Suite.preferred(new long[] {1, 2}));
    }

    /** Whether every item of a join payload has an assigned type (padding, pipe, frames, decision). */
    private static boolean onlyAssigned(byte[] payload) throws Exception {
        boolean[] only = {true};
        Tlv.parse(payload, (type, value) -> {
            if (type != 0 && (type < 5 || type > 7)) only[0] = false;
            return true;
        });
        return only[0];
    }
}
