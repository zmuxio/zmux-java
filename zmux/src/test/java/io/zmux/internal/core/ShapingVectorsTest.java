package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.HandshakeShaper;
import io.zmux.carrier.HandshakeShaper.UnitPlan;
import io.zmux.internal.join.Acceptor;
import io.zmux.internal.join.Frames;
import io.zmux.internal.join.Joiner;
import io.zmux.internal.join.Mode;
import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameDecoder;
import io.zmux.internal.wire.FrameEncoder;
import io.zmux.shaping.RecordPlan;
import io.zmux.shaping.ShapingLimits;
import io.zmux.shaping.ShapingMeters;
import io.zmux.shaping.WireAccount;
import io.zmux.testutil.Json;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Lengths on the carrier against the shared cases, from the test resources, or the directory of {@code ZMUX_SHAPING_VECTORS}. */
class ShapingVectorsTest {
    static Map<String, Object> pad() throws Exception { return file("pad.json"); }

    static Map<String, Object> file(String name) throws Exception {
        String dir = System.getenv().getOrDefault("ZMUX_SHAPING_VECTORS", "src/test/resources/spec/shaping");
        Path p = Path.of(dir, name);
        assertTrue(Files.exists(p), "no shared cases at " + p);
        return Json.object(Files.readString(p));
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> cases(Map<String, Object> m, String k) { return (List<Map<String, Object>>) m.get(k); }

    static int num(Map<String, Object> m, String k) {
        return m.get(k) == null ? 0 : ((Number) m.get(k)).intValue();
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> obj(Map<String, Object> m, String k) { return (Map<String, Object>) m.get(k); }

    static UnitPlan plan(Map<String, Object> p) {
        long d = p.get("delay") == null ? 0 : ((Number) p.get("delay")).longValue();
        return new UnitPlan(num(p, "pad_to"), num(p, "pad_multiple"), num(p, "pad"), d == 0 ? null : Duration.ofNanos(d * 1000));
    }

    @Test void padForLimitsAndFill() throws Exception {
        Map<String, Object> v = pad();
        for (var c : cases(v, "pad_for")) assertEquals(num(c, "result"), plan(obj(c, "plan")).padFor(num(c, "n")), c.toString());
        for (var c : cases(v, "limits")) {
            var l = obj(c, "limits");
            ShapingLimits lim = ShapingLimits.defaults().withNoPadding(Boolean.TRUE.equals(l.get("no_padding")))
                    .withNoPause(Boolean.TRUE.equals(l.get("no_pause"))).withMaxDelay(Duration.ofNanos(num(l, "max_delay") * 1000L));
            assertEquals(plan(obj(c, "result")), withNull(plan(obj(c, "plan")).limited(lim)), c.toString());
        }
        for (var c : cases(v, "item")) {
            int item = UnitPlan.itemFor(num(c, "gap"), num(c, "room"));
            if (c.get("value_len") == null) assertEquals(HandshakeShaper.NO_ITEM, item, c.toString());
            else assertEquals(num(c, "value_len"), item, c.toString());
            assertEquals(num(c, "added"), item == HandshakeShaper.NO_ITEM ? 0 : UnitPlan.itemSize(item), c.toString());
        }
        for (var c : cases(v, "raw_fill")) {
            assertEquals(num(c, "fill"), new UnitPlan(0, 0, Math.max(num(c, "gap"), 0), null).fill(0, num(c, "room")), c.toString());
        }
    }

    private static UnitPlan withNull(UnitPlan p) {
        return p.delay() != null && p.delay().isZero() ? new UnitPlan(p.padTo(), p.padMultiple(), p.pad(), null) : p;
    }

    @Test void wholeUnits() throws Exception {
        for (var c : cases(pad(), "unit")) {
            UnitPlan p = plan(obj(c, "plan"));
            int w = num(c, "w"), room = num(c, "room");
            assertEquals(num(c, "target"), p.padFor(w), c.toString());
            int added;
            if ("item".equals(c.get("filler"))) {
                int item = p.item(w, room);
                if (c.get("value_len") == null) assertEquals(HandshakeShaper.NO_ITEM, item, c.toString());
                else assertEquals(num(c, "value_len"), item, c.toString());
                added = item == HandshakeShaper.NO_ITEM ? 0 : UnitPlan.itemSize(item);
            } else {
                added = p.fill(w, room);
            }
            assertEquals(num(c, "added"), added, c.toString());
            assertEquals(num(c, "wire"), w + added, c.toString());
        }
    }

    /** The conversion Shaping applies when it plans a record. */
    @Test void records() throws Exception {
        for (var c : cases(pad(), "record")) {
            boolean sf = Boolean.TRUE.equals(c.get("size_field"));
            int over = Boolean.TRUE.equals(c.get("write_start")) ? num(c, "write_overhead") : 0;
            var p = obj(c, "plan");
            var want = obj(c, "result");
            int mx = num(p, "max_len"), pt = num(p, "pad_to"), fmax = 0;
            boolean clamped = false;
            if (mx > 0) {
                fmax = WireLength.framesOf(mx, sf, over);
                if (fmax < RecordPlan.MIN_MAX_LENGTH) {
                    fmax = RecordPlan.MIN_MAX_LENGTH;
                    clamped = true;
                    if (pt == mx) pt = WireLength.wireOf(fmax, sf, over);
                }
            }
            int data = fmax == 0 ? num(c, "data") : Math.min(num(c, "data"), fmax);
            int frames = Shaping.framesFor(data, sf, over, pt, num(p, "pad_multiple"), num(p, "pad"));
            String what = c.toString();
            assertEquals(num(want, "frames_max"), fmax, what);
            assertEquals(Boolean.TRUE.equals(want.get("clamped")), clamped, what);
            assertEquals(num(want, "data"), data, what);
            assertEquals(num(want, "frames"), frames, what);
            assertEquals(num(want, "padding"), frames - data, what);
            assertEquals(num(want, "wire"), WireLength.wireOf(frames, sf, over), what);
        }
    }

    @Test void replyRoomAndFirstBatch() throws Exception {
        Map<String, Object> v = pad();
        for (var c : cases(v, "reply_room")) {
            Mode m = "trusted".equals(c.get("mode")) ? Mode.TRUSTED : Mode.SEALED;
            var r = Joins.replyRoom(m, num(c, "received"), num(c, "base"), num(c, "want_frames"));
            var want = obj(c, "result");
            assertEquals(num(want, "frames_limit"), r.framesLimit(), c.toString());
            assertEquals(want.get("padding_room") == null ? -1 : num(want, "padding_room"), r.paddingRoom(), c.toString());
        }
        for (var c : cases(v, "first_batch")) {
            int j = num(c, "join_early_hello");
            int limit = j == 0 ? 8192 : Math.max(j, 0);
            assertEquals(num(obj(c, "result"), "limit"), Joins.firstBatchLimit(limit, num(c, "pad_to"), num(c, "base")), c.toString());
        }
    }

    private static io.zmux.internal.join.Framing framing(Map<String, Object> u) { return "stream".equals(u.get("framing")) ? io.zmux.internal.join.Framing.STREAM : io.zmux.internal.join.Framing.MESSAGE; }

    /** The Join units; Establish's are read where its lengths live. */
    @Test void joinHandshakeLengths() throws Exception {
        int seen = 0;
        for (var c : cases(pad(), "handshake_len")) {
            var u = obj(c, "unit");
            String message = (String) u.get("message");
            int over = num(u, "overhead");
            int w;
            switch (message) {
                case "join/hello", "join/hello_early" -> w = Joiner.helloLength(num(u, "suite"), num(u, "locator_len"), num(u, "r_len"), framing(u), num(u, "ciphertext"));
                case "join/reply" -> w = Acceptor.replyLength(num(u, "r_len"), framing(u), num(u, "ciphertext"));
                case "join/confirm" -> w = Frames.unitLength(framing(u), num(u, "sealed".equals(u.get("mode")) ? "tag" : "hash_len"));
                default -> { continue; }
            }
            assertEquals(num(c, "wire"), w + over, c.toString());
            seen++;
        }
        assertTrue(seen > 0);
    }

    private static long u64(Object x) { return x == null ? 0 : ((Number) x).longValue(); }

    @SuppressWarnings("unchecked")
    private static WireAccount account(Object x) {
        Map<String, Object> a = (Map<String, Object>) x;
        return new WireAccount(u64(a.get("initial")), u64(a.get("per_mb")), u64(a.get("per_sec")));
    }

    @Test void accounts() throws Exception {
        for (var c : cases(file("budget.json"), "account")) {
            WireMeter m = new WireMeter(0);
            if (c.get("account") != null) m.set(0, 0, account(c.get("account")));
            for (var st : cases(c, "steps")) {
                long now = u64(st.get("now")), earn = u64(st.get("earn"));
                String what = c.get("name") + " " + st;
                switch ((String) st.get("op")) {
                    case "spend" -> m.spend(u64(st.get("n")));
                    case "charge" -> assertEquals(st.get("granted"), m.charge(now, earn, u64(st.get("n"))), what);
                    case "set" -> m.set(now, earn, st.get("account") == null ? WireAccount.NONE : account(st.get("account")));
                    default -> fail(what);
                }
                m.accrue(now, earn);
                assertEquals(u64(st.get("left")), m.left(), what);
                assertEquals(u64(st.get("spent")), m.spent(), what);
                assertEquals(st.get("level") != null, m.set(), what);
                if (st.get("level") != null) assertEquals(u64(st.get("level")), m.level(), what);
            }
        }
    }

    @Test void rateFloors() throws Exception {
        for (var c : cases(file("budget.json"), "rate_floor")) {
            long rate = u64(c.get("rate")), got = TokenBucket.floor(rate, u64(c.get("max_delay")));
            assertEquals(u64(c.get("want")), got, c.toString());
            assertEquals(c.get("clamped"), got != rate, c.toString());
        }
    }

    @Test void burstFloors() throws Exception {
        for (var c : cases(file("budget.json"), "burst_floor")) {
            long b = u64(c.get("burst"));
            assertEquals(u64(c.get("want")), TokenBucket.burstFloor(b, u64(c.get("record_max"))), c.toString());
        }
    }

    @Test void wirePpm() throws Exception {
        for (var c : cases(file("budget.json"), "wire_ppm")) {
            assertEquals(u64(c.get("want")), ShapingMeters.wirePpm(u64(c.get("s")), u64(c.get("e"))), c.toString());
        }
    }

    @Test void gates() throws Exception {
        for (var c : cases(file("budget.json"), "gate")) {
            TokenBucket b = new TokenBucket();
            b.rate(u64(c.get("rate")));
            b.burst(u64(c.get("burst")));
            b.reset(0);
            for (var st : cases(c, "steps")) {
                String what = c.get("name") + " " + st;
                b.fill(u64(st.get("now")));
                assertEquals(st.get("held"), b.held(), what);
                assertEquals(u64(st.get("tokens")), b.tokens(), what);
                if (st.get("release") != null) assertEquals(u64(st.get("release")), b.release(), what);
                b.take(u64(st.get("take")));
            }
        }
    }

    private static long addPipe(Session s) throws Exception {
        CarrierInfo info = new CarrierInfo(Framing.BYTES, false, false, false, false);
        return s.addPipe(Joins.JoinOptions.builder().mode(Mode.SEALED).in(info).out(info).build());
    }

    private static Session joining(int early) {
        Session s = PipeTargetTest.session(CoreConfig.builder().joinEarlyHello(early));
        Poke.set(s.pipes.get(0).mp().health, "out", Attach.OUT_FAILED);
        return s;
    }

    @Test void helloEarlyNeedsNoUsablePipeAndFirstBatchesOn() throws Exception {
        Session s = joining(8192);
        assertEquals("hello_early", s.joinPrepare(addPipe(s), 0), "whatever is queued");
        s = joining(0);
        assertEquals("hello", s.joinPrepare(addPipe(s), 0), "first batches off");
        s = PipeTargetTest.session(CoreConfig.builder().joinEarlyHello(8192));
        assertEquals("hello", s.joinPrepare(addPipe(s), 0), "another pipe is usable");
    }

    /** A new stream holding n bytes that its peer allows. */
    private static Stream written(Session s, int n) throws Exception {
        s.streams.peerLimits(16, 16);
        Stream st = s.streams.openStream(true);
        s.send.write(st, ByteBuffer.allocate(n), false);
        Poke.set(st.snd, "maxData", Long.MAX_VALUE >> 2);
        return st;
    }

    private static void allowConn(Session s) { Poke.set(s.conn, "sendMax", Long.MAX_VALUE >> 2); }

    /** Each frame's whole length over the batch, ending exactly at its end; the data they hold. */
    private static int walk(byte[] batch) throws Exception {
        var d = new FrameDecoder();
        var f = new Frame();
        ByteBuffer b = ByteBuffer.wrap(batch);
        int pos = 0, data = 0;
        while (pos < batch.length) {
            int n = d.decode(b, pos, batch.length, f);
            assertEquals(FrameEncoder.streamHeaderLength(f.streamId, f.offset, f.dataLen, true) + f.dataLen, n);
            pos += n;
            data += f.dataLen;
        }
        assertEquals(batch.length, pos);
        return data;
    }

    private static byte[] batch(int limit) throws Exception {
        Session s = PipeTargetTest.session(CoreConfig.builder());
        allowConn(s);
        written(s, 100_000);
        return s.send.firstBatch(limit, st -> true, s.path());
    }

    /** Around the Length field's sizes a limit short of a longer header is left at most two bytes unused. */
    @Test void aFirstBatchCountsEncodedBytesAtEveryBoundary() throws Exception {
        int least = FrameEncoder.streamHeaderLength(batchStream(), 0, 1, true) + 1;
        for (int limit = 1; limit <= 16_500; limit = limit == 200 ? 16_300 : limit + 1) {
            byte[] b = batch(limit);
            if (limit < least) {
                assertNull(b, "limit " + limit);
                continue;
            }
            assertTrue(b.length <= limit && b.length >= limit - 2, "limit " + limit + " took " + b.length);
            walk(b);
        }
    }

    private static long batchStream() throws Exception {
        var f = new Frame();
        byte[] b = batch(64);
        new FrameDecoder().decode(ByteBuffer.wrap(b), 0, b.length, f);
        return f.streamId;
    }

    @Test void aFirstBatchCountsEveryFrameOfSeveralStreams() throws Exception {
        Session s = PipeTargetTest.session(CoreConfig.builder());
        allowConn(s);
        written(s, 30);
        written(s, 30);
        byte[] b = s.send.firstBatch(40, st -> true, s.path());
        assertTrue(b.length <= 40);
        int data = walk(b);
        assertTrue(data > 30 && data < 38, "two headers came out of the limit: " + data);
    }

    @Test void aJoinKeepsItsDeadlineWithoutAHandshakeTimeout() throws Exception {
        Session s = PipeTargetTest.session(CoreConfig.builder().handshakeTimeout(Time.NEVER));
        long id = addPipe(s);
        s.joinPrepare(id, 0);
        s.mp().health.pipeTimers(10 * Time.SECOND - 1, true);
        assertEquals(Session.JoinProgress.PENDING, s.joinProgress(id));
        s.mp().health.pipeTimers(10 * Time.SECOND, true);
        assertEquals(Session.JoinProgress.GONE, s.joinProgress(id));
    }
}
