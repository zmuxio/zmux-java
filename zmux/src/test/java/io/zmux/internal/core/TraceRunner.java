package io.zmux.internal.core;

import static io.zmux.internal.core.TraceFrames.getB;
import static io.zmux.internal.core.TraceFrames.getU;
import static io.zmux.internal.core.TraceFrames.hex;
import static io.zmux.internal.core.TraceFrames.list;
import static io.zmux.internal.core.TraceFrames.map;
import static io.zmux.internal.core.TraceFrames.num;

import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameDecoder;
import io.zmux.internal.wire.FrameKind;
import io.zmux.internal.wire.TransportError;
import io.zmux.carrier.VarInt;
import io.zmux.internal.wire.WireException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.opentest4j.TestAbortedException;

/** Drives one core session through a trace (TRACES.md Section 7) on a virtual clock. */
final class TraceRunner {
    static final class Failure extends RuntimeException {
        Failure(String m) {
            super(m);
        }
    }

    static final class RLane {
        byte[] in = new byte[0];
        final List<Map<String, Object>> frames = new ArrayList<>();
        final List<Map<String, Object>> first = new ArrayList<>();
        final List<Integer> recIdx = new ArrayList<>();
        int recs;
        long written;
        String closed = "";
    }

    static final class RPipe {
        final long id;
        final TreeMap<Long, RLane> lanes = new TreeMap<>();
        Framing framing = Framing.BYTES;
        boolean in;
        boolean out;
        boolean writable = true;

        RPipe(long id) {
            this.id = id;
        }

        RLane lane(long l) {
            return lanes.computeIfAbsent(l, k -> new RLane());
        }
    }

    static final class RCarrier {
        String name;
        String via;
        String dir;
        byte[] binding;
        Frame pendJoin;
        byte[] pendMac;
        boolean pendInband;
        boolean held;
        java.net.InetSocketAddress source;
        long deadline;
        Framing framing = Framing.BYTES;
        String conn;
        byte[] buf = new byte[0];
        long pipe;
        long lane;
        boolean attached;
        boolean rejected;
    }

    static final CarrierInfo INFO = new CarrierInfo(Framing.BYTES, false, true, false, true);
    static final CarrierInfo LANE_INFO = new CarrierInfo(Framing.BYTES, false, true, true, true);

    final Map<String, Object> header;
    final Session s;
    long now;
    final Map<String, Stream> streams = new HashMap<>();
    final TreeMap<Long, RPipe> pipes = new TreeMap<>();
    final Map<String, RCarrier> carriers = new HashMap<>();
    final List<Event> events = new ArrayList<>();
    final Map<String, Integer> evSeen = new HashMap<>();
    boolean closeCovered;
    final List<Map<String, Object>> closesSent = new ArrayList<>();
    final Batch b = new Batch();
    final TraceFrames.Encoder enc;
    final FrameDecoder decoder = new FrameDecoder();

    static Map<String, Object> merge(Object a, Object b) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (a != null) out.putAll(map(a));
        if (b != null) out.putAll(map(b));
        return out;
    }

    static long ms(Object v) {
        return Time.millis(num(v));
    }

    TraceRunner(Map<String, Object> header, Map<String, Object> profiles) {
        this.header = header;
        Map<String, Object> p = map(profiles.get((String) header.get("profile")));
        Map<String, Object> ov = header.get("overrides") == null ? Map.of() : map(header.get("overrides"));
        Map<String, Object> tp = merge(p.get("tp"), ov.get("tp"));
        Map<String, Object> cf = merge(p.get("config"), ov.get("config"));
        if (cf.containsKey("sender")) {
            if (!"test-mode".equals(cf.get("sender"))) throw new IllegalArgumentException("unknown sender " + cf.get("sender"));
            cf.putIfAbsent("scheduler", "spread-eta");
            cf.putIfAbsent("probation", 0L);
            cf.putIfAbsent("redundancy", "off");
            cf.putIfAbsent("standby_probe", 0L);
            cf.putIfAbsent("probe_jitter", 0L);
        }
        Map<String, Object> th = merge(p.get("test"), ov.get("test"));
        CoreConfig.Builder cfg = CoreConfig.builder();
        io.zmux.internal.wire.Preamble.Tlv greaseTlv = null;
        long pingSeqStart = 0, testRtt = 0;
        int pipeCap = 0;
        Map<Long, Long> pipeRtt = Map.of();
        cfg.local(TraceFrames.paramsFromMap(tp));
        if (th.get("grease_tp") instanceof Map<?, ?>) {
            Map<String, Object> g = map(th.get("grease_tp"));
            cfg.local().greaseId = num(g.get("id"));
            cfg.local().greaseValue = hex(g.get("value"));
        }
        if (th.get("grease_tlv") instanceof Map<?, ?>) {
            Map<String, Object> g = map(th.get("grease_tlv"));
            greaseTlv = new io.zmux.internal.wire.Preamble.Tlv(num(g.get("type")), hex(g.get("value")));
        }
        if (th.containsKey("ping_seq_start")) pingSeqStart = num(th.get("ping_seq_start"));
        if (th.containsKey("rtt_ms")) testRtt = ms(th.get("rtt_ms"));
        if (th.containsKey("pipe_cap")) pipeCap = (int) num(th.get("pipe_cap"));
        if (th.get("pipe_rtt_ms") instanceof Map<?, ?>) {
            Map<Long, Long> pr = new java.util.HashMap<>();
            for (var e : map(th.get("pipe_rtt_ms")).entrySet()) pr.put(Long.parseLong(e.getKey()), ms(e.getValue()));
            pipeRtt = pr;
        }
        if (th.containsKey("role_nonce")) cfg.local().roleNonce = num(th.get("role_nonce"));
        if (th.containsKey("session_nonce")) System.arraycopy(hex(th.get("session_nonce")), 0, cfg.local().sessionNonce, 0, 16);
        if (th.containsKey("lane_key") && cfg.local().has(io.zmux.internal.wire.TransportParams.LANES)) {
            System.arraycopy(hex(th.get("lane_key")), 0, cfg.local().laneKey, 0, 16);
        }
        boolean responder = "responder".equals(header.get("local_role"));
        CoreConfig.Role localRole = responder ? CoreConfig.Role.RESPONDER : CoreConfig.Role.INITIATOR;
        cfg.role(localRole).transportRole(localRole);
        for (var e : cf.entrySet()) {
            Object v = e.getValue();
            switch (e.getKey()) {
                case "keep_alive_period" -> cfg.keepAlive(ms(v) == 0 ? CoreConfig.OFF : ms(v)); // a trace's 0 is off
                case "handshake_timeout" -> cfg.handshakeTimeout(ms(v));
                case "max_incoming_uni_streams" -> cfg.stagedUni(v == null);
                case "enable_datagrams" -> cfg.datagrams((Boolean) v);
                case "role" -> cfg.role(switch ((String) v) {
                    case "auto" -> CoreConfig.Role.AUTO;
                    case "responder" -> CoreConfig.Role.RESPONDER;
                    default -> CoreConfig.Role.INITIATOR;
                });
                case "max_pipes" -> cfg.maxPipes((int) num(v));
                case "resume_timeout" -> cfg.resume(ms(v));
                case "probation" -> { if (v != null) cfg.probation(ms(v)); }
                case "suspect_floor" -> { if (v != null) cfg.suspectFloor(ms(v)); }
                case "dead_timeout" -> { if (v != null) cfg.deadTimeout(ms(v)); }
                case "overflow_delay" -> { if (v != null) cfg.overflowDelay(ms(v)); }
                case "standby_probe" -> { if (v != null) cfg.standbyProbe(ms(v)); }
                case "probe_jitter" -> { if (v != null) cfg.probeJitter(((Number) v).doubleValue()); }
                case "redundancy" -> cfg.redundancy("urgent".equals(v));
                case "scheduler" -> cfg.policy(TraceApi.policy((String) v));
                case "early_data" -> {
                    Map<String, Object> m = map(v);
                    cfg.earlyZmuxPeer(getB(m, "zmux_peer"));
                    cfg.earlyReplayable(getB(m, "replayable"));
                }
                case "pipe_auth" -> {
                    Map<String, Object> m = map(v);
                    if (m.containsKey("psk")) cfg.psk(hex(m.get("psk")));
                }
                default -> { }
            }
        }
        cfg.test(new CoreConfig.TestHooks(true, greaseTlv, pingSeqStart, testRtt, pipeRtt, pipeCap));
        s = new Session(cfg.build(), 0);
        enc = new TraceFrames.Encoder((pipe, lane) -> {
            RPipe rp = pipes.get(pipe);
            return rp == null ? 0 : rp.lane(lane).written;
        });
    }

    static Failure fail(String m) {
        return new Failure(m);
    }

    void step(Map<String, Object> st) {
        String op = (String) st.get("op");
        switch (op) {
            case "note" -> { return; }
            case "pipe_up" -> { pipeUp(st); return; }
            case "pipe_down" -> { pipeDown(st); return; }
            case "set_writable" -> {
                RPipe p = pipes.get(num(st.get("pipe")));
                p.writable = (Boolean) st.get("writable");
                for (long ln : p.lanes.keySet()) s.setWritable(p.id, ln, p.writable, now);
                return;
            }
            case "peer_send" -> { peerSend(st); return; }
            case "api" -> { new TraceApi(this).call(st); return; }
            case "advance" -> { advance(ms(st.get("ms"))); return; }
            default -> { }
        }
        flush();
        switch (op) {
            case "expect_frames" -> expectFrames(st);
            case "expect_no_frames" -> expectNoFrames(st);
            case "expect_close" -> expectClose(st);
            case "expect_open" -> {
                if (s.state().compareTo(SessionState.CLOSING) >= 0) {
                    throw fail("session is " + s.state() + ": " + (s.error() == null ? "" : s.error().message()));
                }
            }
            case "expect_state" -> expectState(st);
            case "expect_event" -> expectEvent(st, true);
            case "expect_no_event" -> expectEvent(st, false);
            case "expect_pipe_closed" -> expectPipeClosed(st);
            case "expect_reject_carrier" -> {
                RCarrier c = carriers.get((String) st.get("carrier"));
                if (c == null || !c.rejected) throw fail("carrier not rejected");
            }
            default -> throw fail("unknown op " + op);
        }
    }

    void pipeUp(Map<String, Object> st) {
        String via = (String) st.get("via");
        String conn = st.get("conn") == null ? null : ((String) st.get("conn")).intern();
        if ("lane".equals(via)) {
            RCarrier c = new RCarrier();
            c.conn = conn;
            carriers.put((String) st.get("carrier"), c);
            return;
        }
        if ("listener".equals(via)) {
            listenerUp(st);
            return;
        }
        if (via != null && !via.equals("bootstrap")) throw new TestAbortedException("pipe_up via " + via);
        long id = num(st.get("pipe"));
        RPipe p = new RPipe(id);
        if ("records".equals(st.get("framing"))) p.framing = Framing.RECORDS;
        String dir = (String) st.get("dir");
        p.in = !dir.equals("out");
        p.out = !dir.equals("in");
        pipes.put(id, p);
        CarrierInfo ci = new CarrierInfo(p.framing, false, true, conn != null, true);
        s.pipes().attachBootstrap(ci, ci, null);
        if (conn != null) s.pipes().setCarrierConn(id, conn);
        drainEvents();
    }

    void pipeDown(Map<String, Object> st) {
        if (st.get("carrier") instanceof String cn) {
            RCarrier c = carriers.get(cn);
            if (c == null || !c.attached) return;
            s.pipeEnded(c.pipe, c.lane, "eof".equals(st.get("how")), c.buf.length > 0, null, now);
            drainEvents();
            return;
        }
        long id = num(st.get("pipe"));
        long lane = getU(st, "lane");
        String dir = (String) st.get("dir");
        String how = (String) st.get("how");
        if (dir.equals("in") || dir.equals("bidi")) {
            RLane l = pipes.get(id).lane(lane);
            s.pipeEnded(id, lane, how.equals("eof"), l.in.length > 0, null, now);
        }
        if (dir.equals("out") || dir.equals("bidi")) s.writeFailed(id, lane, new java.io.IOException("carrier failed"), now);
        drainEvents();
    }

    void peerSend(Map<String, Object> st) {
        if (st.get("carrier") instanceof String cn) {
            RCarrier c = carriers.get(cn);
            if (c != null && "listener".equals(c.via)) {
                listenerSend(c, st);
                return;
            }
            carrierSend(c, st);
            return;
        }
        long id = num(st.get("pipe"));
        RPipe p = pipes.get(id);
        if (p == null) throw fail("no pipe " + id);
        if (st.containsKey("messages") || st.containsKey("messages_hex")) {
            List<byte[]> msgs = new ArrayList<>();
            if (st.containsKey("messages")) for (Object m : list(st.get("messages"))) msgs.add(enc.body(m));
            else for (Object m : list(st.get("messages_hex"))) msgs.add(hex(m));
            for (byte[] m : msgs) {
                ByteBuffer bb = ByteBuffer.wrap(m);
                s.receiveMessage(id, 0, new ReadBuf(bb, null), bb, 0, m.length, now);
            }
            drainEvents();
            return;
        }
        byte[] data = st.get("hex") instanceof String h ? hex(h) : enc.records(list(st.get("records")));
        try {
            feedChunks(id, p, data, st);
        } finally {
            retryHeld();
        }
    }

    private void feedChunks(long id, RPipe p, byte[] data, Map<String, Object> st) {
        List<Integer> chunks = new ArrayList<>();
        if (st.get("chunks") instanceof List<?> cl) for (Object x : cl) chunks.add((int) num(x));
        else chunks.add(data.length);
        RLane l = p.lane(0);
        int at = 0;
        for (int c : chunks) {
            c = Math.min(c, data.length - at);
            feed(id, l, java.util.Arrays.copyOfRange(data, at, at + c));
            at += c;
        }
        if (at < data.length) feed(id, l, java.util.Arrays.copyOfRange(data, at, data.length));
    }

    private void feed(long id, RLane l, byte[] more) {
        byte[] in = new byte[l.in.length + more.length];
        System.arraycopy(l.in, 0, in, 0, l.in.length);
        System.arraycopy(more, 0, in, l.in.length, more.length);
        ByteBuffer bb = ByteBuffer.wrap(in);
        ReadBuf rb = new ReadBuf(bb, null);
        int n = s.receive(id, 0, rb, bb, 0, in.length, now);
        l.in = java.util.Arrays.copyOfRange(in, n, in.length);
        drainEvents();
    }

    /** Delivers bytes on an inbound lane carrier: the first record is routed by its PIPE_LANE (MULTIPATH.md 5.8). */
    void carrierSend(RCarrier c, Map<String, Object> st) {
        if (c == null) throw fail("unknown carrier");
        byte[] data = st.get("hex") instanceof String h ? hex(h) : enc.records(list(st.get("records")));
        byte[] buf = new byte[c.buf.length + data.length];
        System.arraycopy(c.buf, 0, buf, 0, c.buf.length);
        System.arraycopy(data, 0, buf, c.buf.length, data.length);
        c.buf = buf;
        if (c.rejected) {
            c.buf = new byte[0];
            return;
        }
        ByteBuffer bb = ByteBuffer.wrap(c.buf);
        if (!c.attached) {
            long size = VarInt.get(bb, 0, c.buf.length);
            if (size < 0) return;
            int n = VarInt.lengthOf(c.buf[0]);
            if (c.buf.length - n < size) return;
            Frame f = new Frame();
            boolean ok;
            try {
                decoder.decode(bb, n, n + (int) size, f);
                byte[] key = new byte[16];
                if (f.kind == FrameKind.PIPE_LANE) bb.get(f.laneKeyPos, key);
                ok = f.kind == FrameKind.PIPE_LANE && s.lanes().keyMatches(key);
            } catch (WireException e) {
                ok = false;
            }
            if (!ok) {
                c.rejected = true;
                c.buf = new byte[0];
                return;
            }
            if (!s.lanes().accept(c.conn, f.pipeId, f.lane, LANE_INFO)) {
                c.rejected = s.state().compareTo(SessionState.CLOSING) < 0;
                c.buf = new byte[0];
                drainEvents();
                return;
            }
            c.attached = true;
            c.pipe = f.pipeId;
            c.lane = f.lane;
        }
        int n = s.receive(c.pipe, c.lane, new ReadBuf(bb, null), bb, 0, c.buf.length, now);
        c.buf = java.util.Arrays.copyOfRange(c.buf, n, c.buf.length);
        drainEvents();
    }

    void advance(long d) {
        long target = Time.add(now, d);
        while (true) {
            flush();
            long nt = s.nextTimeout();
            if (nt > target) break;
            if (nt > now) now = nt;
            s.handleTimeout(now);
            drainEvents();
            if (s.nextTimeout() <= now) {
                flush();
                if (s.nextTimeout() <= now) break;
            }
        }
        now = target;
        s.handleTimeout(now);
        drainEvents();
        for (RCarrier c : carriers.values()) {
            if (c.source != null && !c.rejected && now >= c.deadline) {
                admission.release(c.source);
                c.source = null;
                c.rejected = true;
            }
        }
    }

    // ---- listener carriers (MULTIPATH.md 5.4-5.6) ----

    final CarrierAdmission admission = new CarrierAdmission(0, 0);
    final CarrierAdmission.SplitPairing pairing = new CarrierAdmission.SplitPairing(0, 0);
    final List<RCarrier> held = new ArrayList<>();

    void listenerUp(Map<String, Object> st) {
        RCarrier c = new RCarrier();
        c.name = (String) st.get("carrier");
        c.via = "listener";
        c.dir = (String) st.get("dir");
        if ("records".equals(st.get("framing"))) c.framing = Framing.RECORDS;
        if (st.get("carrier_binding") instanceof String cb) c.binding = hex(cb);
        if (st.get("conn") instanceof String cn) c.conn = cn.intern();
        if (st.get("pairing") instanceof String tok) {
            boolean up = "in".equals(c.dir);
            CarrierAdmission.SplitPairing.Result res = pairing.offer(hex(tok), up, null, c, Time.add(now, 10 * Time.SECOND));
            if (res == CarrierAdmission.SplitPairing.Result.REJECT || res == CarrierAdmission.SplitPairing.Result.REFUSE) {
                c.rejected = true;
            } else if (res == CarrierAdmission.SplitPairing.Result.DONE) {
                RCarrier o = (RCarrier) pairing.partner();
                c.attached = o.attached = true;
                c.pipe = o.pipe = 0;
                RPipe p = new RPipe(0);
                p.in = p.out = true;
                pipes.put(0L, p);
                s.pipes().attachBootstrap(INFO, INFO, null);
                drainEvents();
            }
            carriers.put(c.name, c);
            return;
        }
        carriers.put(c.name, c);
        if (st.get("source") instanceof String src) {
            java.net.InetSocketAddress a = io.zmux.TestAddr.literal(src);
            if (!admission.admit(a)) {
                c.rejected = true;
                return;
            }
            c.source = a;
            c.deadline = admission.deadline(now);
        }
        if (st.get("oob_join") instanceof String oob) {
            byte[] b = hex(oob);
            Frame f = CarrierAdmission.parseJoin(b);
            if (f == null) {
                c.rejected = true;
                return;
            }
            tryJoin(c, f, CarrierAdmission.joinMac(f), false);
        }
    }

    CarrierInfo carrierInfo(RCarrier c) {
        return new CarrierInfo(c.framing, false, true, c.conn != null, true);
    }

    void tryJoin(RCarrier c, Frame f, byte[] mac, boolean inband) {
        if (!java.util.Arrays.equals(CarrierAdmission.joinLocator(f), s.locator())) {
            c.rejected = true;
            return;
        }
        boolean hasIn = !"out".equals(c.dir), hasOut = !"in".equals(c.dir);
        CarrierInfo info = carrierInfo(c);
        Joins.JoinResult res = s.acceptJoin(f, mac, c.binding, new Joins.JoinCarrier(hasIn, hasOut, info, info, null, false));
        drainEvents();
        switch (res) {
            case HOLD -> {
                c.held = true;
                c.pendJoin = f;
                c.pendMac = mac;
                c.pendInband = inband;
                held.add(c);
                return;
            }
            case CLOSE, REFUSED -> {
                c.rejected = true;
                c.buf = new byte[0];
                return;
            }
            default -> { }
        }
        c.held = false;
        c.attached = true;
        c.pipe = f.join.pipeId;
        c.lane = 0;
        RPipe p = new RPipe(f.join.pipeId);
        p.framing = c.framing;
        p.in = hasIn && f.join.direction != 1;
        p.out = hasOut && f.join.direction != 0;
        pipes.put(f.join.pipeId, p);
        if (c.conn != null) s.pipes().setCarrierConn(f.join.pipeId, c.conn);
        if (inband) s.pipes().skipJoinFrame(f.join.pipeId);
        feedCarrier(c);
    }

    /** Offers held joins again (after establishment). */
    void retryHeld() {
        List<RCarrier> h = new ArrayList<>(held);
        held.clear();
        for (RCarrier c : h) {
            if (c.held && !c.rejected) {
                c.held = false;
                tryJoin(c, c.pendJoin, c.pendMac, c.pendInband);
            }
        }
    }

    void feedCarrier(RCarrier c) {
        if (!c.attached || c.buf.length == 0) return;
        RPipe p = pipes.get(c.pipe);
        if (p != null && p.framing == Framing.RECORDS) return;
        ByteBuffer bb = ByteBuffer.wrap(c.buf);
        int n = s.receive(c.pipe, c.lane, new ReadBuf(bb, null), bb, 0, c.buf.length, now);
        c.buf = java.util.Arrays.copyOfRange(c.buf, n, c.buf.length);
        drainEvents();
    }

    void listenerSend(RCarrier c, Map<String, Object> st) {
        if (c.framing == Framing.RECORDS) {
            List<byte[]> msgs = new ArrayList<>();
            if (st.get("messages") instanceof List<?> ml) for (Object m : ml) msgs.add(enc.body(m));
            else if (st.get("messages_hex") instanceof List<?> mh) for (Object m : mh) msgs.add(hex(m));
            for (byte[] m : msgs) {
                if (c.rejected) return;
                if (!c.attached) {
                    Frame f = CarrierAdmission.parseJoin(m);
                    if (f == null) {
                        c.rejected = true;
                        return;
                    }
                    tryJoin(c, f, CarrierAdmission.joinMac(f), true);
                    if (!c.attached) continue;
                }
                ByteBuffer bb = ByteBuffer.wrap(m);
                s.receiveMessage(c.pipe, 0, new ReadBuf(bb, null), bb, 0, m.length, now);
                drainEvents();
            }
            return;
        }
        byte[] data = st.get("hex") instanceof String h ? hex(h) : enc.records(list(st.get("records")));
        if (c.rejected) return;
        byte[] buf = new byte[c.buf.length + data.length];
        System.arraycopy(c.buf, 0, buf, 0, c.buf.length);
        System.arraycopy(data, 0, buf, c.buf.length, data.length);
        c.buf = buf;
        if (c.attached) {
            feedCarrier(c);
            return;
        }
        if (c.held) return;
        CarrierAdmission.Classified cl = CarrierAdmission.classify(ByteBuffer.wrap(c.buf), 0, c.buf.length, 65535);
        switch (cl.kind()) {
            case REJECT, SESSION -> {
                c.rejected = true;
                c.buf = new byte[0];
                return;
            }
            case MORE -> {
                return;
            }
            default -> { }
        }
        if (cl.firstRecordLength() == 0) return;
        if (c.source != null) {
            admission.release(c.source);
            c.source = null;
        }
        if (cl.kind() == CarrierAdmission.Kind.LANE) {
            c.via = "lane";
            carrierSend(c, Map.of("hex", ""));
            return;
        }
        int sn = VarInt.lengthOf(c.buf[0]);
        Frame f = CarrierAdmission.parseJoin(java.util.Arrays.copyOfRange(c.buf, sn, cl.firstRecordLength()));
        if (f == null) {
            c.rejected = true;
            c.buf = new byte[0];
            return;
        }
        tryJoin(c, f, CarrierAdmission.joinMac(f), true);
    }

    /** add_pipe: a local join. */
    Map<String, Object> addPipe(Map<String, Object> args) throws CoreException {
        long id = num(args.get("pipe"));
        long next = s.path().nextPipeId();
        if (next == 0) {
            s.addPipe(Joins.JoinOptions.DEFAULT);
            return Map.of("ok", true);
        }
        if (next != id) throw fail("next pipe ID " + next + ", trace expects " + id);
        String dir = (String) args.get("dir");
        Joins.JoinOptions.Builder ob = Joins.JoinOptions.builder()
                .dir(switch (dir) {
                    case "out" -> 0;
                    case "in" -> 1;
                    default -> 2;
                })
                .fateGroup(getU(args, "fate_group"))
                .oob(getB(args, "oob"))
                .carrierBound(getB(args, "carrier_bound"))
                .recvPriority(getU(args, "receive_priority"))
                .recvStandby(getB(args, "receive_standby"))
                .weight(args.containsKey("weight") ? getU(args, "weight") : 1)
                .in(INFO).out(INFO);
        if (args.get("send_priority") != null) ob.sendPriority(num(args.get("send_priority")));
        if (args.get("send_standby") instanceof Boolean b) ob.sendStandby(b);
        if (args.get("probation") != null) ob.probation(ms(args.get("probation")));
        if (getB(args, "carrier_bound") && args.get("carrier_binding") instanceof String b) ob.binding(hex(b));
        Joins.JoinOptions o = ob.build();
        RPipe p = new RPipe(id);
        p.in = !dir.equals("out");
        p.out = !dir.equals("in");
        pipes.put(id, p);
        long r = s.addPipe(o);
        return r < 0 ? Map.of("pending", true) : Map.of("ok", true);
    }

    void drainEvents() {
        for (Event e; (e = s.outbox().pollEvent()) != null; ) events.add(e);
        s.outbox().takeWakes(new ArrayList<>());
        List<Action> acts = new ArrayList<>();
        s.outbox().takeActions(acts);
        for (Action a : acts) {
            RPipe p = pipes.get(a.pipe());
            if (p == null) continue;
            switch (a.kind()) {
                case SHUTDOWN -> {
                    RLane l = p.lane(a.lane());
                    if (l.closed.isEmpty()) l.closed = "graceful";
                }
                case ABORT -> {
                    if (p.lanes.isEmpty()) p.lane(0);
                    for (RLane l : p.lanes.values()) if (l.closed.isEmpty()) l.closed = "abort";
                }
                case OPEN_LANE -> s.lanes().opened(a.pipe(), LANE_INFO, null);
                case RESET_LANE -> {
                    RLane l = p.lane(a.lane());
                    if (l.closed.isEmpty()) l.closed = "abort";
                }
                case CLOSE -> {
                    for (RLane l : p.lanes.values()) if (l.closed.isEmpty()) l.closed = "graceful";
                }
            }
        }
    }

    /** Polls every writable outbound lane until nothing comes out. */
    void flush() {
        for (int i = 0; i < 64; i++) {
            boolean matched = false;
            for (RPipe p : new ArrayList<>(pipes.values())) {
                if (!p.out || !p.writable) continue;
                List<Long> lanes = new ArrayList<>();
                s.pipes().outLanes(p.id, lanes);
                for (long ln : lanes) {
                    while (s.pollTransmit(p.id, ln, b, Session.DEFAULT_BATCH, now)) {
                        matched = true;
                        RLane l = p.lane(ln);
                        if (!l.closed.isEmpty()) throw fail("pipe " + p.id + " lane " + ln + ": a batch after the lane was closed");
                        boolean shut = b.shutdown();
                        capture(p, ln);
                        if (shut && l.closed.isEmpty()) l.closed = "graceful";
                        s.writeDone(b, null, now);
                        drainEvents();
                    }
                }
            }
            drainEvents();
            if (!matched) return;
        }
        throw fail("flush does not converge");
    }

    void capture(RPipe p, long lane) {
        RLane l = p.lane(lane);
        List<byte[]> recs = new ArrayList<>();
        if (p.framing == Framing.RECORDS) {
            for (ByteBuffer message : BatchFixture.messages(b)) {
                byte[] m = new byte[message.remaining()]; message.get(m);
                l.written += VarInt.length(m.length) + m.length;
                recs.add(m);
            }
        } else {
            byte[] data = io.zmux.internal.core.BatchFixture.flat(b);
            l.written += data.length;
            ByteBuffer bb = ByteBuffer.wrap(data);
            int i = 0;
            while (i < data.length) {
                long size = VarInt.get(bb, i, data.length);
                int n = size < 0 ? 0 : VarInt.lengthOf(data[i]);
                if (size < 0 || data.length - i - n < size) throw fail("local endpoint wrote a partial record");
                recs.add(java.util.Arrays.copyOfRange(data, i + n, i + n + (int) size));
                i += n + (int) size;
            }
        }
        for (byte[] rec : recs) {
            l.recs++;
            ByteBuffer bb = ByteBuffer.wrap(rec);
            int i = 0;
            while (i < rec.length) {
                Frame f = new Frame();
                int n;
                try {
                    n = decoder.decode(bb, i, rec.length, f);
                } catch (WireException e) {
                    throw fail("local endpoint wrote an undecodable frame: " + e.getMessage());
                }
                Map<String, Object> o = TraceFrames.frameObj(f);
                if (System.getenv("ZTRACE") != null) System.err.println("pipe " + p.id + " lane " + lane + " t=" + now / 1_000_000 + ": " + o);
                if (f.kind == FrameKind.CONNECTION_CLOSE) closesSent.add(o);
                l.frames.add(o);
                l.recIdx.add(l.recs);
                if (l.first.size() < 64) l.first.add(o);
                i += n;
            }
        }
    }

    List<RLane> lanesFor(Map<String, Object> st) {
        long lane = getU(st, "lane");
        if ("*".equals(st.get("pipe"))) {
            List<RLane> out = new ArrayList<>();
            for (RPipe p : pipes.values()) out.addAll(p.lanes.values());
            return out;
        }
        RPipe p = pipes.get(num(st.get("pipe")));
        return p == null ? List.of() : List.of(p.lane(lane));
    }

    void expectFrames(Map<String, Object> st) {
        List<RLane> lanes = lanesFor(st);
        List<Map<String, Object>> frames = new ArrayList<>();
        List<Integer> recIdx = new ArrayList<>();
        for (RLane l : lanes) {
            frames.addAll(l.frames);
            recIdx.addAll(l.recIdx);
        }
        String mode = (String) st.get("match");
        if (mode.equals("first")) {
            frames = new ArrayList<>();
            for (RLane l : lanes) frames.addAll(l.first);
        }
        List<Map<String, Object>> pats = new ArrayList<>();
        for (Object o : list(st.get("frames"))) pats.add(map(o));
        String err = TraceMatch.match(mode, pats, TraceMatch.nonPadding(frames, pats));
        if (err == null && getB(st, "one_credit_per_record")) err = TraceMatch.oneCreditPerRecord(frames, recIdx);
        for (RLane l : lanes) {
            l.frames.clear();
            l.recIdx.clear();
        }
        if (err != null) throw fail(err + "\nframes:\n" + TraceMatch.dump(frames));
    }

    void expectNoFrames(Map<String, Object> st) {
        List<String> types = new ArrayList<>();
        if (st.get("types") instanceof List<?> ts) for (Object x : ts) types.add((String) x);
        for (RLane l : lanesFor(st)) {
            for (Map<String, Object> f : l.frames) {
                if ("PADDING".equals(f.get("type"))) continue;
                if (types.isEmpty() || types.contains(f.get("type"))) throw fail("unexpected frames:\n" + TraceMatch.dump(l.frames));
            }
            l.frames.clear();
            l.recIdx.clear();
        }
    }

    static Map<String, Object> errorObj(SessionError e) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("remote", e.remote());
        switch (e.kind()) {
            case APPLICATION -> { o.put("space", "application"); o.put("code", e.code()); }
            case TRANSPORT -> { o.put("space", "transport"); o.put("code", TransportError.name(e.code())); }
            case IDLE -> { o.put("space", "local"); o.put("code", "IDLE_TIMEOUT"); }
            case HANDSHAKE_TIMEOUT -> { o.put("space", "local"); o.put("code", "HANDSHAKE_TIMEOUT"); }
            case EARLY_REJECTED -> { o.put("space", "local"); o.put("code", "EARLY_DATA_REJECTED"); }
            case PIPES_LOST -> { o.put("space", "local"); o.put("code", "PIPES_LOST"); }
            case RESUME_TIMEOUT -> { o.put("space", "transport"); o.put("code", "ZMUX_RESUME_TIMEOUT"); }
        }
        return o;
    }

    void expectClose(Map<String, Object> st) {
        SessionError e = s.error();
        if (e == null) throw fail("session not closed (state " + s.state() + ")");
        closeCovered = true;
        Map<String, Object> pat = map(st.get("error"));
        Map<String, Object> got = errorObj(e);
        if (!TraceMatch.matchObj(pat, got)) throw fail("close error " + got + " (" + e.message() + "), want " + pat);
        if (st.get("wire") instanceof Boolean w) {
            Object code = pat.get("code");
            boolean found = false;
            for (Map<String, Object> c : closesSent) {
                if (code instanceof String cs) {
                    long wc = TransportError.code(cs);
                    if (wc >= 0 && num(c.get("code")) == wc && !(Boolean) c.get("app")) found = true;
                } else if (num(c.get("code")) == num(code)) {
                    found = true;
                }
            }
            if (w != found) throw fail("CONNECTION_CLOSE on the wire: " + found + ", want " + w + " (sent " + closesSent + ")");
        }
    }

    void expectState(Map<String, Object> st) {
        if (st.get("session") instanceof String ss && !s.state().name().equals(ss)) {
            throw fail("session state " + s.state() + ", want " + ss);
        }
        if (st.containsKey("pipe")) {
            var ps = s.pipeState(num(st.get("pipe")));
            String got = ps == null ? "RETIRED" : "in".equals(st.get("dir")) ? ps.in().name() : ps.out().name();
            Object want = st.get("state");
            if (want instanceof Map<?, ?>) {
                for (Object x : list(map(want).get("oneof"))) if (x.equals(got)) return;
                throw fail("pipe state " + got + ", want " + want);
            }
            if (!got.equals(want)) throw fail("pipe state " + got + ", want " + want);
        }
    }

    static final List<String> FEATURES = List.of("role", "goaway", "preamble", "priority", "datagram_flows",
            "multipath", "pipe_ack", "resume");

    static Map.Entry<String, Map<String, Object>> eventObj(Event e) {
        Map<String, Object> o = new LinkedHashMap<>();
        String name = switch (e.kind()) {
            case ESTABLISHED -> {
                Features f = e.features();
                boolean[] on = {f.role(), f.goAway(), f.preamble(), f.priority(), f.flows(), f.multipath(), f.pipeAck(), f.resume()};
                List<Object> fs = new ArrayList<>();
                for (int i = 0; i < on.length; i++) if (on[i]) fs.add(FEATURES.get(i));
                o.put("role", e.initiator() ? "initiator" : "responder");
                o.put("features", fs);
                yield "established";
            }
            case CLOSED -> "closed";
            case GOAWAY_RECEIVED -> {
                o.put("max_bidi", e.maxBidi());
                o.put("max_uni", e.maxUni());
                o.put("code", e.code());
                yield "goaway_received";
            }
            case DATAGRAM_DROPPED -> {
                o.put("count", e.count());
                yield "datagram_dropped";
            }
            case SUSPENDED -> "suspended";
            case RESUMED -> "resumed";
            case PIPE_STATE -> {
                o.put("pipe", e.pipe());
                o.put("state", e.state());
                yield "pipe_state";
            }
            case JOIN_AUTH_FAILED -> {
                o.put("reason", e.reason());
                yield "join_auth_failed";
            }
            case OOB_JOIN -> {
                o.put("pipe", e.pipe());
                o.put("frame", hex(e.frame()));
                yield "oob_join";
            }
            default -> "other";
        };
        return Map.entry(name, o);
    }

    void expectEvent(Map<String, Object> st, boolean want) {
        String name = (String) st.get("event");
        int start = evSeen.getOrDefault(name, 0);
        for (int i = start; i < events.size(); i++) {
            var eo = eventObj(events.get(i));
            if (!eo.getKey().equals(name)) continue;
            if (!want) throw fail("event " + name + " reported");
            if (st.get("fields") instanceof Map<?, ?> && !TraceMatch.matchEventFields(map(st.get("fields")), eo.getValue())) continue;
            evSeen.put(name, i + 1);
            return;
        }
        if (want) throw fail("event " + name + " not reported");
        evSeen.put(name, events.size());
    }

    void expectPipeClosed(Map<String, Object> st) {
        RPipe p = pipes.get(num(st.get("pipe")));
        String how = (String) st.get("how");
        java.util.Collection<RLane> lanes = st.containsKey("lane") ? List.of(p.lane(num(st.get("lane")))) : p.lanes.values();
        for (RLane l : lanes) {
            if (l.closed.isEmpty() || !how.equals("any") && !l.closed.equals(how)) throw fail("pipe closed '" + l.closed + "', want " + how);
        }
    }
}
