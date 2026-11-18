package io.zmux.internal.core;

import static io.zmux.internal.core.TraceFrames.getB;
import static io.zmux.internal.core.TraceFrames.getU;
import static io.zmux.internal.core.TraceFrames.hex;
import static io.zmux.internal.core.TraceFrames.list;
import static io.zmux.internal.core.TraceFrames.map;
import static io.zmux.internal.core.TraceFrames.num;

import io.zmux.internal.join.Acceptor;
import io.zmux.internal.join.Hello;
import io.zmux.internal.join.HelloPayload;
import io.zmux.seal.SecureCarrierException;
import io.zmux.internal.join.JoinPipe;
import io.zmux.internal.join.Joiner;
import io.zmux.internal.join.Kdf;
import io.zmux.internal.join.Mode;
import io.zmux.internal.join.ReplyPayload;
import io.zmux.internal.join.Suite;
import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameDecoder;
import io.zmux.internal.wire.FrameKind;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.VarInt;
import io.zmux.internal.wire.WireException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Drives one core session through a trace  on a virtual clock. */
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
        boolean joining;
        byte[] hello;
        Acceptor acc;
        byte[] replyIn = new byte[0];

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
        Hello heldHello;
        Joins.PeerJoin pj;
        Joiner joiner;
        boolean confirm;
        byte[] reply;
        String outcome = "";
        int decision;
        long hsDeadline;
        final List<byte[]> msgs = new ArrayList<>();
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
    final CoreConfig config;
    Set<String> modes;
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
        // Exchanges start only when a trace asks for one.
        cfg.epochs(CoreConfig.Epochs.MANUAL);
        io.zmux.internal.wire.Preamble.Tlv greaseTlv = null;
        long pingSeqStart = 0, testRtt = 0;
        int pipeCap = 0;
        unconfirmedCap = io.zmux.ListenerConfig.defaults().maxPendingPerSession();
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
        if (th.containsKey("unconfirmed_cap")) unconfirmedCap = (int) num(th.get("unconfirmed_cap"));
        if (th.get("pipe_rtt_ms") instanceof Map<?, ?>) {
            Map<Long, Long> pr = new java.util.HashMap<>();
            for (var e : map(th.get("pipe_rtt_ms")).entrySet()) pr.put(Long.valueOf(e.getKey()), ms(e.getValue()));
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
                    if (getB(m, "replayable")) throw new IllegalStateException("no carrier early data in Java");
                }
                case "declared_end_to_end" -> cfg.declaredEndToEnd((Boolean) v);
                case "record_modes" -> {
                    modes = new java.util.HashSet<>();
                    for (Object x : list(v)) modes.add((String) x);
                }
                case "pipe_auth" -> {
                    Map<String, Object> m = map(v);
                    if (m.containsKey("psk")) cfg.psk(hex(m.get("psk")));
                }
                default -> { }
            }
        }
        cfg.test(new CoreConfig.TestHooks(true, greaseTlv, pingSeqStart, testRtt, pipeRtt, pipeCap,
                th.containsKey("epoch_locator_prefix") ? hex(th.get("epoch_locator_prefix")) : null));
        cfg.joinEarlyHello(8 << 10);
        config = cfg.build();
        s = new Session(config, 0);
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
            case "peer_join" -> { peerJoin(st); return; }
            case "peer_epoch" -> { peerEpoch(st); return; }
            case "peer_join_finish" -> { peerJoinFinish(st); return; }
            case "peer_confirm" -> { peerConfirm(st); return; }
            case "peer_join_reply" -> { peerJoinReply(st); return; }
            case "api" -> { new TraceApi(this).call(st); return; }
            case "advance" -> { advance(ms(st.get("ms"))); return; }
            default -> { }
        }
        flush();
        switch (op) {
            case "expect_join_hello" -> expectJoinHello(st);
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
        if (via != null && !via.equals("bootstrap")) throw fail("pipe_up via " + via);
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
        if (dir.equals("out") || dir.equals("bidi")) {
            s.setNow(now);
            s.pipeWriteFailed(id, lane, new java.io.IOException("carrier failed"));
        }
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
        if (p.joining) {
            byte[] data = st.get("hex") instanceof String h ? hex(h) : st.get("records") != null ? enc.records(list(st.get("records"))) : new byte[0];
            joinerFeed(p, data);
            return;
        }
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

    /** Delivers bytes on an inbound lane carrier: the first record is routed by its PIPE_LANE. */
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
            if (!c.rejected && !c.attached && c.hsDeadline != 0 && now >= c.hsDeadline) closeCarrier(c, "reject");
        }
        for (RCarrier c : carriers.values()) {
            if (c.source != null && !c.rejected && now >= c.deadline) {
                admission.release(c.source);
                c.source = null;
                c.rejected = true;
            }
        }
    }

    // ---- listener carriers  ----

    final CarrierAdmission admission = new CarrierAdmission(0, 0, 0, 0, 0);
    final CarrierAdmission.SplitPairing pairing = new CarrierAdmission.SplitPairing();

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
            CarrierAdmission.SplitPairing.Result res = pairing.offer(hex(tok), up, c);
            if (res == CarrierAdmission.SplitPairing.Result.REJECT) {
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
            if (admission.take(a) != CarrierAdmission.TAKEN) {
                c.rejected = true;
                return;
            }
            c.source = a;
            c.deadline = admission.deadline(now);
        }
    }

    CarrierInfo carrierInfo(RCarrier c) {
        return new CarrierInfo(c.framing, false, true, c.conn != null, true);
    }

    private static final Map<String, Integer> DIRS = Map.of("up", 0, "down", 1, "bidi", 2);
    private static final List<String> DIR_NAMES = List.of("up", "down", "bidi");
    /** The keys of the wire examples, for a Hello written before the peer's parameters reached the session. */
    private static final byte[] VECTOR_INIT = hex("00112233445566778899aabbccddeeff"), VECTOR_RESP = hex("ffeeddccbbaa99887766554433221100");

    private static Mode mode(Object name) {
        return Mode.valueOf(((String) name).toUpperCase(java.util.Locale.ROOT));
    }

    private static io.zmux.internal.join.Framing joinFraming(Framing f) {
        return f == Framing.RECORDS ? io.zmux.internal.join.Framing.MESSAGE : io.zmux.internal.join.Framing.STREAM;
    }

    private boolean localInitiator() {
        return s.roleKnown() ? s.initiator() : !"responder".equals(header.get("local_role"));
    }

    /** join_psk of a role and epoch, from the secrets both ends derive. */
    private byte[] joinPsk(boolean initiatorRole, long epoch) {
        byte[][] k = sessionKeys();
        try {
            return Kdf.joinPsk(Suite.AES, epoch > 0 ? epochSecrets.get(epoch) : k[0], initiatorRole, k[1]);
        } catch (SecureCarrierException e) {
            throw fail("join psk: " + e);
        }
    }

    /** The session secret and session_crypto, from the local parameters and the peer's, as both ends compute them. */
    private byte[][] sessionKeys() {
        var local = s.localParams();
        byte[] i = local.sessionNonce, r = VECTOR_RESP;
        var setI = local.sessionCrypto;
        io.zmux.internal.join.SessionCrypto setR;
        try {
            // Before its parameters, the peer of a trace declares the minimal sets.
            setR = io.zmux.internal.join.SessionCrypto.of(new long[] {Suite.AES.id()}, new long[] {2});
        } catch (SecureCarrierException e) {
            throw new AssertionError(e);
        }
        boolean li = localInitiator();
        var peer = s.peerParams();
        if (peer != null) {
            r = peer.sessionNonce;
            setR = peer.sessionCrypto;
        } else if (!li) {
            r = VECTOR_INIT;
        }
        if (!li) {
            byte[] tn = i; i = r; r = tn;
            var ts = setI; setI = setR; setR = ts;
        }
        byte[] secret = Kdf.sessionSecret(i, r, new byte[0], config.psk() == null ? new byte[0] : config.psk());
        return new byte[][] {secret, io.zmux.internal.join.SessionCrypto.session(setI, setR)};
    }

    // ---- epochs: the peer's side of the exchange, with Group 2 ----

    private static final io.zmux.internal.join.Group EPOCH_GROUP = io.zmux.internal.join.Group.X25519_MLKEM768;
    /** Every epoch secret derived. */
    private final Map<Long, byte[]> epochSecrets = new HashMap<>();
    /** The peer's offers: private values and body, by epoch. */
    private final Map<Long, io.zmux.internal.join.KeyShare.Initiator> peerOffers = new HashMap<>();
    private final Map<Long, byte[]> peerOfferBodies = new HashMap<>();
    /** The local endpoint's offers: share and body, by epoch. */
    private final Map<Long, byte[][]> localOffers = new HashMap<>();
    /** The last body of each phase and epoch, by "phase/epoch", the local endpoint's and the peer's frames. */
    private final Map<String, byte[]> localBodies = new HashMap<>();
    private final Map<String, Map<String, Object>> peerFrames = new HashMap<>();

    /** The latest derived epoch of a key phase, 0 for none. */
    private long latestEpoch(int phase) {
        long best = 0;
        for (long n : epochSecrets.keySet()) if ((n & 1) == phase && n > best) best = n;
        return best;
    }

    private void deriveEpoch(long n, byte[] offerBody, byte[] answerBody, byte[][] ss) {
        byte[][] k = sessionKeys();
        byte[] prev = n > 1 ? epochSecrets.get(n - 1) : k[0];
        if (prev == null) throw fail("no secret of epoch " + (n - 1));
        epochSecrets.put(n, Kdf.nextEpochSecret(prev, n, k[1], offerBody, answerBody, ss));
    }

    /** A SESSION_EPOCH the local endpoint wrote: whether it repeats its previous one, and the secret of an answer to the peer's offer. */
    private void localEpoch(Frame f, Map<String, Object> o) {
        byte[] body = f.bodyBytes();
        String key = f.phase + "/" + f.value;
        byte[] prev = localBodies.put(key, body);
        o.put("same_as_previous", prev != null && java.util.Arrays.equals(prev, body));
        if (f.phase == 0) {
            localOffers.put(f.value, new byte[][] {f.dataBytes(), body});
        } else {
            var off = peerOffers.remove(f.value);
            if (off != null) {
                try {
                    deriveEpoch(f.value, peerOfferBodies.get(f.value), body, off.finish(f.dataBytes()));
                } catch (SecureCarrierException e) {
                    throw fail("the local answer's share: " + e);
                }
            }
        }
    }

    void peerEpoch(Map<String, Object> st) {
        String phase = (String) st.get("phase");
        long n = num(st.get("epoch"));
        String key = ("offer".equals(phase) ? 0 : 1) + "/" + n;
        Map<String, Object> fr;
        if (getB(st, "repeat")) {
            fr = peerFrames.get(key);
            if (fr == null) throw fail("no previous " + phase + " of epoch " + n + " to repeat");
        } else {
            String loc = st.get("locator") instanceof String l ? l : "";
            byte[] share;
            try {
                switch (phase) {
                    case "offer" -> {
                        var off = io.zmux.internal.join.KeyShare.initiate(EPOCH_GROUP, null, null);
                        share = off.share().share();
                        peerOffers.put(n, off);
                        peerOfferBodies.put(n, io.zmux.internal.wire.FrameEncoder.sessionEpochBody(0, n, share, hex(loc)));
                    }
                    case "answer" -> {
                        byte[][] lo = localOffers.get(n);
                        if (lo != null) {
                            var r = io.zmux.internal.join.KeyShare.respond(new io.zmux.internal.join.KeyShare(EPOCH_GROUP, lo[0]), null, null);
                            share = r.share().share();
                            deriveEpoch(n, lo[1], io.zmux.internal.wire.FrameEncoder.sessionEpochBody(1, n, share, hex(loc)), r.secrets());
                        } else {
                            share = new byte[EPOCH_GROUP.shareLength(false)];
                        }
                    }
                    default -> throw fail("peer_epoch phase " + phase);
                }
            } catch (SecureCarrierException e) {
                throw fail("peer_epoch: " + e);
            }
            if (st.containsKey("share_len")) share = new byte[(int) num(st.get("share_len"))];
            fr = new LinkedHashMap<>();
            fr.put("type", "SESSION_EPOCH");
            fr.put("phase", phase);
            fr.put("epoch", n);
            fr.put("share", TraceFrames.hex(share));
            fr.put("locator", loc);
            peerFrames.put(key, fr);
        }
        Map<String, Object> send = new HashMap<>();
        send.put("pipe", st.get("pipe"));
        send.put("records", List.of(List.of(fr)));
        peerSend(send);
    }

    /** The session's bound on Unconfirmed carriers: the hook unconfirmed_cap, else the listener's default. */
    private int unconfirmedCap;

    void closeCarrier(RCarrier c, String outcome) {
        if (c.pj != null) s.unconfirmedEnded(c.pj);
        c.rejected = true;
        c.outcome = outcome;
        c.buf = new byte[0];
        c.msgs.clear();
        c.heldHello = null;
        c.pj = null;
        if (c.source != null) {
            admission.release(c.source);
            c.source = null;
        }
    }

    private Joins.JoinCarrier joinCarrier(RCarrier c) {
        CarrierInfo info = carrierInfo(c);
        return new Joins.JoinCarrier(!"out".equals(c.dir), !"in".equals(c.dir), info, info, null, false);
    }

    /** The acceptor's handling from a parsed Hello on; a held join stays pending and is offered again. */
    void offerHello(RCarrier c, Hello h) {
        if (c.hsDeadline == 0) c.hsDeadline = Time.add(now, config.handshakeTimeout());
        if (!s.acceptsLocator(h.locator())) {
            closeCarrier(c, "reject");
            return;
        }
        Joins.Opened o = s.openHello(h, joinFraming(c.framing), c.binding == null ? new byte[0] : c.binding);
        drainEvents();
        switch (o.result()) {
            case HOLD, KEY_WAIT -> {
                c.heldHello = h;
                return;
            }
            case ACCEPTED -> { }
            default -> {
                closeCarrier(c, s.error() != null ? "fatal" : "reject");
                return;
            }
        }
        c.heldHello = null;
        if (modes != null && !modes.contains(o.join().mode().name().toLowerCase(java.util.Locale.ROOT))) {
            closeCarrier(c, "reject"); // Local policy: the Pipe ID's bit stays clear.
            return;
        }
        Joins.Claimed cl = s.claimHello(o.join(), joinCarrier(c), io.zmux.carrier.HandshakeShaper.UnitPlan.NONE, 0, 1 << 20, unconfirmedCap);
        drainEvents();
        switch (cl.result()) {
            case ACCEPTED -> {
                if (cl.evicted() != null) {
                    for (RCarrier e : carriers.values()) if (e.pj == cl.evicted()) closeCarrier(e, "evicted");
                }
                c.reply = cl.reply();
                c.pj = o.join();
                c.outcome = "accept";
                c.pipe = o.join().pipe().id();
                peerReadsReply(c);
                if (c.joiner != null && c.confirm) writeConfirm(c);
            }
            case REFUSED -> {
                c.reply = cl.reply();
                peerReadsReply(c);
                closeCarrier(c, "refuse");
            }
            default -> closeCarrier(c, "reject");
        }
    }

    private static void peerReadsReply(RCarrier c) {
        if (c.joiner == null) return;
        try {
            c.decision = c.joiner.readReply(c.reply, 0, c.reply.length).decision();
        } catch (SecureCarrierException e) {
            throw fail("carrier " + c.name + ": the peer cannot read the Reply: " + e);
        }
    }

    private void writeConfirm(RCarrier c) {
        try {
            readConfirm(c, c.joiner.writeConfirm(0));
        } catch (SecureCarrierException e) {
            throw fail("carrier " + c.name + ": confirmation: " + e);
        }
    }

    /** Feeds confirmation bytes to an Unconfirmed carrier and attaches the pipe once Confirmed. */
    private void readConfirm(RCarrier c, byte[] b) {
        try {
            c.pj.readConfirm(b, 0, b.length);
        } catch (SecureCarrierException e) {
            if (e.kind() != SecureCarrierException.Kind.INCOMPLETE) closeCarrier(c, "reject");
            return;
        }
        Joins.PeerJoin pj = c.pj;
        Joins.JoinCarrier jc = joinCarrier(c);
        Joins.JoinResult res = s.confirmJoin(pj, jc);
        drainEvents();
        if (res != Joins.JoinResult.ACCEPTED) {
            closeCarrier(c, c.outcome);
            return;
        }
        c.pj = null;
        c.attached = true;
        c.lane = 0;
        if (c.source != null) {
            admission.release(c.source);
            c.source = null;
        }
        JoinPipe j = pj.pipe();
        RPipe p = new RPipe(j.id());
        p.framing = c.framing;
        p.in = jc.hasIn() && j.direction() != 1;
        p.out = jc.hasOut() && j.direction() != 0;
        pipes.put(p.id, p);
        if (c.conn != null) s.pipes().setCarrierConn(p.id, c.conn);
        feedCarrier(c);
    }

    /** Offers held joins again. */
    void retryHeld() {
        for (RCarrier c : new ArrayList<>(carriers.values())) {
            if (c.heldHello != null && !c.rejected) offerHello(c, c.heldHello);
        }
    }

    void feedCarrier(RCarrier c) {
        if (!c.attached) return;
        for (byte[] m : c.msgs) {
            ByteBuffer bb = ByteBuffer.wrap(m);
            s.receiveMessage(c.pipe, 0, new ReadBuf(bb, null), bb, 0, m.length, now);
            drainEvents();
        }
        c.msgs.clear();
        if (c.buf.length == 0) return;
        ByteBuffer bb = ByteBuffer.wrap(c.buf);
        int n = s.receive(c.pipe, c.lane, new ReadBuf(bb, null), bb, 0, c.buf.length, now);
        c.buf = java.util.Arrays.copyOfRange(c.buf, n, c.buf.length);
        drainEvents();
    }

    /** The Group a joiner takes by default: Group 1 when both ends declare it, otherwise Group 2. */
    private io.zmux.internal.join.Group defaultGroup() {
        var local = s.localParams().sessionCrypto;
        // Before the peer's parameters only Group 2 is sure to be usable.
        var peer = s.peerParams() != null ? s.peerParams().sessionCrypto : null;
        boolean one = local != null && peer != null && local.hasGroup(1) && peer.hasGroup(1);
        return one ? io.zmux.internal.join.Group.X25519 : io.zmux.internal.join.Group.X25519_MLKEM768;
    }

    void peerJoin(Map<String, Object> st) {
        RCarrier c = carriers.get((String) st.get("carrier"));
        if (c == null) throw fail("unknown carrier");
        Mode m = st.get("mode") != null ? mode(st.get("mode")) : Mode.TRUSTED;
        int phase = (int) getU(st, "phase");
        byte[] psk = joinPsk(!localInitiator(), latestEpoch(phase));
        if (getB(st, "bad_key")) psk[0] ^= (byte) 0xff;
        byte[] hello;
        try {
            io.zmux.internal.join.Group group = st.get("group") != null ? io.zmux.internal.join.Group.of(num(st.get("group"))) : defaultGroup();
            Joiner j = Joiner.create(Suite.AES, m, joinFraming(c.framing), hex(st.get("locator")), psk,
                    st.get("binding") instanceof String b ? hex(b) : new byte[0], group, phase);
            JoinPipe pipe = new JoinPipe(num(st.get("pipe")), DIRS.get((String) st.get("direction")), getU(st, "fate_group"), getU(st, "priority"), getB(st, "standby"));
            HelloPayload payload = new HelloPayload(pipe, st.get("batch") != null ? enc.body(st.get("batch")) : null, 0);
            hello = switch (st.get("key_share") instanceof String k ? k : "") {
                case "" -> j.writeHello(payload);
                case "missing" -> j.writeHelloWithShare(payload, null);
                case "bad_length" -> j.writeHelloWithShare(payload, new io.zmux.internal.join.KeyShare(group, new byte[group.shareLength(true) - 1]));
                case "low_order" -> j.writeHelloWithShare(payload, new io.zmux.internal.join.KeyShare(group, new byte[group.shareLength(true)]));
                default -> throw fail("key_share " + st.get("key_share"));
            };
            c.joiner = j;
            c.confirm = !(st.get("confirm") instanceof Boolean v) || v;
            if (c.rejected) {
                checkOutcome(c, st);
                return;
            }
            offerHello(c, Hello.parse(hello, 0, hello.length, joinFraming(c.framing)));
        } catch (SecureCarrierException e) {
            throw fail("peer_join: " + e);
        }
        if (getB(st, "hold")) {
            if (c.heldHello == null) throw fail("carrier " + c.name + ": the join was not held");
            return;
        }
        checkOutcome(c, st);
    }

    private static void checkOutcome(RCarrier c, Map<String, Object> st) {
        String want = (String) st.get("outcome");
        if (!want.equals(c.outcome)) throw fail("carrier " + c.name + ": outcome '" + c.outcome + "', want '" + want + "' (held " + (c.heldHello != null) + ")");
        switch (want) {
            case "accept", "refuse" -> {
                if ((c.decision == 1) != want.equals("refuse")) throw fail("carrier " + c.name + ": decision " + c.decision + ", want " + want);
            }
            default -> {
                if (c.reply != null && c.reply.length != 0) throw fail("carrier " + c.name + ": wrote a Reply, want " + want);
            }
        }
    }

    void peerJoinFinish(Map<String, Object> st) {
        RCarrier c = carriers.get((String) st.get("carrier"));
        if (c == null) throw fail("unknown carrier");
        retryHeld();
        if (c.heldHello != null) throw fail("carrier " + c.name + ": the join is still held");
        checkOutcome(c, st);
    }

    void peerConfirm(Map<String, Object> st) {
        RCarrier c = carriers.get((String) st.get("carrier"));
        if (c == null || c.pj == null) throw fail("carrier " + st.get("carrier") + " is not Unconfirmed");
        writeConfirm(c);
    }

    /** Every local join writes its Hello once allowed. */
    void writeHellos() {
        for (RPipe p : new ArrayList<>(pipes.values())) {
            if (!p.joining || p.hello != null) continue;
            try {
                if (s.joinPrepare(p.id, now) != null) p.hello = s.joinHello(p.id, joinFraming(p.framing), io.zmux.carrier.HandshakeShaper.UnitPlan.NONE, 0, now);
            } catch (SecureCarrierException | CoreException e) {
                p.joining = false;
            }
            drainEvents();
        }
    }

    private HelloPayload readHello(RPipe p) {
        if (p.hello == null) throw fail("pipe " + p.id + ": no Hello written");
        try {
            Hello h = Hello.parseExact(p.hello, 0, p.hello.length, joinFraming(p.framing));
            helloRead = h;
            p.acc = Acceptor.accept(h, joinFraming(p.framing), joinPsk(localInitiator(), latestEpoch(h.phase())), new byte[0]);
            return p.acc.payload();
        } catch (SecureCarrierException e) {
            throw fail("pipe " + p.id + ": Hello does not verify: " + e);
        }
    }

    private Hello helloRead;

    void expectJoinHello(Map<String, Object> st) {
        RPipe p = pipes.get(num(st.get("pipe")));
        if (p == null) throw fail("no pipe " + st.get("pipe"));
        HelloPayload pl = readHello(p);
        RLane l = p.lanes.get(0L);
        if (l != null && !l.frames.isEmpty()) throw fail("frames besides the Hello:\n" + TraceMatch.dump(l.frames));
        JoinPipe got = pl.pipe();
        if (got.id() != p.id || !DIR_NAMES.get(got.direction()).equals(st.get("direction")) || got.fateGroup() != getU(st, "fate_group")
                || got.priority() != getU(st, "priority") || got.standby() != getB(st, "standby")) {
            throw fail("Hello fields " + got);
        }
        if (!java.util.Arrays.equals(helloRead.locator(), hex(st.get("locator")))) throw fail("Hello locator");
        if (helloRead.mode() != mode(st.get("mode"))) throw fail("Hello mode " + helloRead.mode());
        if (helloRead.phase() != getU(st, "phase")) throw fail("Hello key phase " + helloRead.phase());
        if (st.get("batch") instanceof List<?> fs) {
            List<Map<String, Object>> frames = new ArrayList<>(), pats = new ArrayList<>();
            byte[] b = pl.firstBatch() == null ? new byte[0] : pl.firstBatch();
            ByteBuffer bb = ByteBuffer.wrap(b);
            for (int i = 0; i < b.length; ) {
                Frame f = new Frame();
                try {
                    i += decoder.decode(bb, i, b.length, f);
                } catch (WireException e) {
                    throw fail("first batch: " + e.getMessage());
                }
                frames.add(TraceFrames.frameObj(f));
            }
            for (Object x : fs) pats.add(map(x));
            String err = TraceMatch.match("exact", pats, frames);
            if (err != null) throw fail("first batch: " + err + "\nframes:\n" + TraceMatch.dump(frames));
        }
    }

    void peerJoinReply(Map<String, Object> st) {
        RPipe p = pipes.get(num(st.get("pipe")));
        if (p == null) throw fail("no pipe " + st.get("pipe"));
        if (p.acc == null) readHello(p);
        byte[] reply;
        try {
            reply = p.acc.writeReply(new ReplyPayload((int) getU(st, "decision"), null, 0));
        } catch (SecureCarrierException e) {
            throw fail("Reply: " + e);
        }
        if (getB(st, "leading_parameters")) {
            ByteBuffer blk = ByteBuffer.allocate(2048), tp = ByteBuffer.allocate(4096);
            s.localParams().encode(blk);
            io.zmux.internal.wire.FrameEncoder.transportParameters(tp, java.util.Arrays.copyOf(blk.array(), blk.position()));
            byte[] rec = java.util.Arrays.copyOf(tp.array(), tp.position());
            if (p.framing == Framing.RECORDS) {
                joinerFeed(p, rec);
            } else {
                ByteBuffer out = ByteBuffer.allocate(rec.length + reply.length + 8);
                VarInt.put(out, rec.length);
                out.put(rec).put(reply);
                reply = java.util.Arrays.copyOf(out.array(), out.position());
            }
        }
        joinerFeed(p, reply);
    }

    /** Runs the handshake over bytes read from a JOINING carrier and checks the confirmation of a Reply with decision 0. */
    void joinerFeed(RPipe p, byte[] data) {
        if (!p.joining) return;
        byte[] in = new byte[p.replyIn.length + data.length];
        System.arraycopy(p.replyIn, 0, in, 0, p.replyIn.length);
        System.arraycopy(data, 0, in, p.replyIn.length, data.length);
        p.replyIn = in;
        while (p.joining) {
            Joins.JoinStep step = s.verifyReply(p.id, p.replyIn, 0, p.replyIn.length, now);
            Joins.Finished finished = step.status() == Joins.Step.PROVEN ? s.joinFinish(p.id, io.zmux.carrier.HandshakeShaper.UnitPlan.NONE, 0, now) : null;
            drainEvents();
            p.replyIn = java.util.Arrays.copyOfRange(p.replyIn, step.consumed(), p.replyIn.length);
            switch (step.status()) {
                case MORE -> {
                    if (step.consumed() == 0) return;
                }
                case PROVEN -> {
                    p.joining = false;
                    if (p.acc != null) {
                        try {
                            p.acc.readConfirm(finished.confirm(), 0, finished.confirm().length);
                        } catch (SecureCarrierException e) {
                            throw fail("pipe " + p.id + ": confirmation: " + e);
                        }
                    }
                    if (p.replyIn.length > 0) {
                        byte[] rest = p.replyIn;
                        p.replyIn = new byte[0];
                        feed(p.id, p.lane(0), rest);
                    }
                }
                default -> {
                    p.joining = false;
                    p.replyIn = new byte[0];
                }
            }
        }
    }

    /** Bytes on a listener carrier: classified first, then the handshake's, and after Confirmed the attached pipe's. */
    void listenerSend(RCarrier c, Map<String, Object> st) {
        if (c.framing == Framing.RECORDS) {
            List<byte[]> msgs = new ArrayList<>();
            if (st.get("messages") instanceof List<?> ml) for (Object m : ml) msgs.add(enc.body(m));
            else if (st.get("messages_hex") instanceof List<?> mh) for (Object m : mh) msgs.add(hex(m));
            for (byte[] m : msgs) {
                if (c.rejected) return;
                if (c.attached) {
                    ByteBuffer bb = ByteBuffer.wrap(m);
                    s.receiveMessage(c.pipe, 0, new ReadBuf(bb, null), bb, 0, m.length, now);
                    drainEvents();
                    continue;
                }
                if (c.pj != null || c.heldHello != null) {
                    c.msgs.add(m);
                    continue;
                }
                try {
                    offerHello(c, Hello.parse(m, 0, m.length, io.zmux.internal.join.Framing.MESSAGE));
                } catch (SecureCarrierException e) {
                    closeCarrier(c, "reject");
                    return;
                }
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
        if (c.pj != null || c.heldHello != null) return;
        CarrierAdmission.Classified cl = CarrierAdmission.classify(ByteBuffer.wrap(c.buf), 0, c.buf.length, 65535);
        switch (cl.kind()) {
            case REJECT, SESSION -> {
                closeCarrier(c, "reject");
                return;
            }
            case MORE -> {
                return;
            }
            case JOIN -> {
                Hello h;
                try {
                    h = Hello.parse(c.buf, 0, c.buf.length, io.zmux.internal.join.Framing.STREAM);
                } catch (SecureCarrierException e) {
                    if (e.kind() != SecureCarrierException.Kind.INCOMPLETE) closeCarrier(c, "reject");
                    return;
                }
                c.buf = java.util.Arrays.copyOfRange(c.buf, h.consumed(), c.buf.length);
                offerHello(c, h);
                return;
            }
            default -> { }
        }
        if (cl.firstRecordLength() == 0) return;
        if (c.source != null) {
            admission.release(c.source);
            c.source = null;
        }
        c.via = "lane";
        carrierSend(c, Map.of("hex", ""));
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
                .mode(Mode.TRUSTED)
                .recvPriority(getU(args, "receive_priority"))
                .recvStandby(getB(args, "receive_standby"))
                .weight(args.containsKey("weight") ? getU(args, "weight") : 1)
                .in(INFO).out(INFO);
        if (args.get("send_priority") != null) ob.sendPriority(num(args.get("send_priority")));
        if (args.get("send_standby") instanceof Boolean b) ob.sendStandby(b);
        if (args.get("probation") != null) ob.probation(ms(args.get("probation")));
        RPipe p = new RPipe(id);
        p.joining = true;
        p.in = !dir.equals("out");
        p.out = !dir.equals("in");
        pipes.put(id, p);
        long r = s.addPipe(ob.build());
        return r < 0 ? Map.of("pending", true) : Map.of("ok", true);
    }

    void drainEvents() {
        for (Event e; (e = s.outbox().pollEvent()) != null; ) events.add(e);
        s.outbox().takeWakes(new ArrayList<>());
        List<Action> acts = new ArrayList<>();
        s.outbox().takeActions(acts);
        List<long[]> shut = new ArrayList<>();
        for (Action a : acts) {
            RPipe p = pipes.get(a.pipe());
            if (p == null) continue;
            switch (a.kind()) {
                case SHUTDOWN -> {
                    RLane l = p.lane(a.lane());
                    if (l.closed.isEmpty()) l.closed = "graceful";
                    shut.add(new long[] {a.pipe(), a.lane()});
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
        // A driver's carrier reports its write side shut once the shutdown went out.
        if (!shut.isEmpty()) {
            for (long[] x : shut) s.outputShut(x[0], x[1], now);
            drainEvents();
        }
    }

    /** Polls every writable outbound lane until nothing comes out. */
    void flush() {
        writeHellos();
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
                if (f.kind == FrameKind.SESSION_EPOCH) localEpoch(f, o);
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
            default -> "other";
        };
        return Map.entry(name, o);
    }

    void expectEvent(Map<String, Object> st, boolean want) {
        String name = (String) st.get("event");
        if (name.equals("datagram_dropped")) {
            // Dropped datagrams are reported by the statistics, not as an event.
            long n = s.stats.datagramsDroppedRecv();
            if (want != (n > 0) || (st.get("fields") instanceof Map<?, ?> f && f.get("count") instanceof Number c && c.longValue() != n)) {
                throw fail("datagrams dropped " + n);
            }
            return;
        }
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
        long id = num(st.get("pipe"));
        RPipe p = pipes.get(id);
        String how = (String) st.get("how");
        if (p == null) {
            // A carrier whose join was accepted but never attached (TRACES.md 5.1): closed when the session closed it.
            for (RCarrier c : carriers.values()) {
                if (c.pipe != id || c.attached || (!"accept".equals(c.outcome) && !"evicted".equals(c.outcome))) continue;
                if (!c.rejected) throw fail("pipe " + id + ": its Unconfirmed carrier " + c.name + " is open");
                if (!how.equals("any") && !how.equals("abort")) throw fail("pipe " + id + ": its Unconfirmed carrier was closed, not " + how);
                return;
            }
            throw fail("no pipe " + id);
        }
        java.util.Collection<RLane> lanes = st.containsKey("lane") ? List.of(p.lane(num(st.get("lane")))) : p.lanes.values();
        for (RLane l : lanes) {
            if (l.closed.isEmpty() || (!how.equals("any") && !l.closed.equals(how))) throw fail("pipe closed '" + l.closed + "', want " + how);
        }
    }
}
