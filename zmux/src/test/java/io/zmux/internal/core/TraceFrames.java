package io.zmux.internal.core;

import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameEncoder;
import io.zmux.internal.wire.TransportParams;
import io.zmux.carrier.VarInt;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/** JSON frames of TRACES.md Section 6: encoding peer frames, describing local frames, matching patterns. */
final class TraceFrames {
    private TraceFrames() {}

    static final Map<String, Long> PARAM_IDS = Map.ofEntries(
            Map.entry("max_idle_timeout", TransportParams.ID_MAX_IDLE_TIMEOUT),
            Map.entry("initial_max_data", TransportParams.ID_INITIAL_MAX_DATA),
            Map.entry("initial_max_stream_data_bidi_local", TransportParams.ID_MAX_STREAM_DATA_BIDI_LOCAL),
            Map.entry("initial_max_stream_data_bidi_remote", TransportParams.ID_MAX_STREAM_DATA_BIDI_REMOTE),
            Map.entry("initial_max_stream_data_uni", TransportParams.ID_MAX_STREAM_DATA_UNI),
            Map.entry("initial_max_streams_bidi", TransportParams.ID_MAX_STREAMS_BIDI),
            Map.entry("initial_max_streams_uni", TransportParams.ID_MAX_STREAMS_UNI),
            Map.entry("reset_stream_at", TransportParams.ID_RESET_STREAM_AT),
            Map.entry("max_datagram_frame_size", TransportParams.ID_MAX_DATAGRAM_FRAME_SIZE),
            Map.entry("max_record_size", TransportParams.ID_MAX_RECORD_SIZE),
            Map.entry("zmux_role", TransportParams.ID_ROLE), Map.entry("zmux_goaway", TransportParams.ID_GOAWAY),
            Map.entry("zmux_preamble", TransportParams.ID_PREAMBLE), Map.entry("zmux_priority", TransportParams.ID_PRIORITY),
            Map.entry("zmux_datagram_flows", TransportParams.ID_DATAGRAM_FLOWS),
            Map.entry("zmux_carrier_datagrams", TransportParams.ID_CARRIER_DATAGRAMS),
            Map.entry("zmux_multipath", TransportParams.ID_MULTIPATH),
            Map.entry("zmux_ack_threshold", TransportParams.ID_ACK_THRESHOLD),
            Map.entry("zmux_pipe_ack", TransportParams.ID_PIPE_ACK),
            Map.entry("zmux_max_ack_delay", TransportParams.ID_MAX_ACK_DELAY),
            Map.entry("zmux_max_lanes", TransportParams.ID_MAX_LANES), Map.entry("zmux_resume", TransportParams.ID_RESUME));

    static final Set<String> EMPTY_PARAMS = Set.of("reset_stream_at", "zmux_goaway", "zmux_priority",
            "zmux_datagram_flows", "zmux_pipe_ack");

    static byte[] hex(Object v) {
        return HexFormat.of().parseHex((String) v);
    }

    static String hex(byte[] b) {
        return HexFormat.of().formatHex(b);
    }

    static long num(Object v) {
        return switch (v) {
            case Long l -> l;
            case Integer i -> i;
            case BigInteger b -> b.longValue();
            case Double d -> d.longValue();
            case Boolean b -> b ? 1 : 0;
            case String s -> Long.parseUnsignedLong(s);
            default -> throw new IllegalArgumentException("not a number: " + v);
        };
    }

    static long getU(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v == null ? 0 : num(v);
    }

    static boolean getB(Map<String, Object> m, String k) {
        return m.get(k) instanceof Boolean b && b;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> map(Object o) {
        return (Map<String, Object>) o;
    }

    @SuppressWarnings("unchecked")
    static List<Object> list(Object o) {
        return (List<Object>) o;
    }

    static byte[] varint(long v) {
        ByteBuffer b = ByteBuffer.allocate(8);
        VarInt.put(b, v);
        return java.util.Arrays.copyOf(b.array(), b.position());
    }

    /** Encodes a parameter value from its JSON form. */
    static byte[] encodeParamValue(String name, Object v) {
        if (EMPTY_PARAMS.contains(name)) return new byte[0];
        ByteBuffer b = ByteBuffer.allocate(64);
        switch (name) {
            case "zmux_role" -> {
                Map<String, Object> m = map(v);
                long role = Map.of("initiator", 0L, "responder", 1L, "auto", 2L).get((String) m.get("role"));
                VarInt.put(b, role);
                if (m.containsKey("nonce")) b.putLong(num(m.get("nonce")));
            }
            case "zmux_multipath" -> {
                Map<String, Object> m = map(v);
                b.put((byte) num(m.get("flags")));
                b.put(hex(m.get("nonce")));
                byte[] loc = hex(m.get("locator"));
                b.put((byte) loc.length);
                b.put(loc);
            }
            case "zmux_max_lanes" -> {
                Map<String, Object> m = map(v);
                VarInt.put(b, num(m.get("max_lanes")));
                b.put(hex(m.get("lane_key")));
            }
            default -> VarInt.put(b, num(v));
        }
        return java.util.Arrays.copyOf(b.array(), b.position());
    }

    /** Builds parameters from a profile's tp map without validation. */
    static TransportParams paramsFromMap(Map<String, Object> tp) {
        TransportParams p = new TransportParams();
        for (var e : tp.entrySet()) {
            Object v = e.getValue();
            switch (e.getKey()) {
                case "max_idle_timeout" -> { p.present |= TransportParams.IDLE; p.maxIdleTimeoutMillis = num(v); }
                case "initial_max_data" -> { p.present |= TransportParams.MAX_DATA; p.initialMaxData = num(v); }
                case "initial_max_stream_data_bidi_local" -> { p.present |= TransportParams.BIDI_LOCAL; p.maxStreamDataBidiLocal = num(v); }
                case "initial_max_stream_data_bidi_remote" -> { p.present |= TransportParams.BIDI_REMOTE; p.maxStreamDataBidiRemote = num(v); }
                case "initial_max_stream_data_uni" -> { p.present |= TransportParams.UNI; p.maxStreamDataUni = num(v); }
                case "initial_max_streams_bidi" -> { p.present |= TransportParams.STREAMS_BIDI; p.maxStreamsBidi = num(v); }
                case "initial_max_streams_uni" -> { p.present |= TransportParams.STREAMS_UNI; p.maxStreamsUni = num(v); }
                case "reset_stream_at" -> p.present |= TransportParams.RESET_AT;
                case "max_datagram_frame_size" -> { p.present |= TransportParams.DATAGRAM; p.maxDatagramFrameSize = num(v); }
                case "max_record_size" -> { p.present |= TransportParams.RECORD_SIZE; p.maxRecordSize = num(v); }
                case "zmux_role" -> {
                    Map<String, Object> m = map(v);
                    p.present |= TransportParams.ROLE;
                    p.role = (int) (long) Map.of("initiator", 0L, "responder", 1L, "auto", 2L).get((String) m.get("role"));
                    if (m.containsKey("nonce")) p.roleNonce = num(m.get("nonce"));
                }
                case "zmux_goaway" -> p.present |= TransportParams.GOAWAY;
                case "zmux_preamble" -> { p.present |= TransportParams.PREAMBLE; p.maxPreambleSize = num(v); }
                case "zmux_priority" -> p.present |= TransportParams.PRIORITY;
                case "zmux_datagram_flows" -> p.present |= TransportParams.FLOWS;
                case "zmux_multipath" -> {
                    Map<String, Object> m = map(v);
                    p.present |= TransportParams.MULTIPATH;
                    p.secretFlags = (int) num(m.get("flags"));
                    System.arraycopy(hex(m.get("nonce")), 0, p.sessionNonce, 0, 16);
                    p.locator = hex(m.get("locator"));
                }
                case "zmux_ack_threshold" -> { p.present |= TransportParams.ACK_THRESHOLD; p.ackThreshold = num(v); }
                case "zmux_pipe_ack" -> p.present |= TransportParams.PIPE_ACK;
                case "zmux_carrier_datagrams" -> p.present |= TransportParams.CARRIER_DATAGRAMS;
                case "zmux_max_ack_delay" -> { p.present |= TransportParams.ACK_DELAY; p.maxAckDelayMillis = num(v); }
                case "zmux_max_lanes" -> {
                    Map<String, Object> m = map(v);
                    p.present |= TransportParams.LANES;
                    p.maxLanes = num(m.get("max_lanes"));
                    System.arraycopy(hex(m.get("lane_key")), 0, p.laneKey, 0, 16);
                }
                case "zmux_resume" -> { p.present |= TransportParams.RESUME; p.resumeGraceMillis = num(v); }
                default -> throw new IllegalArgumentException("unknown parameter " + e.getKey());
            }
        }
        return p;
    }

    /** Encodes peer frames; sent resolves {"$sent": pipe, "lane": l, "plus": n} placeholders. */
    static final class Encoder {
        final BiFunction<Long, Long, Long> sent;

        Encoder(BiFunction<Long, Long, Long> sent) {
            this.sent = sent;
        }

        void frame(ByteBuffer b, Map<String, Object> f) {
            String type = (String) f.get("type");
            switch (type) {
                case "PADDING" -> {
                    long n = f.containsKey("count") ? num(f.get("count")) : 1;
                    for (long i = 0; i < n; i++) b.put((byte) 0);
                }
                case "RAW" -> b.put(hex(f.get("bytes")));
                case "REPEAT" -> {
                    long n = num(f.get("count"));
                    for (long i = 0; i < n; i++) frame(b, map(f.get("frame")));
                }
                case "STREAM" -> {
                    byte[] data = hex(f.get("data"));
                    long off = getU(f, "off");
                    boolean hasOff = f.get("has_off") instanceof Boolean x ? x : off > 0;
                    boolean hasLen = f.get("has_len") instanceof Boolean x ? x : true;
                    int t = 0x08 | (hasOff ? 4 : 0) | (hasLen ? 2 : 0) | (getB(f, "fin") ? 1 : 0);
                    b.put((byte) t);
                    VarInt.put(b, getU(f, "id"));
                    if (hasOff) VarInt.put(b, off);
                    if (hasLen) VarInt.put(b, data.length);
                    b.put(data);
                }
                case "RESET_STREAM" -> FrameEncoder.resetStream(b, getU(f, "id"), getU(f, "code"), getU(f, "final_size"));
                case "RESET_STREAM_AT" -> FrameEncoder.resetStreamAt(b, getU(f, "id"), getU(f, "code"), getU(f, "final_size"), getU(f, "reliable_size"));
                case "STOP_SENDING" -> FrameEncoder.stopSending(b, getU(f, "id"), getU(f, "code"));
                case "MAX_DATA" -> FrameEncoder.maxData(b, getU(f, "max"));
                case "MAX_STREAM_DATA" -> FrameEncoder.maxStreamData(b, getU(f, "id"), getU(f, "max"));
                case "MAX_STREAMS" -> FrameEncoder.maxStreams(b, getB(f, "bidi"), getU(f, "max"));
                case "DATA_BLOCKED" -> FrameEncoder.dataBlocked(b, getU(f, "limit"));
                case "STREAM_DATA_BLOCKED" -> FrameEncoder.streamDataBlocked(b, getU(f, "id"), getU(f, "limit"));
                case "STREAMS_BLOCKED" -> FrameEncoder.streamsBlocked(b, getB(f, "bidi"), getU(f, "limit"));
                case "CONNECTION_CLOSE" -> FrameEncoder.close(b, getB(f, "app"), getU(f, "code"), getU(f, "frame_type"),
                        f.get("reason") instanceof String r ? hex(r) : new byte[0]);
                case "DATAGRAM" -> {
                    byte[] data = hex(f.get("data"));
                    boolean hasLen = f.get("has_len") instanceof Boolean x ? x : true;
                    FrameEncoder.datagramHeader(b, data.length, hasLen);
                    b.put(data);
                }
                case "QX_TRANSPORT_PARAMETERS" -> {
                    ByteBuffer block = ByteBuffer.allocate(4096);
                    for (Object pv : list(f.get("params"))) {
                        Map<String, Object> p = map(pv);
                        Object id = p.get("id");
                        String name = id instanceof String s ? s : "";
                        long pid = id instanceof String s ? PARAM_IDS.get(s) : num(id);
                        byte[] v = p.containsKey("raw") ? hex(p.get("raw")) : encodeParamValue(name, p.get("value"));
                        VarInt.put(block, pid);
                        VarInt.put(block, v.length);
                        block.put(v);
                    }
                    FrameEncoder.transportParameters(b, java.util.Arrays.copyOf(block.array(), block.position()));
                }
                case "QX_PING" -> FrameEncoder.ping(b, getB(f, "response"), getU(f, "seq"));
                case "GOAWAY" -> FrameEncoder.goAway(b, getU(f, "max_bidi"), getU(f, "max_uni"), getU(f, "code"));
                case "PRIORITY_UPDATE" -> FrameEncoder.priorityUpdate(b, getU(f, "id"), getU(f, "seq"), (int) getU(f, "urgency"), getB(f, "incremental"));
                case "PIPE_JOIN" -> {
                    int dir = Map.of("up", 0, "down", 1, "bidi", 2).get((String) f.get("direction"));
                    FrameEncoder.joinMacInput(b, hex(f.get("locator")), getU(f, "pipe"), dir, getB(f, "carrier_bound"),
                            getB(f, "standby"), getU(f, "fate_group"), getU(f, "priority"));
                    b.put(hex(f.get("token")));
                }
                case "PIPE_ACK" -> {
                    List<Object> ents = list(f.get("entries"));
                    FrameEncoder.pipeAckHeader(b, ents.size());
                    for (Object ev : ents) {
                        Map<String, Object> e = map(ev);
                        VarInt.put(b, getU(e, "pipe"));
                        VarInt.put(b, getU(e, "lane"));
                        Object pos = e.get("position");
                        if (pos instanceof Map<?, ?> m && m.containsKey("$sent")) {
                            Map<String, Object> pm = map(pos);
                            long v = sent.apply(num(pm.get("$sent")), getU(pm, "lane")) + getU(pm, "plus");
                            VarInt.putN(b, v, 4);
                        } else {
                            VarInt.put(b, num(pos));
                        }
                        VarInt.put(b, getU(e, "delay"));
                    }
                }
                case "PIPE_LANE" -> FrameEncoder.pipeLane(b, hex(f.get("lane_key")), getU(f, "pipe"), getU(f, "lane"));
                case "PIPE_PING" -> FrameEncoder.pipePing(b);
                case "PIPE_ABANDON" -> FrameEncoder.pipeAbandon(b, getU(f, "pipe"), getU(f, "code"));
                case "PIPE_STATUS" -> FrameEncoder.pipeStatus(b, getU(f, "pipe"), getU(f, "seq"),
                        "standby".equals(f.get("status")) ? 1 : 0, getU(f, "priority"));
                default -> throw new IllegalArgumentException("unknown frame type " + type);
            }
        }

        byte[] body(Object rec) {
            ByteBuffer b = ByteBuffer.allocate(1 << 20);
            for (Object fv : list(rec)) frame(b, map(fv));
            return java.util.Arrays.copyOf(b.array(), b.position());
        }

        byte[] records(List<Object> recs) {
            ByteBuffer out = ByteBuffer.allocate(4 << 20);
            for (Object r : recs) {
                byte[] body = body(r);
                VarInt.put(out, body.length);
                out.put(body);
            }
            return java.util.Arrays.copyOf(out.array(), out.position());
        }
    }

    /** A decoded frame as the field map of TRACES.md Section 6. */
    static Map<String, Object> frameObj(Frame f) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("type", f.kind.wireName());
        switch (f.kind) {
            case PADDING -> o.put("count", (long) f.count);
            case STREAM -> {
                o.put("id", f.streamId);
                o.put("off", f.offset);
                o.put("data", hex(f.dataBytes()));
                o.put("fin", f.fin);
                o.put("has_off", f.hasOffset);
                o.put("has_len", f.hasLength);
            }
            case RESET_STREAM -> { o.put("id", f.streamId); o.put("code", f.code); o.put("final_size", f.finalSize); }
            case RESET_STREAM_AT -> {
                o.put("id", f.streamId); o.put("code", f.code); o.put("final_size", f.finalSize); o.put("reliable_size", f.reliableSize);
            }
            case STOP_SENDING -> { o.put("id", f.streamId); o.put("code", f.code); }
            case MAX_DATA -> o.put("max", f.value);
            case MAX_STREAM_DATA -> { o.put("id", f.streamId); o.put("max", f.value); }
            case MAX_STREAMS -> { o.put("bidi", f.bidi); o.put("max", f.value); }
            case DATA_BLOCKED -> o.put("limit", f.value);
            case STREAM_DATA_BLOCKED -> { o.put("id", f.streamId); o.put("limit", f.value); }
            case STREAMS_BLOCKED -> { o.put("bidi", f.bidi); o.put("limit", f.value); }
            case CONNECTION_CLOSE -> {
                o.put("app", f.application); o.put("code", f.code); o.put("reason", hex(f.dataBytes()));
                if (!f.application) o.put("frame_type", f.errorFrameType);
            }
            case DATAGRAM -> { o.put("data", hex(f.dataBytes())); o.put("has_len", f.hasLength); }
            case TRANSPORT_PARAMETERS -> o.put("block", f.dataBytes());
            case PING -> { o.put("response", f.response); o.put("seq", f.value); }
            case GOAWAY -> { o.put("max_bidi", f.maxBidi); o.put("max_uni", f.maxUni); o.put("code", f.code); }
            case PRIORITY_UPDATE -> {
                o.put("id", f.streamId); o.put("seq", f.seq); o.put("urgency", (long) f.urgency); o.put("incremental", f.incremental);
            }
            case PIPE_JOIN -> {
                Frame.Join j = f.join;
                byte[] loc = new byte[j.locatorLen];
                f.src.get(j.locatorPos, loc);
                byte[] tok = new byte[16];
                f.src.get(j.tokenPos, tok);
                o.put("locator", hex(loc)); o.put("pipe", j.pipeId);
                o.put("direction", List.of("up", "down", "bidi").get(j.direction));
                o.put("carrier_bound", j.carrierBound); o.put("standby", j.standby);
                o.put("fate_group", j.fateGroup); o.put("priority", j.priority); o.put("token", hex(tok));
            }
            case PIPE_ACK -> {
                List<Object> ents = new ArrayList<>();
                Frame.AckEntries it = f.entries(new Frame.AckEntries());
                while (it.next()) {
                    Map<String, Object> e = new LinkedHashMap<>();
                    e.put("pipe", it.pipeId); e.put("lane", it.lane); e.put("position", it.position); e.put("delay", it.delay);
                    ents.add(e);
                }
                o.put("entries", ents);
            }
            case PIPE_LANE -> {
                byte[] key = new byte[16];
                f.src.get(f.laneKeyPos, key);
                o.put("lane_key", hex(key)); o.put("pipe", f.pipeId); o.put("lane", f.lane);
            }
            case PIPE_ABANDON -> { o.put("pipe", f.pipeId); o.put("code", f.code); }
            case PIPE_STATUS -> {
                o.put("pipe", f.pipeId); o.put("seq", f.seq); o.put("priority", f.priority);
                o.put("status", f.status == 0 ? "available" : "standby");
            }
            case PIPE_PING -> { }
        }
        return o;
    }

    /** Matches one pattern field (TRACES.md Section 5.6). */
    static boolean matchValue(Object pat, Object got) {
        if ("*".equals(pat)) return true;
        if (pat instanceof Map<?, ?> pm) {
            Map<String, Object> p = map(pat);
            if (p.containsKey("len")) return got instanceof String gs && gs.length() / 2 == num(p.get("len"));
            if (p.containsKey("prefix")) return got instanceof String gs && gs.startsWith((String) p.get("prefix"));
            if (p.containsKey("min") || p.containsKey("max")) {
                if (got == null || got instanceof Boolean) return false;
                long g = num(got);
                if (p.containsKey("min") && g < num(p.get("min"))) return false;
                if (p.containsKey("max") && g > num(p.get("max"))) return false;
                return true;
            }
            return pm.isEmpty() && got instanceof Map;
        }
        if (pat instanceof String s) return s.equals(got);
        if (pat instanceof Boolean b) return b.equals(got);
        if (pat instanceof Long || pat instanceof BigInteger || pat instanceof Double) {
            return (got instanceof Long || got instanceof BigInteger) && num(got) == num(pat);
        }
        if (pat == null) return got == null;
        if (pat instanceof List<?> pl) {
            if (!(got instanceof List<?> gl) || gl.size() != pl.size()) return false;
            for (int i = 0; i < pl.size(); i++) {
                if (pl.get(i) instanceof Map<?, ?> && gl.get(i) instanceof Map<?, ?>) {
                    Map<String, Object> a = map(pl.get(i)), b = map(gl.get(i));
                    for (var e : a.entrySet()) if (!matchValue(e.getValue(), b.get(e.getKey()))) return false;
                    continue;
                }
                if (!matchValue(pl.get(i), gl.get(i))) return false;
            }
            return true;
        }
        return false;
    }

    static boolean typeMatches(Object pat, String got) {
        if (pat instanceof String s) return s.equals("*") || s.equals(got);
        if (pat instanceof Map<?, ?>) {
            for (Object x : list(map(pat).get("oneof"))) if (x.equals(got)) return true;
        }
        return false;
    }

    /** Matches a frame pattern against a decoded frame object. */
    static boolean matchFrame(Map<String, Object> pat, Map<String, Object> got) {
        if (!typeMatches(pat.get("type"), (String) got.get("type"))) return false;
        for (var e : pat.entrySet()) {
            String k = e.getKey();
            switch (k) {
                case "type", "count" -> { }
                case "params_include", "params_exclude", "grease", "params" -> {
                    if (!matchParams(pat, got)) return false;
                }
                default -> {
                    if (!matchValue(e.getValue(), got.get(k))) return false;
                }
            }
        }
        return true;
    }

    static boolean matchParams(Map<String, Object> pat, Map<String, Object> got) {
        byte[] block = got.get("block") instanceof byte[] b ? b : new byte[0];
        ByteBuffer bb = ByteBuffer.wrap(block);
        List<long[]> ids = new ArrayList<>();
        List<byte[]> vals = new ArrayList<>();
        int i = 0;
        while (i < block.length) {
            long id = VarInt.get(bb, i, block.length);
            i += VarInt.lengthOf(block[i]);
            long l = VarInt.get(bb, i, block.length);
            i += VarInt.lengthOf(block[i]);
            byte[] v = java.util.Arrays.copyOfRange(block, i, i + (int) l);
            i += (int) l;
            ids.add(new long[] {id});
            vals.add(v);
        }
        java.util.function.LongFunction<byte[]> find = id -> {
            for (int k = 0; k < ids.size(); k++) if (ids.get(k)[0] == id) return vals.get(k);
            return null;
        };
        if (pat.get("params_include") instanceof Map<?, ?>) {
            for (var e : map(pat.get("params_include")).entrySet()) {
                Long id = PARAM_IDS.get(e.getKey());
                long pid = id != null ? id : Long.parseUnsignedLong(e.getKey());
                byte[] val = find.apply(pid);
                if (val == null) return false;
                if ("*".equals(e.getValue())) continue;
                if (!java.util.Arrays.equals(encodeParamValue(e.getKey(), e.getValue()), val)) return false;
            }
        }
        if (pat.get("params_exclude") instanceof List<?>) {
            for (Object n : list(pat.get("params_exclude"))) if (find.apply(PARAM_IDS.get((String) n)) != null) return false;
        }
        if (Boolean.TRUE.equals(pat.get("grease"))) {
            boolean found = false;
            for (long[] id : ids) if (TransportParams.isGrease(id[0])) found = true;
            if (!found) return false;
        }
        return true;
    }
}
