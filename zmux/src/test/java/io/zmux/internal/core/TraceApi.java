package io.zmux.internal.core;

import static io.zmux.internal.core.TraceFrames.getB;
import static io.zmux.internal.core.TraceFrames.getU;
import static io.zmux.internal.core.TraceFrames.hex;
import static io.zmux.internal.core.TraceFrames.map;
import static io.zmux.internal.core.TraceFrames.num;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.opentest4j.TestAbortedException;

/** The application steps of TRACES.md Section 5.3. */
final class TraceApi {
    private final TraceRunner r;

    TraceApi(TraceRunner r) {
        this.r = r;
    }

    static CoreConfig.Policy policy(String name) {
        return switch (name) {
            case "affinity" -> CoreConfig.Policy.AFFINITY;
            case "session-affinity" -> CoreConfig.Policy.SESSION_AFFINITY;
            case "spread-eta" -> CoreConfig.Policy.SPREAD_ETA;
            case "spread-round-robin" -> CoreConfig.Policy.SPREAD_ROUND_ROBIN;
            case "spread-rate-weighted" -> CoreConfig.Policy.SPREAD_RATE_WEIGHTED;
            default -> throw new IllegalArgumentException("unknown scheduler " + name);
        };
    }

    static Map<String, Object> errResult(CoreException e) {
        Map<String, Object> err = new LinkedHashMap<>();
        switch (e.reason()) {
            case STREAM -> {
                StreamError se = e.streamError();
                err.put("kind", switch (se.kind()) {
                    case RESET -> "stream_reset";
                    case STOPPED -> "stream_stopped";
                    case CLOSED_LOCALLY -> "closed_locally";
                    case REJECTED -> "stream_rejected";
                    case GOING_AWAY -> "going_away";
                    case PREAMBLE_TOO_LARGE -> "preamble_too_large";
                });
                err.put("code", se.code());
                err.put("remote", se.remote());
            }
            case SESSION -> {
                err.putAll(TraceRunner.errorObj(e.sessionError()));
                err.put("kind", "session");
            }
            case DATAGRAM_TOO_LARGE -> err.put("kind", "too_large");
            case NOT_NEGOTIATED, INVALID -> err.put("kind", "unsupported");
            default -> err.put("kind", e.reason().name());
        }
        return Map.of("error", err);
    }

    private Stream stream(Map<String, Object> args) {
        Object n = args.get("stream");
        Stream st = n instanceof String name ? r.streams.get(name) : r.s.streams.get(num(n));
        if (st == null) throw TraceRunner.fail("unknown stream " + n);
        return st;
    }

    void call(Map<String, Object> st) {
        String call = (String) st.get("call");
        Map<String, Object> args = st.get("args") == null ? Map.of() : map(st.get("args"));
        Session s = r.s;
        s.setNow(r.now);
        Map<String, Object> res = new LinkedHashMap<>(Map.of("ok", true));
        try {
            switch (call) {
                case "open_bidi", "open_uni" -> {
                    Stream h = s.streams.openStream(call.equals("open_bidi"));
                    if (st.get("bind") instanceof String b) r.streams.put(b, h);
                    if (args.get("open_info") instanceof String oi) s.streams.setOpenInfo(h, hex(oi));
                    if (args.get("priority") instanceof Map<?, ?>) {
                        Map<String, Object> p = map(args.get("priority"));
                        s.streams.setPeerPriority(h, (int) num(p.get("urgency")), getB(p, "incremental"));
                    }
                }
                case "accept_bidi", "accept_uni" -> {
                    Stream h = s.streams.acceptStream(call.equals("accept_bidi"));
                    if (h == null) {
                        res = Map.of("pending", true);
                        break;
                    }
                    if (st.get("bind") instanceof String b) r.streams.put(b, h);
                    res = new LinkedHashMap<>();
                    res.put("stream", h.id());
                    io.zmux.internal.wire.Preamble pp = h.peerPreamble();
                    if (pp != null) {
                        if (pp.openInfo() != null) res.put("open_info", hex(pp.openInfo()));
                        if (pp.hasPriority()) res.put("priority", Map.of("urgency", (long) pp.urgency(), "incremental", pp.incremental()));
                        res.put("pipe_carrier", pp.pipeCarrier());
                        List<Object> tl = new ArrayList<>();
                        for (var t : pp.applicationTlvs()) tl.add(Map.of("type", t.type(), "value", hex(t.value())));
                        res.put("app_tlvs", tl);
                    } else {
                        res.put("pipe_carrier", false);
                        res.put("app_tlvs", List.of());
                    }
                }
                case "write" -> {
                    Stream h = stream(args);
                    byte[] data;
                    if (args.get("data") instanceof String d) {
                        data = hex(d);
                    } else if (args.get("fill") instanceof Map<?, ?>) {
                        Map<String, Object> f = map(args.get("fill"));
                        data = new byte[(int) num(f.get("len"))];
                        java.util.Arrays.fill(data, (byte) num(f.get("byte")));
                    } else {
                        data = new byte[0];
                    }
                    boolean fin = getB(args, "fin");
                    res = Map.of("accepted", 0L);
                    int n = s.send.write(h, ByteBuffer.wrap(data), fin);
                    if (fin && n == data.length && data.length == 0) s.send.finish(h);
                    res = Map.of("accepted", (long) n);
                }
                case "stream_id" -> res = Map.of("stream", s.streams.pinId(stream(args)));
                case "set_reliable_boundary" -> s.send.setReliableBoundary(stream(args));
                case "finish" -> s.send.finish(stream(args));
                case "reset" -> s.send.reset(stream(args), getU(args, "code"));
                case "stop" -> s.recv.stop(stream(args), getU(args, "code"));
                case "read" -> {
                    Stream h = stream(args);
                    ByteBuffer buf = ByteBuffer.allocate((int) getU(args, "max"));
                    StreamRecv.ReadResult rr = s.recv.read(h, buf);
                    if (rr.n() == 0 && !rr.fin()) {
                        res = Map.of("pending", true);
                    } else {
                        byte[] got = new byte[rr.n()];
                        buf.flip().get(got);
                        res = Map.of("data", hex(got), "fin", rr.fin());
                    }
                }
                case "release" -> s.streams.release(stream(args));
                case "reject" -> s.streams.reject(stream(args));
                case "set_priority" -> s.streams.setPriority(stream(args), (int) getU(args, "urgency"), getB(args, "incremental"));
                case "set_peer_priority" -> s.streams.setPeerPriority(stream(args), (int) getU(args, "urgency"), getB(args, "incremental"));
                case "send_datagram" -> s.sendDatagram(args.containsKey("stream") ? stream(args) : null, getU(args, "context"),
                        hex(args.get("data")));
                case "recv_datagram" -> {
                    if (args.containsKey("stream")) {
                        Datagrams.StreamDatagram d = s.datagrams().receive(stream(args));
                        res = d == null ? Map.of("pending", true) : Map.of("data", hex(d.payload()), "context", d.context());
                    } else {
                        byte[] d = s.datagrams().receive();
                        res = d == null ? Map.of("pending", true) : Map.of("data", hex(d));
                    }
                }
                case "ping" -> s.ping(null);
                case "shutdown" -> s.shutdown(getU(args, "code"), null);
                case "close" -> s.close(getU(args, "code"), args.get("reason") instanceof String rs ? hex(rs) : null);
                case "add_pipe" -> res = r.addPipe(args);
                case "set_pipe_status" -> {
                    Tiers.PipeSettings.Builder o = Tiers.PipeSettings.builder();
                    if (args.get("send_priority") != null) o.sendPriority(num(args.get("send_priority")));
                    if (args.get("receive_priority") != null) o.recvPriority(num(args.get("receive_priority")));
                    if (args.get("weight") != null) o.weight(num(args.get("weight")));
                    if (args.get("send_standby") instanceof Boolean x) o.sendStandby(x);
                    if (args.get("receive_standby") instanceof Boolean x) o.recvStandby(x);
                    if (args.get("probation") != null) o.probation(TraceRunner.ms(args.get("probation")));
                    s.setPipe(getU(args, "pipe"), o.build());
                }
                case "set_scheduler" -> {
                    CoreConfig.Policy pol = policy((String) args.get("policy"));
                    if (args.get("stream") instanceof String) s.setStreamPolicy(stream(args), pol);
                    else s.setPolicy(pol);
                }
                case "set_redundant" -> s.setRedundant(stream(args), args.get("redundant") instanceof Boolean x ? x : null);
                case "revalidate_pipes" -> {
                    if (args.get("pipe") != null) s.revalidatePipes(num(args.get("pipe")), false);
                    else s.revalidatePipes(0, true);
                }
                case "remove_pipe" -> s.removePipe(getU(args, "pipe"), getB(args, "graceful"));
                default -> throw new TestAbortedException("api " + call);
            }
        } catch (CoreException e) {
            res = errResult(e);
        }
        r.drainEvents();
        if (st.get("expect") instanceof Map<?, ?> && !TraceMatch.matchObj(map(st.get("expect")), res)) {
            throw TraceRunner.fail("result " + res + ", want " + st.get("expect"));
        }
    }
}
