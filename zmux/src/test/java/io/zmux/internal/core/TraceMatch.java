package io.zmux.internal.core;

import static io.zmux.internal.core.TraceFrames.getU;
import static io.zmux.internal.core.TraceFrames.hex;
import static io.zmux.internal.core.TraceFrames.list;
import static io.zmux.internal.core.TraceFrames.map;
import static io.zmux.internal.core.TraceFrames.num;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Frame matching of TRACES.md Section 5.6. */
final class TraceMatch {
    private TraceMatch() {}

    static String dump(List<Map<String, Object>> fs) {
        StringBuilder b = new StringBuilder();
        for (Map<String, Object> f : fs) b.append(f).append('\n');
        return b.toString();
    }

    static List<Map<String, Object>> nonPadding(List<Map<String, Object>> fs, List<Map<String, Object>> pats) {
        for (Map<String, Object> p : pats) if ("PADDING".equals(p.get("type"))) return fs;
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> f : fs) if (!"PADDING".equals(f.get("type"))) out.add(f);
        return out;
    }

    static String oneCreditPerRecord(List<Map<String, Object>> frames, List<Integer> recIdx) {
        Set<String> seen = new HashSet<>();
        int last = -1;
        for (int i = 0; i < frames.size(); i++) {
            Map<String, Object> f = frames.get(i);
            if (recIdx.get(i) != last) {
                seen.clear();
                last = recIdx.get(i);
            }
            String k = switch ((String) f.get("type")) {
                case "MAX_DATA" -> "md";
                case "MAX_STREAM_DATA" -> "msd" + f.get("id");
                case "MAX_STREAMS" -> "ms" + f.get("bidi");
                default -> null;
            };
            if (k == null) continue;
            if (!seen.add(k)) return "two credit frames of one scope in a record";
        }
        return null;
    }

    /** Returns null on a match, else the reason. */
    static String match(String mode, List<Map<String, Object>> pats, List<Map<String, Object>> frames) {
        switch (mode) {
            case "exact", "first" -> {
                int i = 0;
                for (int pi = 0; pi < pats.size(); pi++) {
                    Map<String, Object> p = pats.get(pi);
                    if ("STREAM_DATA".equals(p.get("type"))) {
                        int j = streamData(p, frames, i, true);
                        if (j < 0) return "pattern " + pi + " (STREAM_DATA) does not match";
                        i = j;
                        continue;
                    }
                    if (i >= frames.size()) return "pattern " + pi + ": no frame left";
                    if (!TraceFrames.matchFrame(p, frames.get(i))) return "pattern " + pi + " does not match frame " + i;
                    i++;
                }
                if (mode.equals("exact") && i != frames.size()) return (frames.size() - i) + " extra frames";
                return null;
            }
            case "contains" -> {
                int i = 0;
                for (int pi = 0; pi < pats.size(); pi++) {
                    Map<String, Object> p = pats.get(pi);
                    if ("STREAM_DATA".equals(p.get("type"))) {
                        int j = streamData(p, frames, i, false);
                        if (j < 0) return "pattern " + pi + " (STREAM_DATA) not found";
                        i = j;
                        continue;
                    }
                    while (i < frames.size() && !TraceFrames.matchFrame(p, frames.get(i))) i++;
                    if (i >= frames.size()) return "pattern " + pi + " not found";
                    i++;
                }
                return null;
            }
            case "unordered" -> {
                for (int pi = 0; pi < pats.size(); pi++) {
                    Map<String, Object> p = pats.get(pi);
                    if ("STREAM_DATA".equals(p.get("type"))) {
                        if (streamData(p, frames, 0, false) < 0) return "pattern " + pi + " (STREAM_DATA) not found";
                        continue;
                    }
                    long c = 0;
                    for (Map<String, Object> f : frames) if (TraceFrames.matchFrame(p, f)) c++;
                    Object cnt = p.get("count");
                    if (cnt instanceof Map<?, ?>) {
                        Map<String, Object> m = map(cnt);
                        if (m.containsKey("min") && c < num(m.get("min"))) return "pattern " + pi + ": " + c + " matches";
                        if (m.containsKey("max") && c > num(m.get("max"))) return "pattern " + pi + ": " + c + " matches";
                    } else if (cnt != null) {
                        if (c != num(cnt)) return "pattern " + pi + ": " + c + " matches, want " + cnt;
                    } else if (c == 0) {
                        return "pattern " + pi + " not found";
                    }
                }
                return null;
            }
            default -> {
                return "unknown match " + mode;
            }
        }
    }

    /**
     * Matches the STREAM_DATA pseudo-frame from index from; strict: the STREAM frames are
     * consecutive at from. Returns the index after the last STREAM frame covering the range, or -1.
     */
    static int streamData(Map<String, Object> p, List<Map<String, Object>> frames, int from, boolean strict) {
        long id = num(p.get("id"));
        long off = getU(p, "off");
        byte[] want = null;
        int wantLen;
        Object d = p.get("data");
        if (d instanceof String s) {
            want = hex(s);
            wantLen = want.length;
        } else {
            wantLen = (int) num(map(d).get("len"));
        }
        long end = off + wantLen;
        byte[] got = new byte[wantLen];
        boolean[] have = new boolean[wantLen];
        boolean finSeen = false;
        int last = -1;
        for (int i = from; i < frames.size(); i++) {
            Map<String, Object> f = frames.get(i);
            if (!"STREAM".equals(f.get("type")) || num(f.get("id")) != id) {
                if (strict) {
                    if (last >= 0) break;
                    return -1;
                }
                continue;
            }
            long fo = num(f.get("off"));
            byte[] fd = hex(f.get("data"));
            long fe = fo + fd.length;
            if (fo < off || fe > end) {
                if (last >= 0 && strict) break;
                if (fe <= off || fo >= end) continue;
                return -1;
            }
            System.arraycopy(fd, 0, got, (int) (fo - off), fd.length);
            for (long k = fo; k < fe; k++) have[(int) (k - off)] = true;
            if ((Boolean) f.get("fin")) finSeen = fe == end;
            last = i;
            boolean complete = true;
            for (boolean h : have) if (!h) complete = false;
            Object pf = p.get("fin");
            if (complete && (pf == null || pf.equals(finSeen) || !finSeen && (Boolean) pf && i + 1 < frames.size())) {
                if (want != null && !java.util.Arrays.equals(got, want)) return -1;
                if (pf instanceof Boolean fin && fin != finSeen) {
                    if (fin && i + 1 < frames.size()) {
                        Map<String, Object> n = frames.get(i + 1);
                        if ("STREAM".equals(n.get("type")) && num(n.get("id")) == id && (Boolean) n.get("fin")
                                && num(n.get("off")) == end && "".equals(n.get("data"))) {
                            return i + 2;
                        }
                    }
                    continue;
                }
                return i + 1;
            }
        }
        return -1;
    }

    static boolean matchObj(Map<String, Object> pat, Map<String, Object> got) {
        for (var e : pat.entrySet()) {
            if (!got.containsKey(e.getKey())) return false;
            Object g = got.get(e.getKey());
            Object v = e.getValue();
            if (v instanceof Map<?, ?>) {
                if (!(g instanceof Map<?, ?>) || !matchObj(map(v), map(g))) return false;
                continue;
            }
            if (v instanceof List<?> pl) {
                if (!(g instanceof List<?> gl) || gl.size() != pl.size()) return false;
                for (int i = 0; i < pl.size(); i++) {
                    if (pl.get(i) instanceof Map<?, ?>) {
                        if (!(gl.get(i) instanceof Map<?, ?>) || !matchObj(map(pl.get(i)), map(gl.get(i)))) return false;
                    } else if (!TraceFrames.matchValue(pl.get(i), gl.get(i))) {
                        return false;
                    }
                }
                continue;
            }
            if (!TraceFrames.matchValue(v, g)) return false;
        }
        return true;
    }

    static boolean matchEventFields(Map<String, Object> pat, Map<String, Object> got) {
        for (var e : pat.entrySet()) {
            if (e.getKey().equals("features")) {
                List<Object> ps = list(e.getValue());
                List<Object> gs = got.get("features") instanceof List<?> l ? list(l) : List.of();
                if (ps.size() != gs.size() || !gs.containsAll(ps)) return false;
                continue;
            }
            if (!TraceFrames.matchValue(e.getValue(), got.get(e.getKey()))) return false;
        }
        return true;
    }
}
