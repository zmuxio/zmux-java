package io.zmux;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The JSON and RFC 3339 forms of {@link CarrierCache.Snapshot}: a strict parser that keeps what
 * the snapshot needs (integers apart from other numbers, the last of repeated members) and the
 * canonical encoding.
 */
final class SnapshotJson {
    private SnapshotJson() {}

    sealed interface Value permits Null, Bool, Int, Num, Str, Arr, Obj {}
    record Null() implements Value {}
    record Bool(boolean value) implements Value {}
    /** An integer in the range of a long. */
    record Int(long value) implements Value {}
    /** Any other number. */
    record Num() implements Value {}
    record Str(String value) implements Value {}
    record Arr(List<Value> values) implements Value {}
    /** Members in their order; {@link #get} takes the last of a repeated name. */
    record Obj(List<Map.Entry<String, Value>> members) implements Value {
        Value get(String name) {
            for (int i = members.size() - 1; i >= 0; i--) if (members.get(i).getKey().equals(name)) return members.get(i).getValue();
            return null;
        }
    }

    private static final int MAX_DEPTH = 64;

    static Value parse(String text) {
        var p = new Parser(text);
        Value v = p.value(0);
        p.ws();
        if (p.i != text.length()) throw p.error("trailing data");
        return v;
    }

    private static final class Parser {
        private final String s;
        private int i;

        Parser(String s) { this.s = s; }

        IllegalArgumentException error(String what) { return new IllegalArgumentException(what + " at " + i); }

        void ws() {
            while (i < s.length() && (s.charAt(i) == ' ' || s.charAt(i) == '\t' || s.charAt(i) == '\n' || s.charAt(i) == '\r')) i++;
        }

        Value literal(String word, Value v) {
            if (!s.startsWith(word, i)) throw error("unexpected token");
            i += word.length();
            return v;
        }

        Value value(int depth) {
            if (depth > MAX_DEPTH) throw error("nesting too deep");
            ws();
            if (i >= s.length()) throw error("unexpected end");
            char c = s.charAt(i);
            return switch (c) {
                case 'n' -> literal("null", new Null());
                case 't' -> literal("true", new Bool(true));
                case 'f' -> literal("false", new Bool(false));
                case '"' -> new Str(string());
                case '[' -> array(depth);
                case '{' -> object(depth);
                default -> {
                    if (c == '-' || c >= '0' && c <= '9') yield number();
                    throw error("unexpected character");
                }
            };
        }

        Value array(int depth) {
            i++;
            var a = new ArrayList<Value>();
            ws();
            if (i < s.length() && s.charAt(i) == ']') { i++; return new Arr(List.copyOf(a)); }
            while (true) {
                a.add(value(depth + 1));
                ws();
                if (i < s.length() && s.charAt(i) == ',') { i++; continue; }
                if (i < s.length() && s.charAt(i) == ']') { i++; return new Arr(List.copyOf(a)); }
                throw error("expected , or ]");
            }
        }

        Value object(int depth) {
            i++;
            var o = new ArrayList<Map.Entry<String, Value>>();
            ws();
            if (i < s.length() && s.charAt(i) == '}') { i++; return new Obj(List.copyOf(o)); }
            while (true) {
                ws();
                if (i >= s.length() || s.charAt(i) != '"') throw error("expected a name");
                String k = string();
                ws();
                if (i >= s.length() || s.charAt(i) != ':') throw error("expected :");
                i++;
                o.add(Map.entry(k, value(depth + 1)));
                ws();
                if (i < s.length() && s.charAt(i) == ',') { i++; continue; }
                if (i < s.length() && s.charAt(i) == '}') { i++; return new Obj(List.copyOf(o)); }
                throw error("expected , or }");
            }
        }

        private int digits() {
            int start = i;
            while (i < s.length() && s.charAt(i) >= '0' && s.charAt(i) <= '9') i++;
            return i - start;
        }

        Value number() {
            int start = i;
            if (s.charAt(i) == '-') i++;
            int first = i, n = digits();
            if (n == 0 || n > 1 && s.charAt(first) == '0') throw error("bad number");
            boolean integer = true;
            if (i < s.length() && s.charAt(i) == '.') {
                i++; integer = false;
                if (digits() == 0) throw error("bad number");
            }
            if (i < s.length() && (s.charAt(i) == 'e' || s.charAt(i) == 'E')) {
                i++; integer = false;
                if (i < s.length() && (s.charAt(i) == '+' || s.charAt(i) == '-')) i++;
                if (digits() == 0) throw error("bad number");
            }
            if (!integer) return new Num();
            try {
                return new Int(Long.parseLong(s, start, i, 10));
            } catch (NumberFormatException outOfRange) {
                return new Num();
            }
        }

        private int hex4() {
            if (i + 4 > s.length()) throw error("bad \\u escape");
            int v = 0;
            for (int k = 0; k < 4; k++) {
                int d = Character.digit(s.charAt(i + k), 16);
                if (d < 0) throw error("bad \\u escape");
                v = v << 4 | d;
            }
            i += 4;
            return v;
        }

        String string() {
            i++;
            var b = new StringBuilder();
            while (true) {
                if (i >= s.length()) throw error("unterminated string");
                char c = s.charAt(i++);
                if (c == '"') return b.toString();
                if (c < 0x20) throw error("control character in a string");
                if (c != '\\') { b.append(c); continue; }
                if (i >= s.length()) throw error("unterminated string");
                char e = s.charAt(i++);
                switch (e) {
                    case '"', '\\', '/' -> b.append(e);
                    case 'b' -> b.append('\b');
                    case 'f' -> b.append('\f');
                    case 'n' -> b.append('\n');
                    case 'r' -> b.append('\r');
                    case 't' -> b.append('\t');
                    case 'u' -> {
                        int u = hex4();
                        if (Character.isHighSurrogate((char) u)) {
                            if (!s.startsWith("\\u", i)) throw error("lone surrogate");
                            i += 2;
                            int low = hex4();
                            if (!Character.isLowSurrogate((char) low)) throw error("lone surrogate");
                            b.append((char) u).append((char) low);
                        } else if (Character.isLowSurrogate((char) u)) {
                            throw error("lone surrogate");
                        } else {
                            b.append((char) u);
                        }
                    }
                    default -> throw error("bad escape");
                }
            }
        }
    }

    /** Writes s as a JSON string, escaping only the quote, the backslash and control characters. */
    static void writeString(StringBuilder out, String s) {
        out.append('"');
        for (int k = 0; k < s.length(); k++) {
            char c = s.charAt(k);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
                    else out.append(c);
                }
            }
        }
        out.append('"');
    }

    /** The RFC 3339 UTC form of t: seconds, a fraction without trailing zeros when not zero, Z. */
    static String formatTime(Instant t) {
        LocalDateTime d = LocalDateTime.ofEpochSecond(t.getEpochSecond(), 0, ZoneOffset.UTC);
        var b = new StringBuilder(String.format("%04d-%02d-%02dT%02d:%02d:%02d",
                d.getYear(), d.getMonthValue(), d.getDayOfMonth(), d.getHour(), d.getMinute(), d.getSecond()));
        int nanos = t.getNano();
        if (nanos > 0) {
            String f = String.format("%09d", nanos);
            int end = f.length();
            while (f.charAt(end - 1) == '0') end--;
            b.append('.').append(f, 0, end);
        }
        return b.append('Z').toString();
    }

    private static int num(String s, int from, int to) {
        int n = 0;
        for (int k = from; k < to; k++) {
            char c = s.charAt(k);
            if (c < '0' || c > '9') return -1;
            n = n * 10 + (c - '0');
        }
        return n;
    }

    /**
     * An RFC 3339 time with seconds, an optional fraction of up to nine digits and a Z or a
     * numeric offset; null when s is not one.
     */
    static Instant parseTime(String s) {
        if (s.length() < 20 || s.charAt(4) != '-' || s.charAt(7) != '-' || (s.charAt(10) != 'T' && s.charAt(10) != 't')
                || s.charAt(13) != ':' || s.charAt(16) != ':') return null;
        int y = num(s, 0, 4), mo = num(s, 5, 7), d = num(s, 8, 10), h = num(s, 11, 13), mi = num(s, 14, 16), se = num(s, 17, 19);
        if (y < 0 || mo < 1 || mo > 12 || d < 1 || h < 0 || h > 23 || mi < 0 || mi > 59 || se < 0 || se > 59) return null;
        if (d > LocalDate.of(y, mo, 1).lengthOfMonth()) return null;
        int k = 19, nanos = 0;
        if (s.charAt(k) == '.') {
            int start = ++k;
            while (k < s.length() && s.charAt(k) >= '0' && s.charAt(k) <= '9') k++;
            int n = k - start;
            if (n == 0 || n > 9) return null;
            nanos = num(s, start, k);
            for (int z = n; z < 9; z++) nanos *= 10;
        }
        String rest = s.substring(k);
        int offset;
        if (rest.equals("Z") || rest.equals("z")) {
            offset = 0;
        } else if (rest.length() == 6 && (rest.charAt(0) == '+' || rest.charAt(0) == '-') && rest.charAt(3) == ':') {
            int oh = num(rest, 1, 3), om = num(rest, 4, 6);
            if (oh < 0 || oh > 23 || om < 0 || om > 59) return null;
            offset = (oh * 3600 + om * 60) * (rest.charAt(0) == '-' ? -1 : 1);
        } else {
            return null;
        }
        long secs = LocalDate.of(y, mo, d).toEpochDay() * 86_400 + h * 3600L + mi * 60L + se - offset;
        return Instant.ofEpochSecond(secs, nanos);
    }
}
