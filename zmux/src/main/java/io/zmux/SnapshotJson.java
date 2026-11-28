package io.zmux;

import io.zmux.internal.json.JsonReader;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The JSON and RFC 3339 forms of {@link CarrierCache}: a strict parser that keeps what
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
    record Num(double value) implements Value {}
    record Str(String value) implements Value {}
    record Arr(List<Value> values) implements Value {}
    /** Members in their order; {@link #get} takes the last of a repeated name. */
    record Obj(List<Map.Entry<String, Value>> members) implements Value {
        Value get(String name) {
            for (int i = members.size() - 1; i >= 0; i--) if (members.get(i).getKey().equals(name)) return members.get(i).getValue();
            return null;
        }
    }

    static Value parse(String text) { return new Parser(text).read(); }

    private static final class Parser extends JsonReader<Value> {
        Parser(String text) { super(text, true); }

        @Override protected ObjectBuilder<Value> newObject() {
            return new ObjectBuilder<>() {
                private final List<Map.Entry<String, Value>> members = new ArrayList<>();
                @Override public void add(String name, Value value) { members.add(Map.entry(name, value)); }
                @Override public Value finish() { return new Obj(List.copyOf(members)); }
            };
        }

        @Override protected Value array(List<Value> values) { return new Arr(List.copyOf(values)); }
        @Override protected Value string(String value) { return new Str(value); }
        @Override protected Value bool(boolean value) { return new Bool(value); }
        @Override protected Value nil() { return new Null(); }

        @Override protected Value number(String text, int start, int end, boolean integral) {
            if (integral) {
                try { return new Int(Long.parseLong(text, start, end, 10)); }
                catch (NumberFormatException tooLarge) { /* Represent an integer beyond long as a floating-point number. */ }
            }
            return new Num(Double.parseDouble(text.substring(start, end)));
        }

        @Override protected IllegalArgumentException syntax(String message, int offset) {
            return new IllegalArgumentException(message + " at " + offset);
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
                    if (c < 0x20) out.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    else out.append(c);
                }
            }
        }
        out.append('"');
    }

    /** The RFC 3339 UTC form of t: seconds, a fraction without trailing zeros when not zero, Z. */
    static String formatTime(Instant t) {
        LocalDateTime d = LocalDateTime.ofEpochSecond(t.getEpochSecond(), 0, ZoneOffset.UTC);
        var b = new StringBuilder(String.format(Locale.ROOT, "%04d-%02d-%02dT%02d:%02d:%02d",
                d.getYear(), d.getMonthValue(), d.getDayOfMonth(), d.getHour(), d.getMinute(), d.getSecond()));
        int nanos = t.getNano();
        if (nanos > 0) {
            String f = String.format(Locale.ROOT, "%09d", nanos);
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
