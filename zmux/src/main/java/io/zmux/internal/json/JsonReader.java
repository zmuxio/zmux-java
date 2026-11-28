package io.zmux.internal.json;

import java.util.ArrayList;
import java.util.List;

/** JSON syntax shared by the cache snapshot and shaping definition readers. */
public abstract class JsonReader<T> {
    private static final int MAX_DEPTH = 64;
    private final String text;
    private final boolean requirePairedEscapes;
    private int at;

    protected JsonReader(String text, boolean requirePairedEscapes) {
        this.text = text;
        this.requirePairedEscapes = requirePairedEscapes;
    }

    /** The consumer owns member uniqueness, ordering, and representation. */
    protected interface ObjectBuilder<T> {
        default void checkName(String name) {}
        void add(String name, T value);
        T finish();
    }

    protected abstract ObjectBuilder<T> newObject();
    protected abstract T array(List<T> values);
    protected abstract T string(String value);
    protected abstract T number(String text, int start, int end, boolean integral);
    protected abstract T bool(boolean value);
    protected abstract T nil();
    protected abstract IllegalArgumentException syntax(String message, int offset);

    public final T read() {
        T result = value(0);
        whitespace();
        if (at != text.length()) throw error("trailing data");
        return result;
    }

    private T value(int depth) {
        whitespace();
        if (at >= text.length()) throw error("unexpected end");
        return switch (text.charAt(at)) {
            case '{' -> object(depth + 1);
            case '[' -> list(depth + 1);
            case '"' -> string(quoted());
            case 't' -> { literal("true"); yield bool(true); }
            case 'f' -> { literal("false"); yield bool(false); }
            case 'n' -> { literal("null"); yield nil(); }
            default -> number();
        };
    }

    private T object(int depth) {
        checkDepth(depth);
        at++;
        ObjectBuilder<T> result = newObject();
        if (!take('}')) {
            do {
                whitespace();
                if (at >= text.length() || text.charAt(at) != '"') throw error("expected a member name");
                String name = quoted();
                if (!take(':')) throw error("expected ':'");
                result.checkName(name);
                result.add(name, value(depth));
            } while (take(','));
            if (!take('}')) throw error("expected ',' or '}'");
        }
        return result.finish();
    }

    private T list(int depth) {
        checkDepth(depth);
        at++;
        var result = new ArrayList<T>();
        if (!take(']')) {
            do result.add(value(depth)); while (take(','));
            if (!take(']')) throw error("expected ',' or ']'");
        }
        return array(result);
    }

    private void checkDepth(int depth) { if (depth > MAX_DEPTH) throw error("nesting too deep"); }

    private IllegalArgumentException error(String message) { return syntax(message, at); }

    private void whitespace() {
        while (at < text.length()) {
            char c = text.charAt(at);
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r') return;
            at++;
        }
    }

    private boolean take(char c) {
        whitespace();
        if (at == text.length() || text.charAt(at) != c) return false;
        at++;
        return true;
    }

    private void literal(String word) {
        if (!text.startsWith(word, at)) throw error("invalid literal");
        at += word.length();
    }

    private T number() {
        int start = at;
        if (text.charAt(at) == '-') at++;
        if (at < text.length() && text.charAt(at) == '0') at++;
        else digits();
        boolean integral = true;
        if (at < text.length() && text.charAt(at) == '.') {
            at++;
            digits();
            integral = false;
        }
        if (at < text.length() && (text.charAt(at) == 'e' || text.charAt(at) == 'E')) {
            at++;
            if (at < text.length() && (text.charAt(at) == '+' || text.charAt(at) == '-')) at++;
            digits();
            integral = false;
        }
        return number(text, start, at, integral);
    }

    private void digits() {
        int start = at;
        while (at < text.length() && digit(text.charAt(at))) at++;
        if (at == start) throw error("invalid number");
    }

    private static boolean digit(char c) { return c >= '0' && c <= '9'; }

    private String quoted() {
        at++;
        var value = new StringBuilder();
        while (at < text.length()) {
            char c = text.charAt(at++);
            if (c == '"') return value.toString();
            if (c < 0x20) throw error("control character in string");
            if (c != '\\') { value.append(c); continue; }
            if (at == text.length()) break;
            char escape = text.charAt(at++);
            switch (escape) {
                case '"', '\\', '/' -> value.append(escape);
                case 'b' -> value.append('\b');
                case 'f' -> value.append('\f');
                case 'n' -> value.append('\n');
                case 'r' -> value.append('\r');
                case 't' -> value.append('\t');
                case 'u' -> unicode(value);
                default -> throw error("invalid escape");
            }
        }
        throw error("unterminated string");
    }

    private void unicode(StringBuilder value) {
        char first = unit();
        value.append(first);
        if (!requirePairedEscapes || !Character.isSurrogate(first)) return;
        if (!Character.isHighSurrogate(first) || !text.startsWith("\\u", at)) throw error("lone surrogate");
        at += 2;
        char second = unit();
        if (!Character.isLowSurrogate(second)) throw error("lone surrogate");
        value.append(second);
    }

    private char unit() {
        if (text.length() - at < 4) throw error("short escape");
        int result = 0;
        for (int end = at + 4; at < end; at++) {
            char c = text.charAt(at);
            int hex = c <= 'f' ? Character.digit(c, 16) : -1;
            if (hex < 0) throw error("invalid escape");
            result = result * 16 + hex;
        }
        return (char) result;
    }
}
