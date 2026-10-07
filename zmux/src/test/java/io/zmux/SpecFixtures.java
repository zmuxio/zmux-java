package io.zmux;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Loader for the zmux-spec fixture bundle vendored under {@code src/test/resources/io/zmux/fixtures}.
 *
 * <p>The bundle is a byte-for-byte copy of {@code zmux-spec/fixtures/*}; see the README next to it. The JSON
 * reader below is deliberately tiny (objects, arrays, strings, integers, booleans, null) so the test classpath
 * needs no JSON dependency and still compiles with {@code --release 8}.
 */
public final class SpecFixtures {
    public static final String RESOURCE_DIR = "/io/zmux/fixtures/";

    private SpecFixtures() {
    }

    public static List<Map<String, Object>> loadNdjson(String name) {
        List<Map<String, Object>> records = new ArrayList<>();
        int lineNumber = 0;
        for (String line : resourceText(name).split("\n", -1)) {
            lineNumber++;
            if (line.trim().isEmpty()) {
                continue;
            }
            Object value = parseJson(line);
            if (!(value instanceof Map)) {
                throw new IllegalStateException(name + ":" + lineNumber + " is not a JSON object");
            }
            records.add(asMap(value));
        }
        return Collections.unmodifiableList(records);
    }

    public static Map<String, Object> loadJson(String name) {
        return asMap(parseJson(resourceText(name)));
    }

    public static Map<String, Map<String, Object>> byId(List<Map<String, Object>> records) {
        Map<String, Map<String, Object>> out = new LinkedHashMap<>();
        for (Map<String, Object> record : records) {
            String id = string(record, "id");
            if (out.put(id, record) != null) {
                throw new IllegalStateException("duplicate fixture id " + id);
            }
        }
        return out;
    }

    public static String resourceText(String name) {
        try (InputStream input = SpecFixtures.class.getResourceAsStream(RESOURCE_DIR + name)) {
            if (input == null) {
                throw new IllegalStateException("vendored fixture " + RESOURCE_DIR + name + " is missing");
            }
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int read;
            while ((read = input.read(buffer)) != -1) {
                output.write(buffer, 0, read);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static byte[] hex(String text) {
        if (text.length() % 2 != 0) {
            throw new IllegalArgumentException("odd-length hex: " + text);
        }
        byte[] out = new byte[text.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(text.substring(2 * i, 2 * i + 2), 16);
        }
        return out;
    }

    public static String hex(byte[] bytes) {
        StringBuilder out = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            out.append(Character.forDigit((value >>> 4) & 0x0f, 16));
            out.append(Character.forDigit(value & 0x0f, 16));
        }
        return out.toString();
    }

    public static ErrorCode errorCode(String name) {
        return ErrorCode.valueOf(name);
    }

    public static boolean has(Map<String, Object> object, String key) {
        return object != null && object.containsKey(key) && object.get(key) != null;
    }

    public static String string(Map<String, Object> object, String key) {
        Object value = require(object, key);
        if (!(value instanceof String)) {
            throw new IllegalStateException("fixture field " + key + " is not a string: " + value);
        }
        return (String) value;
    }

    public static long longValue(Map<String, Object> object, String key) {
        Object value = require(object, key);
        if (!(value instanceof Long)) {
            throw new IllegalStateException("fixture field " + key + " is not a 64-bit integer: " + value);
        }
        return (Long) value;
    }

    public static boolean bool(Map<String, Object> object, String key) {
        Object value = require(object, key);
        if (!(value instanceof Boolean)) {
            throw new IllegalStateException("fixture field " + key + " is not a boolean: " + value);
        }
        return (Boolean) value;
    }

    public static Map<String, Object> map(Map<String, Object> object, String key) {
        return asMap(require(object, key));
    }

    public static List<Object> list(Map<String, Object> object, String key) {
        Object value = require(object, key);
        if (!(value instanceof List)) {
            throw new IllegalStateException("fixture field " + key + " is not an array: " + value);
        }
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) value;
        return list;
    }

    public static List<String> stringList(Map<String, Object> object, String key) {
        List<String> out = new ArrayList<>();
        for (Object value : list(object, key)) {
            if (!(value instanceof String)) {
                throw new IllegalStateException("fixture field " + key + " holds a non-string: " + value);
            }
            out.add((String) value);
        }
        return out;
    }

    public static List<Map<String, Object>> mapList(Map<String, Object> object, String key) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Object value : list(object, key)) {
            out.add(asMap(value));
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> asMap(Object value) {
        if (!(value instanceof Map)) {
            throw new IllegalStateException("fixture value is not a JSON object: " + value);
        }
        return (Map<String, Object>) value;
    }

    private static Object require(Map<String, Object> object, String key) {
        if (!has(object, key)) {
            throw new IllegalStateException("fixture field " + key + " is missing in " + object);
        }
        return object.get(key);
    }

    static Object parseJson(String text) {
        JsonReader reader = new JsonReader(text);
        Object value = reader.readValue();
        reader.skipWhitespace();
        if (!reader.atEnd()) {
            throw reader.error("trailing characters");
        }
        return value;
    }

    private static final class JsonReader {
        private final String text;
        private int position;

        JsonReader(String text) {
            this.text = text;
        }

        boolean atEnd() {
            return position >= text.length();
        }

        void skipWhitespace() {
            while (!atEnd() && Character.isWhitespace(text.charAt(position))) {
                position++;
            }
        }

        IllegalStateException error(String message) {
            return new IllegalStateException("invalid fixture JSON at offset " + position + ": " + message);
        }

        Object readValue() {
            skipWhitespace();
            if (atEnd()) {
                throw error("unexpected end of input");
            }
            char c = text.charAt(position);
            switch (c) {
                case '{':
                    return readObject();
                case '[':
                    return readArray();
                case '"':
                    return readString();
                case 't':
                    expectLiteral("true");
                    return Boolean.TRUE;
                case 'f':
                    expectLiteral("false");
                    return Boolean.FALSE;
                case 'n':
                    expectLiteral("null");
                    return null;
                default:
                    if (c == '-' || (c >= '0' && c <= '9')) {
                        return readNumber();
                    }
                    throw error("unexpected character '" + c + "'");
            }
        }

        private Map<String, Object> readObject() {
            Map<String, Object> out = new LinkedHashMap<>();
            position++;
            skipWhitespace();
            if (peek() == '}') {
                position++;
                return out;
            }
            while (true) {
                skipWhitespace();
                if (peek() != '"') {
                    throw error("expected object key");
                }
                String key = readString();
                skipWhitespace();
                expect(':');
                Object value = readValue();
                if (out.containsKey(key)) {
                    throw error("duplicate key " + key);
                }
                out.put(key, value);
                skipWhitespace();
                char next = next();
                if (next == '}') {
                    return out;
                }
                if (next != ',') {
                    throw error("expected ',' or '}'");
                }
            }
        }

        private List<Object> readArray() {
            List<Object> out = new ArrayList<>();
            position++;
            skipWhitespace();
            if (peek() == ']') {
                position++;
                return out;
            }
            while (true) {
                out.add(readValue());
                skipWhitespace();
                char next = next();
                if (next == ']') {
                    return out;
                }
                if (next != ',') {
                    throw error("expected ',' or ']'");
                }
            }
        }

        private String readString() {
            expect('"');
            StringBuilder out = new StringBuilder();
            while (true) {
                char c = next();
                if (c == '"') {
                    return out.toString();
                }
                if (c != '\\') {
                    out.append(c);
                    continue;
                }
                char escaped = next();
                switch (escaped) {
                    case '"':
                    case '\\':
                    case '/':
                        out.append(escaped);
                        break;
                    case 'b':
                        out.append('\b');
                        break;
                    case 'f':
                        out.append('\f');
                        break;
                    case 'n':
                        out.append('\n');
                        break;
                    case 'r':
                        out.append('\r');
                        break;
                    case 't':
                        out.append('\t');
                        break;
                    case 'u':
                        if (position + 4 > text.length()) {
                            throw error("truncated unicode escape");
                        }
                        out.append((char) Integer.parseInt(text.substring(position, position + 4), 16));
                        position += 4;
                        break;
                    default:
                        throw error("invalid escape \\" + escaped);
                }
            }
        }

        private Object readNumber() {
            int start = position;
            while (!atEnd() && "+-0123456789.eE".indexOf(text.charAt(position)) >= 0) {
                position++;
            }
            String token = text.substring(start, position);
            if (token.indexOf('.') >= 0 || token.indexOf('e') >= 0 || token.indexOf('E') >= 0) {
                return Double.valueOf(token);
            }
            BigInteger value = new BigInteger(token);
            return value.bitLength() < 64 ? (Object) value.longValue() : value;
        }

        private void expectLiteral(String literal) {
            if (!text.startsWith(literal, position)) {
                throw error("expected " + literal);
            }
            position += literal.length();
        }

        private void expect(char expected) {
            if (next() != expected) {
                throw error("expected '" + expected + "'");
            }
        }

        private char peek() {
            if (atEnd()) {
                throw error("unexpected end of input");
            }
            return text.charAt(position);
        }

        private char next() {
            char c = peek();
            position++;
            return c;
        }
    }
}
