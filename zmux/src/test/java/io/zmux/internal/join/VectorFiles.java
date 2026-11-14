package io.zmux.internal.join;

import io.zmux.seal.SecureCarrierException;
import io.zmux.testutil.Json;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/** Reads the vector files of the secure carrier and their field conventions. */
public final class VectorFiles {
    private VectorFiles() {}

    public static Object load(String name) throws IOException {
        Path p = Path.of(System.getProperty("zmux.spec.dir"), "secure-carrier/vectors", name);
        return Json.parse(Files.readString(p));
    }

    @SuppressWarnings("unchecked") // parsed JSON: the vector file's schema fixes the type
    public static List<Map<String, Object>> list(String name) throws IOException {
        return (List<Map<String, Object>>) load(name);
    }

    @SuppressWarnings("unchecked") // parsed JSON: the vector file's schema fixes the type
    public static Map<String, Object> object(String name) throws IOException {
        return (Map<String, Object>) load(name);
    }

    @SuppressWarnings("unchecked") // parsed JSON: the vector file's schema fixes the type
    public static List<Map<String, Object>> items(Map<String, Object> m, String key) {
        return (List<Map<String, Object>>) m.get(key);
    }

    /** A hex field; an absent one is empty. */
    public static byte[] hex(Map<String, Object> m, String key) {
        Object v = m.get(key);
        return v == null ? new byte[0] : HexFormat.of().parseHex((String) v);
    }

    /** A hex field; an absent or empty one is null. */
    public static byte[] hexOrNull(Map<String, Object> m, String key) {
        byte[] b = hex(m, key);
        return b.length == 0 ? null : b;
    }

    /** A number as an unsigned long; an absent one is 0. */
    public static long num(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (v == null) return 0;
        return v instanceof BigInteger b ? b.longValue() : ((Number) v).longValue();
    }

    public static String str(Map<String, Object> m, String key) {
        Object v = m.get(key);
        return v == null ? "" : (String) v;
    }

    @SuppressWarnings("unchecked") // parsed JSON: the vector file's schema fixes the type
    public static long[] longs(Map<String, Object> m, String key) {
        return ((List<Object>) m.get(key)).stream().mapToLong(x -> ((Number) x).longValue()).toArray();
    }

    public static Framing framing(Map<String, Object> m) {
        return "message".equals(m.get("framing")) ? Framing.MESSAGE : Framing.STREAM;
    }

    /** The error class of an exception, as the vectors name it. */
    public static String errorClass(SecureCarrierException e) {
        return e.code().name().toLowerCase(java.util.Locale.ROOT);
    }

    /** {@code n} repetitions of a byte. */
    public static byte[] fill(int b, int n) {
        byte[] out = new byte[n];
        Arrays.fill(out, (byte) b);
        return out;
    }
}
