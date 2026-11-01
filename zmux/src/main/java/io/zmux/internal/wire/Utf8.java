package io.zmux.internal.wire;

import java.nio.charset.StandardCharsets;

final class Utf8 {
    private Utf8() {}

    /** Truncates {@code s} to at most {@code max} bytes of UTF-8 without splitting a character. */
    static String truncate(String s, int max) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        if (b.length <= max) return s;
        int n = max;
        while (n > 0 && (b[n] & 0xc0) == 0x80) n--;
        return new String(b, 0, n, StandardCharsets.UTF_8);
    }
}
