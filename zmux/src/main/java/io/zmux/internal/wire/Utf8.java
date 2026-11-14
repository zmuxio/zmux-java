package io.zmux.internal.wire;

import java.nio.charset.StandardCharsets;

/** UTF-8 helpers of the wire layer. */
public final class Utf8 {
    private Utf8() {}

    /** The UTF-8 of {@code s}, cut to at most {@code max} bytes without splitting a character. */
    public static byte[] truncate(String s, int max) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        if (b.length <= max) return b;
        int n = max;
        while (n > 0 && (b[n] & 0xc0) == 0x80) n--;
        return java.util.Arrays.copyOf(b, n);
    }
}
