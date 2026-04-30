package io.zmux;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

final class TextSupport {
    private TextSupport() {
    }

    static byte[] utf8Bytes(String value, String field) {
        return Objects.requireNonNull(value, field).getBytes(StandardCharsets.UTF_8);
    }

    static String utf8String(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }
}
