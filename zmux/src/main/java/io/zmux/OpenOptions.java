package io.zmux;

import java.util.Arrays;
import java.util.Objects;

public final class OpenOptions {
    private static final byte[] EMPTY_OPEN_INFO = new byte[0];
    private static final OpenOptions EMPTY = new OpenOptions(null, null, EMPTY_OPEN_INFO);

    private final Long initialPriority;
    private final Long initialGroup;
    private final byte[] openInfo;

    public OpenOptions(Long initialPriority, Long initialGroup, byte[] openInfo) {
        initialPriority = requireOptionalVarint62(initialPriority, "initialPriority");
        initialGroup = requireOptionalVarint62(initialGroup, "initialGroup");
        this.initialPriority = initialPriority;
        this.initialGroup = initialGroup;
        this.openInfo = normalizeOpenInfo(openInfo);
    }

    public static OpenOptions empty() {
        return EMPTY;
    }

    private static byte[] normalizeOpenInfo(byte[] openInfo) {
        if (openInfo == null || openInfo.length == 0) {
            return EMPTY_OPEN_INFO;
        }
        return Arrays.copyOf(openInfo, openInfo.length);
    }

    private static Long requireOptionalVarint62(Long value, String field) {
        if (value == null) {
            return null;
        }
        if (value < 0L || value > Protocol.MAX_VARINT62) {
            throw new IllegalArgumentException("zmux open options " + field + " must be within varint62 range");
        }
        return value;
    }

    public Long initialPriority() {
        return initialPriority;
    }

    public Long initialGroup() {
        return initialGroup;
    }

    public byte[] openInfo() {
        return openInfo.length == 0 ? EMPTY_OPEN_INFO : Arrays.copyOf(openInfo, openInfo.length);
    }

    public int openInfoLength() {
        return openInfo.length;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof OpenOptions)) {
            return false;
        }
        OpenOptions that = (OpenOptions) other;
        return Objects.equals(initialPriority, that.initialPriority)
                && Objects.equals(initialGroup, that.initialGroup)
                && Arrays.equals(openInfo, that.openInfo);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(initialPriority, initialGroup);
        result = 31 * result + Arrays.hashCode(openInfo);
        return result;
    }

    @Override
    public String toString() {
        return "OpenOptions[initialPriority="
                + initialPriority
                + ", initialGroup="
                + initialGroup
                + ", openInfoLength="
                + openInfo.length
                + "]";
    }
}
