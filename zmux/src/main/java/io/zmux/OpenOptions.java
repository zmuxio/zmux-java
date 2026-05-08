package io.zmux;

import io.zmux.protocol.Protocol;

import java.nio.charset.StandardCharsets;
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

    public static OpenOptions of(Long initialPriority, Long initialGroup, byte[] openInfo) {
        if (initialPriority == null && initialGroup == null && (openInfo == null || openInfo.length == 0)) {
            return EMPTY;
        }
        return new OpenOptions(initialPriority, initialGroup, openInfo);
    }

    public static OpenOptions priority(long initialPriority) {
        return of(initialPriority, null, null);
    }

    public static OpenOptions group(long initialGroup) {
        return of(null, initialGroup, null);
    }

    public static OpenOptions withOpenInfo(byte[] openInfo) {
        return of(null, null, openInfo);
    }

    public static OpenOptions withOpenInfo(String openInfo) {
        return withOpenInfo(Objects.requireNonNull(openInfo, "openInfo").getBytes(StandardCharsets.UTF_8));
    }

    public static Builder builder() {
        return new Builder();
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

    public boolean hasInitialPriority() {
        return initialPriority != null;
    }

    public Long initialGroup() {
        return initialGroup;
    }

    public boolean hasInitialGroup() {
        return initialGroup != null;
    }

    public byte[] openInfo() {
        return openInfo.length == 0 ? EMPTY_OPEN_INFO : Arrays.copyOf(openInfo, openInfo.length);
    }

    public int openInfoLength() {
        return openInfo.length;
    }

    public boolean hasOpenInfo() {
        return openInfo.length != 0;
    }

    public boolean isEmpty() {
        return initialPriority == null && initialGroup == null && openInfo.length == 0;
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

    public static final class Builder {
        private Long initialPriority;
        private Long initialGroup;
        private byte[] openInfo = EMPTY_OPEN_INFO;

        private Builder() {
        }

        public Builder initialPriority(long initialPriority) {
            this.initialPriority = requireOptionalVarint62(initialPriority, "initialPriority");
            return this;
        }

        public Builder priority(long initialPriority) {
            return initialPriority(initialPriority);
        }

        public Builder initialGroup(long initialGroup) {
            this.initialGroup = requireOptionalVarint62(initialGroup, "initialGroup");
            return this;
        }

        public Builder group(long initialGroup) {
            return initialGroup(initialGroup);
        }

        public Builder openInfo(byte[] openInfo) {
            this.openInfo = normalizeOpenInfo(openInfo);
            return this;
        }

        public Builder openInfo(String openInfo) {
            return openInfo(Objects.requireNonNull(openInfo, "openInfo").getBytes(StandardCharsets.UTF_8));
        }

        public OpenOptions build() {
            return OpenOptions.of(initialPriority, initialGroup, openInfo);
        }
    }
}
