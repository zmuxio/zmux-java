package io.zmux;

import io.zmux.protocol.Protocol;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

public final class StreamMetadata {
    private static final byte[] EMPTY_OPEN_INFO = new byte[0];
    private static final StreamMetadata EMPTY = new StreamMetadata(0L, null, EMPTY_OPEN_INFO);

    private final long priority;
    private final Long group;
    private final byte[] openInfo;

    public StreamMetadata(long priority, Long group, byte[] openInfo) {
        requireVarint62(priority, "priority");
        requireOptionalVarint62(group, "group");
        this.priority = priority;
        this.group = normalizeGroup(group);
        this.openInfo = normalizeOpenInfo(openInfo);
    }

    public static StreamMetadata empty() {
        return EMPTY;
    }

    public static StreamMetadata of(long priority, Long group, byte[] openInfo) {
        if (priority == 0L && group == null && (openInfo == null || openInfo.length == 0)) {
            return EMPTY;
        }
        return new StreamMetadata(priority, group, openInfo);
    }

    public static StreamMetadata withOpenInfo(byte[] openInfo) {
        return of(0L, null, openInfo);
    }

    public static StreamMetadata withOpenInfo(String openInfo) {
        return withOpenInfo(Objects.requireNonNull(openInfo, "openInfo").getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] normalizeOpenInfo(byte[] openInfo) {
        if (openInfo == null || openInfo.length == 0) {
            return EMPTY_OPEN_INFO;
        }
        return Arrays.copyOf(openInfo, openInfo.length);
    }

    private static void requireVarint62(long value, String field) {
        if (value < 0L || value > Protocol.MAX_VARINT62) {
            throw new IllegalArgumentException("zmux stream metadata " + field + " must be within varint62 range");
        }
    }

    private static void requireOptionalVarint62(Long value, String field) {
        if (value == null) {
            return;
        }
        requireVarint62(value, field);
    }

    private static Long normalizeGroup(Long group) {
        return group == null || group == 0L ? null : group;
    }

    public long priority() {
        return priority;
    }

    public Long group() {
        return group;
    }

    public boolean hasGroup() {
        return group != null;
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
        return priority == 0L && group == null && openInfo.length == 0;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StreamMetadata)) {
            return false;
        }
        StreamMetadata that = (StreamMetadata) other;
        return priority == that.priority
                && Objects.equals(group, that.group)
                && Arrays.equals(openInfo, that.openInfo);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(priority, group);
        result = 31 * result + Arrays.hashCode(openInfo);
        return result;
    }

    @Override
    public String toString() {
        return "StreamMetadata[priority="
                + priority
                + ", group="
                + group
                + ", openInfoLength="
                + openInfo.length
                + "]";
    }
}
