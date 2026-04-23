package io.zmux;

import java.util.Objects;

public final class MetadataUpdate {
    private final Long priority;
    private final Long group;

    public MetadataUpdate(Long priority, Long group) {
        priority = requireOptionalVarint62(priority, "priority");
        group = requireOptionalVarint62(group, "group");
        this.priority = priority;
        this.group = group;
    }

    private static Long requireOptionalVarint62(Long value, String field) {
        if (value == null) {
            return null;
        }
        if (value < 0L || value > Protocol.MAX_VARINT62) {
            throw new IllegalArgumentException("zmux metadata update " + field + " must be within varint62 range");
        }
        return value;
    }

    public boolean empty() {
        return priority == null && group == null;
    }

    public Long priority() {
        return priority;
    }

    public Long group() {
        return group;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof MetadataUpdate)) {
            return false;
        }
        MetadataUpdate that = (MetadataUpdate) other;
        return Objects.equals(priority, that.priority) && Objects.equals(group, that.group);
    }

    @Override
    public int hashCode() {
        return Objects.hash(priority, group);
    }

    @Override
    public String toString() {
        return "MetadataUpdate[priority=" + priority + ", group=" + group + "]";
    }
}
