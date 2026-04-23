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

    public static MetadataUpdate of(Long priority, Long group) {
        return new MetadataUpdate(priority, group);
    }

    public static MetadataUpdate priority(long priority) {
        return new MetadataUpdate(priority, null);
    }

    public static MetadataUpdate group(long group) {
        return new MetadataUpdate(null, group);
    }

    public static Builder builder() {
        return new Builder();
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

    public static final class Builder {
        private Long priority;
        private Long group;

        private Builder() {
        }

        public Builder priority(long priority) {
            this.priority = requireOptionalVarint62(priority, "priority");
            return this;
        }

        public Builder group(long group) {
            this.group = requireOptionalVarint62(group, "group");
            return this;
        }

        public MetadataUpdate build() {
            return new MetadataUpdate(priority, group);
        }
    }
}
