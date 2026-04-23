package io.zmux;

public enum SchedulerHint {
    UNSPECIFIED_OR_BALANCED(0),
    LATENCY(1),
    BALANCED_FAIR(2),
    BULK_THROUGHPUT(3),
    GROUP_FAIR(4);

    private final long code;

    SchedulerHint(long code) {
        this.code = code;
    }

    public static SchedulerHint fromCode(long code) {
        for (SchedulerHint value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        return UNSPECIFIED_OR_BALANCED;
    }

    public long code() {
        return code;
    }
}
