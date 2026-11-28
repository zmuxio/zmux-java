package io.zmux;

/** Enables completed-record accounting and bounds the distinct physical sources of each attachment. */
public record AccountingPolicy(boolean enabled, int sourceLimit, boolean strict) {
    public static final AccountingPolicy OFF = new AccountingPolicy(false, 0, false);

    public AccountingPolicy {
        if (sourceLimit < 0) throw new IllegalArgumentException("negative accounting source limit");
        if (sourceLimit == 0) sourceLimit = 16;
    }
}
