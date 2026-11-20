package io.zmux;

import java.time.Duration;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * The bounds of a {@link ZmuxListener} that {@link ZmuxListener#setLimits} changes while it runs,
 * immutable; {@link #defaults()} or {@link #builder()}. Each value means what the {@link
 * ListenerConfig} accessor of the same name means; {@link ListenerConfig#limits()} gives the
 * initial values, the listener holds the live ones ({@link ZmuxListener#limits()}).
 *
 * <p>The other settings of a {@link ListenerConfig} are fixed for the listener's life: the
 * backlog (the capacity of the accept queue), the source prefixes (they key the slots live
 * carriers hold), the instance ID, the callbacks and the session configuration, whose handshake
 * timeout bounds the carriers. The
 * bounds of a session itself are its {@link ZmuxConfig}: {@link ListenerConfig.SessionConfigs},
 * called for every new session, returns the one current at the time; an established session keeps
 * the configuration it started with.
 */
public final class ListenerLimits {
    /** {@link #maxPending()} unlimited. */
    private static final int NO_MAX_PENDING = -1;
    /** {@link #memoryBudget()} unlimited. */
    private static final long NO_BUDGET = -1;
    private static final ListenerLimits DEFAULTS = builder().build();

    private final int maxPending; // NO_MAX_PENDING: unlimited
    private final int maxPendingPerSource;
    private final int maxPendingPerSession;
    private final int maxSuspended;
    private final Duration firstRecordTimeout;
    private final long memoryBudget; // NO_BUDGET: unlimited

    private ListenerLimits(Builder b) {
        maxPending = b.maxPending; maxPendingPerSource = b.maxPendingPerSource; maxPendingPerSession = b.maxPendingPerSession;
        maxSuspended = b.maxSuspended; firstRecordTimeout = b.firstRecordTimeout; memoryBudget = b.memoryBudget;
    }

    public static ListenerLimits defaults() { return DEFAULTS; }

    public static Builder builder() { return new Builder(); }

    /** A builder starting from these limits. */
    public Builder toBuilder() {
        Builder b = new Builder();
        b.maxPending = maxPending; b.maxPendingPerSource = maxPendingPerSource; b.maxPendingPerSession = maxPendingPerSession;
        b.maxSuspended = maxSuspended; b.firstRecordTimeout = firstRecordTimeout; b.memoryBudget = memoryBudget;
        return b;
    }

    /** {@link ListenerConfig#maxPending()}; empty: unlimited. */
    public OptionalInt maxPending() { return maxPending == NO_MAX_PENDING ? OptionalInt.empty() : OptionalInt.of(maxPending); }
    /** {@link ListenerConfig#maxPendingPerSource()}. */
    public int maxPendingPerSource() { return maxPendingPerSource; }
    /** {@link ListenerConfig#maxPendingPerSession()}. */
    public int maxPendingPerSession() { return maxPendingPerSession; }
    /** {@link ListenerConfig#maxSuspended()}. */
    public int maxSuspended() { return maxSuspended; }
    /** {@link ListenerConfig#firstRecordTimeout()}. */
    public Duration firstRecordTimeout() { return firstRecordTimeout; }
    /** {@link ListenerConfig#memoryBudget()}; empty: unlimited. */
    public OptionalLong memoryBudget() { return memoryBudget == NO_BUDGET ? OptionalLong.empty() : OptionalLong.of(memoryBudget); }

    @Override
    public boolean equals(Object o) {
        return o instanceof ListenerLimits l && maxPending == l.maxPending && maxPendingPerSource == l.maxPendingPerSource
                && maxPendingPerSession == l.maxPendingPerSession && maxSuspended == l.maxSuspended
                && firstRecordTimeout.equals(l.firstRecordTimeout) && memoryBudget == l.memoryBudget;
    }

    @Override
    public int hashCode() { return Objects.hash(maxPending, maxPendingPerSource, maxPendingPerSession, maxSuspended, firstRecordTimeout, memoryBudget); }

    @Override
    public String toString() {
        return "ListenerLimits[maxPending=" + (maxPending == NO_MAX_PENDING ? "unlimited" : maxPending) + ", maxPendingPerSource=" + maxPendingPerSource
                + ", maxPendingPerSession=" + maxPendingPerSession + ", maxSuspended=" + maxSuspended + ", firstRecordTimeout=" + firstRecordTimeout
                + ", memoryBudget=" + (memoryBudget == NO_BUDGET ? "unlimited" : memoryBudget) + "]";
    }

    /** Builds {@link ListenerLimits}; every setter validates its value, the only validation of these bounds. */
    public static final class Builder {
        private int maxPending = 1024;
        private int maxPendingPerSource = 8;
        private int maxPendingPerSession = 4;
        private int maxSuspended = 1024;
        private Duration firstRecordTimeout = ListenerConfig.DEFAULT_FIRST_RECORD_TIMEOUT;
        private long memoryBudget = 1L << 30;

        private Builder() {}

        /** The bound of the listener's carriers before a session ({@link ListenerConfig#maxPending()}), default 1024; at least 1. */
        public Builder maxPending(int n) {
            if (n < 1) throw new IllegalArgumentException("maxPending below 1");
            maxPending = n; return this;
        }

        /** No bound of the listener's carriers before a session; the per-source bound still applies. */
        public Builder noMaxPending() { maxPending = NO_MAX_PENDING; return this; }

        /** Carriers of one source before a session, default 8; at least 1. */
        public Builder maxPendingPerSource(int n) {
            if (n < 1) throw new IllegalArgumentException("maxPendingPerSource below 1");
            maxPendingPerSource = n; return this;
        }

        /** The Unconfirmed join carriers one session keeps, default 4; at least 1. */
        public Builder maxPendingPerSession(int n) {
            if (n < 1) throw new IllegalArgumentException("maxPendingPerSession below 1");
            maxPendingPerSession = n; return this;
        }

        /** The suspended session bound, default 1024; zero permits none. */
        public Builder maxSuspended(int n) {
            if (n < 0) throw new IllegalArgumentException("negative maxSuspended");
            maxSuspended = n; return this;
        }

        /** How long a new carrier may take to send its first record, default {@link ListenerConfig#DEFAULT_FIRST_RECORD_TIMEOUT}; positive. */
        public Builder firstRecordTimeout(Duration d) {
            if (d.isNegative() || d.isZero()) throw new IllegalArgumentException("firstRecordTimeout must be positive");
            firstRecordTimeout = d; return this;
        }

        /** The listener's memory budget ({@link ListenerConfig#memoryBudget()}), default 1 GiB; not negative. */
        public Builder memoryBudget(long n) {
            if (n < 0) throw new IllegalArgumentException("negative memoryBudget");
            memoryBudget = n; return this;
        }

        /** An unlimited memory budget. */
        public Builder noMemoryBudget() { memoryBudget = NO_BUDGET; return this; }

        public ListenerLimits build() { return new ListenerLimits(this); }
    }
}
