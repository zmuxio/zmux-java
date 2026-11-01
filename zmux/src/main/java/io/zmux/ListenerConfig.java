package io.zmux;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Configuration of a {@link ZmuxListener}, immutable; {@link #defaults()}, {@link #of(ZmuxConfig)}
 * or {@link #builder()}.
 */
public final class ListenerConfig {
    /** The longest instance ID. */
    public static final int MAX_INSTANCE_ID = 8;

    private static final ListenerConfig DEFAULTS = builder().build();

    private final ZmuxConfig session;
    private final TlsOptions tls;
    private final int backlog;
    private final java.util.OptionalLong memoryBudget;
    private final int maxPendingPerSource;
    private final Duration firstRecordTimeout;
    private final byte[] instanceId;
    private final int maxSuspended;
    private final BiConsumer<Long, Long> memoryPressureListener;
    private final Admission admission;
    private final Admission.Fallback fallback;
    private final InboundTransform inboundTransform;
    private final boolean answerUnknownJoins;

    private ListenerConfig(Builder b) {
        session = b.session; tls = b.tls; backlog = b.backlog; memoryBudget = b.memoryBudget;
        maxPendingPerSource = b.maxPendingPerSource; firstRecordTimeout = b.firstRecordTimeout;
        instanceId = b.instanceId.clone(); maxSuspended = b.maxSuspended; memoryPressureListener = b.memoryPressureListener;
        admission = b.admission; fallback = b.fallback;
        inboundTransform = b.inboundTransform; answerUnknownJoins = b.answerUnknownJoins;
    }

    public static ListenerConfig defaults() { return DEFAULTS; }

    /** The defaults with this session configuration. */
    public static ListenerConfig of(ZmuxConfig session) { return builder().session(session).build(); }

    public static Builder builder() { return new Builder(); }

    /** A builder starting from this configuration. */
    public Builder toBuilder() {
        Builder b = new Builder();
        b.session = session; b.tls = tls; b.backlog = backlog; b.memoryBudget = memoryBudget;
        b.maxPendingPerSource = maxPendingPerSource; b.firstRecordTimeout = firstRecordTimeout;
        b.instanceId = instanceId.clone(); b.maxSuspended = maxSuspended; b.memoryPressureListener = memoryPressureListener;
        b.admission = admission; b.fallback = fallback;
        b.inboundTransform = inboundTransform; b.answerUnknownJoins = answerUnknownJoins;
        return b;
    }

    /** The configuration of accepted and dialled sessions. */
    public ZmuxConfig session() { return session; }
    /** Server TLS of accepted socket carriers; null accepts plain TCP. */
    public TlsOptions tls() { return tls; }
    /** Sessions waiting for {@link ZmuxListener#accept()}; later ones are refused. */
    public int backlog() { return backlog; }
    /**
     * The bound of the sum over the listener's sessions of buffered, retained and
     * window-growth bytes, default 1 GiB; empty ({@link Builder#noMemoryBudget()}) is unlimited. It replaces
     * the sessions' own budget.
     */
    public java.util.OptionalLong memoryBudget() { return memoryBudget; }
    /** Carriers of one source address that have not yet sent their first record. */
    public int maxPendingPerSource() { return maxPendingPerSource; }
    /** How long a new carrier may take to send its first record. */
    public Duration firstRecordTimeout() { return firstRecordTimeout; }
    /** The leading bytes of every 16-byte session locator; random bytes fill the rest. */
    public byte[] instanceId() { return instanceId.clone(); }
    /** Suspended session bound; zero permits none. */
    public int maxSuspended() { return maxSuspended; }
    /** Ordered off-loop callback with (used, limit) on pressure entry and recovery; may be null. */
    public BiConsumer<Long, Long> memoryPressureListener() { return memoryPressureListener; }
    /**
     * The admission of every new carrier: the listener's socket and TLS carriers and those
     * given to {@link ZmuxListener#handle}; null: none.
     */
    public Admission admission() { return admission; }
    /** The fallback target of the listener's socket and TLS carriers; null: none (a fallback stalls). */
    public Admission.Fallback fallback() { return fallback; }

    /** The transform of every carrier after its admission; null: none. */
    public InboundTransform inboundTransform() { return inboundTransform; }
    /**
     * Whether the listener answers an admitted, well-formed join whose locator starts with the
     * {@link #instanceId()} but names no session here with CONNECTION_CLOSE
     * ZMUX_RESUME_FAILED, so that a suspended client fails at once instead of at its resume
     * timeout; false by default.
     */
    public boolean answerUnknownJoins() { return answerUnknownJoins; }

    /** Builds a {@link ListenerConfig}; every setter validates its value. */
    public static final class Builder {
        private ZmuxConfig session = ZmuxConfig.defaults();
        private TlsOptions tls;
        private int backlog = 128;
        private java.util.OptionalLong memoryBudget = java.util.OptionalLong.of(1L << 30);
        private int maxPendingPerSource = 8;
        private Duration firstRecordTimeout = Duration.ofSeconds(2);
        private byte[] instanceId = new byte[0];
        private int maxSuspended = 1024;
        private BiConsumer<Long, Long> memoryPressureListener;
        private Admission admission;
        private Admission.Fallback fallback;
        private InboundTransform inboundTransform;
        private boolean answerUnknownJoins;

        private Builder() {}

        /** The transform of every carrier after its admission ({@link InboundTransform}); null (the default): none. */
        public Builder inboundTransform(InboundTransform t) { inboundTransform = t; return this; }

        /**
         * Answers an admitted carrier whose well-formed join (in band or out of band) names a
         * locator that starts with the instance ID but no session here: the listener writes
         * CONNECTION_CLOSE with ZMUX_RESUME_FAILED and ends the carrier (MULTIPATH.md 5.4); the
         * carrier still counts as rejected, and every other rejected carrier gets no byte. Off
         * by default, for correctness: where a load balancer can route a join to an instance
         * with the same instance ID that does not hold its session, the answer would end a
         * session that lives on. Turn it on for a single instance or unique instance IDs.
         */
        public Builder answerUnknownJoins(boolean on) { answerUnknownJoins = on; return this; }

        /** The admission of every new carrier; null (the default): none. */
        public Builder admission(Admission a) { admission = a; return this; }

        /** The fallback target of the listener's socket and TLS carriers; null (the default): a fallback stalls. */
        public Builder fallback(Admission.Fallback f) { fallback = f; return this; }

        public Builder session(ZmuxConfig cfg) { session = Objects.requireNonNull(cfg); return this; }

        /** Server TLS for socket carriers; null (the default) accepts plain TCP. */
        public Builder tls(TlsOptions options) {
            if (options != null && options.serverName() != null) throw new IllegalArgumentException("server TLS has no server name");
            tls = options; return this;
        }

        public Builder backlog(int n) {
            if (n < 1) throw new IllegalArgumentException("backlog below 1");
            backlog = n; return this;
        }

        public Builder memoryBudget(long n) {
            if (n < 0) throw new IllegalArgumentException("negative memoryBudget");
            memoryBudget = java.util.OptionalLong.of(n); return this;
        }

        /** An unlimited memory budget. */
        public Builder noMemoryBudget() { memoryBudget = java.util.OptionalLong.empty(); return this; }

        public Builder maxPendingPerSource(int n) {
            if (n < 1) throw new IllegalArgumentException("maxPendingPerSource below 1");
            maxPendingPerSource = n; return this;
        }

        public Builder firstRecordTimeout(Duration d) {
            if (d.isNegative() || d.isZero()) throw new IllegalArgumentException("firstRecordTimeout must be positive");
            firstRecordTimeout = d; return this;
        }

        /** At most {@link #MAX_INSTANCE_ID} bytes. */
        public Builder instanceId(byte[] id) {
            byte[] v = id == null ? new byte[0] : id.clone();
            if (v.length > MAX_INSTANCE_ID) throw new IllegalArgumentException("instanceId is " + v.length + " bytes, at most " + MAX_INSTANCE_ID);
            instanceId = v; return this;
        }

        public Builder maxSuspended(int n) {
            if (n < 0) throw new IllegalArgumentException("negative maxSuspended");
            maxSuspended = n; return this;
        }

        public Builder memoryPressureListener(BiConsumer<Long, Long> listener) { memoryPressureListener = listener; return this; }

        public ListenerConfig build() { return new ListenerConfig(this); }
    }
}
