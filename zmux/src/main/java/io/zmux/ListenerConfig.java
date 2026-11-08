package io.zmux;

import java.time.Duration;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.BiConsumer;

/**
 * Configuration of a {@link ZmuxListener}, immutable; {@link #defaults()}, {@link #of(ZmuxConfig)}
 * or {@link #builder()}.
 */
public final class ListenerConfig {
    /** The longest instance ID. */
    public static final int MAX_INSTANCE_ID = 8;

    private static final ListenerConfig DEFAULTS = builder().build();
    /** {@link #memoryBudget} unlimited. */
    private static final long NO_BUDGET = -1;

    private final ZmuxConfig session;
    private final TlsOptions tls;
    private final int backlog;
    private final long memoryBudget; // NO_BUDGET: unlimited
    private final int maxPendingPerSource;
    private final int maxPendingPerSession;
    private final Duration firstRecordTimeout;
    private final byte[] instanceId;
    private final int maxSuspended;
    private final BiConsumer<Long, Long> memoryPressureListener;
    private final Admission admission;
    private final Admission.Fallback fallback;
    private final InboundTransform inboundTransform;
    private final java.util.function.Function<RejectInfo, RejectInfo.Action> onReject;
    private final SessionConfigs sessionConfigs;
    private final System.Logger logger;

    private ListenerConfig(Builder b) {
        session = b.session; tls = b.tls; backlog = b.backlog; memoryBudget = b.memoryBudget;
        maxPendingPerSource = b.maxPendingPerSource; maxPendingPerSession = b.maxPendingPerSession; firstRecordTimeout = b.firstRecordTimeout;
        instanceId = b.instanceId.clone(); maxSuspended = b.maxSuspended; memoryPressureListener = b.memoryPressureListener;
        admission = b.admission; fallback = b.fallback;
        inboundTransform = b.inboundTransform; onReject = b.onReject; sessionConfigs = b.sessionConfigs; logger = b.logger;
    }

    public static ListenerConfig defaults() { return DEFAULTS; }

    /** The defaults with this session configuration. */
    public static ListenerConfig of(ZmuxConfig session) { return builder().session(session).build(); }

    public static Builder builder() { return new Builder(); }

    /** A builder starting from this configuration. */
    public Builder toBuilder() {
        Builder b = new Builder();
        b.session = session; b.tls = tls; b.backlog = backlog; b.memoryBudget = memoryBudget;
        b.maxPendingPerSource = maxPendingPerSource; b.maxPendingPerSession = maxPendingPerSession; b.firstRecordTimeout = firstRecordTimeout;
        b.instanceId = instanceId.clone(); b.maxSuspended = maxSuspended; b.memoryPressureListener = memoryPressureListener;
        b.admission = admission; b.fallback = fallback;
        b.inboundTransform = inboundTransform; b.onReject = onReject; b.sessionConfigs = sessionConfigs; b.logger = logger;
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
    public OptionalLong memoryBudget() { return memoryBudget == NO_BUDGET ? OptionalLong.empty() : OptionalLong.of(memoryBudget); }
    /** Carriers of one source address that have not yet sent their first record. */
    public int maxPendingPerSource() { return maxPendingPerSource; }
    /**
     * The Unconfirmed join carriers one session keeps (authenticated Hellos whose Confirm has not
     * arrived): one more evicts the session's oldest.
     */
    public int maxPendingPerSession() { return maxPendingPerSession; }
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
     * The answer to an unauthenticated carrier the listener refused (a Join, an unknown Locator
     * included, or an Establish a carrier transform refused); null: the default, which stalls an
     * Establish refused before authentication and closes everything else.
     */
    public java.util.function.Function<RejectInfo, RejectInfo.Action> onReject() { return onReject; }

    /** The chooser of each new session's configuration; null: {@link #session()} for all. */
    public SessionConfigs sessionConfigs() { return sessionConfigs; }

    /** The listener's own logger; null: the logger of {@link #session()}. */
    public System.Logger logger() { return logger; }

    /**
     * Chooses each new session's configuration, for example {@link PipeAuth#exporter()} where the
     * process terminates TLS and {@link PipeAuth#deferred()} where an intermediary does, or
     * per-session shaping limits. Called once per carrier that starts a session, never for a
     * join, outside every lock, on the listener's event loop: it must not block. What belongs to
     * the listener stays its own (its memory budget, its loops, the carrier-level bounds before a
     * session is known): the returned configuration's values for those are not used.
     */
    @FunctionalInterface
    public interface SessionConfigs {
        /**
         * The configuration of the session carrier starts; null keeps {@link #session()}. An
         * exception, like an invalid configuration, closes the carrier without a byte, counted
         * as rejected.
         *
         * @param admitted the admission's {@link Admission.Decision#value()}, or null
         */
        ZmuxConfig choose(Admission.CarrierInfo info, Object admitted);
    }

    /** Builds a {@link ListenerConfig}; every setter validates its value. */
    public static final class Builder {
        private ZmuxConfig session = ZmuxConfig.defaults();
        private TlsOptions tls;
        private int backlog = 128;
        private long memoryBudget = 1L << 30;
        private int maxPendingPerSource = 8;
        private int maxPendingPerSession = 4;
        private Duration firstRecordTimeout = Duration.ofSeconds(2);
        private byte[] instanceId = new byte[0];
        private int maxSuspended = 1024;
        private BiConsumer<Long, Long> memoryPressureListener;
        private Admission admission;
        private Admission.Fallback fallback;
        private InboundTransform inboundTransform;
        private java.util.function.Function<RejectInfo, RejectInfo.Action> onReject;
        private SessionConfigs sessionConfigs;
        private System.Logger logger;

        private Builder() {}

        /** The chooser of each new session's configuration; null (the default): {@link ListenerConfig#session()}. */
        public Builder sessionConfigs(SessionConfigs c) { sessionConfigs = c; return this; }

        /**
         * The logger of the listener's own warnings, failures of its host callbacks ({@link #onReject},
         * {@link #sessionConfigs}) among them. It is called outside every lock and must not block. Null
         * (the default): the logger of {@link #session}. Sessions log to their own configuration's.
         */
        public Builder logger(System.Logger l) { logger = l; return this; }

        /** The transform of every carrier after its admission ({@link InboundTransform}); null (the default): none. */
        public Builder inboundTransform(InboundTransform t) { inboundTransform = t; return this; }

        /**
         * Answers a refused unauthenticated carrier, nothing written either way. A stalled Join
         * ends one join timeout of {@link #session} after the refusal, whatever the session its
         * Locator names configures; a known Locator declined under memory pressure is refused as
         * {@code memory_pressure} and answered the same way. A stalled Establish at the handshake timeout, and never later than
         * the maximum stall duration (the admission's, or one drawn when there is none). A source
         * already holding its maximum of stalled carriers has the carrier closed at once
         * ({@link AdmissionStats#stallOverflow}). Null (the default): an Establish refused before
         * authentication stalls, a Join and every other refusal closes. A failing function closes.
         */
        public Builder onReject(java.util.function.Function<RejectInfo, RejectInfo.Action> f) { onReject = f; return this; }

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
            memoryBudget = n; return this;
        }

        /** An unlimited memory budget. */
        public Builder noMemoryBudget() { memoryBudget = NO_BUDGET; return this; }

        public Builder maxPendingPerSource(int n) {
            if (n < 1) throw new IllegalArgumentException("maxPendingPerSource below 1");
            maxPendingPerSource = n; return this;
        }

        /** The Unconfirmed join carriers one session keeps, default 4; at least 1. */
        public Builder maxPendingPerSession(int n) {
            if (n < 1) throw new IllegalArgumentException("maxPendingPerSession below 1");
            maxPendingPerSession = n; return this;
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
