package io.zmux;

import java.time.Duration;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Configuration of a {@link ZmuxListener}, immutable; {@link #defaults()}, {@link #of(ZmuxConfig)}
 * or {@link #builder()}.
 */
public final class ListenerConfig {
    /** The longest instance ID. */
    public static final int MAX_INSTANCE_ID = 8;

    /**
     * The default {@link #firstRecordTimeout()}, 2 s; also what {@link Zmux#handleLane} gives a lane carrier to
     * deliver its header, which is its first record (the session it belongs to is not known before it is read).
     */
    public static final Duration DEFAULT_FIRST_RECORD_TIMEOUT = Duration.ofSeconds(2);

    private static final ListenerConfig DEFAULTS = builder().build();

    private final ZmuxConfig session;
    private final TlsOptions tls;
    private final int backlog;
    private final ListenerLimits limits;
    private final int sourcePrefixV4, sourcePrefixV6;
    private final byte[] instanceId;
    private final BiConsumer<Long, Long> memoryPressureListener;
    private final Admission admission;
    private final Admission.Fallback fallback;
    private final InboundTransform inboundTransform;
    private final Consumer<RejectInfo> onReject;
    private final RejectInfo.Action rejection;
    private final SessionConfigs sessionConfigs;
    private final System.Logger logger;

    private ListenerConfig(Builder b) {
        session = b.session; tls = b.tls; backlog = b.backlog;
        limits = b.limits.build();
        sourcePrefixV4 = b.sourcePrefixV4; sourcePrefixV6 = b.sourcePrefixV6;
        instanceId = b.instanceId.clone(); memoryPressureListener = b.memoryPressureListener;
        admission = b.admission; fallback = b.fallback;
        inboundTransform = b.inboundTransform; onReject = b.onReject; rejection = b.rejection;
        sessionConfigs = b.sessionConfigs; logger = b.logger;
    }

    public static ListenerConfig defaults() { return DEFAULTS; }

    /** The defaults with this session configuration. */
    public static ListenerConfig of(ZmuxConfig session) { return builder().session(session).build(); }

    public static Builder builder() { return new Builder(); }

    /** A builder starting from this configuration. */
    public Builder toBuilder() {
        Builder b = new Builder();
        b.session = session; b.tls = tls; b.backlog = backlog;
        b.limits = limits.toBuilder();
        b.sourcePrefixV4 = sourcePrefixV4; b.sourcePrefixV6 = sourcePrefixV6;
        b.instanceId = instanceId.clone(); b.memoryPressureListener = memoryPressureListener;
        b.admission = admission; b.fallback = fallback;
        b.inboundTransform = inboundTransform; b.onReject = onReject; b.rejection = rejection;
        b.sessionConfigs = sessionConfigs; b.logger = logger;
        return b;
    }

    /** The configuration of accepted and dialled sessions. */
    public ZmuxConfig session() { return session; }
    /** Server TLS of accepted socket carriers; null accepts plain TCP. */
    public TlsOptions tls() { return tls; }
    /** Sessions waiting for {@link ZmuxListener#accept()}; later ones are refused. */
    public int backlog() { return backlog; }
    /**
     * The listener's shared budget for buffered and retained memory and advertised
     * receive-window reservations. The default is 1 GiB. The empty value
     * ({@link Builder#noMemoryBudget()}) means unlimited. This replaces each session's own budget.
     */
    public OptionalLong memoryBudget() { return limits.memoryBudget(); }
    /**
     * The bound of the listener's carriers before a session, from arrival through the outer
     * handshake, the admission, the inbound transform, a split half's wait for its pair, the
     * classification, a join's verification and the exchange of a new session's transport
     * parameters, and through a stall; default 1024, empty ({@link Builder#noMaxPending()}) is
     * unlimited. A carrier holds its slot as long as its slot of {@link #maxPendingPerSource()}.
     * When every slot is taken, a new carrier closes the oldest stalled carrier; with none
     * stalled it is closed at once, without a byte ({@link ListenerStats#rejected()}). Stalled
     * carriers hold at most half of it (rounded down); a further stall is closed at once
     * ({@link AdmissionStats#stallOverflow()}).
     */
    public OptionalInt maxPending() { return limits.maxPending(); }
    /**
     * Carriers of one source (as {@link #sourcePrefixV4()} and {@link #sourcePrefixV6()} aggregate
     * it) before a session, over the same stages as {@link #maxPending()}; default 8.
     */
    public int maxPendingPerSource() { return limits.maxPendingPerSource(); }
    /**
     * The leading bits of an IPv4 source address that name its client, 1 to 32, default 32 (the
     * address on its own). An IPv4-mapped IPv6 address counts as IPv4. The one aggregation of
     * everything the listener keeps per source: {@link #maxPendingPerSource()}, the count of a
     * source's stalled carriers and the stall duration. A shorter prefix groups more clients into
     * one share, which tightens the bound against one network and also lets its clients crowd
     * each other out; the defaults suit public listeners.
     */
    public int sourcePrefixV4() { return sourcePrefixV4; }
    /** The leading bits of an IPv6 source address that name its client, 1 to 128, default 64; as {@link #sourcePrefixV4()}. */
    public int sourcePrefixV6() { return sourcePrefixV6; }
    /** How long a new carrier may take to send its first record; default {@link #DEFAULT_FIRST_RECORD_TIMEOUT}. */
    public Duration firstRecordTimeout() { return limits.firstRecordTimeout(); }
    /** The leading bytes of every 16-byte session locator; random bytes fill the rest. */
    public byte[] instanceId() { return instanceId.clone(); }
    /** Suspended session bound; zero permits none. */
    public int maxSuspended() { return limits.maxSuspended(); }
    /**
     * The bounds of this configuration that {@link ZmuxListener#setLimits} changes later: the
     * listener's initial {@link ZmuxListener#limits()}.
     */
    public ListenerLimits limits() { return limits; }
    /** Ordered off-loop callback with (used, limit) on pressure entry and recovery; may be null. */
    public BiConsumer<Long, Long> memoryPressureListener() { return memoryPressureListener; }
    /**
     * The admission of every new carrier: the listener's socket and TLS carriers and those
     * given to {@link io.zmux.adapter.ListenerAdapter#handle}; null: none.
     */
    public Admission admission() { return admission; }
    /**
     * The carrier handler for every {@link AdmissionVerdict#TAKEOVER} and for
     * {@link AdmissionVerdict#FALLBACK} on a byte stream or upgraded transport,
     * including a WebSocket after upgrade. An HTTP request's fallback is its adapter's
     * HTTP handler. A decision that selects a missing handler stalls the carrier.
     */
    public Admission.Fallback fallback() { return fallback; }

    /** The transform of every carrier after its admission; null: none. */
    public InboundTransform inboundTransform() { return inboundTransform; }
    /** Observes local rejection diagnostics. */
    public Consumer<RejectInfo> onReject() { return onReject; }

    /** The silent refusal policy for an unauthenticated Establish or Join. */
    public RejectInfo.Action rejection() { return rejection; }

    /** The chooser of each new session's configuration; null: {@link #session()} for all. */
    public SessionConfigs sessionConfigs() { return sessionConfigs; }

    /** The listener's own logger; null: the logger of {@link #session()}. */
    public System.Logger logger() { return logger; }

    /**
     * Chooses each new session's configuration, for example {@link PipeAuth#exporter()} where the
     * process terminates TLS and {@link PipeAuth#deferred()} where an intermediary does, or
     * per-session shaping limits. Called once per carrier that starts a session, never for a
     * join, outside every lock, on the listener's event loop: it must not block, and it must
     * return well before the carrier's handshake deadline, which keeps running while it does
     * (a carrier whose deadline passes is closed). What belongs to
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
        private ListenerLimits.Builder limits = ListenerLimits.builder();
        private int sourcePrefixV4 = io.zmux.internal.core.CarrierAdmission.DEFAULT_PREFIX_V4;
        private int sourcePrefixV6 = io.zmux.internal.core.CarrierAdmission.DEFAULT_PREFIX_V6;
        private byte[] instanceId = new byte[0];
        private BiConsumer<Long, Long> memoryPressureListener;
        private Admission admission;
        private Admission.Fallback fallback;
        private InboundTransform inboundTransform;
        private Consumer<RejectInfo> onReject;
        private RejectInfo.Action rejection = RejectInfo.Action.CLOSE;
        private SessionConfigs sessionConfigs;
        private System.Logger logger;

        private Builder() {}

        /** The chooser of each new session's configuration; null (the default): {@link ListenerConfig#session()}. */
        public Builder sessionConfigs(SessionConfigs c) { sessionConfigs = c; return this; }

        /**
         * The logger of the listener's own warnings, failures of its host callbacks ({@link #onReject()},
         * {@link #sessionConfigs()}) among them. It is called outside every lock and must not block. Null
         * (the default): the logger of {@link #session()}. Sessions log to their own configuration's.
         */
        public Builder logger(System.Logger l) { logger = l; return this; }

        /** The transform of every carrier after its admission ({@link InboundTransform}); null (the default): none. */
        public Builder inboundTransform(InboundTransform t) { inboundTransform = t; return this; }

        /**
         * Observes local first-message rejections. The callback runs outside every zmux lock
         * and must return promptly. {@link #rejection} selects the same outcome for all reasons.
         */
        public Builder onReject(Consumer<RejectInfo> observer) { onReject = observer; return this; }

        /**
         * Selects the entry's silent close or stall for Establish and Join refusals.
         * CLOSE is the default. A stall uses the stage's handshake deadline and the
         * listener's existing stall bounds and source limits.
         */
        public Builder rejection(RejectInfo.Action action) {
            rejection = Objects.requireNonNull(action);
            return this;
        }

        /** The admission of every new carrier; null (the default): none. */
        public Builder admission(Admission a) { admission = a; return this; }

        /** Sets the listener's carrier handler. A decision selecting this handler stalls if it is null. */
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

        /** The listener's memory budget ({@link ListenerConfig#memoryBudget()}), default 1 GiB; not negative. */
        public Builder memoryBudget(long n) { limits.memoryBudget(n); return this; }

        /** An unlimited memory budget. */
        public Builder noMemoryBudget() { limits.noMemoryBudget(); return this; }

        /** The bound of the listener's carriers before a session ({@link ListenerConfig#maxPending()}), default 1024; at least 1. */
        public Builder maxPending(int n) { limits.maxPending(n); return this; }

        /** No bound of the listener's carriers before a session; the per-source bound still applies. */
        public Builder noMaxPending() { limits.noMaxPending(); return this; }

        /** Carriers of one source before a session ({@link ListenerConfig#maxPendingPerSource()}), default 8; at least 2. */
        public Builder maxPendingPerSource(int n) { limits.maxPendingPerSource(n); return this; }

        /** The IPv4 source prefix ({@link ListenerConfig#sourcePrefixV4()}), default 32; from 1 to 32. */
        public Builder sourcePrefixV4(int bits) {
            if (bits < 1 || bits > 32) throw new IllegalArgumentException("sourcePrefixV4 outside 1 to 32");
            sourcePrefixV4 = bits; return this;
        }

        /** The IPv6 source prefix ({@link ListenerConfig#sourcePrefixV6()}), default 64; from 1 to 128. */
        public Builder sourcePrefixV6(int bits) {
            if (bits < 1 || bits > 128) throw new IllegalArgumentException("sourcePrefixV6 outside 1 to 128");
            sourcePrefixV6 = bits; return this;
        }


        /** How long a new carrier may take to send its first record, default {@link #DEFAULT_FIRST_RECORD_TIMEOUT}; positive. */
        public Builder firstRecordTimeout(Duration d) { limits.firstRecordTimeout(d); return this; }

        /** At most {@link #MAX_INSTANCE_ID} bytes. */
        public Builder instanceId(byte[] id) {
            byte[] v = id == null ? new byte[0] : id.clone();
            if (v.length > MAX_INSTANCE_ID) throw new IllegalArgumentException("instanceId is " + v.length + " bytes, at most " + MAX_INSTANCE_ID);
            instanceId = v; return this;
        }

        /** The suspended session bound, default 1024; zero permits none. */
        public Builder maxSuspended(int n) { limits.maxSuspended(n); return this; }

        /** All the bounds {@link ListenerLimits} holds at once, replacing those set before. */
        public Builder limits(ListenerLimits l) { limits = l.toBuilder(); return this; }

        public Builder memoryPressureListener(BiConsumer<Long, Long> listener) { memoryPressureListener = listener; return this; }

        public ListenerConfig build() { return new ListenerConfig(this); }
    }
}
