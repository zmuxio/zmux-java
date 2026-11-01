package io.zmux;

import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.core.Time;
import io.zmux.internal.wire.TransportParams;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Session configuration (API.md Section 6), immutable; {@link #defaults()} or {@link #builder()}.
 * Durations accept any non-negative value; one beyond the clock's range never fires.
 */
public final class ZmuxConfig {
    /** The default ALPN identifier (SPEC.md Section 6.2). */
    public static final String DEFAULT_ALPN = "qmux-02";

    private static final java.security.SecureRandom RANDOM = new java.security.SecureRandom();
    private static final ZmuxConfig DEFAULTS = builder().build();

    final Timeouts timeouts;
    final FlowControl flow;
    final Limits limits;
    final MultipathConfig multipath;
    final boolean datagrams;
    final EarlyData earlyData;
    final List<String> alpn;
    final EventLoops eventLoops;
    final io.zmux.shaping.ShapingLimits shapingLimits;
    final io.zmux.shaping.ShaperFactory shaper;
    final Credential credential;
    final io.zmux.internal.loop.DriverHooks hooks;

    private ZmuxConfig(Builder b) {
        timeouts = b.timeouts; flow = b.flow; limits = b.limits; multipath = b.multipath;
        datagrams = b.datagrams; earlyData = b.earlyData; alpn = List.copyOf(b.alpn); eventLoops = b.eventLoops;
        shapingLimits = b.shapingLimits;
        shaper = b.shaper;
        credential = b.credential;
        SessionObserver observer = b.observer;
        hooks = new io.zmux.internal.loop.DriverHooks(observer == null ? null : e -> {
            if (e.kind() == io.zmux.internal.core.Event.Kind.ESTABLISHED) observer.established();
            else if (e.kind() == io.zmux.internal.core.Event.Kind.CLOSED) observer.closed(Errors.session(e.error()));
            else { SessionEvent event = Events.publicEvent(e); if (event != null) observer.event(event); }
        }, observer == null ? null : observer::dropped, b.observerQueue, b.allowWindowGrowth,
                b.datagramSend, b.datagramReceive, Scheduling.picker(multipath.scheduler()));
    }

    /** This configuration with early data turned off. */
    ZmuxConfig withoutEarlyData() {
        return earlyData == EarlyData.OFF ? this : new ZmuxConfig(this, EarlyData.OFF);
    }

    private ZmuxConfig(ZmuxConfig o, EarlyData earlyData) {
        timeouts = o.timeouts; flow = o.flow; limits = o.limits; multipath = o.multipath;
        datagrams = o.datagrams; this.earlyData = earlyData; alpn = o.alpn; eventLoops = o.eventLoops;
        shapingLimits = o.shapingLimits; shaper = o.shaper; credential = o.credential; hooks = o.hooks;
    }

    public Timeouts timeouts() { return timeouts; }
    public FlowControl flowControl() { return flow; }
    public Limits limits() { return limits; }
    public MultipathConfig multipathConfig() { return multipath; }
    public boolean datagrams() { return datagrams; }
    public EarlyData earlyData() { return earlyData; }

    /**
     * The credential presented on the carrier of a client session by the dialers that open it
     * with this configuration ({@link Zmux#connect(java.net.InetSocketAddress, ZmuxConfig)},
     * {@link Zmux#connectTls}, {@link ZmuxListener#connect}, {@link ZmuxListener#connectTls}
     * and the zmux-quic session dialers), and by the TCP and TLS pipes a session of this
     * configuration dials ({@link Paths#add(java.net.InetSocketAddress, PipeOptions)},
     * {@link Paths#addTls}); null: none. Pipes the application opens present their own
     * ({@link Credential}).
     */
    public Credential credential() { return credential; }

    /** Whether sessions offer multipath (more than one pipe, or resume). */
    public boolean multipath() {
        return multipath.enabled();
    }

    public static ZmuxConfig defaults() {
        return DEFAULTS;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The ALPN identifiers, in preference order. */
    public List<String> alpn() {
        return alpn;
    }

    public EventLoops eventLoops() {
        return eventLoops;
    }

    /** A loop of the configured group for a new session; the group's owner closes it. */
    io.zmux.internal.loop.EventLoop nextLoop() {
        return eventLoops.next();
    }

    /**
     * The ALPN list a server accepts: the configured identifiers, then their draft-01
     * aliases (SPEC.md Section 6.2).
     */
    public static List<String> withAliases(List<String> ids) {
        java.util.ArrayList<String> out = new java.util.ArrayList<>(ids);
        for (String id : ids) {
            String alias = null;
            if (id.equals("qmux-02")) alias = "qmux-01";
            else if (id.startsWith("qmux-02.")) alias = "qmux-01." + id.substring("qmux-02.".length());
            else if (id.endsWith("-02")) alias = id.substring(0, id.length() - 3) + "-01";
            if (alias != null && !out.contains(alias)) out.add(alias);
        }
        return out;
    }

    static long nanos(Duration d) {
        try {
            return d.toNanos();
        } catch (ArithmeticException e) {
            return Time.NEVER;
        }
    }

    /** The handshake timeout as a bound; one beyond the clock's range for no timeout. */
    Duration handshakeBound() {
        return timeouts.handshake().orElse(java.time.temporal.ChronoUnit.FOREVER.getDuration());
    }

    /** The handshake timeout in nanoseconds; {@link Time#NEVER} for no timeout. */
    long handshakeNanos() {
        return timeouts.handshake().map(ZmuxConfig::nanos).orElse(Time.NEVER);
    }

    /** Whole milliseconds of d, saturating. */
    static long millis(Duration d) {
        try {
            return d.toMillis();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    /** The core configuration for a session in the given transport role. */
    CoreConfig toCore(CoreConfig.Role role, CoreConfig.Role transportRole) {
        CoreConfig.Builder c = CoreConfig.builder();
        c.role(role);
        c.transportRole(transportRole);
        c.handshakeTimeout(handshakeNanos());
        c.keepAlive(timeouts.keepAlive().map(ZmuxConfig::nanos).orElse(CoreConfig.OFF));
        TransportParams p = c.local();
        p.maxIdleTimeoutMillis = timeouts.idle().map(ZmuxConfig::millis).orElse(0L);
        p.initialMaxData = flow.initialConnectionWindow();
        p.maxStreamDataBidiLocal = flow.initialStreamWindow();
        p.maxStreamDataBidiRemote = flow.initialStreamWindow();
        p.maxStreamDataUni = flow.initialStreamWindow();
        p.maxStreamsBidi = limits.incomingStreams();
        if (limits.incomingUniStreams() >= 0) {
            c.stagedUni(false);
            p.maxStreamsUni = limits.incomingUniStreams();
        }
        c.streamWindowCap(flow.maxStreamWindow());
        c.connWindowCap(flow.maxConnectionWindow());
        c.memoryBudget(flow.memoryBudget().orElse(CoreConfig.UNLIMITED));
        c.windowPredicate(hooks.allowWindowGrowth() != null);
        c.datagramReceiveLimited(hooks.datagramReceive() != null);
        c.sendBuffer(flow.sendBuffer());
        c.sendBufferMax(flow.maxSendBuffer() == 0 ? 0 : Math.max(flow.maxSendBuffer(), flow.sendBuffer()));
        if (datagrams) {
            c.datagrams(true);
            p.present |= TransportParams.DATAGRAM | TransportParams.FLOWS;
            p.maxDatagramFrameSize = 65535;
            c.datagramSendQueue(limits.datagramSendQueue());
            c.datagramReceiveQueue(limits.datagramReceiveQueue());
            c.datagramStreamQueue(limits.datagramStreamQueue());
            c.datagramReceiveBytes(limits.datagramReceiveBytes());
        }
        p.maxLanes = limits.lanes();
        System.arraycopy(randomBytes(16), 0, p.laneKey, 0, 16);
        if (role == CoreConfig.Role.AUTO) {
            p.present |= TransportParams.ROLE;
            p.role = TransportParams.ROLE_AUTO;
            p.roleNonce = RANDOM.nextLong();
        }
        long g = Math.floorMod(RANDOM.nextLong(), 1L << 20);
        p.greaseId = 31 * g + 27;
        p.greaseValue = randomBytes(RANDOM.nextInt(17));
        c.earlyZmuxPeer(earlyData == EarlyData.ZMUX_PEER);
        c.shapingLimits(shapingLimits);
        c.shaper(shaper);
        MultipathConfig m = multipath;
        c.maxPipes(m.maxPipes());
        c.policy(Scheduling.policy(m.scheduler()));
        c.spreadChunk(m.spreadChunk()); c.overflowDelay(nanos(m.overflowDelay())); c.standbyProbe(m.standbyProbe().map(ZmuxConfig::nanos).orElse(0L));
        c.probeInterval(m.probeInterval().map(ZmuxConfig::nanos).orElse(0L)); c.probeJitter(m.probeJitter()); c.redundancy(m.redundancy()); c.latencyBudget(nanos(flow.latencyBudget()));
        m.suspectFloor().ifPresent(d -> c.suspectFloor(nanos(d)));
        m.deadTimeout().ifPresent(d -> c.deadTimeout(nanos(d)));
        m.probation().ifPresent(d -> c.probation(nanos(d)));
        PipeAuth pipeAuth = m.auth();
        if (multipath()) {
            c.resume(nanos(m.resumeTimeout()));
            p.present |= TransportParams.MULTIPATH;
            if (pipeAuth.useExporter()) p.secretFlags |= TransportParams.SECRET_EXPORTER;
            if (pipeAuth.psk() != null) {
                p.secretFlags |= TransportParams.SECRET_PSK;
                c.psk(pipeAuth.psk());
            }
            if (pipeAuth.allowInsecure()) p.secretFlags |= TransportParams.SECRET_INSECURE;
            System.arraycopy(randomBytes(16), 0, p.sessionNonce, 0, 16);
            if (!m.resumeTimeout().isZero()) {
                p.present |= TransportParams.RESUME;
                p.resumeGraceMillis = Math.max(1, m.resumeTimeout().toMillis());
            }
        }
        return c.build();
    }

    private static byte[] randomBytes(int n) {
        byte[] b = new byte[n];
        RANDOM.nextBytes(b);
        return b;
    }

    /** Builds a {@link ZmuxConfig}; every group validates its values. */
    public static final class Builder {
        private Timeouts timeouts = Timeouts.defaults();
        private FlowControl flow = FlowControl.defaults();
        private Limits limits = Limits.defaults();
        private MultipathConfig multipath = MultipathConfig.defaults();
        private boolean datagrams;
        private EarlyData earlyData = EarlyData.OFF;
        private List<String> alpn = List.of(DEFAULT_ALPN);
        private EventLoops eventLoops = EventLoops.shared();
        private io.zmux.shaping.ShapingLimits shapingLimits = io.zmux.shaping.ShapingLimits.none();
        private io.zmux.shaping.ShaperFactory shaper;
        private SessionObserver observer;
        private int observerQueue = 256;
        private java.util.function.LongPredicate allowWindowGrowth;
        private io.zmux.helper.Limiter datagramSend;
        private io.zmux.helper.Limiter datagramReceive;
        private Credential credential;

        private Builder() {}

        /** The credential client dialers present to a server's admission ({@link ZmuxConfig#credential()}); null: none. */
        public Builder credential(Credential c) { credential = c; return this; }

        public Builder timeouts(Timeouts t) { timeouts = Objects.requireNonNull(t); return this; }
        public Builder flowControl(FlowControl f) { flow = Objects.requireNonNull(f); return this; }
        public Builder limits(Limits l) { limits = Objects.requireNonNull(l); return this; }
        public Builder multipath(MultipathConfig m) { multipath = Objects.requireNonNull(m); return this; }

        /** Enables datagrams and datagram flows (EXTENSIONS.md Section 6). */
        public Builder datagrams(boolean on) { datagrams = on; return this; }

        public Builder earlyData(EarlyData mode) { earlyData = Objects.requireNonNull(mode); return this; }

        /** The ALPN identifiers in preference order (default qmux-02). */
        public Builder alpn(List<String> ids) {
            if (ids.isEmpty()) throw new IllegalArgumentException("empty ALPN list");
            alpn = List.copyOf(ids);
            return this;
        }

        public Builder eventLoops(EventLoops loops) { eventLoops = Objects.requireNonNull(loops); return this; }

        /** Upper bounds of {@link io.zmux.shaping.Shaper} decisions over a session (default: none). */
        public Builder shapingLimits(io.zmux.shaping.ShapingLimits limits) { shapingLimits = Objects.requireNonNull(limits); return this; }

        /**
         * The shaper factory every session of this configuration starts with, so that each
         * outbound pipe, including those of listener-accepted sessions and joined pipes, is
         * shaped from its first record; the factory is called once per pipe when the pipe sends
         * its first record. {@link ZmuxSession#setShaper} replaces it. Null (the default): none.
         */
        public Builder shaper(io.zmux.shaping.ShaperFactory factory) { shaper = factory; return this; }

        /** Ordered asynchronous notifications; capacity bounds queued events (default 256). */
        public Builder observer(SessionObserver o, int capacity) {
            if (capacity < 1) throw new IllegalArgumentException("observer queue below 1");
            observer = o; observerQueue = capacity; return this;
        }

        /** Called off-loop with the proposed connection-window increase in bytes. */
        public Builder allowConnectionWindowIncrease(java.util.function.LongPredicate predicate) {
            allowWindowGrowth = predicate; return this;
        }

        /** Nonblocking whole-payload datagram admission in each direction; null disables a limiter. */
        public Builder datagramLimiters(io.zmux.helper.Limiter send, io.zmux.helper.Limiter receive) {
            datagramSend = send; datagramReceive = receive; return this;
        }

        /**
         * The configuration.
         *
         * @throws IllegalArgumentException when multipath or resume is on without {@link MultipathConfig#auth()}
         */
        public ZmuxConfig build() {
            if (multipath.enabled() && multipath.auth() == null) throw new IllegalArgumentException("multipath needs auth");
            return new ZmuxConfig(this);
        }
    }
}
