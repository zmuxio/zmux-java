package io.zmux;

import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.core.Time;
import io.zmux.internal.wire.TransportParams;
import java.time.Duration;
import java.util.List;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * Session configuration, immutable; {@link #defaults()} or {@link #builder()}. The builder has
 * one method per knob and per parameter, flat: timers, windows, limits,
 * multipath, resume and scheduling are set on it directly. Not
 * calling a method keeps the setting's default; a value outside its range is rejected with
 * {@link IllegalArgumentException} when it is set, and a combination that cannot work (such as
 * multipath without a secret source) when the configuration is built or, for what depends on the
 * first pipe, when the session starts, before any byte is written. No value is ever raised,
 * lowered or dropped instead. A duration beyond the clock's range never fires.
 *
 * <p>An accessor that returns an empty {@link Optional}, {@link OptionalInt} or
 * {@link OptionalLong} means the option's one setting that is not a value, as its documentation
 * names it: off or none ({@link #handshakeTimeout()}, {@link #resumeTimeout()}), unlimited
 * ({@link #memoryBudget()}), automatic ({@link #maxStreamReceiveWindow()},
 * {@link #maxConnectionReceiveWindow()}, {@link #sendBufferMax()}, {@link #replayWindow()},
 * {@link #probation()}) or staged ({@link #incomingUniStreams()}). No option has two such settings.
 */
public final class ZmuxConfig {
    /** The default ALPN identifier, as of QMux draft-02. */
    public static final String DEFAULT_ALPN = "qmux-02";
    /** The default handshake timeout. */
    public static final Duration DEFAULT_HANDSHAKE_TIMEOUT = Duration.ofSeconds(10);
    /** The default idle timeout. */
    public static final Duration DEFAULT_MAX_IDLE_TIMEOUT = Duration.ofSeconds(30);
    /** The default initial stream and connection receive windows. */
    public static final long DEFAULT_INITIAL_STREAM_RECEIVE_WINDOW = 256 << 10, DEFAULT_INITIAL_CONNECTION_RECEIVE_WINDOW = 1 << 20;
    /** The default send buffer. */
    public static final long DEFAULT_SEND_BUFFER = 4L << 20;
    /** The default memory budget of a session without a listener. */
    public static final long DEFAULT_MEMORY_BUDGET = 1L << 30;
    /** The default join timeout. */
    public static final Duration DEFAULT_JOIN_TIMEOUT = Duration.ofSeconds(10);
    /** The longest join deadline: no {@link Builder#joinTimeout join timeout} is longer. */
    public static final Duration MAX_JOIN_DEADLINE = Duration.ofSeconds(60);

    /** The default number of extra attachments the peer may hold beyond {@link #maxPipes()}. */
    public static final int DEFAULT_PIPE_HEADROOM = 8;
    /** The default size of a Hello's first batch. */
    public static final int DEFAULT_JOIN_EARLY_HELLO = 8 << 10;

    private static final java.security.SecureRandom RANDOM = new java.security.SecureRandom();
    private static final ZmuxConfig DEFAULTS = builder().build();

    final Duration handshakeTimeout, maxIdleTimeout; // null: no timeout
    final Liveness liveness;
    final long initialStreamReceiveWindow, initialConnectionReceiveWindow;
    final long maxStreamReceiveWindow, maxConnectionReceiveWindow; // 0: auto
    final long memoryBudget; // -1: unlimited
    final Duration latencyBudget;
    final long incomingStreams;
    final long incomingUniStreams; // -1: staged credit
    final long maxLanes;
    final boolean datagrams;
    final int datagramSendQueue, datagramReceiveQueue, datagramStreamQueue;
    final long datagramReceiveBytes;
    final long datagramStreamBytes; // -1: no byte bound
    final long sendBuffer, sendBufferMax; // sendBufferMax 0: auto
    final EarlyData earlyData;
    final boolean holdFirstFlight;
    final List<String> alpn;
    final EventLoops eventLoops;
    final io.zmux.shaping.ShapingLimits shapingLimits;
    final java.util.function.Supplier<io.zmux.shaping.ShapingBudget> shapingBudget;
    final io.zmux.shaping.ShaperFactory shaper;
    final CarrierCredential carrierCredential;
    final io.zmux.internal.loop.DriverHooks hooks;
    final int joinEarlyHello; // 0: off
    final int maxPipes, pipeHeadroom, replayWindow; // replayWindow 0: unset
    final PipeAuth pipeAuth;
    final Duration resumeTimeout; // null: off
    final Duration joinTimeout;
    final Duration epochPeriod; // null: off
    final long epochJoins; // negative: off
    final boolean epochAfterFirstPipe, epochAfterResume;
    final Redial redial;
    final Backoff redialBackoff;
    final Scheduler scheduler;
    final Duration probation; // null: derived; zero: off
    final int spreadChunk;
    final Duration overflowDelay;
    final boolean redundancy;
    final System.Logger logger;

    private ZmuxConfig(Builder b) {
        handshakeTimeout = b.handshakeTimeout; maxIdleTimeout = b.maxIdleTimeout; liveness = b.liveness;
        initialStreamReceiveWindow = b.initialStreamReceiveWindow; initialConnectionReceiveWindow = b.initialConnectionReceiveWindow;
        maxStreamReceiveWindow = b.maxStreamReceiveWindow; maxConnectionReceiveWindow = b.maxConnectionReceiveWindow;
        memoryBudget = b.memoryBudget; latencyBudget = b.latencyBudget;
        incomingStreams = b.incomingStreams; incomingUniStreams = b.incomingUniStreams; maxLanes = b.maxLanes;
        datagramSendQueue = b.datagramSendQueue; datagramReceiveQueue = b.datagramReceiveQueue;
        datagramStreamQueue = b.datagramStreamQueue; datagramReceiveBytes = b.datagramReceiveBytes;
        datagramStreamBytes = b.datagramStreamBytes;
        sendBuffer = b.sendBuffer; sendBufferMax = b.sendBufferMax;
        datagrams = b.datagrams; earlyData = b.earlyData; holdFirstFlight = b.holdFirstFlight; alpn = List.copyOf(b.alpn); eventLoops = b.eventLoops;
        shapingLimits = b.shapingLimits;
        shapingBudget = b.shapingBudget;
        shaper = b.shaper;
        carrierCredential = b.carrierCredential;
        maxPipes = b.maxPipes; pipeHeadroom = b.pipeHeadroom; replayWindow = b.replayWindow; pipeAuth = b.pipeAuth;
        resumeTimeout = b.resumeTimeout; joinTimeout = b.joinTimeout; redial = b.redial;
        epochPeriod = b.epochPeriod; epochJoins = b.epochJoins;
        epochAfterFirstPipe = b.epochAfterFirstPipe; epochAfterResume = b.epochAfterResume;
        redialBackoff = b.redialBackoff;
        scheduler = b.scheduler; probation = b.probation;
        spreadChunk = b.spreadChunk; overflowDelay = b.overflowDelay;
        redundancy = b.redundancy; logger = b.logger;
        Tracer tracer = b.tracer;
        hooks = new io.zmux.internal.loop.DriverHooks(tracer == null ? null : e -> {
            if (e.kind() == io.zmux.internal.core.Event.Kind.ESTABLISHED) tracer.established();
            else if (e.kind() == io.zmux.internal.core.Event.Kind.CLOSED) tracer.closed(Errors.session(e.error()));
            else { SessionEvent event = Events.publicEvent(e); if (event != null) tracer.event(event); }
        }, tracer == null ? null : tracer::dropped, b.tracerQueue, b.allowWindowGrowth,
                b.datagramSend, b.datagramReceive, Scheduling.picker(scheduler), joinPolicy(b), b.handshakeShaper, b.logger,
                b.redial == null ? null : new io.zmux.internal.loop.DriverHooks.Redial(b.redial, nanos(b.redialBackoff.min()), nanos(b.redialBackoff.max()), b.redialBackoff.jitter()));
        joinEarlyHello = b.joinEarlyHello;
    }

    private static io.zmux.internal.loop.DriverHooks.JoinPolicy joinPolicy(Builder b) {
        RecordPolicy policy = b.recordPolicy;
        AllowRecordMode allow = b.allowRecordMode;
        java.util.function.BiPredicate<ZmuxSession, PipeId> authorize = b.authorizeJoin;
        return new io.zmux.internal.loop.DriverHooks.JoinPolicy() {
            private RecordPolicyInput carrier(io.zmux.internal.loop.Transport c) {
                return new RecordPolicyInput(c.exporterPresent(), c.bindable(), Optional.ofNullable(c.info().carrier().label()), false);
            }

            @Override public boolean allow(io.zmux.internal.loop.Transport c, io.zmux.internal.join.Mode mode, Object peer) {
                return allow == null ? mode == io.zmux.internal.join.Mode.SEALED : allow.allow(carrier(c), io.zmux.internal.join.Mode.record(mode), peer);
            }

            @Override public boolean authorize(Object host, long pipe) {
                return authorize == null || authorize.test((ZmuxSession) host, PipeId.of(pipe));
            }

            @Override public io.zmux.internal.join.Mode choose(io.zmux.internal.loop.Transport c) {
                return policy == null ? io.zmux.internal.join.Mode.SEALED : io.zmux.internal.join.Mode.of(policy.choose(carrier(c)));
            }
        };
    }

    /** This configuration with early data turned off. */
    ZmuxConfig withoutEarlyData() {
        return earlyData == EarlyData.OFF ? this : new ZmuxConfig(this, EarlyData.OFF);
    }

    private ZmuxConfig(ZmuxConfig o, EarlyData earlyData) {
        handshakeTimeout = o.handshakeTimeout; maxIdleTimeout = o.maxIdleTimeout; liveness = o.liveness;
        initialStreamReceiveWindow = o.initialStreamReceiveWindow; initialConnectionReceiveWindow = o.initialConnectionReceiveWindow;
        maxStreamReceiveWindow = o.maxStreamReceiveWindow; maxConnectionReceiveWindow = o.maxConnectionReceiveWindow;
        memoryBudget = o.memoryBudget; latencyBudget = o.latencyBudget;
        incomingStreams = o.incomingStreams; incomingUniStreams = o.incomingUniStreams; maxLanes = o.maxLanes;
        datagramSendQueue = o.datagramSendQueue; datagramReceiveQueue = o.datagramReceiveQueue;
        datagramStreamQueue = o.datagramStreamQueue; datagramReceiveBytes = o.datagramReceiveBytes;
        datagramStreamBytes = o.datagramStreamBytes;
        sendBuffer = o.sendBuffer; sendBufferMax = o.sendBufferMax;
        datagrams = o.datagrams; this.earlyData = earlyData; holdFirstFlight = false; alpn = o.alpn; eventLoops = o.eventLoops;
        shapingLimits = o.shapingLimits; shapingBudget = o.shapingBudget; shaper = o.shaper; carrierCredential = o.carrierCredential; hooks = o.hooks; joinEarlyHello = o.joinEarlyHello;
        maxPipes = o.maxPipes; pipeHeadroom = o.pipeHeadroom; replayWindow = o.replayWindow; pipeAuth = o.pipeAuth;
        resumeTimeout = o.resumeTimeout; joinTimeout = o.joinTimeout; redial = o.redial;
        epochPeriod = o.epochPeriod; epochJoins = o.epochJoins;
        epochAfterFirstPipe = o.epochAfterFirstPipe; epochAfterResume = o.epochAfterResume;
        redialBackoff = o.redialBackoff;
        scheduler = o.scheduler; probation = o.probation;
        spreadChunk = o.spreadChunk; overflowDelay = o.overflowDelay;
        redundancy = o.redundancy; logger = o.logger;
    }

    /** The handshake timeout; empty: none. */
    public Optional<Duration> handshakeTimeout() { return Optional.ofNullable(handshakeTimeout); }
    /** The advertised idle timeout; empty: none. */
    public Optional<Duration> maxIdleTimeout() { return Optional.ofNullable(maxIdleTimeout); }
    /** The liveness settings of sessions of this configuration. */
    public Liveness liveness() { return liveness; }
    public long initialStreamReceiveWindow() { return initialStreamReceiveWindow; }
    /** The fixed cap of stream window auto-tuning; empty: automatic. */
    public OptionalLong maxStreamReceiveWindow() { return maxStreamReceiveWindow == 0 ? OptionalLong.empty() : OptionalLong.of(maxStreamReceiveWindow); }
    /** The window auto-tuner's latency budget. */
    public Duration latencyBudget() { return latencyBudget; }
    public long initialConnectionReceiveWindow() { return initialConnectionReceiveWindow; }
    /** The fixed cap of connection window auto-tuning; empty: automatic. */
    public OptionalLong maxConnectionReceiveWindow() { return maxConnectionReceiveWindow == 0 ? OptionalLong.empty() : OptionalLong.of(maxConnectionReceiveWindow); }
    /** The session's own memory budget; empty: unlimited. */
    public OptionalLong memoryBudget() { return memoryBudget < 0 ? OptionalLong.empty() : OptionalLong.of(memoryBudget); }
    /** Concurrent peer bidirectional streams; 0: none. */
    public long incomingStreams() { return incomingStreams; }
    /** Concurrent peer unidirectional streams advertised at once (0: none); empty: staged credit. */
    public OptionalLong incomingUniStreams() { return incomingUniStreams < 0 ? OptionalLong.empty() : OptionalLong.of(incomingUniStreams); }
    /** Lanes accepted on each inbound pipe; 1: none. */
    public long maxLanes() { return maxLanes; }
    public int datagramSendQueue() { return datagramSendQueue; }
    public int datagramReceiveQueue() { return datagramReceiveQueue; }
    public int datagramStreamQueue() { return datagramStreamQueue; }
    public long datagramReceiveBytes() { return datagramReceiveBytes; }
    /** The payload bytes queued per stream and direction; empty: no byte bound. */
    public OptionalLong datagramStreamBytes() { return datagramStreamBytes < 0 ? OptionalLong.empty() : OptionalLong.of(datagramStreamBytes); }
    public long sendBuffer() { return sendBuffer; }
    /** The fixed maximum the send buffer grows to in multipath sessions; empty: automatic. */
    public OptionalLong sendBufferMax() { return sendBufferMax == 0 ? OptionalLong.empty() : OptionalLong.of(sendBufferMax); }

    /** s's budget; none without s, when it fails, or when it gives none. */
    static io.zmux.shaping.ShapingBudget askBudget(java.util.function.Supplier<io.zmux.shaping.ShapingBudget> s) {
        if (s == null) return io.zmux.shaping.ShapingBudget.NONE;
        try {
            var b = s.get();
            return b == null ? io.zmux.shaping.ShapingBudget.NONE : b;
        } catch (RuntimeException e) {
            return io.zmux.shaping.ShapingBudget.NONE;
        }
    }
    /** Whether datagrams and datagram flows are on. */
    public boolean isDatagrams() { return datagrams; }
    public EarlyData earlyData() { return earlyData; }
    /** Whether a client holds its first carrier write ({@link Builder#holdFirstFlight}). */
    public boolean isHoldFirstFlight() { return holdFirstFlight; }

    /**
     * The credential presented on the carrier of a client session by the dialers that open it
     * with this configuration ({@link Zmux#connect(java.net.InetSocketAddress, ZmuxConfig)},
     * {@link Zmux#connectTls}, {@link ZmuxListener#connect}, {@link ZmuxListener#connectTls}
     * and the zmux-quic session dialers), and by the TCP and TLS pipes a session of this
     * configuration dials ({@link ZmuxSession#addPipe(java.net.InetSocketAddress, PipeOptions)},
     * {@link ZmuxSession#addTlsPipe}); null: none. Pipes the application opens present their own
     * ({@link CarrierCredential}).
     */
    public Optional<CarrierCredential> carrierCredential() { return Optional.ofNullable(carrierCredential); }

    /** Whether sessions offer multipath: more than one pipe, or resume. */
    public boolean isMultipath() { return maxPipes > 1 || resumeTimeout != null; }

    /** The attachments a session aims for with its own joins; 1: multipath off. */
    public int maxPipes() { return maxPipes; }
    /** The attachments the peer may hold beyond {@link #maxPipes()}. */
    public int pipeHeadroom() { return pipeHeadroom; }
    /** The replay window of joins; empty: automatic, max(64, 2 x (maxPipes + headroom)). */
    public OptionalInt replayWindow() { return replayWindow == 0 ? OptionalInt.empty() : OptionalInt.of(replayWindow); }
    /** The secret sources of joins; empty: none (a secure first pipe is its own source). */
    public Optional<PipeAuth> pipeAuth() { return Optional.ofNullable(pipeAuth); }
    /** The size of a Hello's first batch while no other pipe is usable; empty: off. */
    public OptionalInt joinEarlyHello() { return joinEarlyHello == 0 ? OptionalInt.empty() : OptionalInt.of(joinEarlyHello); }
    /** The resume grace period; empty: resume off. */
    public Optional<Duration> resumeTimeout() { return Optional.ofNullable(resumeTimeout); }
    /** The deadline of every join and of {@link ZmuxSession#addPipe} (independent of the handshake timeout). */
    public Duration joinTimeout() { return joinTimeout; }
    /** The time in Stable after which a multipath session advances its epoch; empty: never by time. */
    public Optional<Duration> epochPeriod() { return Optional.ofNullable(epochPeriod); }
    /** The joins in one epoch after which a multipath session advances it; empty: never by count. */
    public OptionalLong epochJoins() { return epochJoins < 0 ? OptionalLong.empty() : OptionalLong.of(epochJoins); }
    /** Whether a session whose secret comes from a TLS exporter or a pre-shared key advances once when its first Sealed or Bound pipe exists. */
    public boolean isEpochAfterFirstPipe() { return epochAfterFirstPipe; }
    /** Whether a multipath session advances once after leaving a suspension, back in Stable. */
    public boolean isEpochAfterResume() { return epochAfterResume; }
    /** The redial hook; empty: none. */
    public Optional<Redial> redial() { return Optional.ofNullable(redial); }
    /** The backoff of the redial hook's attempts. */
    public Backoff redialBackoff() { return redialBackoff; }
    public Scheduler scheduler() { return scheduler; }
    /** The base of a new pipe's probation, {@link Duration#ZERO} turning it off; empty: derived from the path (automatic). */
    public Optional<Duration> probation() { return Optional.ofNullable(probation); }
    public int spreadChunk() { return spreadChunk; }
    public Duration overflowDelay() { return overflowDelay; }
    /** Whether urgent data and the first chunk after an idle period are sent redundantly. */
    public boolean isRedundancy() { return redundancy; }
    /** The logger of warnings the application would otherwise not see; empty: nothing is logged. */
    public Optional<System.Logger> logger() { return Optional.ofNullable(logger); }

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
     * aliases (qmux-02 accepts qmux-01, and each identifier ending in -02 its -01 form).
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
        return handshakeTimeout().orElse(java.time.temporal.ChronoUnit.FOREVER.getDuration());
    }

    /** The handshake timeout in nanoseconds; {@link Time#NEVER} for no timeout. */
    long handshakeNanos() {
        return handshakeTimeout().map(ZmuxConfig::nanos).orElse(Time.NEVER);
    }

    /** The join timeout in nanoseconds. */
    long joinNanos() { return nanos(joinTimeout); }

    /** Whole milliseconds of d, saturating. */
    static long millis(Duration d) {
        try {
            return d.toMillis();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    /** Whole milliseconds of d, a fraction rounded up (the unit of a transport parameter), saturating. */
    static long millisUp(Duration d) {
        long m = millis(d);
        return m == Long.MAX_VALUE || d.minusMillis(m).isZero() ? m : m + 1;
    }

    /**
     * Checks what depends on the role: early data toward a declared zmux peer needs a known
     * stream ID parity, which the symmetric role learns only from the peer's parameters; a
     * held first flight is a client's.
     */
    void checkRole(CoreConfig.Role role) {
        if (earlyData == EarlyData.ZMUX_PEER && role == CoreConfig.Role.AUTO) {
            throw new IllegalArgumentException("early data toward a zmux peer with the symmetric role: the stream ID parity is unknown before the peer's parameters");
        }
        if (holdFirstFlight && role != CoreConfig.Role.INITIATOR) {
            throw new IllegalArgumentException("holdFirstFlight on a session that is no client");
        }
    }

    /**
     * Checks a session that has no listener: it cannot accept joins, so neither more than one
     * pipe nor a rejoin works on it, unless it is the client, which joins its own.
     */
    void checkWithoutListener(CoreConfig.Role role) {
        if (role == CoreConfig.Role.INITIATOR) return;
        if (maxPipes > 1) throw new IllegalArgumentException("maxPipes above 1 on a server or peer session without a listener, which cannot accept joins");
        if (resumeTimeout != null) throw new IllegalArgumentException("resumeTimeout on a server or peer session without a listener, which cannot accept a rejoin");
    }

    /** The core configuration for a session in the given transport role. */
    CoreConfig toCore(CoreConfig.Role role, CoreConfig.Role transportRole) {
        checkRole(role);
        CoreConfig.Builder c = CoreConfig.builder();
        c.role(role);
        c.transportRole(transportRole);
        c.handshakeTimeout(handshakeNanos());
        c.joinTimeout(joinNanos());
        CoreConfig.Epochs ed = CoreConfig.Epochs.DEFAULT;
        long period = epochPeriod == null ? Time.NEVER : nanos(epochPeriod);
        long joins = epochJoins < 0 ? Long.MAX_VALUE : epochJoins;
        c.epochs(new CoreConfig.Epochs(period, joins, epochAfterFirstPipe, epochAfterResume, ed.locatorPrefix()));
        c.redial(redial != null);
        c.keepAlive(liveness.keepAlivePeriod().map(ZmuxConfig::nanos).orElse(CoreConfig.OFF));
        TransportParams p = c.local();
        p.maxIdleTimeoutMillis = maxIdleTimeout == null ? 0L : millisUp(maxIdleTimeout);
        p.initialMaxData = initialConnectionReceiveWindow;
        p.maxStreamDataBidiLocal = initialStreamReceiveWindow;
        p.maxStreamDataBidiRemote = initialStreamReceiveWindow;
        p.maxStreamDataUni = initialStreamReceiveWindow;
        p.maxStreamsBidi = incomingStreams;
        if (incomingUniStreams >= 0) {
            c.stagedUni(false);
            p.maxStreamsUni = incomingUniStreams;
        }
        c.streamWindowCap(maxStreamReceiveWindow);
        c.connWindowCap(maxConnectionReceiveWindow);
        c.memoryBudget(memoryBudget < 0 ? CoreConfig.UNLIMITED : memoryBudget);
        c.windowPredicate(hooks.allowWindowGrowth() != null);
        c.datagramReceiveLimited(hooks.datagramReceive() != null);
        c.sendBuffer(sendBuffer);
        c.sendBufferMax(sendBufferMax);
        if (datagrams) {
            c.datagrams(true);
            p.present |= TransportParams.DATAGRAM | TransportParams.FLOWS;
            p.maxDatagramFrameSize = 65535;
            c.datagramSendQueue(datagramSendQueue);
            c.datagramReceiveQueue(datagramReceiveQueue);
            c.datagramStreamQueue(datagramStreamQueue);
            c.datagramReceiveBytes(datagramReceiveBytes);
            c.datagramStreamBytes(datagramStreamBytes);
        }
        p.maxLanes = maxLanes;
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
        c.holdFirstFlight(holdFirstFlight);
        c.shapingLimits(shapingLimits);
        c.shapingBudget(askBudget(shapingBudget));
        c.shaper(shaper);
        c.maxPipes(maxPipes);
        c.headroom(pipeHeadroom);
        c.replayWindow(replayWindow);
        c.policy(Scheduling.policy(scheduler));
        c.spreadChunk(spreadChunk); c.overflowDelay(nanos(overflowDelay));
        c.standbyProbe(liveness.standbyProbe().map(ZmuxConfig::nanos).orElse(0L));
        c.probeInterval(liveness.probeInterval().map(ZmuxConfig::nanos).orElse(0L)); c.probeJitter(liveness.probeJitter());
        c.redundancy(redundancy); c.latencyBudget(nanos(latencyBudget));
        c.suspectFloor(nanos(liveness.suspectFloor()));
        liveness.deadTimeout().ifPresent(d -> c.deadTimeout(nanos(d)));
        if (probation != null) c.probation(nanos(probation));
        if (isMultipath()) {
            c.resume(resumeTimeout == null ? 0 : nanos(resumeTimeout));
            p.present |= TransportParams.MULTIPATH;
            if (pipeAuth != null && pipeAuth.useExporter()) p.secretFlags |= TransportParams.SECRET_EXPORTER;
            if (pipeAuth != null && pipeAuth.deferredPsk()) {
                p.secretFlags |= TransportParams.SECRET_PSK;
                c.deferredPsk(true);
            }
            if (pipeAuth != null && pipeAuth.psk() != null) {
                p.secretFlags |= TransportParams.SECRET_PSK;
                c.psk(pipeAuth.psk());
            }
            if (pipeAuth != null && pipeAuth.endToEnd()) c.declaredEndToEnd(true);
            p.present |= TransportParams.SESSION_CRYPTO;
            p.sessionCrypto = io.zmux.internal.join.SessionCrypto.supported();
            c.joinEarlyHello(joinEarlyHello);
            System.arraycopy(randomBytes(16), 0, p.sessionNonce, 0, 16);
            if (resumeTimeout != null) {
                p.present |= TransportParams.RESUME;
                p.resumeGraceMillis = millisUp(resumeTimeout);
            }
        }
        return c.build();
    }

    private static byte[] randomBytes(int n) {
        byte[] b = new byte[n];
        RANDOM.nextBytes(b);
        return b;
    }

    /** Dials a replacement pipe for a session that runs on one pipe at a time. */
    @FunctionalInterface
    public interface Redial {
        /**
         * Opens a carrier for the direction the session lacks; the returned pipe input belongs to
         * zmux. It must cooperate with cancellation and interruption. A failure, or an exception,
         * is a failed attempt: the next one waits the backoff.
         */
        io.zmux.carrier.PipeIo dial(PipeOptions.Direction need, Cancellation cancellation) throws IOException, InterruptedException;
    }

    /** Builds a {@link ZmuxConfig}; each method checks its value. */
    public static final class Builder {
        private Duration handshakeTimeout = DEFAULT_HANDSHAKE_TIMEOUT, maxIdleTimeout = DEFAULT_MAX_IDLE_TIMEOUT;
        private Liveness liveness = Liveness.defaults();
        private long initialStreamReceiveWindow = DEFAULT_INITIAL_STREAM_RECEIVE_WINDOW;
        private long initialConnectionReceiveWindow = DEFAULT_INITIAL_CONNECTION_RECEIVE_WINDOW;
        private long maxStreamReceiveWindow, maxConnectionReceiveWindow;
        private long memoryBudget = DEFAULT_MEMORY_BUDGET;
        private Duration latencyBudget = Duration.ofNanos(CoreConfig.DEFAULT_LATENCY_BUDGET);
        private long incomingStreams = 256, incomingUniStreams = -1, maxLanes = 258;
        private boolean datagrams;
        private int datagramSendQueue = 64, datagramReceiveQueue = 128, datagramStreamQueue = 32;
        private long datagramReceiveBytes = 1 << 20;
        private long datagramStreamBytes = io.zmux.internal.core.CoreConfig.DEFAULT_DATAGRAM_STREAM_BYTES;
        private long sendBuffer = DEFAULT_SEND_BUFFER, sendBufferMax;
        private EarlyData earlyData = EarlyData.OFF;
        private boolean holdFirstFlight;
        private List<String> alpn = List.of(DEFAULT_ALPN);
        private EventLoops eventLoops = EventLoops.shared();
        private io.zmux.shaping.ShapingLimits shapingLimits = io.zmux.shaping.ShapingLimits.defaults();
        private java.util.function.Supplier<io.zmux.shaping.ShapingBudget> shapingBudget;
        private io.zmux.shaping.ShaperFactory shaper;
        private Tracer tracer;
        private int tracerQueue = 256;
        private java.util.function.LongPredicate allowWindowGrowth;
        private io.zmux.helper.Limiter datagramSend;
        private io.zmux.helper.Limiter datagramReceive;
        private CarrierCredential carrierCredential;
        private RecordPolicy recordPolicy;
        private AllowRecordMode allowRecordMode;
        private java.util.function.BiPredicate<ZmuxSession, PipeId> authorizeJoin;
        private int joinEarlyHello = DEFAULT_JOIN_EARLY_HELLO;
        private io.zmux.carrier.HandshakeShaper handshakeShaper;
        private int maxPipes = 1, pipeHeadroom = DEFAULT_PIPE_HEADROOM, replayWindow;
        private PipeAuth pipeAuth;
        private Duration resumeTimeout;
        private Duration joinTimeout = DEFAULT_JOIN_TIMEOUT;
        private Duration epochPeriod = Duration.ofNanos(CoreConfig.Epochs.DEFAULT.period());
        private long epochJoins = CoreConfig.Epochs.DEFAULT.joins();
        private boolean epochAfterFirstPipe = true, epochAfterResume = true;
        private Redial redial;
        private Backoff redialBackoff = Backoff.defaults();
        private Scheduler scheduler = Scheduler.AFFINITY;
        private Duration probation;
        private int spreadChunk = CoreConfig.DEFAULT_SPREAD_CHUNK;
        private Duration overflowDelay = Duration.ofNanos(CoreConfig.DEFAULT_OVERFLOW_DELAY);
        private boolean redundancy = true;
        private System.Logger logger;

        private Builder() {}

        /** Chooses the record mode of each carrier this endpoint joins; null (the default): Sealed. */
        public Builder recordPolicy(RecordPolicy p) { recordPolicy = p; return this; }

        /** The acceptor's check of a Hello's mode; null (the default): only Sealed passes. */
        public Builder allowRecordMode(AllowRecordMode a) { allowRecordMode = a; return this; }

        /** The acceptor's authorization of a join, after its mode; null (the default): none. */
        public Builder authorizeJoin(java.util.function.BiPredicate<ZmuxSession, PipeId> a) { authorizeJoin = a; return this; }

        /** The size of a Hello's first batch while no other pipe is usable, default 8 KiB, at least 1. */
        public Builder joinEarlyHello(int bytes) {
            if (bytes < 1) throw new IllegalArgumentException("joinEarlyHello below 1");
            joinEarlyHello = bytes; return this;
        }

        /** No first batch in a Hello. */
        public Builder noJoinEarlyHello() { joinEarlyHello = 0; return this; }

        /** Plans the Hello, Reply and Confirm of this session's joins; null: none. */
        public Builder handshakeShaper(io.zmux.carrier.HandshakeShaper shaper) { handshakeShaper = shaper; return this; }

        /** The carrier credential client dialers present to a server's admission ({@link ZmuxConfig#carrierCredential()}); not calling it: none. */
        public Builder carrierCredential(CarrierCredential c) { carrierCredential = Objects.requireNonNull(c, "carrierCredential"); return this; }

        /** How long establishment may take, default 10 s, any non-negative duration. It bounds establishment only. */
        public Builder handshakeTimeout(Duration d) { handshakeTimeout = nonNegative(d, "handshakeTimeout"); return this; }

        /** No handshake timeout. */
        public Builder noHandshakeTimeout() { handshakeTimeout = null; return this; }

        /**
         * The advertised idle timeout, default 30 s, at least 1 ms (the unit of the transport
         * parameter, a fraction rounded up).
         */
        public Builder maxIdleTimeout(Duration d) { maxIdleTimeout = Liveness.atLeast(d, Duration.ofMillis(1), "maxIdleTimeout"); return this; }

        /** No idle timeout; none is announced. */
        public Builder noIdleTimeout() { maxIdleTimeout = null; return this; }

        /**
         * The liveness settings: keep-alive, failure suspicion and probes (default
         * {@link Liveness#defaults()}).
         */
        public Builder liveness(Liveness l) { liveness = Objects.requireNonNull(l, "liveness"); return this; }

        /** The initial stream receive window, default 256 KiB, at least 4 KiB. */
        public Builder initialStreamReceiveWindow(long n) {
            if (n < 4096) throw new IllegalArgumentException("initialStreamReceiveWindow below 4096");
            initialStreamReceiveWindow = n; return this;
        }

        /**
         * A fixed cap of stream window auto-tuning, at least 1; not calling it: automatic, at least
         * 64 MiB and beyond that within the memory budget.
         */
        public Builder maxStreamReceiveWindow(long n) {
            if (n < 1) throw new IllegalArgumentException("maxStreamReceiveWindow below 1");
            maxStreamReceiveWindow = n; return this;
        }

        /** The latency budget B of window auto-tuning, default 100 us, non-negative. */
        public Builder latencyBudget(Duration d) { latencyBudget = nonNegative(d, "latencyBudget"); return this; }

        /** The initial connection receive window, default 1 MiB, at least 16 KiB. */
        public Builder initialConnectionReceiveWindow(long n) {
            if (n < 16384) throw new IllegalArgumentException("initialConnectionReceiveWindow below 16384");
            initialConnectionReceiveWindow = n; return this;
        }

        /**
         * A fixed cap of connection window auto-tuning, at least 1; not calling it: automatic, at
         * least 96 MiB and beyond that within the memory budget.
         */
        public Builder maxConnectionReceiveWindow(long n) {
            if (n < 1) throw new IllegalArgumentException("maxConnectionReceiveWindow below 1");
            maxConnectionReceiveWindow = n; return this;
        }

        /**
         * The memory budget of a session without a listener, default 1 GiB; 0 lets nothing grow
         * beyond the floors. A listener's budget replaces it.
         */
        public Builder memoryBudget(long n) {
            if (n < 0) throw new IllegalArgumentException("negative memoryBudget");
            memoryBudget = n; return this;
        }

        /** An unlimited memory budget. */
        public Builder noMemoryBudget() { memoryBudget = -1; return this; }

        /**
         * Concurrent peer bidirectional streams, default 256: 0 (none), or from 4 to
         * {@link Zmux#MAX_STREAM_CREDIT}.
         */
        public Builder incomingStreams(long n) {
            if ((n != 0 && n < 4) || n > Zmux.MAX_STREAM_CREDIT)
                throw new IllegalArgumentException("incomingStreams must be 0 or from 4 to Zmux.MAX_STREAM_CREDIT");
            incomingStreams = n; return this;
        }

        /**
         * Concurrent peer unidirectional streams advertised at once, 0 for none, at most
         * {@link Zmux#MAX_STREAM_CREDIT}; not calling it: staged credit, 16 at first and 256 once
         * the application first accepts a unidirectional stream.
         */
        public Builder incomingUniStreams(long n) {
            if (n < 0 || n > Zmux.MAX_STREAM_CREDIT)
                throw new IllegalArgumentException("incomingUniStreams outside 0 to Zmux.MAX_STREAM_CREDIT");
            incomingUniStreams = n; return this;
        }

        /** Lanes accepted on each inbound pipe, default 258, at least 1; 1: no lanes toward this endpoint. */
        public Builder maxLanes(long n) {
            if (n < 1) throw new IllegalArgumentException("maxLanes below 1");
            maxLanes = n; return this;
        }

        /** Queued outgoing datagrams, default 64, at least 1. */
        public Builder datagramSendQueue(int n) { datagramSendQueue = positive(n, "datagramSendQueue"); return this; }

        /** Queued incoming session datagrams, default 128, at least 1. */
        public Builder datagramReceiveQueue(int n) { datagramReceiveQueue = positive(n, "datagramReceiveQueue"); return this; }

        /** Queued incoming datagrams per stream flow, default 32, at least 1. */
        public Builder datagramStreamQueue(int n) { datagramStreamQueue = positive(n, "datagramStreamQueue"); return this; }

        /**
         * Queued incoming bytes of session datagrams and of datagrams for peer streams not opened
         * yet, default 1 MiB, at least 1. Stream datagram queues have their own bound
         * ({@link #datagramStreamBytes()}) and do not count against it.
         */
        public Builder datagramReceiveBytes(long n) {
            if (n < 1) throw new IllegalArgumentException("datagramReceiveBytes below 1");
            datagramReceiveBytes = n; return this;
        }

        /**
         * Queued incoming payload bytes per stream flow, default 512 KiB, at least 1. A datagram
         * that would exceed this bound or {@link #datagramStreamQueue()} drops the oldest datagrams of
         * that stream's queue until both hold, each counted as dropped; a single datagram larger
         * than the bound is kept alone.
         */
        public Builder datagramStreamBytes(long n) {
            if (n < 1) throw new IllegalArgumentException("datagramStreamBytes below 1");
            datagramStreamBytes = n; return this;
        }

        /** No byte bound on a stream's datagram queue: {@link #datagramStreamQueue()} alone bounds it. */
        public Builder noDatagramStreamBytes() { datagramStreamBytes = -1; return this; }

        /**
         * The initial written-but-unreleased budget, default 4 MiB, at least 1; multipath grows it up
         * to {@link #sendBufferMax()}. Each direction has a 64 KiB floor, subject to its explicit stream limit.
         */
        public Builder sendBuffer(long n) {
            if (n < 1) throw new IllegalArgumentException("sendBuffer below 1");
            sendBuffer = n; return this;
        }

        /**
         * A fixed maximum of multipath send-buffer growth, at least {@link #sendBuffer()} (checked by
         * {@link #build()}); not calling it: automatic, at least 64 MiB and beyond that within the memory budget.
         */
        public Builder sendBufferMax(long n) {
            if (n < 1) throw new IllegalArgumentException("sendBufferMax below 1");
            sendBufferMax = n; return this;
        }

        /**
         * The attachments a session aims for with its own joins, at least 1 (default 1, multipath
         * off; 8 is the recommended value with multipath). Above 1 the session offers multipath,
         * which needs a secret source ({@link #pipeAuth()}, or a first pipe on the secure carrier)
         * and, for a server, a listener.
         */
        public Builder maxPipes(int n) {
            if (n < 1) throw new IllegalArgumentException("maxPipes below 1");
            maxPipes = n; return this;
        }

        /** The attachments the peer may hold beyond maxPipes, at least 0 (default 8). */
        public Builder pipeHeadroom(int h) {
            if (h < 0) throw new IllegalArgumentException("negative pipeHeadroom");
            pipeHeadroom = h; return this;
        }

        /**
         * The replay window of joins, at least maxPipes + pipeHeadroom (checked by {@link #build()});
         * default max(64, 2 x (maxPipes + pipeHeadroom)).
         */
        public Builder replayWindow(int w) {
            if (w < 1) throw new IllegalArgumentException("replayWindow below 1");
            replayWindow = w; return this;
        }

        /**
         * The secret sources of joins. Multipath and resume need one, unless the first pipe is on
         * the secure carrier, whose Establish is the source: then a pre-shared key or the exporter
         * here is rejected when the session starts, and {@link PipeAuth#withEndToEnd()} alone is
         * accepted without effect. Null (the default): none.
         */
        public Builder pipeAuth(PipeAuth auth) { pipeAuth = auth; return this; }

        /**
         * Enables resume with this grace period, at least 1 ms (sent in milliseconds, a fraction
         * rounded up); not calling it leaves resume off. Resume needs a secret source and, for a
         * server, a listener; a client should have a redial source ({@link #redial()} or a pool).
         */
        public Builder resumeTimeout(Duration d) {
            resumeTimeout = Liveness.atLeast(d, Duration.ofMillis(1), "resumeTimeout"); return this;
        }

        /**
         * The deadline of every join, of a lane's confirmation and of {@link ZmuxSession#addPipe},
         * default 10 s, a positive duration of at most the maximum join deadline of 60 s, on
         * which the retention of session epochs rests; there is no setting without one. It is
         * independent of the handshake timeout, which bounds establishment only.
         */
        public Builder joinTimeout(Duration d) {
            if (Objects.requireNonNull(d, "joinTimeout").isNegative() || d.isZero()) throw new IllegalArgumentException("joinTimeout must be positive");
            if (d.compareTo(MAX_JOIN_DEADLINE) > 0) throw new IllegalArgumentException("joinTimeout " + d + " above the maximum join deadline " + MAX_JOIN_DEADLINE);
            joinTimeout = d; return this;
        }

        /**
         * The time in Stable after which a multipath session advances its epoch (see
         * {@link ZmuxSession#advanceEpoch}), positive, default one hour. A carrier then lives at
         * most twice as long before a new join replaces it.
         */
        public Builder epochPeriod(Duration d) { epochPeriod = positive(d, "epochPeriod"); return this; }

        /** No epoch advance by time. */
        public Builder noEpochPeriod() { epochPeriod = null; return this; }

        /** The joins in one epoch after which a multipath session advances it, at least 1, default 65536. */
        public Builder epochJoins(long n) {
            if (n < 1) throw new IllegalArgumentException("epochJoins below 1");
            epochJoins = n; return this;
        }

        /** No epoch advance by the count of joins. */
        public Builder noEpochJoins() { epochJoins = -1; return this; }

        /**
         * Whether a session whose secret comes from a TLS exporter or a pre-shared key, rather than from
         * an Establish on its first pipe, advances its epoch once as soon as its first Sealed or Bound
         * pipe exists; true (the default).
         */
        public Builder epochAfterFirstPipe(boolean on) {
            epochAfterFirstPipe = on; return this;
        }

        /** Whether a multipath session advances its epoch once after leaving a suspension, back in Stable; true (the default). */
        public Builder epochAfterResume(boolean on) {
            epochAfterResume = on; return this;
        }

        /**
         * The hook that replaces the pipe of a session running on one pipe at a time, called when
         * the session suspends or lacks a direction; null (the default): none.
         */
        public Builder redial(Redial hook) { redial = hook; return this; }

        /**
         * The backoff of the redial hook's attempts, each wait measured from the start of the
         * previous attempt (default {@link Backoff#defaults()}).
         */
        public Builder redialBackoff(Backoff b) { redialBackoff = Objects.requireNonNull(b, "redialBackoff"); return this; }

        /** The scheduling policy (default {@link Scheduler#AFFINITY}). */
        public Builder scheduler(Scheduler s) { scheduler = Objects.requireNonNull(s); return this; }

        /** The base of a new pipe's probation, positive; not calling it: derived. */
        public Builder probation(Duration d) { probation = positive(d, "probation"); return this; }

        /** Probation off. */
        public Builder noProbation() { probation = Duration.ZERO; return this; }

        /** The placement quantum of the spreading policies in bytes, at least 1. */
        public Builder spreadChunk(int n) {
            if (n < 1) throw new IllegalArgumentException("spreadChunk below 1");
            spreadChunk = n; return this;
        }

        /** How long saturation must persist before spilling into a lower-preference tier (default 1 s). */
        public Builder overflowDelay(Duration d) { overflowDelay = nonNegative(d, "overflowDelay"); return this; }

        /** Urgent data and the first chunk after an idle period sent redundantly (default true). */
        public Builder redundancy(boolean on) { redundancy = on; return this; }

        /**
         * The logger of what the application would otherwise not see: the removal of the last
         * pipe of a direction while streams are open, a resumable session suspending with nothing
         * to redial it, and failures inside host callbacks that zmux contains. It is called
         * outside every lock and must not block. Null (the default): nothing is logged.
         */
        public Builder logger(System.Logger l) { logger = l; return this; }

        /** Datagrams and datagram flows on or off (default off). */
        public Builder datagrams(boolean on) { datagrams = on; return this; }

        /**
         * Early data: {@link EarlyData#ZMUX_PEER} declares the peer a zmux endpoint. It is
         * rejected when the session starts with the role {@link Role#AUTO}, whose
         * stream ID parity is unknown before the peer's parameters.
         */
        public Builder earlyData(EarlyData mode) { earlyData = Objects.requireNonNull(mode); return this; }

        /**
         * Keeps the bootstrap pipe's first carrier write until {@link io.zmux.adapter.SessionAdapter#releaseFirstFlight},
         * or until the handshake timeout has passed since the start ({@link SessionStats#firstFlightTimeouts});
         * the handshake timeout runs from the release. Streams written before the release leave in
         * the same write as the transport parameters. A client with {@link EarlyData#ZMUX_PEER} only.
         */
        public Builder holdFirstFlight(boolean on) { holdFirstFlight = on; return this; }

        /** The ALPN identifiers in preference order (default qmux-02). */
        public Builder alpn(List<String> ids) {
            if (ids.isEmpty()) throw new IllegalArgumentException("empty ALPN list");
            alpn = List.copyOf(ids);
            return this;
        }

        public Builder eventLoops(EventLoops loops) { eventLoops = Objects.requireNonNull(loops); return this; }

        /** The application's bounds on shaping (default: {@link io.zmux.shaping.ShapingLimits#defaults()}). */
        public Builder shapingLimits(io.zmux.shaping.ShapingLimits limits) { shapingLimits = Objects.requireNonNull(limits); return this; }

        /**
         * Gives the shaping configuration's budget, called once per session; null (the default): none. It cannot widen
         * {@link #shapingLimits}. A runtime exception is no budget.
         */
        public Builder shapingBudget(java.util.function.Supplier<io.zmux.shaping.ShapingBudget> budget) { shapingBudget = budget; return this; }

        /**
         * The shaper factory every session of this configuration starts with, so that each
         * outbound pipe, including those of listener-accepted sessions and joined pipes, is
         * shaped from its first record; the factory is called once per pipe when the pipe sends
         * its first record. {@link ZmuxSession#setShaper} replaces it. Null (the default): none.
         */
        public Builder shaper(io.zmux.shaping.ShaperFactory factory) { shaper = factory; return this; }

        /** The event hook: ordered asynchronous notifications; capacity bounds queued events (default 256). */
        public Builder tracer(Tracer t, int capacity) {
            if (capacity < 1) throw new IllegalArgumentException("tracer queue below 1");
            tracer = t; tracerQueue = capacity; return this;
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
         * The configuration. Rejects the combinations that cannot work: a held first flight
         * without early data, a replay window below maxPipes + pipeHeadroom, a redial minimum
         * above its maximum, and multipath or resume without {@link #pipeAuth()} carrying a source
         * is left to the session's start, where a secure first pipe can supply it.
         */
        public ZmuxConfig build() {
            if (holdFirstFlight && earlyData == EarlyData.OFF) throw new IllegalArgumentException("holdFirstFlight without early data");
            if (replayWindow != 0 && (long) replayWindow < (long) maxPipes + pipeHeadroom) {
                throw new IllegalArgumentException("replayWindow below maxPipes + pipeHeadroom");
            }
            if (sendBufferMax != 0 && sendBufferMax < sendBuffer) throw new IllegalArgumentException("sendBufferMax below sendBuffer");
            return new ZmuxConfig(this);
        }

        private static int positive(int n, String what) {
            if (n < 1) throw new IllegalArgumentException(what + " below 1");
            return n;
        }

        private static Duration positive(Duration d, String what) {
            if (Objects.requireNonNull(d, what).isNegative() || d.isZero()) throw new IllegalArgumentException(what + " must be positive");
            return d;
        }
    }

    static Duration nonNegative(Duration d, String what) {
        if (Objects.requireNonNull(d, what).isNegative()) throw new IllegalArgumentException(what + " must not be negative");
        return d;
    }

}
