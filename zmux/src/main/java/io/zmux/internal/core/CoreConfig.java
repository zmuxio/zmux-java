package io.zmux.internal.core;

import io.zmux.internal.wire.Preamble;
import io.zmux.internal.wire.TransportParams;
import java.security.SecureRandom;
import java.util.Map;
import java.util.random.RandomGenerator;

/**
 * The resolved, immutable configuration of a session. The public layer builds it from the
 * application's configuration; the trace runner from a profile. Durations are
 * nanoseconds; {@link Time#NEVER} means never. {@link #local} is the parameter set the
 * endpoint sends; it is owned by the session once the configuration is handed over.
 */
public record CoreConfig(
        Role role,
        Role transportRole,
        TransportParams local,
        RandomGenerator random,
        long handshakeTimeout,
        long joinTimeout,
        boolean redial,
        boolean stagedUni,
        long uniTarget,
        long streamWindowCap,
        long connWindowCap,
        long latencyBudget,
        long memoryBudget,
        SharedMemory budget,
        boolean noAutotune,
        boolean windowPredicate,
        long sendBuffer,
        long sendBufferMax,
        boolean datagrams,
        int datagramSendQueue,
        int datagramReceiveQueue,
        int datagramStreamQueue,
        long datagramReceiveBytes,
        long datagramStreamBytes,
        boolean datagramReceiveLimited,
        int maxPipes,
        int headroom,
        int replayWindow,
        long resume,
        Policy policy,
        int spreadChunk,
        long probation,
        long overflowDelay,
        boolean redundancy,
        byte[] psk,
        java.util.function.Supplier<byte[]> exporter,
        boolean deferredPsk,
        boolean declaredEndToEnd,
        boolean secureFirstPipe,
        io.zmux.internal.join.Mode firstPipeMode,
        int joinEarlyHello,
        Live live,
        boolean earlyZmuxPeer,
        boolean holdFirstFlight,
        io.zmux.shaping.ShapingLimits shapingLimits,
        io.zmux.shaping.ShapingBudget shapingBudget,
        io.zmux.shaping.ShaperFactory shaper,
        Epochs epochs,
        TestHooks test) {
    /** The per-stream datagram byte bound by default: 32 datagrams of 9000 bytes fit. */
    public static final long DEFAULT_DATAGRAM_STREAM_BYTES = 512 << 10;


    /**
     * The policy of session epochs: an exchange after period in Stable ({@link Time#NEVER}: never by time) or
     * after joins joins in one epoch ({@link Long#MAX_VALUE}: never by count), once after the first Sealed or
     * Bound pipe of a session whose secret is not from an Establish, once after a resumption; and the
     * instance identifier that begins every new local Locator.
     */
    public record Epochs(long period, long joins, boolean afterFirstPipe, boolean afterResume, byte[] locatorPrefix) {
        /** The default policy: an exchange every hour or every 2^16 joins, after the first secure pipe and after a resumption. */
        public static final Epochs DEFAULT = new Epochs(3600 * Time.SECOND, 1 << 16, true, true, new byte[0]);
        /** No exchange but those asked for. */
        public static final Epochs MANUAL = new Epochs(Time.NEVER, Long.MAX_VALUE, false, false, new byte[0]);

        public Epochs {
            locatorPrefix = locatorPrefix.clone();
        }

        /** This policy with another Locator prefix. */
        public Epochs withLocatorPrefix(byte[] prefix) { return new Epochs(period, joins, afterFirstPipe, afterResume, prefix); }
    }

    /** The longest join deadline: no join timeout is longer. */
    public static final long MAX_JOIN_DEADLINE = 60 * Time.SECOND;
    /** How long an endpoint keeps accepting the previous epoch after it starts using the new one: three maximum join deadlines. */
    public static final long EPOCH_RETENTION = 3 * MAX_JOIN_DEADLINE;

    /** The default join timeout. */
    public static final long DEFAULT_JOIN_TIMEOUT = 10 * Time.SECOND;

    /**
     * The deadline of a join, of a lane's confirmation and of a held or stalled join carrier: the
     * join timeout, which is independent of the handshake timeout.
     */
    public long joinDeadline() { return joinTimeout; }

    /** The role argument of session creation. */
    public enum Role { INITIATOR, RESPONDER, AUTO }

    /** Built-in scheduling policies. */
    public enum Policy { AFFINITY, SESSION_AFFINITY, SPREAD_ETA, SPREAD_ROUND_ROBIN, SPREAD_RATE_WEIGHTED, CUSTOM }

    public static final int DEFAULT_SPREAD_CHUNK = 16384;
    public static final long DEFAULT_OVERFLOW_DELAY = Time.SECOND;
    public static final long DEFAULT_STANDBY_PROBE = Time.SECOND;
    public static final long DEFAULT_PROBE_INTERVAL = 15 * Time.SECOND;
    public static final long DEFAULT_LATENCY_BUDGET = 100 * Time.MICROSECOND;
    /**
     * The liveness settings in effect, one set per built configuration: owned by the session's
     * loop and replaced while it runs ({@link Session#setLiveness}).
     */
    public static final class Live {
        private long keepAlive, suspectFloor, deadTimeout, standbyProbe, probeInterval;
        private double probeJitter;

        /** Takes the values of l. */
        void set(Live l) {
            keepAlive = l.keepAlive; suspectFloor = l.suspectFloor; deadTimeout = l.deadTimeout;
            standbyProbe = l.standbyProbe; probeInterval = l.probeInterval; probeJitter = l.probeJitter;
        }

        public Live(long keepAlive, long suspectFloor, long deadTimeout, long standbyProbe, long probeInterval, double probeJitter) {
            this.keepAlive = keepAlive; this.suspectFloor = suspectFloor; this.deadTimeout = deadTimeout;
            this.standbyProbe = standbyProbe; this.probeInterval = probeInterval; this.probeJitter = probeJitter;
        }

        public long keepAlive() { return keepAlive; }
        public long suspectFloor() { return suspectFloor; }
        /** The dead timeout: 0 derived, negative stall-based failure off. */
        public long deadTimeout() { return deadTimeout; }
        public long standbyProbe() { return standbyProbe; }
        public long probeInterval() { return probeInterval; }
        public double probeJitter() { return probeJitter; }
    }

    public long keepAlive() { return live.keepAlive(); }
    public long suspectFloor() { return live.suspectFloor(); }
    public long deadTimeout() { return live.deadTimeout(); }
    public long standbyProbe() { return live.standbyProbe(); }
    public long probeInterval() { return live.probeInterval(); }
    public double probeJitter() { return live.probeJitter(); }

    /** {@link #keepAlive()} turned off; 0 is a period of zero. */
    public static final long OFF = -1;
    /** An unlimited {@link #memoryBudget()}; 0 is a budget of no byte. */
    public static final long UNLIMITED = Long.MAX_VALUE;

    /** The trace runner's deterministic values. epochLocatorPrefix: when set, the local Locator of epoch e is this prefix and e in decimal ASCII. */
    public record TestHooks(boolean enabled, Preamble.Tlv greaseTlv, long pingSeqStart, long rtt,
                            Map<Long, Long> pipeRtt, int pipeCap, byte[] epochLocatorPrefix) {
        public static final TestHooks NONE = new TestHooks(false, null, 0, 0, Map.of(), 0, null);

        public TestHooks {
            pipeRtt = Map.copyOf(pipeRtt);
        }
    }

    /** The default configuration with the parameters a zmux endpoint sends. */
    public static CoreConfig defaults() {
        return builder().build();
    }

    /** A builder holding the defaults. */
    public static Builder builder() {
        return new Builder();
    }

    /** A builder holding this configuration's values. */
    public Builder toBuilder() {
        Builder b = new Builder();
        b.role = role;
        b.transportRole = transportRole;
        b.local = local;
        b.random = random;
        b.handshakeTimeout = handshakeTimeout;
        b.joinTimeout = joinTimeout;
        b.epochs = epochs;
        b.redial = redial;
        b.declaredEndToEnd = declaredEndToEnd;
        b.secureFirstPipe = secureFirstPipe;
        b.firstPipeMode = firstPipeMode;
        b.joinEarlyHello = joinEarlyHello;
        b.keepAlive = live.keepAlive();
        b.stagedUni = stagedUni;
        b.uniTarget = uniTarget;
        b.streamWindowCap = streamWindowCap;
        b.connWindowCap = connWindowCap;
        b.latencyBudget = latencyBudget;
        b.memoryBudget = memoryBudget;
        b.budget = budget;
        b.noAutotune = noAutotune;
        b.windowPredicate = windowPredicate;
        b.sendBuffer = sendBuffer;
        b.sendBufferMax = sendBufferMax;
        b.datagrams = datagrams;
        b.datagramSendQueue = datagramSendQueue;
        b.datagramReceiveQueue = datagramReceiveQueue;
        b.datagramStreamQueue = datagramStreamQueue;
        b.datagramReceiveBytes = datagramReceiveBytes;
        b.datagramStreamBytes = datagramStreamBytes;
        b.datagramReceiveLimited = datagramReceiveLimited;
        b.maxPipes = maxPipes;
        b.headroom = headroom;
        b.replayWindow = replayWindow;
        b.resume = resume;
        b.policy = policy;
        b.spreadChunk = spreadChunk;
        b.suspectFloor = live.suspectFloor();
        b.deadTimeout = live.deadTimeout();
        b.probation = probation;
        b.overflowDelay = overflowDelay;
        b.standbyProbe = live.standbyProbe();
        b.probeInterval = live.probeInterval();
        b.probeJitter = live.probeJitter();
        b.redundancy = redundancy;
        b.psk = psk;
        b.exporter = exporter;
        b.deferredPsk = deferredPsk;
        b.earlyZmuxPeer = earlyZmuxPeer;
        b.holdFirstFlight = holdFirstFlight;
        b.shapingLimits = shapingLimits;
        b.shapingBudget = shapingBudget;
        b.shaper = shaper;
        b.test = test;
        return b;
    }

    /** The parameters a zmux endpoint sends by default. */
    static TransportParams endpointParams() {
        TransportParams p = new TransportParams();
        p.present = TransportParams.IDLE | TransportParams.MAX_DATA | TransportParams.BIDI_LOCAL
                | TransportParams.BIDI_REMOTE | TransportParams.UNI | TransportParams.STREAMS_BIDI
                | TransportParams.STREAMS_UNI | TransportParams.RESET_AT | TransportParams.RECORD_SIZE
                | TransportParams.GOAWAY | TransportParams.PREAMBLE | TransportParams.PRIORITY
                | TransportParams.PIPE_ACK | TransportParams.LANES;
        p.maxIdleTimeoutMillis = 30000;
        p.initialMaxData = 1 << 20;
        p.maxStreamDataBidiLocal = 256 << 10;
        p.maxStreamDataBidiRemote = 256 << 10;
        p.maxStreamDataUni = 256 << 10;
        p.maxStreamsBidi = 256;
        p.maxStreamsUni = 16;
        p.maxRecordSize = 65535;
        p.maxPreambleSize = 4096;
        p.maxLanes = io.zmux.ZmuxConfig.DEFAULT_MAX_LANES;
        return p;
    }

    /** Collects the values of a {@link CoreConfig}; every setter returns the builder. */
    public static final class Builder {
        private Role role = Role.INITIATOR;
        private Role transportRole = Role.INITIATOR;
        private TransportParams local = endpointParams();
        private RandomGenerator random = new SecureRandom();
        private long handshakeTimeout = 10 * Time.SECOND;
        private long joinTimeout = DEFAULT_JOIN_TIMEOUT;
        private boolean redial;
        private long keepAlive = OFF;
        private boolean stagedUni = true;
        private long uniTarget = 256;
        private long streamWindowCap;
        private long connWindowCap;
        private long latencyBudget = DEFAULT_LATENCY_BUDGET;
        private long memoryBudget = 1L << 30;
        private SharedMemory budget;
        private boolean noAutotune;
        private boolean windowPredicate;
        private long sendBuffer = 4L << 20;
        private long sendBufferMax;
        private boolean datagrams;
        private int datagramSendQueue = 64;
        private int datagramReceiveQueue = 128;
        private int datagramStreamQueue = 32;
        private long datagramReceiveBytes = 1L << 20;
        private long datagramStreamBytes = DEFAULT_DATAGRAM_STREAM_BYTES;
        private boolean datagramReceiveLimited;
        private int maxPipes = 1;
        private int headroom = 8;
        private int replayWindow;
        private long resume;
        private Policy policy = Policy.AFFINITY;
        private int spreadChunk = DEFAULT_SPREAD_CHUNK;
        private long suspectFloor = 250 * Time.MILLISECOND;
        private long deadTimeout;
        private long probation = -1;
        private long overflowDelay = DEFAULT_OVERFLOW_DELAY;
        private long standbyProbe = DEFAULT_STANDBY_PROBE;
        private long probeInterval = DEFAULT_PROBE_INTERVAL;
        private double probeJitter = io.zmux.Liveness.DEFAULT_PROBE_JITTER;
        private boolean redundancy = true;
        private byte[] psk;
        private java.util.function.Supplier<byte[]> exporter;
        private boolean deferredPsk;
        private boolean earlyZmuxPeer;
        private boolean holdFirstFlight;
        private boolean declaredEndToEnd;
        private boolean secureFirstPipe;
        private io.zmux.internal.join.Mode firstPipeMode;
        private int joinEarlyHello;
        private TestHooks test = TestHooks.NONE;
        private Epochs epochs = Epochs.DEFAULT;
        private io.zmux.shaping.ShapingLimits shapingLimits = io.zmux.shaping.ShapingLimits.defaults();
        private io.zmux.shaping.ShapingBudget shapingBudget = io.zmux.shaping.ShapingBudget.NONE;
        private io.zmux.shaping.ShaperFactory shaper;

        private Builder() { }

        public CoreConfig build() {
            return new CoreConfig(role, transportRole, local, random, handshakeTimeout, joinTimeout, redial, stagedUni, uniTarget, streamWindowCap, connWindowCap, latencyBudget, memoryBudget, budget, noAutotune, windowPredicate, sendBuffer, sendBufferMax, datagrams, datagramSendQueue, datagramReceiveQueue, datagramStreamQueue, datagramReceiveBytes, datagramStreamBytes, datagramReceiveLimited, maxPipes, headroom, replayWindow, resume, policy, spreadChunk, probation, overflowDelay, redundancy, psk, exporter, deferredPsk, declaredEndToEnd, secureFirstPipe, firstPipeMode, joinEarlyHello, new Live(keepAlive, suspectFloor, deadTimeout, standbyProbe, probeInterval, probeJitter), earlyZmuxPeer, holdFirstFlight, shapingLimits, shapingBudget, shaper, epochs, test);
        }

        public Builder role(Role v) { role = v; return this; }

        public Builder epochs(Epochs v) { epochs = java.util.Objects.requireNonNull(v); return this; }

        public Builder shapingLimits(io.zmux.shaping.ShapingLimits v) { shapingLimits = java.util.Objects.requireNonNull(v); return this; }

        /** The configuration's budget for this session. */
        public Builder shapingBudget(io.zmux.shaping.ShapingBudget v) { shapingBudget = java.util.Objects.requireNonNull(v); return this; }

        /** The shaper factory installed at session creation; null: none. */
        public Builder shaper(io.zmux.shaping.ShaperFactory v) { shaper = v; return this; }

        /** The carrier role (who dialled the bootstrap carrier) when role negotiation is not in use. */
        public Builder transportRole(Role v) { transportRole = v; return this; }

        /** Exactly what the endpoint sends (GREASE included); the builder starts from the zmux endpoint defaults. */
        public Builder local(TransportParams v) { local = v; return this; }

        /** The session's random source (GREASE choices); tests inject a seeded one. */
        public Builder random(RandomGenerator v) { random = v; return this; }

        public Builder handshakeTimeout(long v) { handshakeTimeout = v; return this; }
        public Builder joinTimeout(long v) { joinTimeout = v; return this; }
        /** A redial hook dials for the session when it suspends. */
        public Builder redial(boolean v) { redial = v; return this; }

        public Builder keepAlive(long v) { keepAlive = v; return this; }

        /** initial_max_streams_uni is staged and raised to uniTarget on the first accept. */
        public Builder stagedUni(boolean v) { stagedUni = v; return this; }

        public Builder uniTarget(long v) { uniTarget = v; return this; }

        /** 0 = auto (64 MiB floor + budget). */
        public Builder streamWindowCap(long v) { streamWindowCap = v; return this; }

        /** 0 = auto (96 MiB floor + budget). */
        public Builder connWindowCap(long v) { connWindowCap = v; return this; }

        public Builder latencyBudget(long v) { latencyBudget = v; return this; }

        /** 0 = unlimited. */
        public Builder memoryBudget(long v) { memoryBudget = v; return this; }

        /** A shared listener budget, replacing memoryBudget; null when none. */
        public Builder budget(SharedMemory v) { budget = v; return this; }

        /** Fixed windows (tests). */
        public Builder noAutotune(boolean v) { noAutotune = v; return this; }

        /** Session window growth waits for the application's predicate (run by the driver). */
        public Builder windowPredicate(boolean v) { windowPredicate = v; return this; }

        public Builder sendBuffer(long v) { sendBuffer = v; return this; }

        /** 0 = auto. */
        public Builder sendBufferMax(long v) { sendBufferMax = v; return this; }

        public Builder datagrams(boolean v) { datagrams = v; return this; }

        public Builder datagramSendQueue(int v) { datagramSendQueue = v; return this; }

        public Builder datagramReceiveQueue(int v) { datagramReceiveQueue = v; return this; }

        public Builder datagramStreamQueue(int v) { datagramStreamQueue = v; return this; }

        public Builder datagramReceiveBytes(long v) { datagramReceiveBytes = v; return this; }

        /** Payload bytes queued per stream and direction; -1: no byte bound. */
        public Builder datagramStreamBytes(long v) { datagramStreamBytes = v; return this; }

        /** Received datagrams wait for the driver's receive limiter before they are queued. */
        public Builder datagramReceiveLimited(boolean v) { datagramReceiveLimited = v; return this; }

        public Builder maxPipes(int v) { maxPipes = v; return this; }

        public Builder headroom(int v) { headroom = v; return this; }

        public Builder replayWindow(int v) { replayWindow = v; return this; }

        public Builder resume(long v) { resume = v; return this; }

        /** The initial scheduling policy; the session owns the current one. */
        public Builder policy(Policy v) { policy = v; return this; }

        public Builder spreadChunk(int v) { spreadChunk = v; return this; }

        public Builder suspectFloor(long v) { suspectFloor = v; return this; }

        /** 0 = default. */
        public Builder deadTimeout(long v) { deadTimeout = v; return this; }

        /** Negative = default. */
        public Builder probation(long v) { probation = v; return this; }

        public Builder overflowDelay(long v) { overflowDelay = v; return this; }

        public Builder standbyProbe(long v) { standbyProbe = v; return this; }

        public Builder probeInterval(long v) { probeInterval = v; return this; }

        /** The default probe jitter of the session's pipes, {@code 0 <= v < 1}. */
        public Builder probeJitter(double v) { probeJitter = v; return this; }

        public Builder redundancy(boolean v) { redundancy = v; return this; }

        public Builder psk(byte[] v) { psk = v; return this; }

        /** Exporter of pipe 0's innermost end-to-end handshake (E flag asserted by the application), read once when the peer's parameters arrive; null from it: not available. */
        public Builder exporter(java.util.function.Supplier<byte[]> v) { exporter = v; return this; }

        /** The PSK is supplied after establishment. */
        public Builder deferredPsk(boolean v) { deferredPsk = v; return this; }
        public Builder declaredEndToEnd(boolean v) { declaredEndToEnd = v; return this; }
        // Internal packages are exported only to this project's modules (qualified export), so these signatures name types that stay unexported.
        //noinspection ClassEscapesItsScope
        public Builder secureFirstPipe(io.zmux.internal.join.Mode m) { secureFirstPipe = m != null; firstPipeMode = m; return this; }
        public Builder joinEarlyHello(int v) { joinEarlyHello = v; return this; }

        public Builder earlyZmuxPeer(boolean v) { earlyZmuxPeer = v; return this; }

        public Builder holdFirstFlight(boolean v) { holdFirstFlight = v; return this; }

        /** Test hooks; never set from production configuration. */
        public Builder test(TestHooks v) { test = v; return this; }

        /** The parameter set being built, for in-place adjustment. */
        public TransportParams local() { return local; }
    }
}
