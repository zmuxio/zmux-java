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
        long keepAlive,
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
        boolean datagramReceiveLimited,
        int maxPipes,
        int headroom,
        int replayWindow,
        long resume,
        Policy policy,
        int spreadChunk,
        long suspectFloor,
        long deadTimeout,
        long probation,
        long overflowDelay,
        long standbyProbe,
        long probeInterval,
        double probeJitter,
        boolean redundancy,
        byte[] psk,
        byte[] exporter,
        boolean earlyZmuxPeer,
        boolean earlyReplayable,
        TransportParams earlyRemembered,
        io.zmux.shaping.ShapingLimits shapingLimits,
        io.zmux.shaping.ShaperFactory shaper,
        TestHooks test) {

    /** The role argument of session creation. */
    public enum Role { INITIATOR, RESPONDER, AUTO }

    /** Built-in scheduling policies. */
    public enum Policy { AFFINITY, SESSION_AFFINITY, SPREAD_ETA, SPREAD_ROUND_ROBIN, SPREAD_RATE_WEIGHTED, CUSTOM }

    public static final int DEFAULT_SPREAD_CHUNK = 16384;
    public static final long DEFAULT_OVERFLOW_DELAY = Time.SECOND;
    public static final long DEFAULT_STANDBY_PROBE = Time.SECOND;
    public static final long DEFAULT_PROBE_INTERVAL = 15 * Time.SECOND;
    public static final long DEFAULT_LATENCY_BUDGET = 100 * Time.MICROSECOND;
    /** {@link #keepAlive()} turned off; 0 is a period of zero. */
    public static final long OFF = -1;
    /** An unlimited {@link #memoryBudget()}; 0 is a budget of no byte. */
    public static final long UNLIMITED = Long.MAX_VALUE;

    /** The trace runner's deterministic values. */
    public record TestHooks(boolean enabled, Preamble.Tlv greaseTlv, long pingSeqStart, long rtt,
                            Map<Long, Long> pipeRtt, int pipeCap) {
        public static final TestHooks NONE = new TestHooks(false, null, 0, 0, Map.of(), 0);

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
        b.keepAlive = keepAlive;
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
        b.datagramReceiveLimited = datagramReceiveLimited;
        b.maxPipes = maxPipes;
        b.headroom = headroom;
        b.replayWindow = replayWindow;
        b.resume = resume;
        b.policy = policy;
        b.spreadChunk = spreadChunk;
        b.suspectFloor = suspectFloor;
        b.deadTimeout = deadTimeout;
        b.probation = probation;
        b.overflowDelay = overflowDelay;
        b.standbyProbe = standbyProbe;
        b.probeInterval = probeInterval;
        b.probeJitter = probeJitter;
        b.redundancy = redundancy;
        b.psk = psk;
        b.exporter = exporter;
        b.earlyZmuxPeer = earlyZmuxPeer;
        b.earlyReplayable = earlyReplayable;
        b.earlyRemembered = earlyRemembered;
        b.shapingLimits = shapingLimits;
        b.shaper = shaper;
        b.test = test;
        return b;
    }

    /** The parameters a zmux endpoint sends by default (SPEC.md Sections 5.3 and 5.5). */
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
        p.maxLanes = 258;
        return p;
    }

    /** Collects the values of a {@link CoreConfig}; every setter returns the builder. */
    public static final class Builder {
        private Role role = Role.INITIATOR;
        private Role transportRole = Role.INITIATOR;
        private TransportParams local = endpointParams();
        private RandomGenerator random = new SecureRandom();
        private long handshakeTimeout = 10 * Time.SECOND;
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
        private double probeJitter = io.zmux.MultipathConfig.DEFAULT_PROBE_JITTER;
        private boolean redundancy = true;
        private byte[] psk;
        private byte[] exporter;
        private boolean earlyZmuxPeer;
        private boolean earlyReplayable;
        private TransportParams earlyRemembered;
        private TestHooks test = TestHooks.NONE;
        private io.zmux.shaping.ShapingLimits shapingLimits = io.zmux.shaping.ShapingLimits.none();
        private io.zmux.shaping.ShaperFactory shaper;

        private Builder() { }

        public CoreConfig build() {
            return new CoreConfig(role, transportRole, local, random, handshakeTimeout, keepAlive, stagedUni, uniTarget, streamWindowCap, connWindowCap, latencyBudget, memoryBudget, budget, noAutotune, windowPredicate, sendBuffer, sendBufferMax, datagrams, datagramSendQueue, datagramReceiveQueue, datagramStreamQueue, datagramReceiveBytes, datagramReceiveLimited, maxPipes, headroom, replayWindow, resume, policy, spreadChunk, suspectFloor, deadTimeout, probation, overflowDelay, standbyProbe, probeInterval, probeJitter, redundancy, psk, exporter, earlyZmuxPeer, earlyReplayable, earlyRemembered, shapingLimits, shaper, test);
        }

        public Builder role(Role v) { role = v; return this; }

        public Builder shapingLimits(io.zmux.shaping.ShapingLimits v) { shapingLimits = java.util.Objects.requireNonNull(v); return this; }

        /** The shaper factory installed at session creation; null: none. */
        public Builder shaper(io.zmux.shaping.ShaperFactory v) { shaper = v; return this; }

        /** The carrier role (who dialled the bootstrap carrier) when role negotiation is not in use. */
        public Builder transportRole(Role v) { transportRole = v; return this; }

        /** Exactly what the endpoint sends (GREASE included); the builder starts from the zmux endpoint defaults (SPEC.md Sections 5.3 and 5.5). */
        public Builder local(TransportParams v) { local = v; return this; }

        /** The session's random source (GREASE choices); tests inject a seeded one. */
        public Builder random(RandomGenerator v) { random = v; return this; }

        public Builder handshakeTimeout(long v) { handshakeTimeout = v; return this; }

        public Builder keepAlive(long v) { keepAlive = v; return this; }

        /** initial_max_streams_uni is staged and raised to uniTarget on the first accept (SPEC.md 8.5 rule 4). */
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

        /** Exporter of pipe 0's innermost end-to-end handshake (E flag asserted by the application). */
        public Builder exporter(byte[] v) { exporter = v; return this; }

        public Builder earlyZmuxPeer(boolean v) { earlyZmuxPeer = v; return this; }

        public Builder earlyReplayable(boolean v) { earlyReplayable = v; return this; }

        /** Peer parameters remembered from an earlier session for carrier early data; null when none. */
        public Builder earlyRemembered(TransportParams v) { earlyRemembered = v; return this; }

        /** Test hooks (TRACES.md Section 3.3); never set from production configuration. */
        public Builder test(TestHooks v) { test = v; return this; }

        /** The parameter set being built, for in-place adjustment. */
        public TransportParams local() { return local; }
    }
}
