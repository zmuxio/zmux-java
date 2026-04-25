package io.zmux.internal;

import io.zmux.*;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("resource")
public final class SessionRuntime implements ZmuxNativeSession {
    private static final int MAX_BATCH_FRAMES = 32;
    private static final int MAX_EXPLICIT_GROUPS = 16;
    private static final long FALLBACK_GROUP_BUCKET = Long.MAX_VALUE;
    private static final int DEFAULT_ADMISSION_SOFT_CAP = 32;
    private static final int DEFAULT_ADMISSION_HARD_CAP = 64;
    private static final long PROVISIONAL_OPEN_BASE_MAX_AGE_NANOS = TimeUnit.SECONDS.toNanos(5L);
    private static final long PROVISIONAL_OPEN_MAX_AGE_ADAPTIVE_CAP_NANOS = TimeUnit.SECONDS.toNanos(20L);
    private static final long PROVISIONAL_OPEN_RTT_ADAPTIVE_SLACK_NANOS = TimeUnit.MILLISECONDS.toNanos(250L);
    private static final Duration GOAWAY_DRAIN_INTERVAL = Duration.ofMillis(10L);
    private static final Duration GOAWAY_DRAIN_INTERVAL_MAX = Duration.ofMillis(250L);
    private static final Duration DEFAULT_GRACEFUL_CLOSE_DRAIN_TIMEOUT = Duration.ofMillis(500L);
    private static final Duration DEFAULT_GRACEFUL_CLOSE_DRAIN_TIMEOUT_MAX = Duration.ofSeconds(5L);
    private static final long DEFAULT_GRACEFUL_CLOSE_RTT_ADAPTIVE_SLACK_NANOS = TimeUnit.MILLISECONDS.toNanos(100L);
    private static final long STOP_SENDING_ADAPTIVE_DRAIN_WINDOW_MAX_NANOS = TimeUnit.SECONDS.toNanos(2L);
    private static final long SESSION_NONCE_GAMMA = -7046029254386353131L;
    private static final AtomicLong SESSION_NONCE_SEED_COUNTER = new AtomicLong();
    private static final Duration ESTABLISHMENT_FAILURE_WRITE_WAIT = Duration.ofMillis(250L);
    private static final Duration ESTABLISHMENT_SUCCESS_WRITE_WAIT = Duration.ofSeconds(1L);
    private static final Duration ESTABLISHMENT_CLOSE_DRAIN_DELAY = Duration.ofMillis(10L);
    private static final long DEFAULT_ABUSE_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(5L);
    private static final int DEFAULT_INBOUND_CONTROL_FRAME_BUDGET = 2048;
    private static final int DEFAULT_INBOUND_EXT_FRAME_BUDGET = 1024;
    private static final long MIN_INBOUND_CONTROL_BYTES_BUDGET = 256L << 10;
    private static final long MIN_INBOUND_EXT_BYTES_BUDGET = 256L << 10;
    private static final int DEFAULT_NO_OP_CONTROL_FLOOD_THRESHOLD = 128;
    private static final int DEFAULT_NO_OP_MAX_DATA_FLOOD_THRESHOLD = 128;
    private static final int DEFAULT_NO_OP_BLOCKED_FLOOD_THRESHOLD = 128;
    private static final int DEFAULT_NO_OP_ZERO_DATA_FLOOD_THRESHOLD = 128;
    private static final int DEFAULT_NO_OP_PRIORITY_UPDATE_FLOOD_THRESHOLD = 128;
    private static final int DEFAULT_GROUP_REBUCKET_CHURN_THRESHOLD = 256;
    private static final int DEFAULT_INBOUND_PING_FLOOD_THRESHOLD = 128;
    private static final long DEFAULT_HIDDEN_ABORT_CHURN_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(1L);
    private static final int DEFAULT_HIDDEN_ABORT_CHURN_THRESHOLD = 128;
    private static final long DEFAULT_VISIBLE_TERMINAL_CHURN_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(1L);
    private static final int DEFAULT_VISIBLE_TERMINAL_CHURN_THRESHOLD = 128;
    private static final long HIDDEN_CONTROL_RETAINED_MAX_AGE_NANOS = TimeUnit.SECONDS.toNanos(1L);
    private static final int DEFAULT_VISIBLE_ACCEPT_BACKLOG_LIMIT = 128;
    private static final long MIN_PENDING_CONTROL_BUDGET = 65536L;
    private static final long MIN_PENDING_PRIORITY_BUDGET = 65536L;
    private static final long MIN_SESSION_MEMORY_HARD_CAP = 0x800000L;
    private static final long MIN_RETAINED_STATE_UNIT = 4096L;
    private static final long MIN_COMPACT_TERMINAL_STATE_UNIT = 64L;
    private static final int DEFAULT_TOMBSTONE_LIMIT = 4096;
    private static final String SESSION_CLOSED_REASON = SessionClosedException.MESSAGE;
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte[][] EMPTY_PARTS = new byte[0][];
    private static final int INBOUND_PAYLOAD_POOL_DEPTH_PER_LENGTH = 4;
    private static final int MAX_INBOUND_POOLED_PAYLOAD_BYTES = 64 * 1024;
    private static final int MAX_PENDING_READ_LOOP_PROTOCOL_TASKS = 256;
    private final DuplexConnection connection;
    private final ZmuxConfig config;
    private final Preface localPreface;
    private final FrameCodec.Decoder input;
    private final BufferedOutputStream output;
    private final InboundPayloadPool inboundPayloadPool;
    private final Object lock = new Object();
    private final SessionEventDispatcher eventDispatcher;
    private final SessionTelemetryState telemetry;
    private final CountDownLatch terminated = new CountDownLatch(1);
    private Deque<OutboundFrame> urgentQueue = new ArrayDeque<>();
    private Deque<StreamRuntime> advisoryQueue = new ArrayDeque<>();
    private Deque<OutboundFrame> dataQueue = new ArrayDeque<>();
    private ArrayDeque<ReadLoopProtocolTask> readLoopProtocolTasks =
            new ArrayDeque<>(MAX_PENDING_READ_LOOP_PROTOCOL_TASKS);
    private final Map<Long, StreamRuntime> streams = new HashMap<>();
    private final SessionAcceptRegistry acceptRegistry;
    private final SessionOutboundQueueBookkeeping outboundQueueBookkeeping;
    private final SessionPriorityUpdateCoordinator priorityUpdateCoordinator;
    private final SessionFlowControlUpdateRegistry flowControlUpdateRegistry;
    private final SessionFlowControlCoordinator flowControlCoordinator;
    private final SessionOpeningCoordinator openingCoordinator;
    private final SessionOutboundDataCoordinator outboundDataCoordinator;
    private final SessionStopSendingGracefulCoordinator stopSendingGracefulCoordinator;
    private final SessionStreamBookkeeping streamBookkeeping;
    private final Map<Long, Long> resetReasonCounts = new HashMap<>();
    private final Map<Long, Long> abortReasonCounts = new HashMap<>();
    private final int[] lockWaitersByKind = new int[LockWaitKind.values().length];
    private final OrdinaryBatchOrderer.RetainedBias ordinaryBatchBias = new OrdinaryBatchOrderer.RetainedBias();
    private final SessionExplicitGroupTracker explicitGroupTracker =
            new SessionExplicitGroupTracker(ordinaryBatchBias, MAX_EXPLICIT_GROUPS, FALLBACK_GROUP_BUCKET);
    private final SessionTerminalBookkeeping terminalBookkeeping;
    private final SessionLocalOpenTracker localOpenTracker;
    private final SessionEstablishmentCoordinator establishmentCoordinator;
    private final SessionLifecycleCoordinator lifecycleRuntime;
    private final SessionReaderCoordinator readerRuntime;
    private final SessionWriterCoordinator writerRuntime;
    private final SessionStatsCollector statsCollector;
    private long pingNonceState;
    private long resetReasonOverflowCount;
    private long abortReasonOverflowCount;
    private List<OutboundFrame> inflightBatch = Collections.emptyList();
    private volatile Preface peerPreface;
    private volatile Negotiated negotiated;
    private volatile SessionState state = SessionState.INVALID;
    private volatile ApplicationError peerGoAwayError;
    private volatile ApplicationError peerCloseError;
    private volatile IOException terminalError;
    private long nextLocalBidi;
    private long nextLocalUni;
    private long nextPeerBidi;
    private long nextPeerUni;
    private long lastAcceptedPeerBidi;
    private long lastAcceptedPeerUni;
    private long localGoAwayBidi = 0x3FFFFFFFFFFFFFFFL;
    private long localGoAwayUni = 0x3FFFFFFFFFFFFFFFL;
    private boolean localGoAwayIssued;
    private long peerGoAwayBidi = 0x3FFFFFFFFFFFFFFFL;
    private long peerGoAwayUni = 0x3FFFFFFFFFFFFFFFL;
    private long sessionSendLimit;
    private long sessionSentBytes;
    private long sessionReservedSendBytes;
    private long sessionQueuedDataBytes;
    private long bufferedReceiveBytes;
    private long bufferedReceiveStorageBytes;
    private long recvSessionAdvertised;
    private long recvSessionReceivedBytes;
    private long recvSessionPending;
    private boolean receiveReplenishRetry;
    private long retainedOpenInfoBytes;
    private long retainedPeerReasonBytes;
    private long writerHeldRetainedBytes;
    private long aggregateLateDataReceived;
    private long sentFrames;
    private long receivedFrames;
    private long sentDataBytes;
    private long receivedDataBytes;
    private long inboundControlBudgetWindowStartedAtNanos;
    private int inboundControlFrameCount;
    private long inboundControlBytes;
    private long inboundExtBudgetWindowStartedAtNanos;
    private int inboundExtFrameCount;
    private long inboundExtBytes;
    private long inboundMixedBudgetWindowStartedAtNanos;
    private int inboundMixedFrameCount;
    private long inboundMixedBytes;
    private long inboundPingFloodWindowStartedAtNanos;
    private int inboundPingFloodCount;
    private long noOpControlWindowStartedAtNanos;
    private int noOpControlCount;
    private long noOpMaxDataWindowStartedAtNanos;
    private int noOpMaxDataCount;
    private long noOpBlockedWindowStartedAtNanos;
    private int noOpBlockedCount;
    private long noOpZeroDataWindowStartedAtNanos;
    private int noOpZeroDataCount;
    private long noOpPriorityUpdateWindowStartedAtNanos;
    private int noOpPriorityUpdateCount;
    private long droppedPriorityUpdateCount;
    private long droppedLocalPriorityUpdateCount;
    private long coalescedTerminalSignalsCount;
    private long supersededTerminalSignalsCount;
    private long skippedCloseOnDeadIoCount;
    private long closeFrameFlushErrorCount;
    private long hiddenAbortChurnWindowStartedAtNanos;
    private int hiddenAbortChurnCount;
    private long visibleTerminalChurnEventCount;
    private long visibleTerminalChurnWindowStartedAtNanos;
    private int visibleTerminalChurnCount;
    private long groupRebucketEventCount;
    private long groupRebucketWindowStartedAtNanos;
    private int groupRebucketCount;
    private long lateDataAfterCloseReadBytes;
    private long lateDataAfterResetBytes;
    private long lateDataAfterAbortBytes;
    private long hiddenStreamsRefused;
    private long hiddenStreamsReaped;
    private long hiddenUnreadBytesDiscarded;
    private long provisionalOpenLimitedCount;
    private long provisionalOpenExpiredCount;
    private boolean closeFrameQueued;
    private boolean closedTransport;
    private boolean gracefulCloseActive;
    private boolean terminalCleanupApplied;
    private boolean readLoopProtocolWorkerStarted;
    private int lockWaiters;

    private SessionRuntime(DuplexConnection connection, ZmuxConfig config) {
        this.connection = connection;
        this.config = config;
        this.localPreface = config.localPreface();
        this.input = FrameCodec.decoder(connection.input());
        this.output = new BufferedOutputStream(connection.output());
        this.inboundPayloadPool = new InboundPayloadPool(
                inboundPayloadPoolLimit(config.settings()),
                INBOUND_PAYLOAD_POOL_DEPTH_PER_LENGTH
        );
        this.eventDispatcher = new SessionEventDispatcher(this.lock, config.eventHandler());
        this.telemetry = new SessionTelemetryState(new SessionTelemetryOwner(this), config, System.nanoTime(), Instant.now());
        this.terminalBookkeeping = new SessionTerminalBookkeeping(
                new SessionTerminalBookkeepingOwner(this),
                MIN_COMPACT_TERMINAL_STATE_UNIT,
                HIDDEN_CONTROL_RETAINED_MAX_AGE_NANOS
        );
        this.acceptRegistry = new SessionAcceptRegistry(new SessionAcceptRegistryOwner(this));
        this.outboundQueueBookkeeping = new SessionOutboundQueueBookkeeping(
                new SessionOutboundQueueBookkeepingOwner(this)
        );
        this.priorityUpdateCoordinator = new SessionPriorityUpdateCoordinator(this);
        this.flowControlUpdateRegistry = new SessionFlowControlUpdateRegistry(
                new SessionFlowControlUpdateRegistryOwner(this)
        );
        this.flowControlCoordinator = new SessionFlowControlCoordinator(this);
        this.openingCoordinator = new SessionOpeningCoordinator(this);
        this.outboundDataCoordinator = new SessionOutboundDataCoordinator(this);
        this.stopSendingGracefulCoordinator = new SessionStopSendingGracefulCoordinator(this);
        this.streamBookkeeping = new SessionStreamBookkeeping();
        this.establishmentCoordinator = new SessionEstablishmentCoordinator(
                new SessionEstablishmentCoordinatorOwner(this),
                ESTABLISHMENT_FAILURE_WRITE_WAIT,
                ESTABLISHMENT_SUCCESS_WRITE_WAIT,
                ESTABLISHMENT_CLOSE_DRAIN_DELAY
        );
        this.readerRuntime = new SessionReaderCoordinator(new SessionReaderCoordinatorOwner(this));
        this.lifecycleRuntime = new SessionLifecycleCoordinator(new SessionLifecycleCoordinatorOwner(this));
        this.writerRuntime = new SessionWriterCoordinator(
                new SessionWriterCoordinatorOwner(this),
                new SessionWriterTransport(new SessionWriterTransportOwner(this))
        );
        this.localOpenTracker = new SessionLocalOpenTracker(new SessionLocalOpenTrackerOwner(this));
        this.statsCollector = new SessionStatsCollector(this);
    }

    public static SessionRuntime open(DuplexConnection connection, ZmuxConfig config) throws IOException {
        Objects.requireNonNull(connection, "connection");
        ZmuxConfig effectiveConfig = config == null ? ZmuxConfig.defaults() : config;
        SessionRuntime sessionRuntime = new SessionRuntime(connection, effectiveConfig);
        sessionRuntime.establish();
        return sessionRuntime;
    }

    private static int inboundPayloadPoolLimit(Settings settings) {
        if (settings == null || settings.maxFramePayload() <= 0L) {
            return 0;
        }
        return (int) Math.min(MAX_INBOUND_POOLED_PAYLOAD_BYTES, settings.maxFramePayload());
    }

    static int retainedQueueBytes(OutboundFrame outboundFrame) {
        if (outboundFrame == null) {
            return 0;
        }
        int prefixLength = outboundFrame.payloadPrefix == null ? 0 : outboundFrame.payloadPrefix.length;
        long retained = (long) prefixLength + Math.max(0, outboundFrame.payloadLength);
        if (outboundFrame.frame().type() == FrameType.DATA && outboundFrame.dataBytes > 0) {
            retained = Math.max(0L, retained - outboundFrame.dataBytes);
        }
        long cost = SessionRuntime.saturatingAdd(1L, retained);
        return cost >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) cost;
    }

    private static long outboundBatchCost(OutboundFrame outboundFrame) {
        if (outboundFrame == null) {
            return 1L;
        }
        int prefixLength = outboundFrame.payloadPrefix == null ? 0 : outboundFrame.payloadPrefix.length;
        long payloadBytes = (long) prefixLength + Math.max(0, outboundFrame.payloadLength);
        return 1L + payloadBytes;
    }

    private static long outboundBatchCost(List<OutboundFrame> outboundFrames) {
        long total = 0L;
        if (outboundFrames == null) {
            return total;
        }
        for (OutboundFrame outboundFrame : outboundFrames) {
            total = SessionRuntime.saturatingAdd(total, SessionRuntime.outboundBatchCost(outboundFrame));
        }
        return total;
    }

    private static long retainedBatchBytes(List<OutboundFrame> outboundFrames) {
        long total = 0L;
        if (outboundFrames == null) {
            return total;
        }
        for (OutboundFrame outboundFrame : outboundFrames) {
            total = SessionRuntime.saturatingAdd(total, SessionRuntime.retainedQueueBytes(outboundFrame));
        }
        return total;
    }

    static long retainedUrgentControlBytes(List<OutboundFrame> outboundFrames) {
        long total = 0L;
        if (outboundFrames == null) {
            return total;
        }
        for (OutboundFrame outboundFrame : outboundFrames) {
            if (SessionRuntime.isUrgentControlFrame(outboundFrame)) {
                total = SessionRuntime.saturatingAdd(total, SessionRuntime.retainedQueueBytes(outboundFrame));
            }
        }
        return total;
    }

    private static boolean isUrgentControlFrame(OutboundFrame outboundFrame) {
        return outboundFrame != null
                && outboundFrame.frame().type() != FrameType.DATA
                && outboundFrame.frame().type() != FrameType.EXT;
    }

    private static boolean isTerminalControlFrame(OutboundFrame outboundFrame) {
        if (outboundFrame == null || outboundFrame.frame() == null || outboundFrame.frame().streamId() == 0L) {
            return false;
        }
        FrameType type = outboundFrame.frame().type();
        return type == FrameType.STOP_SENDING || type == FrameType.RESET || type == FrameType.ABORT;
    }

    private static boolean sameStreamTerminalControl(OutboundFrame queued, OutboundFrame candidate) {
        return SessionRuntime.isTerminalControlFrame(queued)
                && SessionRuntime.isTerminalControlFrame(candidate)
                && queued.frame().streamId() == candidate.frame().streamId();
    }

    private static boolean queuedTerminalControlCoalescesCandidate(OutboundFrame queued, OutboundFrame candidate) {
        FrameType queuedType = queued.frame().type();
        FrameType candidateType = candidate.frame().type();
        if (queuedType == FrameType.ABORT && candidateType != FrameType.ABORT) {
            return true;
        }
        return queuedType == candidateType
                && Arrays.equals(queued.frame().payload(), candidate.frame().payload());
    }

    private static boolean shouldReplaceQueuedTerminalControl(OutboundFrame queued, OutboundFrame candidate) {
        FrameType queuedType = queued.frame().type();
        FrameType candidateType = candidate.frame().type();
        switch (candidateType) {
            case ABORT:
                return queuedType == FrameType.STOP_SENDING
                        || queuedType == FrameType.RESET
                        || (queuedType == FrameType.ABORT
                        && !Arrays.equals(queued.frame().payload(), candidate.frame().payload()));
            case RESET:
                return queuedType == FrameType.RESET
                        && !Arrays.equals(queued.frame().payload(), candidate.frame().payload());
            case STOP_SENDING:
                return queuedType == FrameType.STOP_SENDING
                        && !Arrays.equals(queued.frame().payload(), candidate.frame().payload());
            default:
                return false;
        }
    }

    private static int saturatingIncrement(int value) {
        return value == Integer.MAX_VALUE ? value : value + 1;
    }

    private static long initSessionNonceState(long seed) {
        return seed != 0L ? seed : SESSION_NONCE_SEED_COUNTER.addAndGet(SESSION_NONCE_GAMMA);
    }

    private static Map<Long, Long> copyReasonCounts(Map<Long, Long> reasons) {
        if (reasons == null || reasons.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new HashMap<>(reasons));
    }

    private static boolean countsQueuedData(OutboundFrame outboundFrame) {
        return outboundFrame != null
                && outboundFrame.stream != null
                && outboundFrame.frame().type() == FrameType.DATA
                && outboundFrame.dataBytes > 0;
    }

    private static boolean urgentOutboundPrecedes(OutboundFrame candidate, OutboundFrame currentBest) {
        int candidateRank = urgentFrameRank(candidate);
        int currentRank = urgentFrameRank(currentBest);
        if (candidateRank != currentRank) {
            return candidateRank < currentRank;
        }

        boolean candidateScoped = candidate.frame().streamId() != 0L;
        boolean currentScoped = currentBest.frame().streamId() != 0L;
        if (candidateScoped != currentScoped) {
            return candidateScoped;
        }
        return candidateScoped && candidate.frame().streamId() < currentBest.frame().streamId();
    }

    private static int countRetainedPayloadSegments(byte[][] parts, int partIndex, int partOffset, int length) {
        int index = partIndex;
        int offset = partOffset;
        int remaining = length;
        int segmentCount = 0;
        while (remaining > 0 && index < parts.length) {
            byte[] part = Objects.requireNonNull(parts[index], "parts[" + index + "]");
            if (offset >= part.length) {
                ++index;
                offset = 0;
                continue;
            }
            int take = Math.min(part.length - offset, remaining);
            remaining -= take;
            ++segmentCount;
            ++index;
            offset = 0;
        }
        if (remaining != 0) {
            throw new IllegalArgumentException("multipart payload underrun");
        }
        return segmentCount;
    }

    private static UncheckedIOException internalIoFailure(String operation, IOException error) {
        return new UncheckedIOException("zmux: internal IO helper failed while attempting to " + operation, error);
    }

    static long adaptiveRttTimeout(
            long lastRttNanos,
            long baseTimeoutNanos,
            long maxTimeoutNanos,
            int multiplier,
            long slackNanos
    ) {
        if (baseTimeoutNanos <= 0L) {
            return 0L;
        }
        long timeoutNanos = baseTimeoutNanos;
        long candidateTimeoutNanos = adaptiveRttFloor(lastRttNanos, multiplier, slackNanos);
        if (candidateTimeoutNanos > 0L) {
            if (candidateTimeoutNanos > timeoutNanos) {
                timeoutNanos = candidateTimeoutNanos;
            }
        }
        if (maxTimeoutNanos > 0L && timeoutNanos > maxTimeoutNanos) {
            return maxTimeoutNanos;
        }
        return timeoutNanos;
    }

    static long adaptiveRttFloor(long lastRttNanos, int multiplier, long slackNanos) {
        if (lastRttNanos <= 0L || multiplier <= 0) {
            return 0L;
        }
        return RuntimeFlow.saturatingAdd(
                RuntimeFlow.saturatingMultiply(lastRttNanos, multiplier),
                Math.max(0L, slackNanos)
        );
    }

    private static long positiveNanos(Duration duration) {
        return durationToPositiveNanosSaturated(duration);
    }

    private static long quarterThreshold(long value) {
        return value <= 0L ? 0L : Math.max(1L, value / 4L);
    }

    static boolean shouldReplenishPendingWindow(long remaining, long target, long advertised, long pending, long emergencyThreshold, long minPending) {
        if (remaining <= emergencyThreshold) {
            return true;
        }
        if (remaining <= SessionRuntime.quarterThreshold(target) && advertised >= target) {
            return true;
        }
        return pending >= minPending;
    }

    static long replenishMinPending(long target, long payload) {
        long minPending = SessionRuntime.quarterThreshold(target);
        if (minPending == 0L) {
            minPending = 1L;
        }
        if (payload > 0L && payload < minPending) {
            minPending = payload;
        }
        return minPending;
    }

    static long clampVarint62(long value) {
        if (value <= 0L) {
            return 0L;
        }
        return Math.min(value, Protocol.MAX_VARINT62);
    }

    private static long averageFloor(long left, long right) {
        if (left <= right) {
            return left + (right - left) / 2L;
        }
        return right + (left - right) / 2L;
    }

    private static int urgentFrameRank(OutboundFrame outboundFrame) {
        if (outboundFrame == null) {
            return Integer.MAX_VALUE;
        }
        switch (outboundFrame.frame().type()) {
            case CLOSE:
                return 0;
            case GOAWAY:
                return 1;
            case ABORT:
                return 2;
            case RESET:
                return 3;
            case STOP_SENDING:
                return 4;
            case MAX_DATA:
                return 5;
            case BLOCKED:
                return 6;
            case PONG:
                return 7;
            case PING:
                return 8;
            default:
                return 100;
        }
    }

    private static SessionState publicState(SessionState state) {
        return state == null ? SessionState.INVALID : state;
    }

    static long deadlineNanos(Instant deadline) {
        if (deadline == null) {
            return 0L;
        }
        Instant nowInstant = Instant.now();
        long nowNanos = System.nanoTime();
        if (!deadline.isAfter(nowInstant)) {
            return nowNanos;
        }
        long deltaNanos = durationToPositiveNanosSaturated(Duration.between(nowInstant, deadline));
        if (deltaNanos <= 0L) {
            return nowNanos;
        }
        return saturatingAdd(nowNanos, deltaNanos);
    }

    static ZmuxInterruptedIOException interruptedIo(String message,
                                                    String operation,
                                                    io.zmux.ZmuxErrorScope scope,
                                                    io.zmux.ZmuxErrorDirection direction,
                                                    InterruptedException cause) {
        return new ZmuxInterruptedIOException(
                message,
                operation,
                scope,
                io.zmux.ZmuxErrorSource.LOCAL,
                direction,
                cause
        );
    }

    static ZmuxInterruptedException interrupted(String message,
                                                String operation,
                                                io.zmux.ZmuxErrorScope scope,
                                                io.zmux.ZmuxErrorDirection direction,
                                                InterruptedException cause) {
        return new ZmuxInterruptedException(
                message,
                operation,
                scope,
                io.zmux.ZmuxErrorSource.LOCAL,
                direction,
                cause
        );
    }

    private static boolean awaitLatch(CountDownLatch latch, Duration duration) throws InterruptedException {
        if (latch == null) {
            return true;
        }
        TimeoutBudget budget = TimeoutBudget.fromTimeout(duration);
        if (!budget.bounded()) {
            latch.await();
            return true;
        }
        long timeoutNanos = budget.remainingNanos();
        if (timeoutNanos <= 0L) {
            return latch.getCount() == 0L;
        }
        return latch.await(timeoutNanos, TimeUnit.NANOSECONDS);
    }

    private static void sleepDuration(Duration duration) throws InterruptedException {
        long sleepNanos = durationToPositiveNanosSaturated(duration);
        if (sleepNanos <= 0L) {
            return;
        }
        TimeUnit.NANOSECONDS.sleep(sleepNanos);
    }

    static long durationToPositiveNanosSaturated(Duration duration) {
        if (duration == null || duration.isNegative() || duration.isZero()) {
            return 0L;
        }
        try {
            return duration.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static int admissionSoftCap(int pendingLimit) {
        if (pendingLimit <= 0) {
            return DEFAULT_ADMISSION_SOFT_CAP;
        }
        return Math.max(16, pendingLimit / 4);
    }

    private static int admissionHardCap(int pendingLimit) {
        if (pendingLimit <= 0) {
            return DEFAULT_ADMISSION_HARD_CAP;
        }
        return Math.max(32, pendingLimit / 2);
    }

    static long visibleAcceptBacklogBytesHardCapFor(long maxFramePayload) {
        return RuntimeFlow.visibleAcceptBacklogBytesHardCap(maxFramePayload);
    }

    static long firstLocalStreamId(Role role, boolean bidirectional) {
        if (role == Role.INITIATOR) {
            return bidirectional ? 4L : 2L;
        }
        if (role == Role.RESPONDER) {
            return bidirectional ? 1L : 3L;
        }
        return 0L;
    }

    static long firstPeerStreamId(Role role, boolean bidirectional) {
        if (role == Role.INITIATOR) {
            return bidirectional ? 1L : 3L;
        }
        if (role == Role.RESPONDER) {
            return bidirectional ? 4L : 2L;
        }
        return 0L;
    }

    static long saturatingAdd(long base, long delta) {
        if (delta <= 0L) {
            return base;
        }
        if (base > Long.MAX_VALUE - delta) {
            return Long.MAX_VALUE;
        }
        return base + delta;
    }

    private static long saturatingMultiply(long left, long right) {
        if (left <= 0L || right <= 0L) {
            return 0L;
        }
        if (left > Long.MAX_VALUE / right) {
            return Long.MAX_VALUE;
        }
        return left * right;
    }

    private static long minNonZeroPositive(long a, long b) {
        if (a <= 0L) {
            return b;
        }
        if (b <= 0L) {
            return a;
        }
        return Math.min(a, b);
    }

    static ZmuxException sessionError(ErrorCode code,
                                      String operation,
                                      String message,
                                      ZmuxErrorSource source,
                                      ZmuxErrorDirection direction) {
        return sessionError(code, operation, message, null, source, direction);
    }

    static ZmuxException sessionError(ErrorCode code,
                                      String operation,
                                      String message,
                                      Throwable cause,
                                      ZmuxErrorSource source,
                                      ZmuxErrorDirection direction) {
        return new ZmuxException(
                code.code(),
                operation,
                message,
                cause,
                ZmuxErrorScope.SESSION,
                source == null ? ZmuxErrorSource.UNKNOWN : source,
                direction == null ? ZmuxErrorDirection.BOTH : direction,
                ZmuxTerminationKind.UNKNOWN
        );
    }

    static ZmuxException sessionInternalError(String operation, String message) {
        return sessionInternalError(operation, message, null);
    }

    static ZmuxException sessionInternalError(String operation, String message, Throwable cause) {
        return new ZmuxException(
                ErrorCode.INTERNAL.code(),
                operation,
                message,
                cause,
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        );
    }

    static ZmuxException openLimitedError(String message) {
        return sessionError(ErrorCode.STREAM_LIMIT, "open", message, ZmuxErrorSource.LOCAL, ZmuxErrorDirection.BOTH);
    }

    static boolean streamIsBidi(long streamId) {
        return (streamId & 2L) == 0L;
    }

    static boolean streamIsLocal(Role role, long streamId) {
        return SessionRuntime.streamOpener(streamId) == role;
    }

    static Role streamOpener(long streamId) {
        switch ((int) (streamId & 3L)) {
            case 0:
            case 2:
                return Role.INITIATOR;
            case 1:
            case 3:
                return Role.RESPONDER;
            default:
                return Role.AUTO;
        }
    }

    @Override
    public ZmuxNativeStream acceptStream() throws IOException, InterruptedException {
        return this.acceptStream(null);
    }

    @Override
    public ZmuxNativeStream acceptStream(Duration duration) throws IOException, InterruptedException {
        StreamRuntime streamRuntime = this.acceptRuntimeFromQueue(true, duration);
        this.emitPendingEvents();
        return streamRuntime;
    }

    @Override
    public ZmuxNativeRecvStream acceptUniStream() throws IOException, InterruptedException {
        return this.acceptUniStream(null);
    }

    @Override
    public ZmuxNativeRecvStream acceptUniStream(Duration duration) throws IOException, InterruptedException {
        StreamRuntime streamRuntime = this.acceptRuntimeFromQueue(false, duration);
        this.emitPendingEvents();
        return streamRuntime.recvView();
    }

    @Override
    public ZmuxNativeStream openStream() throws IOException {
        return this.openStream(OpenOptions.empty());
    }

    @Override
    public ZmuxNativeStream openStream(OpenOptions options) throws IOException {
        synchronized (this.lock) {
            return this.newLocalStreamLocked(true, options);
        }
    }

    @Override
    public ZmuxNativeStream openStreamWithTimeout(Duration timeout) throws IOException {
        return this.openStreamWithTimeout(OpenOptions.empty(), timeout);
    }

    @Override
    public ZmuxNativeStream openStreamWithTimeout(OpenOptions options, Duration timeout) throws IOException {
        TimeoutBudget budget = TimeoutBudget.fromTimeout(timeout);
        synchronized (this.lock) {
            return this.newLocalStreamLocked(true, options, budget);
        }
    }

    @Override
    public ZmuxNativeSendStream openUniStream() throws IOException {
        return this.openUniStream(OpenOptions.empty());
    }

    @Override
    public ZmuxNativeSendStream openUniStream(OpenOptions options) throws IOException {
        synchronized (this.lock) {
            return this.newLocalStreamLocked(false, options).sendView();
        }
    }

    @Override
    public ZmuxNativeSendStream openUniStreamWithTimeout(Duration timeout) throws IOException {
        return this.openUniStreamWithTimeout(OpenOptions.empty(), timeout);
    }

    @Override
    public ZmuxNativeSendStream openUniStreamWithTimeout(OpenOptions options, Duration timeout) throws IOException {
        TimeoutBudget budget = TimeoutBudget.fromTimeout(timeout);
        synchronized (this.lock) {
            return this.newLocalStreamLocked(false, options, budget).sendView();
        }
    }

    @Override
    public ZmuxNativeStream openAndSend(byte[] payload) throws IOException {
        return this.openAndSend(OpenOptions.empty(), payload);
    }

    @Override
    public ZmuxNativeStream openAndSend(OpenOptions openOptions, byte[] payload) throws IOException {
        StreamRuntime streamRuntime;
        synchronized (this.lock) {
            streamRuntime = this.newLocalStreamLocked(true, openOptions);
        }
        if (payload != null && payload.length > 0) {
            streamRuntime.write(payload);
        }
        return streamRuntime;
    }

    @Override
    public ZmuxNativeStream openAndSendWithTimeout(Duration timeout, byte[] payload) throws IOException {
        return this.openAndSendWithTimeout(OpenOptions.empty(), timeout, payload);
    }

    @Override
    public ZmuxNativeStream openAndSendWithTimeout(OpenOptions openOptions, Duration timeout, byte[] payload)
            throws IOException {
        TimeoutBudget budget = TimeoutBudget.fromTimeout(timeout);
        StreamRuntime streamRuntime;
        synchronized (this.lock) {
            streamRuntime = this.newLocalStreamLocked(true, openOptions, budget);
        }
        if (payload == null || payload.length == 0) {
            return streamRuntime;
        }
        if (budget.bounded()) {
            streamRuntime.setWriteDeadlineNanos(budget.deadlineNanos());
        }
        try {
            streamRuntime.write(payload);
            return streamRuntime;
        } finally {
            if (budget.bounded()) {
                streamRuntime.setWriteDeadlineNanos(0L);
            }
        }
    }

    @Override
    public ZmuxNativeSendStream openUniAndSend(byte[] payload) throws IOException {
        return this.openUniAndSend(OpenOptions.empty(), payload);
    }

    @Override
    public ZmuxNativeSendStream openUniAndSend(OpenOptions openOptions, byte[] payload) throws IOException {
        StreamRuntime streamRuntime;
        synchronized (this.lock) {
            streamRuntime = this.newLocalStreamLocked(false, openOptions);
        }
        streamRuntime.writeFinal(payload == null ? EMPTY_BYTES : payload);
        return streamRuntime.sendView();
    }

    @Override
    public ZmuxNativeSendStream openUniAndSendWithTimeout(Duration timeout, byte[] payload) throws IOException {
        return this.openUniAndSendWithTimeout(OpenOptions.empty(), timeout, payload);
    }

    @Override
    public ZmuxNativeSendStream openUniAndSendWithTimeout(OpenOptions openOptions, Duration timeout, byte[] payload)
            throws IOException {
        TimeoutBudget budget = TimeoutBudget.fromTimeout(timeout);
        StreamRuntime streamRuntime;
        synchronized (this.lock) {
            streamRuntime = this.newLocalStreamLocked(false, openOptions, budget);
        }
        if (budget.bounded()) {
            streamRuntime.setWriteDeadlineNanos(budget.deadlineNanos());
        }
        try {
            streamRuntime.writeFinal(payload == null ? EMPTY_BYTES : payload);
            return streamRuntime.sendView();
        } finally {
            if (budget.bounded()) {
                streamRuntime.setWriteDeadlineNanos(0L);
            }
        }
    }

    @Override
    public void closeWithError(long code, String reason) throws IOException {
        try {
            synchronized (this.lock) {
                if (this.state.terminal() || this.closeFrameQueued || this.state == SessionState.CLOSING) {
                    return;
                }
                byte[] closePayload = FrameCodec.buildErrorPayload(code, reason, this.controlPayloadLimitLocked());
                ApplicationError error = new ApplicationError(
                        code,
                        reason,
                        ZmuxErrorScope.SESSION,
                        ZmuxErrorSource.LOCAL,
                        ZmuxErrorDirection.BOTH,
                        ZmuxTerminationKind.SESSION_TERMINATION
                );
                SessionState terminalState = this.terminalStateForSessionError(error);
                this.beginSessionTerminationLocked(error, terminalState);
                this.closeFrameQueued = true;
                try {
                    this.enqueueControlLocked(new FrameCodec.Frame(FrameType.CLOSE, 0, 0L, closePayload));
                } catch (IOException queueError) {
                    this.finishSessionLocked(queueError, this.terminalStateForSessionError(queueError));
                    throw queueError;
                }
                this.notifyLockWaitersLocked();
            }
        } finally {
            this.emitPendingEvents();
        }
    }

    @Override
    public boolean awaitTermination(Duration duration) throws InterruptedException {
        try {
            if (duration == null) {
                this.terminated.await();
                return true;
            }
            return awaitLatch(this.terminated, duration);
        } catch (InterruptedException interrupted) {
            throw interrupted(
                    "zmux: interrupted while waiting for session termination",
                    "wait",
                    ZmuxErrorScope.SESSION,
                    ZmuxErrorDirection.BOTH,
                    interrupted
            );
        }
    }

    @Override
    public Optional<IOException> terminationCause() {
        synchronized (this.lock) {
            return Optional.ofNullable(this.terminationCauseLocked());
        }
    }

    private IOException terminationCauseLocked() {
        IOException error = this.terminalError != null ? this.terminalError : this.peerCloseError;
        if (error instanceof ApplicationError
                && ((ApplicationError) error).code() == ErrorCode.NO_ERROR.code()) {
            return null;
        }
        if (error != null) {
            return error;
        }
        if (this.state == SessionState.FAILED) {
            return new SessionClosedException(ZmuxErrorSource.UNKNOWN);
        }
        return null;
    }

    @Override
    public boolean isClosed() {
        return this.state.terminal();
    }

    @Override
    public SessionState state() {
        return this.publicState();
    }

    @Override
    public SessionStats stats() {
        synchronized (this.lock) {
            return this.statsCollector.collectLocked();
        }
    }

    void emitPendingEvents() {
        this.eventDispatcher.emitPendingEvents();
    }

    void enqueueSessionClosedEventLocked(IOException error) {
        this.eventDispatcher.enqueueSessionClosedEventLocked(this.state, error);
    }

    void enqueueStreamEventLocked(StreamRuntime streamRuntime, ZmuxEventType type, IOException error) {
        this.eventDispatcher.enqueueStreamEventLocked(this.state, streamRuntime, type, error);
    }

    @Override
    public Duration ping(byte[] payload, Duration timeout) throws IOException, InterruptedException {
        return this.telemetry.ping(payload, timeout);
    }

    @Override
    public void goAway(long bidiWatermark, long uniWatermark, long errorCode, String reason) throws IOException {
        synchronized (this.lock) {
            if (this.shouldFailSessionOperationsLocked()) {
                throw this.sessionOperationErrorLocked("close", this.currentErrorLocked());
            }
            this.validateOutgoingGoAwayWatermarkLocked(bidiWatermark, true);
            this.validateOutgoingGoAwayWatermarkLocked(uniWatermark, false);
            if (bidiWatermark > this.localGoAwayBidi || uniWatermark > this.localGoAwayUni) {
                if (this.localGoAwayIssued
                        && this.localGoAwayBidi <= bidiWatermark
                        && this.localGoAwayUni <= uniWatermark) {
                    return;
                }
                throw sessionError(
                        ErrorCode.PROTOCOL,
                        "goAway",
                        "GOAWAY watermarks must be non-increasing",
                        ZmuxErrorSource.LOCAL,
                        ZmuxErrorDirection.WRITE
                );
            }
            if (this.localGoAwayIssued
                    && bidiWatermark == this.localGoAwayBidi
                    && uniWatermark == this.localGoAwayUni) {
                return;
            }
            byte[] payload = FrameCodec.buildGoAwayPayload(
                    bidiWatermark,
                    uniWatermark,
                    errorCode,
                    reason,
                    this.controlPayloadLimitLocked()
            );
            OutboundFrame outboundFrame = new OutboundFrame(
                    new FrameCodec.Frame(FrameType.GOAWAY, 0, 0L, payload),
                    null,
                    0,
                    false,
                    false
            );
            this.enqueueReplacingQueuedGoAwayLocked(outboundFrame);
            this.localGoAwayBidi = bidiWatermark;
            this.localGoAwayUni = uniWatermark;
            this.localGoAwayIssued = true;
            if (this.state == SessionState.READY) {
                this.state = SessionState.DRAINING;
            }
            this.notifyLockWaitersLocked();
        }
    }

    @Override
    public ApplicationError peerGoAwayError() {
        return this.peerGoAwayError;
    }

    @Override
    public ApplicationError peerCloseError() {
        return this.peerCloseError;
    }

    @Override
    public Preface localPreface() {
        return this.localPreface;
    }

    @Override
    public Preface peerPreface() {
        return this.peerPreface;
    }

    @Override
    public Negotiated negotiated() {
        return this.negotiated;
    }

    @Override
    public void close() throws IOException {
        try {
            Duration drainTimeout;
            boolean gracefulDrain;
            boolean awaitExistingClose;
            boolean sendInitialGoAway;
            long initialBidiWatermark;
            long initialUniWatermark;
            synchronized (this.lock) {
                if (this.state.terminal()) {
                    return;
                }
                drainTimeout = this.gracefulCloseDrainTimeout();
                if (this.closeFrameQueued || this.state == SessionState.CLOSING || this.gracefulCloseActive) {
                    gracefulDrain = false;
                    awaitExistingClose = true;
                    sendInitialGoAway = false;
                    initialBidiWatermark = 0L;
                    initialUniWatermark = 0L;
                } else if (this.hasGracefulClosePendingWorkLocked()) {
                    this.gracefulCloseActive = true;
                    if (this.state == SessionState.READY) {
                        this.state = SessionState.DRAINING;
                    }
                    gracefulDrain = true;
                    awaitExistingClose = false;
                    sendInitialGoAway = this.localGoAwayBidi == 0x3FFFFFFFFFFFFFFFL && this.localGoAwayUni == 0x3FFFFFFFFFFFFFFFL;
                    initialBidiWatermark = this.effectiveGoAwaySendWatermarkLocked(true);
                    initialUniWatermark = this.effectiveGoAwaySendWatermarkLocked(false);
                } else {
                    this.gracefulCloseActive = true;
                    gracefulDrain = false;
                    awaitExistingClose = false;
                    sendInitialGoAway = false;
                    initialBidiWatermark = 0L;
                    initialUniWatermark = 0L;
                }
                if (!awaitExistingClose) {
                    this.notifyLockWaitersLocked();
                }
            }
            if (awaitExistingClose) {
                this.awaitCloseCompletion(drainTimeout.plusSeconds(1L));
                return;
            }
            IOException closeError = null;
            if (gracefulDrain) {
                if (sendInitialGoAway) {
                    if (!this.trySendGracefulGoAway(initialBidiWatermark, initialUniWatermark)) {
                        this.awaitCloseCompletion(drainTimeout.plusSeconds(1L));
                        return;
                    }
                }
                this.awaitGoAwayDrainInterval();
                boolean sendRefinedGoAway;
                long refinedBidiWatermark;
                long refinedUniWatermark;
                synchronized (this.lock) {
                    if (this.state.terminal()) {
                        return;
                    }
                    refinedBidiWatermark = Math.min(this.localGoAwayBidi, this.lastAcceptedPeerBidi);
                    refinedUniWatermark = Math.min(this.localGoAwayUni, this.lastAcceptedPeerUni);
                    sendRefinedGoAway = refinedBidiWatermark < this.localGoAwayBidi || refinedUniWatermark < this.localGoAwayUni;
                }
                if (sendRefinedGoAway) {
                    if (!this.trySendGracefulGoAway(refinedBidiWatermark, refinedUniWatermark)) {
                        this.awaitCloseCompletion(drainTimeout.plusSeconds(1L));
                        return;
                    }
                }
                synchronized (this.lock) {
                    this.reclaimGracefulCloseLocalStreamsLocked();
                }
                if (!this.waitForGracefulCloseDrain(drainTimeout)) {
                    synchronized (this.lock) {
                        this.recordGracefulCloseTimeoutLocked();
                    }
                    closeError = new GracefulCloseTimeoutException();
                }
            }
            this.closeWithError(ErrorCode.NO_ERROR.code(), "");
            this.awaitCloseCompletion(drainTimeout.plusSeconds(1L));
            if (closeError != null) {
                throw closeError;
            }
        } finally {
            this.emitPendingEvents();
        }
    }

    private boolean trySendGracefulGoAway(long bidiWatermark, long uniWatermark) throws IOException {
        try {
            this.goAway(bidiWatermark, uniWatermark, ErrorCode.NO_ERROR.code(), "");
            return true;
        } catch (IOException error) {
            synchronized (this.lock) {
                if (this.state.terminal() || this.closeFrameQueued || this.state == SessionState.CLOSING) {
                    return false;
                }
            }
            throw error;
        }
    }

    Object lock() {
        return this.lock;
    }

    Role localRole() {
        return this.negotiated.localRole();
    }

    long capabilities() {
        return this.negotiated.capabilities();
    }

    Settings peerSettings() {
        return this.negotiated.peerSettings();
    }

    Settings localSettings() {
        return this.config.settings();
    }

    DuplexConnection connection() {
        return this.connection;
    }

    long retainedOpenInfoBudgetLocked() {
        long budget = this.config.retainedOpenInfoBytesBudget();
        if (budget > 0L) {
            return budget;
        }
        long maxPayload = Math.max(this.localSettings().maxFramePayload(), this.peerSettings().maxFramePayload());
        if (maxPayload <= 0L) {
            maxPayload = Settings.defaults().maxFramePayload();
        }
        budget = SessionRuntime.saturatingMultiply(maxPayload, 8L);
        return Math.max(65536L, budget);
    }

    long retainedPeerReasonBudgetLocked() {
        long budget = this.config.retainedPeerReasonBytesBudget();
        if (budget > 0L) {
            return budget;
        }
        long maxPayload = this.localSettings().maxControlPayloadBytes();
        if (maxPayload <= 0L) {
            maxPayload = Settings.defaults().maxControlPayloadBytes();
        }
        budget = SessionRuntime.saturatingMultiply(maxPayload, 8L);
        return Math.max(65536L, budget);
    }

    private long negotiatedFramePayloadForReplenishLocked() {
        return RuntimeFlow.negotiatedFramePayload(this.localSettings(), this.peerSettings());
    }

    long sessionWindowTargetLocked() {
        return Math.max(this.localSettings().initialMaxData(), SessionRuntime.saturatingMultiply(this.sessionQueuedDataHighWatermarkLocked(), 4L));
    }

    long streamWindowTargetLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null) {
            return 0L;
        }
        return Math.max(streamRuntime.initialReceiveWindow(), SessionRuntime.saturatingMultiply(this.perStreamQueuedDataHighWatermarkLocked(), 2L));
    }

    long sessionEmergencyThresholdLocked() {
        return SessionRuntime.saturatingMultiply(this.negotiatedFramePayloadForReplenishLocked(), 2L);
    }

    long streamEmergencyThresholdLocked(long target) {
        long payload = this.negotiatedFramePayloadForReplenishLocked();
        if (payload <= 0L) {
            return 0L;
        }
        long quarter = SessionRuntime.quarterThreshold(target);
        if (quarter <= 0L) {
            return payload;
        }
        return Math.min(payload, quarter);
    }

    long sessionReplenishMinPendingLocked(long target) {
        return SessionRuntime.replenishMinPending(target, this.negotiatedFramePayloadForReplenishLocked());
    }

    long streamReplenishMinPendingLocked(long target) {
        return SessionRuntime.replenishMinPending(target, this.negotiatedFramePayloadForReplenishLocked());
    }

    long perStreamQueuedDataHighWatermarkLocked() {
        long configured = this.config.perStreamQueuedDataHwm();
        if (configured > 0L) {
            return configured;
        }
        return RuntimeFlow.repoDefaultPerStreamDataHighWatermark(this.negotiatedFramePayloadForReplenishLocked());
    }

    long sessionQueuedDataHighWatermarkLocked() {
        long configured = this.config.sessionQueuedDataHwm();
        if (configured > 0L) {
            return configured;
        }
        return RuntimeFlow.repoDefaultSessionDataHighWatermark(this.perStreamQueuedDataHighWatermarkLocked());
    }

    private long perStreamQueuedDataLowWatermarkLocked() {
        return RuntimeFlow.lowWatermark(this.perStreamQueuedDataHighWatermarkLocked());
    }

    private long sessionQueuedDataLowWatermarkLocked() {
        return RuntimeFlow.lowWatermark(this.sessionQueuedDataHighWatermarkLocked());
    }

    long urgentQueuedBytesHardCapLocked() {
        long configured = this.config.urgentQueuedBytesCap();
        if (configured > 0L) {
            return configured;
        }
        return RuntimeFlow.repoDefaultUrgentLaneCap(this.negotiatedControlPayloadLimitLocked());
    }

    long pendingControlBytesBudgetLocked() {
        long configured = this.config.pendingControlBytesBudget();
        if (configured > 0L) {
            return configured;
        }
        long maxPayload = this.peerSettings().maxControlPayloadBytes();
        if (maxPayload <= 0L) {
            maxPayload = this.localSettings().maxControlPayloadBytes();
        }
        if (maxPayload <= 0L) {
            maxPayload = Settings.defaults().maxControlPayloadBytes();
        }
        return Math.max(MIN_PENDING_CONTROL_BUDGET, SessionRuntime.saturatingMultiply(maxPayload, 8L));
    }

    long pendingPriorityBytesBudgetLocked() {
        long configured = this.config.pendingPriorityBytesBudget();
        if (configured > 0L) {
            return configured;
        }
        long maxPayload = this.peerSettings().maxExtensionPayloadBytes();
        if (maxPayload <= 0L) {
            maxPayload = this.localSettings().maxExtensionPayloadBytes();
        }
        if (maxPayload <= 0L) {
            maxPayload = Settings.defaults().maxExtensionPayloadBytes();
        }
        return Math.max(MIN_PENDING_PRIORITY_BUDGET, SessionRuntime.saturatingMultiply(maxPayload, 8L));
    }

    long sessionMemoryHardCapLocked() {
        long configured = this.config.sessionMemoryCap();
        if (configured > 0L) {
            return configured;
        }
        long hardCap = this.sessionWindowTargetLocked();
        hardCap = SessionRuntime.saturatingAdd(hardCap, this.sessionQueuedDataHighWatermarkLocked());
        hardCap = SessionRuntime.saturatingAdd(hardCap, this.urgentQueuedBytesHardCapLocked());
        hardCap = SessionRuntime.saturatingAdd(hardCap, this.pendingControlBytesBudgetLocked());
        hardCap = SessionRuntime.saturatingAdd(hardCap, this.pendingPriorityBytesBudgetLocked());
        hardCap = SessionRuntime.saturatingAdd(hardCap, this.retainedOpenInfoBudgetLocked());
        hardCap = SessionRuntime.saturatingAdd(hardCap, this.retainedPeerReasonBudgetLocked());
        return Math.max(MIN_SESSION_MEMORY_HARD_CAP, hardCap);
    }

    int tombstoneLimitLocked() {
        int configured = this.config.tombstoneLimit();
        return configured > 0 ? configured : DEFAULT_TOMBSTONE_LIMIT;
    }

    int markerOnlyUsedStreamHardCapLocked() {
        int configured = this.config.markerOnlyUsedStreamLimit();
        if (configured > 0) {
            return configured;
        }
        long derived = this.sessionMemoryHardCapLocked() / MIN_COMPACT_TERMINAL_STATE_UNIT;
        if (derived <= 1L) {
            return 1;
        }
        return derived >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) derived;
    }

    long retainedStateUnitLocked() {
        long unit = this.localSettings().maxFramePayload();
        unit = Math.max(unit, this.localSettings().maxControlPayloadBytes());
        unit = Math.max(unit, this.localSettings().maxExtensionPayloadBytes());
        if (unit <= 0L) {
            Settings defaults = Settings.defaults();
            unit = defaults.maxFramePayload();
            unit = Math.max(unit, defaults.maxControlPayloadBytes());
            unit = Math.max(unit, defaults.maxExtensionPayloadBytes());
        }
        return Math.max(MIN_RETAINED_STATE_UNIT, unit);
    }

    int hiddenControlStateRetainedLocked() {
        return this.terminalBookkeeping.hiddenControlStateRetainedLocked();
    }

    int visibleTombstoneRetainedLocked() {
        return this.terminalBookkeeping.visibleTombstoneRetainedLocked();
    }

    int markerOnlyRetainedLocked() {
        return this.terminalBookkeeping.markerOnlyRetainedLocked();
    }

    int markerOnlyRangeCountLocked() {
        return this.terminalBookkeeping.markerOnlyRangeCountLocked();
    }

    private int totalProvisionalCountLocked() {
        return this.localOpenTracker.totalProvisionalCountLocked();
    }

    long trackedRetainedStateMemoryLocked() {
        long retainedUnit = this.retainedStateUnitLocked();
        long total = this.terminalBookkeeping.trackedRetainedStateMemoryLocked();
        total = SessionRuntime.saturatingAdd(
                total,
                SessionRuntime.saturatingMultiply(this.acceptRegistry.pendingAcceptedCountLocked(), retainedUnit)
        );
        total = SessionRuntime.saturatingAdd(total, SessionRuntime.saturatingMultiply(this.totalProvisionalCountLocked(), retainedUnit));
        return total;
    }

    long trackedSessionMemoryLocked() {
        long total = this.sessionReservedSendBytes;
        total = SessionRuntime.saturatingAdd(total, this.bufferedReceiveStorageBytes);
        total = SessionRuntime.saturatingAdd(total, this.outboundQueueBookkeeping.urgentQueuedControlBytesLocked());
        total = SessionRuntime.saturatingAdd(total, this.outboundQueueBookkeeping.ordinaryQueuedControlBytesLocked());
        total = SessionRuntime.saturatingAdd(total, this.outboundQueueBookkeeping.pendingControlBytesLocked());
        total = SessionRuntime.saturatingAdd(total, this.outboundQueueBookkeeping.pendingPriorityBytesLocked());
        total = SessionRuntime.saturatingAdd(total, this.writerHeldRetainedBytes);
        total = SessionRuntime.saturatingAdd(total, this.outstandingPingBytesLocked());
        total = SessionRuntime.saturatingAdd(total, this.retainedOpenInfoBytes);
        total = SessionRuntime.saturatingAdd(total, this.retainedPeerReasonBytes);
        total = SessionRuntime.saturatingAdd(total, this.trackedRetainedStateMemoryLocked());
        return total;
    }

    long projectedTrackedSessionMemoryWithAdditionalLocked(long additional) {
        return SessionRuntime.saturatingAdd(this.trackedSessionMemoryLocked(), additional);
    }

    long sessionMemoryHighThresholdLocked() {
        long hardCap = this.sessionMemoryHardCapLocked();
        if (hardCap <= 4L) {
            return hardCap;
        }
        return hardCap - hardCap / 4L;
    }

    boolean sessionWriteMemoryBlockedLocked(long additional) {
        if (additional <= 0L) {
            return false;
        }
        return this.projectedTrackedSessionMemoryWithAdditionalLocked(additional) > this.sessionMemoryHighThresholdLocked();
    }

    boolean sessionMemoryWakeNeededLocked(long previousTracked) {
        long threshold = this.sessionMemoryHighThresholdLocked();
        return RuntimeFlow.memoryWakeNeeded(previousTracked, this.trackedSessionMemoryLocked(), threshold);
    }

    boolean sessionMemoryPressureHighLocked() {
        return this.trackedSessionMemoryLocked() >= this.sessionMemoryHighThresholdLocked();
    }

    boolean sessionStandingGrowthAllowedLocked() {
        if (this.sessionMemoryPressureHighLocked()) {
            return false;
        }
        long hwm = this.sessionQueuedDataHighWatermarkLocked();
        if (this.bufferedReceiveBytes >= hwm) {
            return false;
        }
        return SessionRuntime.saturatingAdd(this.bufferedReceiveBytes, this.recvSessionPending) < hwm;
    }

    boolean streamStandingGrowthAllowedLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null) {
            return false;
        }
        if (this.sessionMemoryPressureHighLocked()) {
            return false;
        }
        long hwm = this.perStreamQueuedDataHighWatermarkLocked();
        long buffered = streamRuntime.readBufferSizeLocked();
        if (buffered >= hwm) {
            return false;
        }
        return SessionRuntime.saturatingAdd(buffered, streamRuntime.recvPendingLocked()) < hwm;
    }

    private long outstandingPingBytesLocked() {
        return this.telemetry.outstandingPingBytesLocked();
    }

    IOException sessionMemoryCapErrorLocked(String operation) {
        return this.sessionMemoryCapErrorWithAdditionalLocked(operation, 0L);
    }

    private IOException sessionMemoryCapErrorWithAdditionalLocked(String operation, long additional) {
        long tracked = this.projectedTrackedSessionMemoryWithAdditionalLocked(additional);
        long hardCap = this.sessionMemoryHardCapLocked();
        if (tracked <= hardCap) {
            return null;
        }
        return sessionInternalError(
                operation,
                "session memory cap exceeded: tracked=" + tracked + " cap=" + hardCap
        );
    }

    boolean hasTerminalMarkerLocked(long streamId) {
        return this.terminalBookkeeping.hasTerminalMarkerLocked(streamId);
    }

    IOException urgentControlWriterBatchMemoryErrorLocked(long retainedBytes) {
        if (retainedBytes <= 0L) {
            return null;
        }
        return this.sessionMemoryCapErrorWithAdditionalLocked("queue urgent control", retainedBytes);
    }

    void recordProtocolBacklogBlockedLocked() {
        this.outboundQueueBookkeeping.recordProtocolBacklogBlockedLocked();
    }

    private boolean enqueueUrgentFrameLocked(OutboundFrame outboundFrame) throws IOException {
        if (outboundFrame == null) {
            return false;
        }
        TerminalControlMerge terminalMerge = this.planTerminalControlMergeLocked(outboundFrame);
        if (terminalMerge.coalesced()) {
            if (terminalMerge.replacedBytes() > 0L) {
                this.dropReplacedTerminalControlsLocked(outboundFrame);
            }
            if (terminalMerge.superseded()) {
                this.recordSupersededTerminalSignalLocked();
            }
            this.recordCoalescedTerminalSignalLocked();
            return false;
        }
        int bytes = SessionRuntime.retainedQueueBytes(outboundFrame);
        IOException admissionError = terminalMerge.replacedBytes() > 0L
                ? this.urgentQueueReplacementAdmissionErrorLocked(bytes, terminalMerge.replacedBytes())
                : this.urgentQueueAdmissionErrorLocked(bytes);
        if (admissionError != null) {
            this.outboundQueueBookkeeping.recordProtocolBacklogBlockedLocked();
            if (outboundFrame.frame().type() != FrameType.CLOSE) {
                this.failSession(admissionError);
            }
            throw admissionError;
        }
        if (terminalMerge.replacedBytes() > 0L) {
            this.dropReplacedTerminalControlsLocked(outboundFrame);
        }
        if (terminalMerge.superseded()) {
            this.recordSupersededTerminalSignalLocked();
        }
        this.outboundQueueBookkeeping.noteUrgentFrameEnqueuedLocked(bytes);
        this.urgentQueue.offerLast(outboundFrame);
        return true;
    }

    private TerminalControlMerge planTerminalControlMergeLocked(OutboundFrame candidate) {
        if (!SessionRuntime.isTerminalControlFrame(candidate)) {
            return TerminalControlMerge.none();
        }
        boolean coalesced = false;
        boolean superseded = false;
        long replacedBytes = 0L;
        for (OutboundFrame queued : this.urgentQueue) {
            if (!SessionRuntime.sameStreamTerminalControl(queued, candidate)) {
                continue;
            }
            if (SessionRuntime.queuedTerminalControlCoalescesCandidate(queued, candidate)) {
                coalesced = true;
                continue;
            }
            if (!SessionRuntime.shouldReplaceQueuedTerminalControl(queued, candidate)) {
                continue;
            }
            if (candidate.frame().type() == FrameType.ABORT
                    && queued.frame().type() != FrameType.ABORT) {
                superseded = true;
            }
            replacedBytes = SessionRuntime.saturatingAdd(
                    replacedBytes,
                    SessionRuntime.retainedQueueBytes(queued)
            );
        }
        if (!coalesced && !superseded && replacedBytes == 0L) {
            return TerminalControlMerge.none();
        }
        return new TerminalControlMerge(coalesced, superseded, replacedBytes);
    }

    private void dropReplacedTerminalControlsLocked(OutboundFrame candidate) {
        if (!SessionRuntime.isTerminalControlFrame(candidate)) {
            return;
        }
        Iterator<OutboundFrame> iterator = this.urgentQueue.iterator();
        while (iterator.hasNext()) {
            OutboundFrame queued = iterator.next();
            if (!SessionRuntime.sameStreamTerminalControl(queued, candidate)
                    || !SessionRuntime.shouldReplaceQueuedTerminalControl(queued, candidate)) {
                continue;
            }
            iterator.remove();
            this.onUrgentFrameDequeuedLocked(queued);
        }
    }

    private IOException urgentQueueAdmissionErrorLocked(int retainedBytes) {
        if (retainedBytes <= 0) {
            return null;
        }
        IOException memoryError = this.sessionMemoryCapErrorWithAdditionalLocked("queue urgent control", retainedBytes);
        if (memoryError != null) {
            return memoryError;
        }
        long projectedUrgent = SessionRuntime.saturatingAdd(
                this.outboundQueueBookkeeping.urgentQueuedControlBytesLocked(),
                retainedBytes
        );
        long hardCap = this.urgentQueuedBytesHardCapLocked();
        if (projectedUrgent <= hardCap) {
            return null;
        }
        return sessionInternalError(
                "queue urgent control",
                "urgent control queue cap exceeded: queued=" + projectedUrgent + " cap=" + hardCap
        );
    }

    private void enqueueOrdinaryFrameLocked(OutboundFrame outboundFrame) {
        if (outboundFrame == null) {
            return;
        }
        int bytes = SessionRuntime.retainedQueueBytes(outboundFrame);
        this.outboundQueueBookkeeping.noteOrdinaryFrameEnqueuedLocked(bytes);
        this.dataQueue.offerLast(outboundFrame);
    }

    private void onUrgentFrameDequeuedLocked(OutboundFrame outboundFrame) {
        int bytes = SessionRuntime.retainedQueueBytes(outboundFrame);
        this.outboundQueueBookkeeping.noteUrgentFrameDequeuedLocked(bytes);
        this.notifyLockWaitersLocked();
    }

    private void onOrdinaryFrameDequeuedLocked(OutboundFrame outboundFrame) {
        int bytes = SessionRuntime.retainedQueueBytes(outboundFrame);
        this.outboundQueueBookkeeping.noteOrdinaryFrameDequeuedLocked(bytes);
    }

    void onQueuedFrameDequeuedLocked(Deque<OutboundFrame> deque, OutboundFrame outboundFrame) {
        if (deque == this.urgentQueue) {
            this.onUrgentFrameDequeuedLocked(outboundFrame);
        } else if (deque == this.dataQueue) {
            this.onOrdinaryFrameDequeuedLocked(outboundFrame);
        }
    }

    void retainWriterHeldFrameLocked(OutboundFrame outboundFrame) {
        int bytes = SessionRuntime.retainedQueueBytes(outboundFrame);
        if (bytes <= 0) {
            return;
        }
        this.writerHeldRetainedBytes = SessionRuntime.saturatingAdd(this.writerHeldRetainedBytes, bytes);
    }

    void retainWriterHeldFramesLocked(List<OutboundFrame> outboundFrames) {
        long bytes = SessionRuntime.retainedBatchBytes(outboundFrames);
        if (bytes <= 0L) {
            return;
        }
        this.writerHeldRetainedBytes = SessionRuntime.saturatingAdd(this.writerHeldRetainedBytes, bytes);
    }

    void releaseWriterHeldFrameLocked(OutboundFrame outboundFrame) {
        int bytes = SessionRuntime.retainedQueueBytes(outboundFrame);
        if (bytes <= 0) {
            return;
        }
        long previousTracked = this.trackedSessionMemoryLocked();
        this.writerHeldRetainedBytes = Math.max(0L, this.writerHeldRetainedBytes - bytes);
        if (this.sessionMemoryWakeNeededLocked(previousTracked)) {
            this.notifyStreamWriteWaitersLocked();
        }
    }

    private void releaseWriterHeldFramesLocked(List<OutboundFrame> outboundFrames) {
        long bytes = SessionRuntime.retainedBatchBytes(outboundFrames);
        if (bytes <= 0L) {
            return;
        }
        long previousTracked = this.trackedSessionMemoryLocked();
        this.writerHeldRetainedBytes = Math.max(0L, this.writerHeldRetainedBytes - bytes);
        if (this.sessionMemoryWakeNeededLocked(previousTracked)) {
            this.notifyStreamWriteWaitersLocked();
        }
    }

    void addBatchFrameLocked(List<OutboundFrame> list, OutboundFrame outboundFrame, boolean trackWriterHeld) {
        if (outboundFrame == null) {
            return;
        }
        list.add(outboundFrame);
        if (trackWriterHeld) {
            this.retainWriterHeldFrameLocked(outboundFrame);
        }
    }

    boolean replacePendingControlBytesLocked(long oldBytes, long newBytes) {
        return this.outboundQueueBookkeeping.replacePendingControlBytesLocked(oldBytes, newBytes);
    }

    SessionOutboundQueueBookkeeping.PendingPriorityReplaceResult replacePendingPriorityBytesLocked(long oldBytes, long newBytes) {
        return this.outboundQueueBookkeeping.replacePendingPriorityBytesLocked(oldBytes, newBytes);
    }

    long pendingControlValueBytesLocked(long value) {
        try {
            return Varint62.length(value);
        } catch (ZmuxException e) {
            throw internalIoFailure("compute pending control value bytes", e);
        }
    }

    long pendingControlFrameBytesLocked(long streamId, long value) {
        long bytes = this.pendingControlValueBytesLocked(value);
        if (streamId != 0L) {
            bytes = SessionRuntime.saturatingAdd(bytes, this.pendingControlValueBytesLocked(streamId));
        }
        return bytes;
    }

    void releasePendingControlBytesLocked(long bytes) {
        this.outboundQueueBookkeeping.releasePendingControlBytesAndNotifyLocked(bytes);
    }

    void releasePendingControlBytesForHandoffLocked(long bytes) {
        this.outboundQueueBookkeeping.releasePendingControlBytesLocked(bytes);
    }

    void onStreamOpenInfoUpdatedLocked(int previousLength, int currentLength) {
        if (currentLength > previousLength) {
            this.retainedOpenInfoBytes = SessionRuntime.saturatingAdd(
                    this.retainedOpenInfoBytes,
                    (long) currentLength - previousLength
            );
            return;
        }
        if (previousLength > currentLength) {
            this.retainedOpenInfoBytes = Math.max(
                    0L,
                    this.retainedOpenInfoBytes - ((long) previousLength - currentLength)
            );
        }
    }

    void releaseStreamPeerReasonBudgetLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null) {
            return;
        }
        long total = SessionRuntime.saturatingAdd(SessionRuntime.saturatingAdd(streamRuntime.sendStopReasonBytesLocked(), streamRuntime.recvResetReasonBytesLocked()), streamRuntime.recvAbortReasonBytesLocked());
        this.releasePeerReasonBytesLocked(total);
        streamRuntime.clearRetainedPeerReasonBytesLocked();
    }

    void releasePeerReasonBytesLocked(long bytes) {
        if (bytes <= 0L) {
            return;
        }
        this.retainedPeerReasonBytes = Math.max(0L, this.retainedPeerReasonBytes - bytes);
    }

    String retainPeerReasonLocked(long previousBytes, String reason) {
        int codePoint;
        int width;
        this.releasePeerReasonBytesLocked(previousBytes);
        if (reason == null || reason.isEmpty()) {
            return "";
        }
        long budget = this.retainedPeerReasonBudgetLocked();
        if (budget <= 0L || this.retainedPeerReasonBytes >= budget) {
            return "";
        }
        long available = budget - this.retainedPeerReasonBytes;
        if (available <= 0L) {
            return "";
        }
        int originalBytes = reason.getBytes(StandardCharsets.UTF_8).length;
        if ((long) originalBytes <= available) {
            this.retainedPeerReasonBytes = SessionRuntime.saturatingAdd(this.retainedPeerReasonBytes, originalBytes);
            return reason;
        }
        int end = 0;
        int remaining = (int) available;
        while (end < reason.length()) {
            codePoint = reason.codePointAt(end);
            width = new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8).length;
            if (width > remaining) {
                break;
            }
            remaining -= width;
            end += Character.charCount(codePoint);
        }
        String trimmed = reason.substring(0, end);
        this.retainedPeerReasonBytes = SessionRuntime.saturatingAdd(this.retainedPeerReasonBytes, trimmed.getBytes(StandardCharsets.UTF_8).length);
        return trimmed;
    }

    private long abuseWindowNanosLocked() {
        long configured = SessionRuntime.positiveNanos(this.config.abuseWindow());
        return configured > 0L ? configured : DEFAULT_ABUSE_WINDOW_NANOS;
    }

    private boolean abuseWindowExpiredLocked(long startedAtNanos, long nowNanos) {
        return startedAtNanos == 0L || RuntimeFlow.elapsedExceeds(
                nowNanos,
                startedAtNanos,
                this.abuseWindowNanosLocked()
        );
    }

    private int inboundControlFrameBudgetLocked() {
        int configured = this.config.inboundControlFrameBudget();
        return configured > 0 ? configured : DEFAULT_INBOUND_CONTROL_FRAME_BUDGET;
    }

    private long inboundControlBytesBudgetLocked() {
        long configured = this.config.inboundControlBytesBudget();
        if (configured > 0L) {
            return configured;
        }
        long maxPayload = this.localSettings().maxControlPayloadBytes();
        if (maxPayload <= 0L) {
            maxPayload = Settings.defaults().maxControlPayloadBytes();
        }
        return Math.max(MIN_INBOUND_CONTROL_BYTES_BUDGET, SessionRuntime.saturatingMultiply(maxPayload, 64L));
    }

    private int inboundExtFrameBudgetLocked() {
        int configured = this.config.inboundExtFrameBudget();
        return configured > 0 ? configured : DEFAULT_INBOUND_EXT_FRAME_BUDGET;
    }

    private long inboundExtBytesBudgetLocked() {
        long configured = this.config.inboundExtBytesBudget();
        if (configured > 0L) {
            return configured;
        }
        long maxPayload = this.localSettings().maxExtensionPayloadBytes();
        if (maxPayload <= 0L) {
            maxPayload = Settings.defaults().maxExtensionPayloadBytes();
        }
        return Math.max(MIN_INBOUND_EXT_BYTES_BUDGET, SessionRuntime.saturatingMultiply(maxPayload, 64L));
    }

    private int inboundMixedFrameBudgetLocked() {
        int configured = this.config.inboundMixedFrameBudget();
        return configured > 0
                ? configured
                : Math.max(this.inboundControlFrameBudgetLocked(), this.inboundExtFrameBudgetLocked());
    }

    private long inboundMixedBytesBudgetLocked() {
        long configured = this.config.inboundMixedBytesBudget();
        return configured > 0L
                ? configured
                : Math.max(this.inboundControlBytesBudgetLocked(), this.inboundExtBytesBudgetLocked());
    }

    private int noOpControlFloodThresholdLocked() {
        int configured = this.config.noOpControlFloodThreshold();
        return configured > 0 ? configured : DEFAULT_NO_OP_CONTROL_FLOOD_THRESHOLD;
    }

    private int noOpMaxDataFloodThresholdLocked() {
        int configured = this.config.noOpMaxDataFloodThreshold();
        return configured > 0 ? configured : DEFAULT_NO_OP_MAX_DATA_FLOOD_THRESHOLD;
    }

    private int noOpBlockedFloodThresholdLocked() {
        int configured = this.config.noOpBlockedFloodThreshold();
        return configured > 0 ? configured : DEFAULT_NO_OP_BLOCKED_FLOOD_THRESHOLD;
    }

    private int noOpZeroDataFloodThresholdLocked() {
        int configured = this.config.noOpZeroDataFloodThreshold();
        return configured > 0 ? configured : DEFAULT_NO_OP_ZERO_DATA_FLOOD_THRESHOLD;
    }

    private int noOpPriorityUpdateFloodThresholdLocked() {
        int configured = this.config.noOpPriorityUpdateFloodThreshold();
        return configured > 0 ? configured : DEFAULT_NO_OP_PRIORITY_UPDATE_FLOOD_THRESHOLD;
    }

    private int inboundPingFloodThresholdLocked() {
        int configured = this.config.inboundPingFloodThreshold();
        return configured > 0 ? configured : DEFAULT_INBOUND_PING_FLOOD_THRESHOLD;
    }

    private int groupRebucketChurnThresholdLocked() {
        int configured = this.config.groupRebucketChurnThreshold();
        return configured > 0 ? configured : DEFAULT_GROUP_REBUCKET_CHURN_THRESHOLD;
    }

    private long hiddenAbortChurnWindowNanosLocked() {
        long configured = SessionRuntime.positiveNanos(this.config.hiddenAbortChurnWindow());
        return configured > 0L ? configured : DEFAULT_HIDDEN_ABORT_CHURN_WINDOW_NANOS;
    }

    private int hiddenAbortChurnThresholdLocked() {
        int configured = this.config.hiddenAbortChurnThreshold();
        return configured > 0 ? configured : DEFAULT_HIDDEN_ABORT_CHURN_THRESHOLD;
    }

    private long visibleTerminalChurnWindowNanosLocked() {
        long configured = SessionRuntime.positiveNanos(this.config.visibleTerminalChurnWindow());
        return configured > 0L ? configured : DEFAULT_VISIBLE_TERMINAL_CHURN_WINDOW_NANOS;
    }

    private int visibleTerminalChurnThresholdLocked() {
        int configured = this.config.visibleTerminalChurnThreshold();
        return configured > 0 ? configured : DEFAULT_VISIBLE_TERMINAL_CHURN_THRESHOLD;
    }

    void recordInboundBudgetsLocked(FrameEnvelopeCodec.InboundFrame frame) throws IOException {
        recordInboundBudgetsLocked(frame.type(), frame.payload().length);
    }

    private void recordInboundBudgetsLocked(FrameType frameType, int payloadBytes) throws IOException {
        if (frameType == FrameType.CLOSE) {
            if (this.ignorePeerCloseFrameLocked()) {
                return;
            }
        } else if (this.ignorePeerNonCloseFrameLocked(frameType)) {
            return;
        }
        long nowNanos = System.nanoTime();
        boolean control = frameType != FrameType.DATA && frameType != FrameType.EXT;
        boolean ext = frameType == FrameType.EXT;
        if (control) {
            if (this.abuseWindowExpiredLocked(this.inboundControlBudgetWindowStartedAtNanos, nowNanos)) {
                this.inboundControlBudgetWindowStartedAtNanos = nowNanos;
                this.inboundControlFrameCount = 0;
                this.inboundControlBytes = 0L;
            }
            this.inboundControlFrameCount = SessionRuntime.saturatingIncrement(this.inboundControlFrameCount);
            this.inboundControlBytes = SessionRuntime.saturatingAdd(this.inboundControlBytes, payloadBytes);
            if (this.inboundControlFrameCount > this.inboundControlFrameBudgetLocked()) {
                throw sessionError(
                        ErrorCode.PROTOCOL,
                        "handle " + frameType,
                        "inbound control-frame budget exceeded",
                        ZmuxErrorSource.REMOTE,
                        ZmuxErrorDirection.READ
                );
            }
            if (this.inboundControlBytes > this.inboundControlBytesBudgetLocked()) {
                throw sessionError(
                        ErrorCode.PROTOCOL,
                        "handle " + frameType,
                        "inbound control-byte budget exceeded",
                        ZmuxErrorSource.REMOTE,
                        ZmuxErrorDirection.READ
                );
            }
        }
        if (ext) {
            if (this.abuseWindowExpiredLocked(this.inboundExtBudgetWindowStartedAtNanos, nowNanos)) {
                this.inboundExtBudgetWindowStartedAtNanos = nowNanos;
                this.inboundExtFrameCount = 0;
                this.inboundExtBytes = 0L;
            }
            this.inboundExtFrameCount = SessionRuntime.saturatingIncrement(this.inboundExtFrameCount);
            this.inboundExtBytes = SessionRuntime.saturatingAdd(this.inboundExtBytes, payloadBytes);
            if (this.inboundExtFrameCount > this.inboundExtFrameBudgetLocked()) {
                throw sessionError(
                        ErrorCode.PROTOCOL,
                        "handle EXT",
                        "inbound ext-frame budget exceeded",
                        ZmuxErrorSource.REMOTE,
                        ZmuxErrorDirection.READ
                );
            }
            if (this.inboundExtBytes > this.inboundExtBytesBudgetLocked()) {
                throw sessionError(
                        ErrorCode.PROTOCOL,
                        "handle EXT",
                        "inbound ext-byte budget exceeded",
                        ZmuxErrorSource.REMOTE,
                        ZmuxErrorDirection.READ
                );
            }
        }
        if (control || ext) {
            if (this.abuseWindowExpiredLocked(this.inboundMixedBudgetWindowStartedAtNanos, nowNanos)) {
                this.inboundMixedBudgetWindowStartedAtNanos = nowNanos;
                this.inboundMixedFrameCount = 0;
                this.inboundMixedBytes = 0L;
            }
            this.inboundMixedFrameCount = SessionRuntime.saturatingIncrement(this.inboundMixedFrameCount);
            this.inboundMixedBytes = SessionRuntime.saturatingAdd(this.inboundMixedBytes, payloadBytes);
            if (this.inboundMixedFrameCount > this.inboundMixedFrameBudgetLocked()) {
                throw sessionError(
                        ErrorCode.PROTOCOL,
                        "handle " + frameType,
                        "inbound mixed-frame budget exceeded",
                        ZmuxErrorSource.REMOTE,
                        ZmuxErrorDirection.READ
                );
            }
            if (this.inboundMixedBytes > this.inboundMixedBytesBudgetLocked()) {
                throw sessionError(
                        ErrorCode.PROTOCOL,
                        "handle " + frameType,
                        "inbound mixed-byte budget exceeded",
                        ZmuxErrorSource.REMOTE,
                        ZmuxErrorDirection.READ
                );
            }
        }
    }

    void recordInboundPingFloodLocked() throws IOException {
        int threshold = this.inboundPingFloodThresholdLocked();
        long nowNanos = System.nanoTime();
        if (this.abuseWindowExpiredLocked(this.inboundPingFloodWindowStartedAtNanos, nowNanos)) {
            this.inboundPingFloodWindowStartedAtNanos = nowNanos;
            this.inboundPingFloodCount = 0;
        }
        this.inboundPingFloodCount = SessionRuntime.saturatingIncrement(this.inboundPingFloodCount);
        if (this.inboundPingFloodCount > threshold) {
            throw sessionError(
                    ErrorCode.PROTOCOL,
                    "handle PING",
                    "inbound PING flood",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ
            );
        }
    }

    void recordNoOpControlLocked(String description) throws IOException {
        long nowNanos = System.nanoTime();
        if (this.abuseWindowExpiredLocked(this.noOpControlWindowStartedAtNanos, nowNanos)) {
            this.noOpControlWindowStartedAtNanos = nowNanos;
            this.noOpControlCount = 0;
        }
        this.noOpControlCount = SessionRuntime.saturatingIncrement(this.noOpControlCount);
        if (this.noOpControlCount > this.noOpControlFloodThresholdLocked()) {
            throw sessionError(
                    ErrorCode.PROTOCOL,
                    description,
                    "no-op control flood",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ
            );
        }
    }

    private void clearNoOpControlBudgetsLocked() {
        this.noOpControlWindowStartedAtNanos = 0L;
        this.noOpControlCount = 0;
        this.noOpMaxDataWindowStartedAtNanos = 0L;
        this.noOpMaxDataCount = 0;
        this.noOpBlockedWindowStartedAtNanos = 0L;
        this.noOpBlockedCount = 0;
        this.noOpPriorityUpdateWindowStartedAtNanos = 0L;
        this.noOpPriorityUpdateCount = 0;
    }

    void clearNoOpControlLocked() {
        this.clearNoOpControlBudgetsLocked();
    }

    void recordNoOpMaxDataLocked() throws IOException {
        this.recordNoOpControlLocked("handle MAX_DATA");
        long nowNanos = System.nanoTime();
        if (this.abuseWindowExpiredLocked(this.noOpMaxDataWindowStartedAtNanos, nowNanos)) {
            this.noOpMaxDataWindowStartedAtNanos = nowNanos;
            this.noOpMaxDataCount = 0;
        }
        this.noOpMaxDataCount = SessionRuntime.saturatingIncrement(this.noOpMaxDataCount);
        if (this.noOpMaxDataCount > this.noOpMaxDataFloodThresholdLocked()) {
            throw sessionError(
                    ErrorCode.PROTOCOL,
                    "handle MAX_DATA",
                    "no-op max_data flood",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ
            );
        }
    }

    void clearNoOpMaxDataLocked() {
        this.clearNoOpControlBudgetsLocked();
    }

    void recordNoOpBlockedLocked() throws IOException {
        this.recordNoOpControlLocked("handle BLOCKED");
        long nowNanos = System.nanoTime();
        if (this.abuseWindowExpiredLocked(this.noOpBlockedWindowStartedAtNanos, nowNanos)) {
            this.noOpBlockedWindowStartedAtNanos = nowNanos;
            this.noOpBlockedCount = 0;
        }
        this.noOpBlockedCount = SessionRuntime.saturatingIncrement(this.noOpBlockedCount);
        if (this.noOpBlockedCount > this.noOpBlockedFloodThresholdLocked()) {
            throw sessionError(
                    ErrorCode.PROTOCOL,
                    "handle BLOCKED",
                    "no-op blocked flood",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ
            );
        }
    }

    void clearNoOpBlockedLocked() {
        this.clearNoOpControlBudgetsLocked();
    }

    void recordNoOpZeroDataLocked() throws IOException {
        long nowNanos = System.nanoTime();
        if (this.abuseWindowExpiredLocked(this.noOpZeroDataWindowStartedAtNanos, nowNanos)) {
            this.noOpZeroDataWindowStartedAtNanos = nowNanos;
            this.noOpZeroDataCount = 0;
        }
        this.noOpZeroDataCount = SessionRuntime.saturatingIncrement(this.noOpZeroDataCount);
        if (this.noOpZeroDataCount > this.noOpZeroDataFloodThresholdLocked()) {
            throw sessionError(
                    ErrorCode.PROTOCOL,
                    "handle DATA",
                    "no-op zero-length data flood",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ
            );
        }
    }

    void clearNoOpZeroDataLocked() {
        this.noOpZeroDataWindowStartedAtNanos = 0L;
        this.noOpZeroDataCount = 0;
    }

    void recordNoOpPriorityUpdateLocked() throws IOException {
        this.recordNoOpControlLocked("handle PRIORITY_UPDATE");
        long nowNanos = System.nanoTime();
        if (this.abuseWindowExpiredLocked(this.noOpPriorityUpdateWindowStartedAtNanos, nowNanos)) {
            this.noOpPriorityUpdateWindowStartedAtNanos = nowNanos;
            this.noOpPriorityUpdateCount = 0;
        }
        this.noOpPriorityUpdateCount = SessionRuntime.saturatingIncrement(this.noOpPriorityUpdateCount);
        if (this.noOpPriorityUpdateCount > this.noOpPriorityUpdateFloodThresholdLocked()) {
            throw sessionError(
                    ErrorCode.PROTOCOL,
                    "handle PRIORITY_UPDATE",
                    "no-op priority_update flood",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ
            );
        }
    }

    void clearNoOpPriorityUpdateLocked() {
        this.clearNoOpControlBudgetsLocked();
    }

    void recordDroppedPriorityUpdateLocked() {
        this.droppedPriorityUpdateCount = SessionRuntime.saturatingAdd(this.droppedPriorityUpdateCount, 1L);
    }

    void recordDroppedLocalPriorityUpdateLocked() {
        this.droppedLocalPriorityUpdateCount = SessionRuntime.saturatingAdd(this.droppedLocalPriorityUpdateCount, 1L);
    }

    private void recordCoalescedTerminalSignalLocked() {
        this.coalescedTerminalSignalsCount = SessionRuntime.saturatingAdd(this.coalescedTerminalSignalsCount, 1L);
    }

    private void recordSupersededTerminalSignalLocked() {
        this.supersededTerminalSignalsCount = SessionRuntime.saturatingAdd(this.supersededTerminalSignalsCount, 1L);
    }

    void recordSkippedCloseOnDeadIOLocked() {
        this.skippedCloseOnDeadIoCount = SessionRuntime.saturatingAdd(this.skippedCloseOnDeadIoCount, 1L);
    }

    void recordCloseFrameFlushErrorLocked() {
        this.closeFrameFlushErrorCount = SessionRuntime.saturatingAdd(this.closeFrameFlushErrorCount, 1L);
    }

    void recordCloseCompletionTimeoutLocked() {
        this.telemetry.recordCloseCompletionTimeoutLocked();
    }

    private void recordGracefulCloseTimeoutLocked() {
        this.telemetry.recordGracefulCloseTimeoutLocked();
    }

    private void recordKeepaliveTimeoutLocked() {
        this.telemetry.recordKeepaliveTimeoutLocked();
    }

    private void recordVisibleTerminalChurnEventLocked() {
        this.visibleTerminalChurnEventCount = SessionRuntime.saturatingAdd(this.visibleTerminalChurnEventCount, 1L);
    }

    private void recordGroupRebucketEventLocked() {
        this.groupRebucketEventCount = SessionRuntime.saturatingAdd(this.groupRebucketEventCount, 1L);
    }

    void noteResetReasonLocked(long code) {
        this.resetReasonOverflowCount = this.noteReasonLocked(
                this.resetReasonCounts,
                this.resetReasonOverflowCount,
                code
        );
    }

    void noteAbortReasonLocked(long code) {
        this.abortReasonOverflowCount = this.noteReasonLocked(
                this.abortReasonCounts,
                this.abortReasonOverflowCount,
                code
        );
    }

    private long noteReasonLocked(Map<Long, Long> reasons, long overflowCount, long code) {
        if (reasons == null) {
            return SessionRuntime.saturatingAdd(overflowCount, 1L);
        }
        Long current = reasons.get(code);
        if (current != null || reasons.size() < SessionStats.ReasonStats.MAX_TRACKED_CODES) {
            reasons.put(code, SessionRuntime.saturatingAdd(current == null ? 0L : current, 1L));
            return overflowCount;
        }
        return SessionRuntime.saturatingAdd(overflowCount, 1L);
    }

    void noteLateDataDiscardLocked(int length, LateDataCause cause) {
        if (length <= 0 || cause == null || cause == LateDataCause.NONE) {
            return;
        }
        switch (cause) {
            case CLOSE_READ:
                this.lateDataAfterCloseReadBytes = SessionRuntime.saturatingAdd(this.lateDataAfterCloseReadBytes, length);
                break;
            case RESET:
                this.lateDataAfterResetBytes = SessionRuntime.saturatingAdd(this.lateDataAfterResetBytes, length);
                break;
            case ABORT:
                this.lateDataAfterAbortBytes = SessionRuntime.saturatingAdd(this.lateDataAfterAbortBytes, length);
                break;
            default:
                break;
        }
    }

    void onHiddenUnreadBytesDiscardedLocked(long bytes) {
        if (bytes <= 0) {
            return;
        }
        this.hiddenUnreadBytesDiscarded = SessionRuntime.saturatingAdd(this.hiddenUnreadBytesDiscarded, bytes);
    }

    private void noteHiddenStreamRefusedLocked() {
        this.hiddenStreamsRefused = SessionRuntime.saturatingAdd(this.hiddenStreamsRefused, 1L);
    }

    private void noteHiddenStreamReapedLocked() {
        this.hiddenStreamsReaped = SessionRuntime.saturatingAdd(this.hiddenStreamsReaped, 1L);
    }

    void recordProvisionalOpenLimitedLocked() {
        this.provisionalOpenLimitedCount = SessionRuntime.saturatingAdd(this.provisionalOpenLimitedCount, 1L);
    }

    void recordProvisionalOpenExpiredLocked() {
        this.provisionalOpenExpiredCount = SessionRuntime.saturatingAdd(this.provisionalOpenExpiredCount, 1L);
    }

    void recordHiddenAbortChurnLocked(long nowNanos) throws IOException {
        long effectiveNow = nowNanos > 0L ? nowNanos : System.nanoTime();
        if (this.hiddenAbortChurnWindowStartedAtNanos == 0L
                || RuntimeFlow.elapsedExceeds(
                effectiveNow,
                this.hiddenAbortChurnWindowStartedAtNanos,
                this.hiddenAbortChurnWindowNanosLocked()
        )) {
            this.hiddenAbortChurnWindowStartedAtNanos = effectiveNow;
            this.hiddenAbortChurnCount = 0;
        }
        this.hiddenAbortChurnCount = SessionRuntime.saturatingIncrement(this.hiddenAbortChurnCount);
        if (this.hiddenAbortChurnCount > this.hiddenAbortChurnThresholdLocked()) {
            throw sessionError(
                    ErrorCode.PROTOCOL,
                    "handle ABORT",
                    "hidden abort churn exceeded",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ
            );
        }
    }

    void recordVisibleTerminalChurnLocked(StreamRuntime streamRuntime) throws IOException {
        if (!this.shouldRecordVisibleTerminalChurnLocked(streamRuntime)) {
            return;
        }
        streamRuntime.markChurnCountedLocked();
        long nowNanos = System.nanoTime();
        this.recordVisibleTerminalChurnEventLocked();
        if (this.visibleTerminalChurnWindowStartedAtNanos == 0L
                || RuntimeFlow.elapsedExceeds(
                nowNanos,
                this.visibleTerminalChurnWindowStartedAtNanos,
                this.visibleTerminalChurnWindowNanosLocked()
        )) {
            this.visibleTerminalChurnWindowStartedAtNanos = nowNanos;
            this.visibleTerminalChurnCount = 0;
        }
        this.visibleTerminalChurnCount = SessionRuntime.saturatingIncrement(this.visibleTerminalChurnCount);
        if (this.visibleTerminalChurnCount > this.visibleTerminalChurnThresholdLocked()) {
            throw sessionError(
                    ErrorCode.PROTOCOL,
                    "handle terminal churn",
                    "visible terminal churn exceeded",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ
            );
        }
    }

    private boolean shouldRecordVisibleTerminalChurnLocked(StreamRuntime streamRuntime) {
        return streamRuntime != null
                && !streamRuntime.openedLocally()
                && streamRuntime.applicationVisible()
                && !streamRuntime.acceptedLocked()
                && !streamRuntime.churnCountedLocked()
                && streamRuntime.effectivelyFullyTerminalLocked();
    }

    void recordGroupRebucketLocked() throws IOException {
        long nowNanos = System.nanoTime();
        this.recordGroupRebucketEventLocked();
        if (this.abuseWindowExpiredLocked(this.groupRebucketWindowStartedAtNanos, nowNanos)) {
            this.groupRebucketWindowStartedAtNanos = nowNanos;
            this.groupRebucketCount = 0;
        }
        this.groupRebucketCount = SessionRuntime.saturatingIncrement(this.groupRebucketCount);
        if (this.groupRebucketCount > this.groupRebucketChurnThresholdLocked()) {
            throw sessionError(
                    ErrorCode.PROTOCOL,
                    "handle EXT",
                    "group rebucket churn exceeded",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ
            );
        }
    }

    void clearBlockedFrameLocked(long streamId) {
        this.flowControlUpdateRegistry.clearBlockedFrameLocked(streamId);
    }

    void moveOpeningFrameToUrgentLocked(StreamRuntime streamRuntime) {
        this.openingCoordinator.moveOpeningFrameToUrgentLocked(streamRuntime);
    }

    private StreamRuntime acceptRuntimeFromQueue(boolean bidirectional, Duration timeout) throws IOException, InterruptedException {
        TimeoutBudget budget = TimeoutBudget.fromTimeout(timeout);
        if (budget.expired()) {
            throw new AcceptTimeoutException();
        }
        synchronized (this.lock) {
            while (true) {
                StreamRuntime streamRuntime = this.acceptRegistry.pollAcceptedHeadLocked(bidirectional);
                if (streamRuntime != null) {
                    this.streamBookkeeping.recordAcceptedStreamLocked();
                    this.enqueueStreamEventLocked(streamRuntime, ZmuxEventType.STREAM_ACCEPTED, null);
                    return streamRuntime;
                }
                if (this.shouldFailSessionOperationsLocked()) {
                    throw this.sessionOperationErrorLocked("accept", this.currentErrorLocked());
                }
                if (!budget.bounded()) {
                    try {
                        this.waitOnLock(LockWaitKind.ACCEPT);
                    } catch (InterruptedException interrupted) {
                        throw interrupted(
                                "zmux: interrupted while accepting stream",
                                "accept",
                                ZmuxErrorScope.SESSION,
                                ZmuxErrorDirection.BOTH,
                                interrupted
                        );
                    }
                    continue;
                }
                long remainingNanos = budget.remainingNanos();
                if (remainingNanos <= 0L) {
                    throw new AcceptTimeoutException();
                }
                try {
                    this.waitOnLockNanos(remainingNanos, LockWaitKind.ACCEPT);
                } catch (InterruptedException interrupted) {
                    throw interrupted(
                            "zmux: interrupted while accepting stream",
                            "accept",
                            ZmuxErrorScope.SESSION,
                            ZmuxErrorDirection.BOTH,
                            interrupted
                    );
                }
            }
        }
    }

    private StreamRuntime newLocalStreamLocked(boolean bidirectional, OpenOptions openOptions) throws IOException {
        return this.openingCoordinator.newLocalStreamLocked(bidirectional, openOptions);
    }

    private StreamRuntime newLocalStreamLocked(boolean bidirectional, OpenOptions openOptions, TimeoutBudget budget)
            throws IOException {
        return this.openingCoordinator.newLocalStreamLocked(bidirectional, openOptions, budget);
    }

    IOException sessionErrorLocked() {
        return this.lifecycleRuntime.sessionErrorLocked();
    }

    IOException localClosingErrorLocked() {
        return this.lifecycleRuntime.localClosingErrorLocked();
    }

    IOException currentErrorLocked() {
        return this.lifecycleRuntime.currentErrorLocked();
    }

    IOException sessionOperationErrorLocked(String operation, IOException error) {
        return SessionLifecycleCoordinator.sessionOperationError(operation, error);
    }

    private void readerLoop() {
        this.readerRuntime.run();
    }

    private void writerLoop() {
        this.writerRuntime.run();
    }

    private List<OutboundFrame> collectReadyBatchLocked() throws IOException {
        return this.writerRuntime.collectReadyBatchLocked();
    }

    private ReadyBatch collectReadyBatchStateLocked(boolean orderOrdinary, boolean trackWriterHeld) throws IOException {
        return this.writerRuntime.collectReadyBatchStateLocked(orderOrdinary, trackWriterHeld);
    }

    private List<OutboundFrame> finishOrdinaryBatchLocked(List<OutboundFrame> batch, long batchCost, long costLimit)
            throws IOException, InterruptedException {
        return this.writerRuntime.finishOrdinaryBatchLocked(batch, batchCost, costLimit);
    }

    void enqueueQueuedOutboundLocked(Deque<OutboundFrame> deque, OutboundFrame outboundFrame) throws IOException {
        if (outboundFrame == null) {
            return;
        }
        this.trackQueuedDataAddedLocked(outboundFrame);
        try {
            this.enqueueExistingOutboundLocked(deque, outboundFrame);
        } catch (IOException error) {
            this.trackQueuedDataRemovedLocked(outboundFrame);
            throw error;
        }
    }

    void enqueueExistingOutboundLocked(Deque<OutboundFrame> deque, OutboundFrame outboundFrame) throws IOException {
        if (outboundFrame == null) {
            return;
        }
        if (deque == this.urgentQueue) {
            if (this.enqueueUrgentFrameLocked(outboundFrame)) {
                this.noteOutboundQueuedLocked();
            }
            return;
        }
        if (deque == this.dataQueue) {
            this.enqueueOrdinaryFrameLocked(outboundFrame);
            this.noteOutboundQueuedLocked();
            return;
        }
        deque.offerLast(outboundFrame);
        this.noteOutboundQueuedLocked();
    }

    boolean canAdmitUrgentOutboundLocked(OutboundFrame outboundFrame) {
        return outboundFrame == null
                || this.urgentQueueAdmissionErrorLocked(SessionRuntime.retainedQueueBytes(outboundFrame)) == null;
    }

    void enqueueAdmittedExistingUrgentOutboundLocked(OutboundFrame outboundFrame) {
        if (outboundFrame == null) {
            return;
        }
        int bytes = SessionRuntime.retainedQueueBytes(outboundFrame);
        this.outboundQueueBookkeeping.noteUrgentFrameEnqueuedLocked(bytes);
        this.urgentQueue.offerLast(outboundFrame);
        this.noteOutboundQueuedLocked();
    }

    void enqueueExistingOrdinaryOutboundLocked(OutboundFrame outboundFrame) {
        if (outboundFrame == null) {
            return;
        }
        this.enqueueOrdinaryFrameLocked(outboundFrame);
        this.noteOutboundQueuedLocked();
    }

    private void noteOutboundQueuedLocked() {
        if (!this.telemetry.keepaliveEnabledLocked()) {
            return;
        }
        this.telemetry.resetWriteIdlePingDueLocked(System.nanoTime());
    }

    OutboundFrame pollQueuedOutboundLocked(Deque<OutboundFrame> deque) {
        OutboundFrame outboundFrame = deque.pollFirst();
        if (deque == this.urgentQueue) {
            this.onUrgentFrameDequeuedLocked(outboundFrame);
        } else if (deque == this.dataQueue) {
            this.onOrdinaryFrameDequeuedLocked(outboundFrame);
        }
        return outboundFrame;
    }

    private void trackQueuedDataAddedLocked(OutboundFrame outboundFrame) {
        if (!SessionRuntime.countsQueuedData(outboundFrame)) {
            return;
        }
        this.sessionQueuedDataBytes = SessionRuntime.saturatingAdd(this.sessionQueuedDataBytes, outboundFrame.dataBytes);
        outboundFrame.stream.reserveQueuedDataBytesLocked(outboundFrame.dataBytes);
    }

    private void trackQueuedDataRemovedLocked(OutboundFrame outboundFrame) {
        if (!SessionRuntime.countsQueuedData(outboundFrame)) {
            return;
        }
        this.releaseQueuedDataAccountingLocked(outboundFrame.stream, outboundFrame.dataBytes);
    }

    void releaseQueuedDataAccountingLocked(StreamRuntime streamRuntime, int releasedBytes) {
        if (streamRuntime == null || releasedBytes <= 0) {
            return;
        }
        long previousSession = this.sessionQueuedDataBytes;
        long previousStream = streamRuntime.queuedDataBytesLocked();
        this.sessionQueuedDataBytes = Math.max(0L, this.sessionQueuedDataBytes - (long) releasedBytes);
        streamRuntime.releaseQueuedDataBytesLocked(releasedBytes);
        long sessionLowWatermark = this.sessionQueuedDataLowWatermarkLocked();
        long streamLowWatermark = this.perStreamQueuedDataLowWatermarkLocked();
        if (RuntimeFlow.crossedLowWatermark(previousSession, this.sessionQueuedDataBytes, sessionLowWatermark)
                || RuntimeFlow.crossedLowWatermark(previousStream, streamRuntime.queuedDataBytesLocked(), streamLowWatermark)) {
            this.notifyStreamWriteWaitersLocked();
        }
    }

    void failSession(IOException error) {
        this.lifecycleRuntime.failSession(error);
    }

    void failSessionAsync(IOException error) {
        if (error == null) {
            return;
        }
        CompletableFuture.runAsync(() -> this.failSession(error));
    }

    void finishSessionLocked(IOException error, SessionState sessionState) {
        this.lifecycleRuntime.finishSessionLocked(error, sessionState);
    }

    private void beginSessionTerminationLocked(IOException error, SessionState sessionState) {
        this.lifecycleRuntime.beginSessionTerminationLocked(error, sessionState);
    }

    void closeTransport() {
        this.lifecycleRuntime.closeTransport();
    }

    void enqueueControlLocked(FrameCodec.Frame frame) throws IOException {
        this.enqueueQueuedOutboundLocked(this.urgentQueue, new OutboundFrame(frame, null, 0, false, false));
    }

    private void enqueueReadLoopProtocolFrameLocked(OutboundFrame outboundFrame, boolean droppable) throws IOException {
        if (outboundFrame == null || this.ignorePeerNonCloseFrameLocked(outboundFrame.frame().type())) {
            return;
        }
        if (this.readLoopProtocolTasks.size() >= MAX_PENDING_READ_LOOP_PROTOCOL_TASKS) {
            this.outboundQueueBookkeeping.recordProtocolBacklogBlockedLocked();
            if (droppable) {
                return;
            }
            throw sessionInternalError(
                    "queue protocol action",
                    "pending read-loop protocol backlog exceeded"
            );
        }
        this.readLoopProtocolTasks.offerLast(new ReadLoopProtocolTask(outboundFrame));
        this.startReadLoopProtocolWorkerLocked();
        this.notifyLockWaitersLocked();
    }

    private void startReadLoopProtocolWorkerLocked() {
        if (this.readLoopProtocolWorkerStarted) {
            return;
        }
        this.readLoopProtocolWorkerStarted = true;
        Thread thread = new Thread(this::readLoopProtocolLoop, "zmux-protocol");
        thread.setDaemon(true);
        thread.start();
    }

    private void readLoopProtocolLoop() {
        ArrayList<ReadLoopProtocolTask> tasks = new ArrayList<>(MAX_PENDING_READ_LOOP_PROTOCOL_TASKS);
        try {
            while (true) {
                tasks.clear();
                synchronized (this.lock) {
                    while (this.readLoopProtocolTasks.isEmpty() && !this.state.terminal()) {
                        try {
                            this.waitOnLock(LockWaitKind.GENERAL);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw interruptedIo(
                                    "zmux: protocol action worker interrupted",
                                    "queue protocol action",
                                    ZmuxErrorScope.SESSION,
                                    ZmuxErrorDirection.BOTH,
                                    interrupted
                            );
                        }
                    }
                    if (this.readLoopProtocolTasks.isEmpty() && this.state.terminal()) {
                        return;
                    }
                    while (!this.readLoopProtocolTasks.isEmpty()) {
                        tasks.add(this.readLoopProtocolTasks.pollFirst());
                    }
                    if (tasks.size() >= MAX_PENDING_READ_LOOP_PROTOCOL_TASKS) {
                        this.readLoopProtocolTasks = new ArrayDeque<>(MAX_PENDING_READ_LOOP_PROTOCOL_TASKS);
                    }
                }
                for (ReadLoopProtocolTask task : tasks) {
                    try {
                        synchronized (this.lock) {
                            this.executeReadLoopProtocolTaskLocked(task);
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw interruptedIo(
                                "zmux: protocol action worker interrupted",
                                "queue protocol action",
                                ZmuxErrorScope.SESSION,
                                ZmuxErrorDirection.BOTH,
                                interrupted
                        );
                    }
                }
            }
        } catch (IOException error) {
            this.handleReadLoopProtocolActionError(error);
        } finally {
            this.emitPendingEvents();
        }
    }

    private void executeReadLoopProtocolTaskLocked(ReadLoopProtocolTask task)
            throws IOException, InterruptedException {
        if (task == null || task.outboundFrame == null) {
            return;
        }
        OutboundFrame outboundFrame = task.outboundFrame;
        while (true) {
            if (this.ignorePeerNonCloseFrameLocked(outboundFrame.frame().type())) {
                return;
            }
            IOException memoryError = this.readLoopProtocolUrgentMemoryErrorLocked(outboundFrame);
            if (memoryError != null) {
                throw memoryError;
            }
            if (this.readLoopProtocolUrgentCapacityAvailableLocked(outboundFrame)) {
                this.enqueueQueuedOutboundLocked(this.urgentQueue, outboundFrame);
                this.notifyWriterWaitersLocked();
                return;
            }
            this.waitOnLock(LockWaitKind.GENERAL);
        }
    }

    private IOException readLoopProtocolUrgentMemoryErrorLocked(OutboundFrame outboundFrame) {
        if (outboundFrame == null) {
            return null;
        }
        TerminalControlMerge terminalMerge = this.planTerminalControlMergeLocked(outboundFrame);
        if (terminalMerge.coalesced()) {
            return null;
        }
        int retainedBytes = SessionRuntime.retainedQueueBytes(outboundFrame);
        long replacedBytes = terminalMerge.replacedBytes();
        long currentTracked = this.trackedSessionMemoryLocked();
        long projectedTracked = SessionRuntime.saturatingAdd(
                Math.max(0L, currentTracked - replacedBytes),
                retainedBytes
        );
        long hardCap = this.sessionMemoryHardCapLocked();
        if (projectedTracked <= hardCap) {
            return null;
        }
        return sessionInternalError(
                "queue urgent control",
                "session memory cap exceeded: tracked=" + projectedTracked + " cap=" + hardCap
        );
    }

    private boolean readLoopProtocolUrgentCapacityAvailableLocked(OutboundFrame outboundFrame) {
        if (outboundFrame == null) {
            return true;
        }
        TerminalControlMerge terminalMerge = this.planTerminalControlMergeLocked(outboundFrame);
        if (terminalMerge.coalesced()) {
            return true;
        }
        long currentUrgent = this.outboundQueueBookkeeping.urgentQueuedControlBytesLocked();
        long projectedUrgent = SessionRuntime.saturatingAdd(
                Math.max(0L, currentUrgent - terminalMerge.replacedBytes()),
                SessionRuntime.retainedQueueBytes(outboundFrame)
        );
        return projectedUrgent <= this.urgentQueuedBytesHardCapLocked();
    }

    private boolean readLoopProtocolUrgentAdmissibleLocked(OutboundFrame outboundFrame) throws IOException {
        IOException memoryError = this.readLoopProtocolUrgentMemoryErrorLocked(outboundFrame);
        if (memoryError != null) {
            throw memoryError;
        }
        return this.readLoopProtocolUrgentCapacityAvailableLocked(outboundFrame);
    }

    private void handleReadLoopProtocolActionError(IOException error) {
        if (error == null) {
            return;
        }
        synchronized (this.lock) {
            if (this.state.terminal()) {
                return;
            }
        }
        this.failSession(error);
    }

    long beginLocalOpenLocked(StreamRuntime streamRuntime) throws IOException {
        return this.openingCoordinator.beginLocalOpenLocked(streamRuntime);
    }

    long beginLocalOpenForWriteLocked(StreamRuntime streamRuntime) throws IOException {
        return this.openingCoordinator.beginLocalOpenForWriteLocked(streamRuntime);
    }

    long beginLocalOpenForCloseLocked(StreamRuntime streamRuntime) throws IOException {
        return this.openingCoordinator.beginLocalOpenForCloseLocked(streamRuntime);
    }

    void queueOpeningDataLocked(StreamRuntime streamRuntime, byte[] openingPrefix, byte[] payload, boolean fin)
            throws IOException {
        this.queueOpeningDataLocked(
                streamRuntime,
                openingPrefix,
                payload,
                0,
                payload.length,
                fin,
                PayloadOwnership.BORROWED
        );
    }

    void queueOpeningDataLocked(
            StreamRuntime streamRuntime,
            byte[] openingPrefix,
            byte[] payload,
            int payloadOffset,
            int payloadLength,
            boolean fin
    ) throws IOException {
        this.queueOpeningDataLocked(
                streamRuntime,
                openingPrefix,
                payload,
                payloadOffset,
                payloadLength,
                fin,
                PayloadOwnership.BORROWED
        );
    }

    void queueOpeningDataLocked(
            StreamRuntime streamRuntime,
            byte[] openingPrefix,
            byte[] payload,
            int payloadOffset,
            int payloadLength,
            boolean fin,
            PayloadOwnership payloadOwnership
    ) throws IOException {
        this.openingCoordinator.queueOpeningDataLocked(
                streamRuntime,
                openingPrefix,
                payload,
                payloadOffset,
                payloadLength,
                fin,
                payloadOwnership
        );
    }

    void queueOpeningFinLocked(StreamRuntime streamRuntime, byte[] openingPrefix) throws IOException {
        this.openingCoordinator.queueOpeningFinLocked(streamRuntime, openingPrefix);
    }

    void queueOpeningDataLocked(StreamRuntime streamRuntime,
                                byte[] prefix,
                                byte[][] parts,
                                int partIndex,
                                int partOffset,
                                int length,
                                boolean fin,
                                PayloadOwnership payloadOwnership) throws IOException {
        this.openingCoordinator.queueOpeningDataLocked(
                streamRuntime,
                prefix,
                parts,
                partIndex,
                partOffset,
                length,
                fin,
                payloadOwnership
        );
    }

    void queueDataLocked(StreamRuntime streamRuntime, byte[] payload, boolean fin) throws IOException {
        this.queueDataLocked(streamRuntime, payload, 0, payload.length, fin, PayloadOwnership.BORROWED);
    }

    void queueDataLocked(StreamRuntime streamRuntime, byte[] payload, int payloadOffset, int payloadLength, boolean fin)
            throws IOException {
        this.queueDataLocked(streamRuntime, payload, payloadOffset, payloadLength, fin, PayloadOwnership.BORROWED);
    }

    void queueDataLocked(
            StreamRuntime streamRuntime,
            byte[] payload,
            int payloadOffset,
            int payloadLength,
            boolean fin,
            PayloadOwnership payloadOwnership
    ) throws IOException {
        this.outboundDataCoordinator.queueDataLocked(
                streamRuntime,
                payload,
                payloadOffset,
                payloadLength,
                fin,
                payloadOwnership
        );
    }

    void queueDataLocked(StreamRuntime streamRuntime,
                         byte[][] parts,
                         int partIndex,
                         int partOffset,
                         int length,
                         boolean fin,
                         PayloadOwnership payloadOwnership) throws IOException {
        this.outboundDataCoordinator.queueDataLocked(
                streamRuntime,
                parts,
                partIndex,
                partOffset,
                length,
                fin,
                payloadOwnership
        );
    }

    byte[] retainPayload(byte[] source, int offset, int length, PayloadOwnership payloadOwnership) {
        if (length == 0) {
            return EMPTY_BYTES;
        }
        if (payloadOwnership == PayloadOwnership.OWNED && offset == 0 && length == source.length) {
            return source;
        }
        return Arrays.copyOfRange(source, offset, offset + length);
    }

    byte[][] retainPayloadParts(byte[][] parts,
                                int partIndex,
                                int partOffset,
                                int length,
                                PayloadOwnership payloadOwnership) {
        if (length == 0) {
            return EMPTY_PARTS;
        }
        int segmentCount = countRetainedPayloadSegments(parts, partIndex, partOffset, length);
        if (segmentCount == 0) {
            return EMPTY_PARTS;
        }

        byte[][] retained = new byte[segmentCount][];
        int retainedIndex = 0;
        int index = partIndex;
        int offset = partOffset;
        int remaining = length;
        while (remaining > 0) {
            byte[] part = Objects.requireNonNull(parts[index], "parts[" + index + "]");
            if (offset >= part.length) {
                ++index;
                offset = 0;
                continue;
            }
            int take = Math.min(part.length - offset, remaining);
            retained[retainedIndex++] = payloadOwnership == PayloadOwnership.OWNED && offset == 0 && take == part.length
                    ? part
                    : Arrays.copyOfRange(part, offset, offset + take);
            remaining -= take;
            ++index;
            offset = 0;
        }
        if (retainedIndex != retained.length) {
            throw new IllegalStateException("multipart payload segment count mismatch");
        }
        return retained;
    }

    void prepareLocalControlOpenerLocked(
            StreamRuntime streamRuntime,
            byte[] openingPrefix,
            boolean replaceQueuedPayload,
            boolean preserveAfterSendClose
    ) throws IOException {
        this.openingCoordinator.prepareLocalControlOpenerLocked(
                streamRuntime,
                openingPrefix,
                replaceQueuedPayload,
                preserveAfterSendClose
        );
    }

    void enqueueResetLocked(StreamRuntime streamRuntime, long code, String reason) throws IOException {
        this.enqueueResetLocked(streamRuntime, code, this.buildControlErrorPayloadLocked(code, reason));
    }

    void enqueueResetLocked(StreamRuntime streamRuntime, long code, byte[] payload) throws IOException {
        this.enqueueResetLocked(streamRuntime, code, payload, true);
    }

    void enqueueResetLocked(StreamRuntime streamRuntime, long code, byte[] payload, boolean notifyWaiters)
            throws IOException {
        streamRuntime.markLocalSendStartedLocked();
        this.noteResetReasonLocked(code);
        this.enqueueQueuedOutboundLocked(
                this.urgentQueue,
                new OutboundFrame(
                        new FrameCodec.Frame(
                                FrameType.RESET,
                                0,
                                streamRuntime.streamIdInternal(),
                                payload
                        ),
                        streamRuntime,
                        0,
                        false,
                        false
                )
        );
        if (notifyWaiters) {
            this.notifyWriterWaitersLocked();
        }
    }

    void enqueueAbortLocked(StreamRuntime streamRuntime, long code, String reason) throws IOException {
        this.enqueueAbortLocked(streamRuntime, code, this.buildControlErrorPayloadLocked(code, reason));
    }

    void enqueueAbortLocked(StreamRuntime streamRuntime, long code, byte[] payload) throws IOException {
        this.enqueueAbortLocked(streamRuntime, code, payload, true);
    }

    void enqueueReadLoopAbortLocked(StreamRuntime streamRuntime, long code, byte[] payload) throws IOException {
        streamRuntime.markLocalSendStartedLocked();
        this.noteAbortReasonLocked(code);
        OutboundFrame outboundFrame = this.abortFrameLocked(streamRuntime, payload);
        if (streamRuntime.openedLocally() && streamRuntime.openingFramePendingLocked()) {
            this.orderStreamControlLocked(outboundFrame);
            this.notifyWriterWaitersLocked();
            return;
        }
        if (this.readLoopProtocolUrgentAdmissibleLocked(outboundFrame)) {
            this.orderStreamControlLocked(outboundFrame);
            this.notifyWriterWaitersLocked();
            return;
        }
        this.enqueueReadLoopProtocolFrameLocked(outboundFrame, true);
    }

    void enqueueAbortLocked(StreamRuntime streamRuntime, long code, byte[] payload, boolean notifyWaiters)
            throws IOException {
        streamRuntime.markLocalSendStartedLocked();
        this.noteAbortReasonLocked(code);
        OutboundFrame outboundFrame = this.abortFrameLocked(streamRuntime, payload);
        this.orderStreamControlLocked(outboundFrame);
        if (notifyWaiters) {
            this.notifyWriterWaitersLocked();
        }
    }

    private OutboundFrame abortFrameLocked(StreamRuntime streamRuntime, byte[] payload) {
        boolean openingFrame = streamRuntime.openedLocally()
                && streamRuntime.idAssigned()
                && !streamRuntime.peerVisibleLocked();
        return new OutboundFrame(
                new FrameCodec.Frame(
                        FrameType.ABORT,
                        0,
                        streamRuntime.streamIdInternal(),
                        payload
                ),
                streamRuntime,
                0,
                openingFrame,
                false
        );
    }

    void enqueueStopSendingLocked(StreamRuntime streamRuntime, long code, String reason) throws IOException {
        this.enqueueStopSendingLocked(streamRuntime, code, this.buildControlErrorPayloadLocked(code, reason));
    }

    void enqueueStopSendingLocked(StreamRuntime streamRuntime, long code, byte[] payload) throws IOException {
        this.enqueueQueuedOutboundLocked(
                this.urgentQueue,
                new OutboundFrame(
                        new FrameCodec.Frame(
                                FrameType.STOP_SENDING,
                                0,
                                streamRuntime.streamIdInternal(),
                                payload
                        ),
                        streamRuntime,
                        0,
                        false,
                        false
                )
        );
        this.notifyWriterWaitersLocked();
    }

    void enqueuePriorityUpdateLocked(StreamRuntime streamRuntime, Long priority, Long group) throws IOException {
        this.priorityUpdateCoordinator.enqueuePriorityUpdateLocked(streamRuntime, priority, group);
    }

    void discardPendingPriorityUpdateLocked(StreamRuntime streamRuntime) {
        this.priorityUpdateCoordinator.discardPendingPriorityUpdateLocked(streamRuntime);
    }

    boolean shouldEmitPriorityUpdateLocked(StreamRuntime streamRuntime) {
        return this.priorityUpdateCoordinator.shouldEmitPriorityUpdateLocked(streamRuntime);
    }

    OutboundFrame takePendingPriorityUpdateForBatchLocked(StreamRuntime streamRuntime) throws IOException {
        return this.priorityUpdateCoordinator.takePendingPriorityUpdateForBatchLocked(streamRuntime);
    }

    private void removeQueuedPriorityUpdateLocked(StreamRuntime streamRuntime) {
        this.priorityUpdateCoordinator.removeQueuedPriorityUpdateLocked(streamRuntime);
    }

    private void discardPendingPriorityQueueLocked() {
        this.priorityUpdateCoordinator.discardPendingPriorityQueueLocked();
    }

    void reserveSendLocked(StreamRuntime streamRuntime, int bytes) throws IOException {
        this.outboundDataCoordinator.reserveSendLocked(streamRuntime, bytes);
    }

    private boolean withinQueuedDataWatermarkLocked(StreamRuntime streamRuntime, int bytes) {
        return this.outboundDataCoordinator.withinQueuedDataWatermarkLocked(streamRuntime, bytes);
    }

    long queuedDataBytesForStreamLocked(StreamRuntime streamRuntime) {
        return this.outboundDataCoordinator.queuedDataBytesForStreamLocked(streamRuntime);
    }

    long inflightQueuedBytesForStreamLocked(StreamRuntime streamRuntime) {
        return this.outboundDataCoordinator.inflightQueuedBytesForStreamLocked(streamRuntime);
    }

    long fragmentCapLocked(StreamRuntime streamRuntime) {
        return this.outboundDataCoordinator.fragmentCapLocked(streamRuntime);
    }

    boolean tryGracefulStopSendingLocked(StreamRuntime streamRuntime) throws IOException {
        return this.stopSendingGracefulCoordinator.tryGracefulStopSendingLocked(streamRuntime);
    }

    void expireStopSendingGracefulDrainsLocked() {
        this.stopSendingGracefulCoordinator.expireStopSendingGracefulDrainsLocked();
    }

    void updateStopSendingGracefulDeadlineLocked(StreamRuntime streamRuntime, long deadlineNanos) {
        this.stopSendingGracefulCoordinator.updateStopSendingGracefulDeadlineLocked(streamRuntime, deadlineNanos);
    }

    long nextStopSendingGracefulDeadlineLocked() {
        return this.stopSendingGracefulCoordinator.nextStopSendingGracefulDeadlineLocked();
    }

    void onDataFrameWrittenLocked(StreamRuntime streamRuntime, int writtenBytes) {
        if (streamRuntime == null) {
            return;
        }
        long previousTracked = this.trackedSessionMemoryLocked();
        if (writtenBytes > 0) {
            streamRuntime.commitReservedSendBytesLocked(writtenBytes);
            this.sessionReservedSendBytes = Math.max(0L, this.sessionReservedSendBytes - writtenBytes);
            this.sessionSentBytes = SessionRuntime.saturatingAdd(this.sessionSentBytes, writtenBytes);
        }
        if (this.sessionMemoryWakeNeededLocked(previousTracked)) {
            this.notifyStreamWriteWaitersLocked();
        }
    }

    private OutboundFrame removeQueuedOpeningFrameLocked(StreamRuntime streamRuntime) {
        return this.openingCoordinator.removeQueuedOpeningFrameLocked(streamRuntime);
    }

    private OutboundFrame removeQueuedOpeningFrameLocked(Deque<OutboundFrame> deque, StreamRuntime streamRuntime) {
        return this.openingCoordinator.removeQueuedOpeningFrameLocked(deque, streamRuntime);
    }

    private void dropQueuedGoAwayLocked() {
        Iterator<OutboundFrame> iterator = this.urgentQueue.iterator();
        while (iterator.hasNext()) {
            OutboundFrame outboundFrame = iterator.next();
            if (outboundFrame != null && outboundFrame.frame().type() == FrameType.GOAWAY) {
                iterator.remove();
                this.onUrgentFrameDequeuedLocked(outboundFrame);
            }
        }
    }

    private void enqueueReplacingQueuedGoAwayLocked(OutboundFrame outboundFrame) throws IOException {
        long replacedBytes = this.queuedGoAwayRetainedBytesLocked();
        IOException admissionError = this.urgentQueueReplacementAdmissionErrorLocked(
                SessionRuntime.retainedQueueBytes(outboundFrame),
                replacedBytes
        );
        if (admissionError != null) {
            this.outboundQueueBookkeeping.recordProtocolBacklogBlockedLocked();
            this.failSession(admissionError);
            throw admissionError;
        }
        this.dropQueuedGoAwayLocked();
        this.enqueueAdmittedExistingUrgentOutboundLocked(outboundFrame);
    }

    private long queuedGoAwayRetainedBytesLocked() {
        long retainedBytes = 0L;
        for (OutboundFrame outboundFrame : this.urgentQueue) {
            if (outboundFrame != null && outboundFrame.frame().type() == FrameType.GOAWAY) {
                retainedBytes = SessionRuntime.saturatingAdd(
                        retainedBytes,
                        SessionRuntime.retainedQueueBytes(outboundFrame)
                );
            }
        }
        return retainedBytes;
    }

    private IOException urgentQueueReplacementAdmissionErrorLocked(int retainedBytes, long replacedBytes) {
        if (retainedBytes <= 0) {
            return null;
        }
        long currentTracked = this.trackedSessionMemoryLocked();
        long projectedTracked = SessionRuntime.saturatingAdd(
                Math.max(0L, currentTracked - replacedBytes),
                retainedBytes
        );
        long memoryHardCap = this.sessionMemoryHardCapLocked();
        if (projectedTracked > memoryHardCap) {
            return sessionInternalError(
                    "queue urgent control",
                    "session memory cap exceeded: tracked=" + projectedTracked + " cap=" + memoryHardCap
            );
        }
        long currentUrgent = this.outboundQueueBookkeeping.urgentQueuedControlBytesLocked();
        long projectedUrgent = SessionRuntime.saturatingAdd(
                Math.max(0L, currentUrgent - replacedBytes),
                retainedBytes
        );
        long urgentHardCap = this.urgentQueuedBytesHardCapLocked();
        if (projectedUrgent <= urgentHardCap) {
            return null;
        }
        return sessionInternalError(
                "queue urgent control",
                "urgent control queue cap exceeded: queued=" + projectedUrgent + " cap=" + urgentHardCap
        );
    }

    void releaseQueuedDataLocked(OutboundFrame outboundFrame) {
        this.outboundDataCoordinator.releaseQueuedDataLocked(outboundFrame);
    }

    void discardQueuedStreamDataLocked(StreamRuntime streamRuntime, boolean preserveAfterSendClose) {
        if (streamRuntime == null) {
            return;
        }
        this.discardQueuedStreamDataLocked(this.urgentQueue, streamRuntime, preserveAfterSendClose);
        this.discardQueuedStreamDataLocked(this.dataQueue, streamRuntime, preserveAfterSendClose);
        if (!this.hasQueuedOpeningFrameLocked(streamRuntime) && !this.hasInflightOpeningFrameLocked(streamRuntime)) {
            streamRuntime.clearOpeningFramePendingLocked();
        }
        this.clearBlockedFrameLocked(streamRuntime.streamIdInternal());
        this.maybeCompactStreamLocked(streamRuntime);
        this.notifyStreamWriteWaitersLocked();
    }

    private void discardQueuedStreamDataLocked(
            Deque<OutboundFrame> deque,
            StreamRuntime streamRuntime,
            boolean preserveAfterSendClose
    ) {
        this.outboundDataCoordinator.discardQueuedStreamDataLocked(deque, streamRuntime, preserveAfterSendClose);
    }

    private boolean hasQueuedOpeningFrameLocked(StreamRuntime streamRuntime) {
        return this.openingCoordinator.hasQueuedOpeningFrameLocked(streamRuntime);
    }

    private boolean hasInflightOpeningFrameLocked(StreamRuntime streamRuntime) {
        return this.openingCoordinator.hasInflightOpeningFrameLocked(streamRuntime);
    }

    void discardPendingOutboundLocked() {
        this.discardPendingOutboundLocked(this.urgentQueue);
        this.discardPendingPriorityQueueLocked();
        this.discardPendingOutboundLocked(this.dataQueue);
        this.closeFrameQueued = false;
        this.sessionReservedSendBytes = 0L;
    }

    private void discardPendingOutboundLocked(Deque<OutboundFrame> deque) {
        OutboundFrame outboundFrame;
        while ((outboundFrame = this.pollQueuedOutboundLocked(deque)) != null) {
            if (outboundFrame.stream != null && outboundFrame.openingFrame) {
                outboundFrame.stream.clearOpeningFramePendingLocked();
            }
            this.releaseQueuedDataLocked(outboundFrame);
        }
    }

    void onReadDiscardLocked(StreamRuntime streamRuntime, boolean acceptQueuedStream) {
        this.readerRuntime.onReadDiscardLocked(streamRuntime, acceptQueuedStream);
    }

    void retryReceiveReplenishLocked() {
        this.readerRuntime.retryReceiveReplenishLocked();
    }

    byte[] encodeVarint(long value) {
        try {
            return Varint62.encode(value);
        } catch (IOException error) {
            throw internalIoFailure("encode control varint", error);
        }
    }

    private void handleStopSendingFrame(FrameCodec.Frame frame) throws IOException {
        this.readerRuntime.handleStopSendingFrame(frame);
    }

    private void handleGoAwayFrame(FrameCodec.Frame frame) throws IOException {
        this.readerRuntime.handleGoAwayFrame(frame);
    }

    private void putTombstoneLocked(long streamId, SessionTerminalBookkeeping.Tombstone tombstone) {
        this.terminalBookkeeping.putTombstoneLocked(streamId, tombstone);
    }

    private void reapExcessTombstonesLocked() {
        this.terminalBookkeeping.reapExcessTombstonesLocked();
    }

    void onReadBufferAddedLocked(StreamRuntime streamRuntime, int addedBytes, int storageBytes) {
        this.readerRuntime.onReadBufferAddedLocked(streamRuntime, addedBytes, storageBytes);
    }

    void onReadBufferReleasedLocked(StreamRuntime streamRuntime, long releasedBytes, long releasedStorageBytes) {
        this.readerRuntime.onReadBufferReleasedLocked(streamRuntime, releasedBytes, releasedStorageBytes);
    }

    void addAcceptQueuedBytesLocked(StreamRuntime streamRuntime, long queuedBytes) {
        this.acceptRegistry.addQueuedBytesLocked(streamRuntime, queuedBytes);
    }

    void enqueueAcceptedLocked(StreamRuntime streamRuntime) {
        this.acceptRegistry.enqueueAcceptedLocked(streamRuntime);
    }

    private StreamRuntime pollAcceptedHeadLocked(boolean bidirectional) {
        return this.acceptRegistry.pollAcceptedHeadLocked(bidirectional);
    }

    void enforceVisibleAcceptBacklogLocked() throws IOException {
        this.acceptRegistry.enforceVisibleBacklogLocked();
    }

    int pendingAcceptedCountLocked() {
        return this.acceptRegistry.pendingAcceptedCountLocked();
    }

    long pendingAcceptedBytesLocked() {
        return this.acceptRegistry.pendingAcceptedBytesLocked();
    }

    void releaseAcceptQueuedBytesLocked(StreamRuntime streamRuntime, long releasedBytes) {
        this.acceptRegistry.releaseQueuedBytesLocked(streamRuntime, releasedBytes);
    }

    private void validateOutgoingGoAwayWatermarkLocked(long watermark, boolean bidirectional) throws IOException {
        if (watermark == 0L) {
            return;
        }
        if (SessionRuntime.streamIsBidi(watermark) != bidirectional) {
            throw sessionError(
                    ErrorCode.PROTOCOL,
                    "goAway",
                    "GOAWAY watermark has wrong direction",
                    ZmuxErrorSource.LOCAL,
                    ZmuxErrorDirection.WRITE
            );
        }
        if (SessionRuntime.streamIsLocal(this.negotiated.localRole(), watermark)) {
            throw sessionError(
                    ErrorCode.PROTOCOL,
                    "goAway",
                    "GOAWAY watermark targets non-peer stream id",
                    ZmuxErrorSource.LOCAL,
                    ZmuxErrorDirection.WRITE
            );
        }
    }

    void maybeCompactStreamLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null
                || !streamRuntime.fullyTerminalLocked()
                || !streamRuntime.readBufferEmptyLocked()) {
            return;
        }
        if (streamRuntime.acceptQueued()) {
            return;
        }
        if (this.streams.get(streamRuntime.streamIdInternal()) != streamRuntime) {
            return;
        }
        this.releaseStreamPeerReasonBudgetLocked(streamRuntime);
        this.unregisterGracefulCloseBlockingLocked(streamRuntime);
        this.flowControlUpdateRegistry.clearStreamStateLocked(streamRuntime.streamIdInternal());
        this.streams.remove(streamRuntime.streamIdInternal());
        this.dropOrdinaryBatchStateLocked(streamRuntime);
        this.localOpenTracker.removeUnseenLocalLocked(streamRuntime);
        this.onStreamOpenInfoUpdatedLocked(streamRuntime.openInfoLengthLocked(), 0);
        boolean hidden = !streamRuntime.applicationVisible();
        long nowNanos = hidden ? System.nanoTime() : 0L;
        if (hidden) {
            this.noteHiddenStreamReapedLocked();
        }
        this.putTombstoneLocked(
                streamRuntime.streamIdInternal(),
                new SessionTerminalBookkeeping.Tombstone(
                        streamRuntime.localReceive(),
                        streamRuntime.receiveGracefulLocked(),
                        streamRuntime.terminalCodeLocked(),
                        streamRuntime.terminalReasonLocked(),
                        streamRuntime.lateDataCauseLocked(),
                        hidden,
                        nowNanos
                )
        );
        this.streamBookkeeping.onStreamFullyClosedLocked(streamRuntime);
        this.notifyLifecycleWaitersLocked();
    }

    void onOutboundSchedulingGroupChangedLocked(StreamRuntime streamRuntime, Long previousGroup) {
        this.explicitGroupTracker.onOutboundSchedulingGroupChangedLocked(
                streamRuntime,
                previousGroup,
                this.peerSettings().schedulerHints(),
                streamRuntime != null && this.streams.get(streamRuntime.streamIdInternal()) == streamRuntime
        );
    }

    void onStreamSendTerminalLocked(StreamRuntime streamRuntime) {
        this.explicitGroupTracker.onStreamSendTerminalLocked(streamRuntime);
    }

    private void dropOrdinaryBatchStateLocked(StreamRuntime streamRuntime) {
        this.explicitGroupTracker.dropOrdinaryBatchStateLocked(
                streamRuntime,
                this.peerSettings().schedulerHints()
        );
    }

    Long outboundSchedulingGroupLocked(StreamRuntime streamRuntime) {
        return this.explicitGroupTracker.outboundSchedulingGroupLocked(streamRuntime);
    }

    void markLocalStreamOpeningCommittedLocked(StreamRuntime streamRuntime) {
        if (streamRuntime == null || !streamRuntime.needsLocalOpenerLocked()) {
            return;
        }
        long nowNanos = System.nanoTime();
        this.noteOpenCommitLocked(streamRuntime.provisionalCreatedAtNanos(), nowNanos);
        streamRuntime.setProvisionalCreatedAtNanosLocked(0L);
        streamRuntime.markOpenedOnWireLocked();
    }

    void markPeerVisibleLocked(StreamRuntime streamRuntime) throws IOException {
        this.openingCoordinator.markPeerVisibleLocked(streamRuntime);
    }

    void refusePeerOpeningStreamLocked(long streamId, boolean recordTombstone, boolean hidden) throws IOException {
        this.noteAbortReasonLocked(ErrorCode.REFUSED_STREAM.code());
        if (hidden) {
            this.noteHiddenStreamRefusedLocked();
        }
        if (recordTombstone) {
            this.putTombstoneLocked(
                    streamId,
                    new SessionTerminalBookkeeping.Tombstone(
                            false,
                            false,
                            ErrorCode.REFUSED_STREAM.code(),
                            "",
                            LateDataCause.NONE,
                            hidden,
                            hidden ? System.nanoTime() : 0L
                    )
            );
        }
        this.enqueueControlLocked(
                new FrameCodec.Frame(
                        FrameType.ABORT,
                        0,
                        streamId,
                        FrameCodec.buildErrorPayload(
                                ErrorCode.REFUSED_STREAM.code(),
                                "",
                                this.controlPayloadLimitLocked()
                        )
                )
        );
        this.notifyLockWaitersLocked();
    }

    void failProvisionalLocalAbortLocked(StreamRuntime streamRuntime, long code, String reason) {
        this.localOpenTracker.failProvisionalLocalAbortLocked(streamRuntime, code, reason);
    }

    boolean isRefusedStreamError(IOException error) {
        return error != null && ZmuxErrors.code(error, -1L) == ErrorCode.REFUSED_STREAM.code();
    }

    boolean hasGracefulClosePendingWorkLocked() {
        return this.streamBookkeeping.hasGracefulCloseBlockingStreamsLocked()
                || this.localOpenTracker.hasProvisionalsLocked();
    }

    void onStreamGracefulCloseBlockingChangedLocked(StreamRuntime streamRuntime, boolean previous, boolean current) {
        if (streamRuntime == null || previous == current) {
            return;
        }
        if (this.streams.get(streamRuntime.streamIdInternal()) != streamRuntime) {
            return;
        }
        if (this.streamBookkeeping.onStreamGracefulCloseBlockingChangedLocked(previous, current)) {
            this.notifyLifecycleWaitersLocked();
        }
    }

    void registerGracefulCloseBlockingLocked(StreamRuntime streamRuntime) {
        this.streamBookkeeping.registerGracefulCloseBlockingLocked(streamRuntime);
    }

    private void unregisterGracefulCloseBlockingLocked(StreamRuntime streamRuntime) {
        this.streamBookkeeping.unregisterGracefulCloseBlockingLocked(streamRuntime);
    }

    private long effectiveGoAwaySendWatermarkLocked(boolean bidirectional) {
        long configuredWatermark = bidirectional ? this.localGoAwayBidi : this.localGoAwayUni;
        if (configuredWatermark != 0x3FFFFFFFFFFFFFFFL) {
            return configuredWatermark;
        }
        long firstPeerStreamId = SessionRuntime.firstPeerStreamId(this.localRole(), bidirectional);
        if (firstPeerStreamId == 0L || firstPeerStreamId > 0x3FFFFFFFFFFFFFFFL) {
            return 0L;
        }
        return firstPeerStreamId + (0x3FFFFFFFFFFFFFFFL - firstPeerStreamId) / 4L * 4L;
    }

    private void reclaimGracefulCloseLocalStreamsLocked() {
        this.localOpenTracker.reclaimGracefulCloseLocalStreamsLocked();
    }

    void releaseAllStreamsForSessionCloseLocked(ApplicationError applicationError) {
        for (StreamRuntime streamRuntime : this.streams.values()) {
            streamRuntime.closeForSessionLocked(applicationError);
        }
        if (applicationError == null && this.localGracefulCloseRefusesProvisionalsLocked()) {
            this.localOpenTracker.rejectGracefulCloseProvisionalsLocked();
            return;
        }
        this.localOpenTracker.forEachProvisionalLocked(streamRuntime -> streamRuntime.closeForSessionLocked(applicationError));
    }

    void clearSessionCloseStateLocked() {
        this.ordinaryBatchBias.release();
        this.explicitGroupTracker.clear();
        this.stopSendingGracefulCoordinator.clear();
        this.streams.clear();
        this.terminalBookkeeping.clear();
        this.clearAcceptQueuesLocked();
        this.localOpenTracker.clear();
        this.flowControlUpdateRegistry.clear();
        this.readLoopProtocolTasks = new ArrayDeque<>(MAX_PENDING_READ_LOOP_PROTOCOL_TASKS);
        this.urgentQueue = new ArrayDeque<>();
        this.advisoryQueue = new ArrayDeque<>();
        this.dataQueue = new ArrayDeque<>();
        this.sessionQueuedDataBytes = 0L;
        this.bufferedReceiveBytes = 0L;
        this.bufferedReceiveStorageBytes = 0L;
        this.recvSessionPending = 0L;
        this.receiveReplenishRetry = false;
        this.retainedOpenInfoBytes = 0L;
        this.retainedPeerReasonBytes = 0L;
        this.outboundQueueBookkeeping.clear();
        this.writerHeldRetainedBytes = 0L;
        this.streamBookkeeping.clear();
        this.telemetry.clearTerminalKeepaliveStateLocked();
        this.notifyLockWaitersLocked();
    }

    ApplicationError sessionCloseStreamErrorLocked(SessionState sessionState) {
        if (sessionState != SessionState.FAILED) {
            return null;
        }
        IOException sessionError = this.terminalError != null ? this.terminalError : this.peerCloseError;
        if (sessionError instanceof ApplicationError) {
            ApplicationError applicationError = (ApplicationError) sessionError;
            return applicationError.code() == ErrorCode.NO_ERROR.code()
                    ? null
                    : new ApplicationError(
                    applicationError.code(),
                    applicationError.reason(),
                    ZmuxErrorScope.STREAM,
                    applicationError.source() == ZmuxErrorSource.UNKNOWN ? ZmuxErrorSource.LOCAL : applicationError.source(),
                    ZmuxErrorDirection.BOTH,
                    ZmuxTerminationKind.SESSION_TERMINATION
            );
        }
        return new ApplicationError(
                ZmuxErrors.code(sessionError, ErrorCode.INTERNAL.code()),
                ZmuxErrors.reason(sessionError),
                ZmuxErrorScope.STREAM,
                ZmuxErrors.source(sessionError) == ZmuxErrorSource.UNKNOWN
                        ? ZmuxErrorSource.LOCAL
                        : ZmuxErrors.source(sessionError),
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        );
    }

    boolean localGracefulCloseRefusesProvisionalsLocked() {
        if (!(this.terminalError instanceof ApplicationError)) {
            return false;
        }
        ApplicationError applicationError = (ApplicationError) this.terminalError;
        if (applicationError.code() != ErrorCode.NO_ERROR.code()) {
            return false;
        }
        return applicationError.source() == ZmuxErrorSource.LOCAL
                || (applicationError.source() == ZmuxErrorSource.UNKNOWN && this.peerCloseError == null);
    }

    SessionState terminalStateForSessionError(IOException error) {
        return this.lifecycleRuntime.terminalStateForSessionError(error);
    }

    private boolean waitForGracefulCloseDrain(Duration duration) throws IOException {
        return this.lifecycleRuntime.waitForGracefulCloseDrain(duration);
    }

    private void awaitGoAwayDrainInterval() throws IOException {
        this.lifecycleRuntime.awaitGoAwayDrainInterval();
    }

    private void awaitCloseCompletion(Duration duration) throws IOException {
        this.lifecycleRuntime.awaitCloseCompletion(duration);
    }

    byte[] buildPingPayloadLocked(byte[] payloadSuffix) throws IOException {
        int suffixLength = payloadSuffix == null ? 0 : payloadSuffix.length;
        long payloadLength = 8L + (long) suffixLength;
        long payloadLimit = this.pingPayloadLimitLocked();
        if (payloadLength > payloadLimit) {
            throw sessionError(
                    ErrorCode.FRAME_SIZE,
                    "ping",
                    "PING payload " + payloadLength + " exceeds control payload limit " + payloadLimit,
                    ZmuxErrorSource.LOCAL,
                    ZmuxErrorDirection.WRITE
            );
        }
        byte[] payload = new byte[(int) payloadLength];
        long token = this.nextPingNonceLocked();
        for (int i = 0; i < 8; ++i) {
            payload[7 - i] = (byte) (token >>> i * 8);
        }
        if (suffixLength > 0) {
            System.arraycopy(payloadSuffix, 0, payload, 8, suffixLength);
        }
        return payload;
    }

    long controlPayloadLimitLocked() {
        long peerLimit = this.peerSettings().maxControlPayloadBytes();
        if (peerLimit <= 0L) {
            peerLimit = Settings.defaults().maxControlPayloadBytes();
        }
        return peerLimit;
    }

    long extensionPayloadLimitLocked() {
        long localLimit = this.localSettings().maxExtensionPayloadBytes();
        long peerLimit = this.peerSettings().maxExtensionPayloadBytes();
        if (localLimit <= 0L) {
            localLimit = Settings.defaults().maxExtensionPayloadBytes();
        }
        if (peerLimit <= 0L) {
            peerLimit = Settings.defaults().maxExtensionPayloadBytes();
        }
        return Math.min(localLimit, peerLimit);
    }

    private long pingPayloadLimitLocked() {
        return this.negotiatedControlPayloadLimitLocked();
    }

    byte[] buildControlErrorPayloadLocked(long code, String reason) throws IOException {
        return FrameCodec.buildErrorPayload(code, reason, this.controlPayloadLimitLocked());
    }

    private long negotiatedControlPayloadLimitLocked() {
        long localLimit = this.localSettings().maxControlPayloadBytes();
        long peerLimit = this.peerSettings().maxControlPayloadBytes();
        if (localLimit <= 0L) {
            localLimit = Settings.defaults().maxControlPayloadBytes();
        }
        if (peerLimit <= 0L) {
            peerLimit = Settings.defaults().maxControlPayloadBytes();
        }
        return Math.min(localLimit, peerLimit);
    }

    void failActivePingLocked(IOException error) {
        this.telemetry.failActivePingLocked(error);
    }

    void noteInboundFrameLocked(long nowNanos) {
        this.telemetry.noteInboundFrameLocked(nowNanos);
    }

    void noteTransportWriteIntentLocked(long nowNanos) {
        this.telemetry.noteTransportWriteIntentLocked(nowNanos);
    }

    private void noteTransportWriteCompletedLocked(long nowNanos) {
        this.telemetry.noteTransportWriteCompletedLocked(nowNanos);
    }

    void noteStreamProgressLocked(long nowNanos) {
        this.telemetry.noteStreamProgressLocked(nowNanos);
    }

    void noteApplicationProgressLocked(long nowNanos) {
        this.telemetry.noteApplicationProgressLocked(nowNanos);
    }

    private void notePingSentLocked(long nowNanos) {
        this.telemetry.notePingSentLocked(nowNanos);
    }

    long sendRateEstimateLocked() {
        return this.telemetry.sendRateEstimateLocked();
    }

    long stopSendingGracefulTailCapLocked() {
        return this.config.stopSendingGracefulTailCap();
    }

    Duration stopSendingGracefulDrainWindowLocked() {
        Duration configured = this.config.stopSendingGracefulDrainWindow();
        if (configured != null && !configured.isNegative() && !configured.isZero()) {
            return configured;
        }
        return Duration.ofNanos(SessionRuntime.adaptiveRttTimeout(
                this.telemetry.lastPingRttNanos(),
                SessionRuntime.positiveNanos(StopSendingGracefulPolicy.REPO_DEFAULT_DRAIN_WINDOW),
                STOP_SENDING_ADAPTIVE_DRAIN_WINDOW_MAX_NANOS,
                2,
                0L
        ));
    }

    long goAwayDrainIntervalNanosLocked() {
        long intervalNanos = SessionRuntime.positiveNanos(GOAWAY_DRAIN_INTERVAL);
        long rttNanos = this.telemetry.lastPingRttNanos();
        if (rttNanos > 0L) {
            intervalNanos = Math.max(intervalNanos, rttNanos / 4L);
        }
        long maxNanos = SessionRuntime.positiveNanos(GOAWAY_DRAIN_INTERVAL_MAX);
        return maxNanos > 0L && intervalNanos > maxNanos ? maxNanos : intervalNanos;
    }

    private void noteCompletedWriteBatchLocked(int batchFrames, long batchBytes, long startedAtNanos, long completedAtNanos) {
        this.telemetry.noteCompletedWriteBatchLocked(batchFrames, batchBytes, startedAtNanos, completedAtNanos);
    }

    void noteBlockedWriteLocked(long blockedNanos) {
        this.telemetry.noteBlockedWriteLocked(blockedNanos);
    }

    private void noteOpenCommitLocked(long createdAtNanos, long nowNanos) {
        this.telemetry.noteOpenCommitLocked(createdAtNanos, nowNanos);
    }

    private void noteSendRateEstimateLocked(long bytes, long writeDurationNanos) {
        this.telemetry.noteSendRateEstimateLocked(bytes, writeDurationNanos);
    }

    private long encodedFrameBytes(OutboundFrame outboundFrame) {
        if (outboundFrame == null) {
            return 0L;
        }
        long payloadLength = this.outboundPayloadLength(outboundFrame);
        long frameLength = SessionRuntime.saturatingAdd(1L + this.safeVarintLength(outboundFrame.frame().streamId()), payloadLength);
        return SessionRuntime.saturatingAdd(frameLength, this.safeVarintLength(frameLength));
    }

    boolean batchContainsCloseFrameLocked(List<OutboundFrame> batch) {
        if (batch == null || batch.isEmpty()) {
            return false;
        }
        for (OutboundFrame outboundFrame : batch) {
            if (outboundFrame != null && outboundFrame.frame().type() == FrameType.CLOSE) {
                return true;
            }
        }
        return false;
    }

    private long outboundPayloadLength(OutboundFrame outboundFrame) {
        if (outboundFrame == null) {
            return 0L;
        }
        int prefixLength = outboundFrame.payloadPrefix == null ? 0 : outboundFrame.payloadPrefix.length;
        return SessionRuntime.saturatingAdd(prefixLength, Math.max(0, outboundFrame.payloadLength));
    }

    private int safeVarintLength(long value) {
        try {
            return Varint62.length(value);
        } catch (ZmuxException invalid) {
            return 8;
        }
    }

    private SessionState publicState() {
        return SessionRuntime.publicState(this.state);
    }

    SessionState publicStateLocked() {
        return SessionRuntime.publicState(this.state);
    }

    void waitOnLockNanos(long waitNanos) throws InterruptedException {
        waitOnLockNanos(waitNanos, LockWaitKind.GENERAL);
    }

    void waitOnLockNanos(long waitNanos, LockWaitKind kind) throws InterruptedException {
        long boundedNanos = Math.max(1L, waitNanos);
        long millis = TimeUnit.NANOSECONDS.toMillis(boundedNanos);
        int nanos = (int) (boundedNanos - TimeUnit.MILLISECONDS.toNanos(millis));
        this.incrementLockWaiters(kind);
        try {
            this.lock.wait(millis, nanos);
        } finally {
            this.decrementLockWaiters(kind);
        }
    }

    void waitOnLock() throws InterruptedException {
        waitOnLock(LockWaitKind.GENERAL);
    }

    void waitOnLock(LockWaitKind kind) throws InterruptedException {
        this.incrementLockWaiters(kind);
        try {
            this.lock.wait();
        } finally {
            this.decrementLockWaiters(kind);
        }
    }

    boolean allowLocalNonCloseControlLocked() {
        return !this.state.terminal() && this.state != SessionState.CLOSING && this.peerCloseError == null && !this.closeFrameQueued;
    }

    boolean shouldFailSessionOperationsLocked() {
        return this.state.terminal()
                || this.state == SessionState.CLOSING
                || this.peerCloseError != null
                || this.closeFrameQueued;
    }

    boolean ignorePeerNonCloseFrameLocked(FrameType frameType) {
        return frameType != FrameType.CLOSE
                && (this.state == SessionState.CLOSING || this.state.terminal() || this.closeFrameQueued);
    }

    boolean ignorePeerCloseFrameLocked() {
        return this.peerCloseError != null
                || this.closeFrameQueued
                || this.state == SessionState.CLOSING
                || this.state.terminal();
    }

    private Duration gracefulCloseDrainTimeout() {
        Duration duration = this.config.gracefulCloseDrainTimeout();
        if (duration != null && !duration.isNegative() && !duration.isZero()) {
            return duration;
        }
        long adaptiveTimeoutNanos = SessionRuntime.adaptiveRttTimeout(
                this.telemetry.lastPingRttNanos(),
                SessionRuntime.positiveNanos(DEFAULT_GRACEFUL_CLOSE_DRAIN_TIMEOUT),
                SessionRuntime.positiveNanos(DEFAULT_GRACEFUL_CLOSE_DRAIN_TIMEOUT_MAX),
                4,
                DEFAULT_GRACEFUL_CLOSE_RTT_ADAPTIVE_SLACK_NANOS
        );
        return Duration.ofNanos(adaptiveTimeoutNanos);
    }

    int visibleAcceptBacklogHardCapLocked() {
        if (this.config.acceptBacklogLimit() > 0) {
            return this.config.acceptBacklogLimit();
        }
        return 128;
    }

    long visibleAcceptBacklogBytesHardCapLocked() {
        if (this.config.acceptBacklogBytesLimit() > 0L) {
            return this.config.acceptBacklogBytesLimit();
        }
        return SessionRuntime.visibleAcceptBacklogBytesHardCapFor(this.localSettings().maxFramePayload());
    }

    int hiddenControlStateHardCapLocked() {
        return SessionRuntime.admissionHardCap(this.visibleAcceptBacklogHardCapLocked());
    }

    int hiddenControlStateSoftCapLocked() {
        return SessionRuntime.admissionSoftCap(this.visibleAcceptBacklogHardCapLocked());
    }

    int provisionalOpenHardCapLocked() {
        return SessionRuntime.admissionHardCap(this.visibleAcceptBacklogHardCapLocked());
    }

    int provisionalOpenSoftCapLocked() {
        return SessionRuntime.admissionSoftCap(this.visibleAcceptBacklogHardCapLocked());
    }

    long provisionalOpenMaxAgeNanosLocked() {
        return SessionRuntime.adaptiveRttTimeout(
                this.telemetry.lastPingRttNanos(),
                PROVISIONAL_OPEN_BASE_MAX_AGE_NANOS,
                PROVISIONAL_OPEN_MAX_AGE_ADAPTIVE_CAP_NANOS,
                6,
                PROVISIONAL_OPEN_RTT_ADAPTIVE_SLACK_NANOS
        );
    }

    private void clearAcceptQueuesLocked() {
        this.acceptRegistry.clearPendingLocked();
    }

    boolean queueSessionMaxDataLocked(long desiredOffset) {
        return this.flowControlCoordinator.queueSessionMaxDataLocked(desiredOffset);
    }

    boolean queueStreamMaxDataLocked(long streamId, long desiredOffset) {
        return this.flowControlCoordinator.queueStreamMaxDataLocked(streamId, desiredOffset);
    }

    boolean hasPendingWindowUpdatesLocked() {
        return this.flowControlCoordinator.hasPendingWindowUpdatesLocked();
    }

    boolean hasPendingMaxDataLocked() {
        return this.flowControlCoordinator.hasPendingMaxDataLocked();
    }

    boolean flushPendingWindowUpdatesLocked(Long preferredStreamId) throws IOException {
        return this.flowControlCoordinator.flushPendingWindowUpdatesLocked(preferredStreamId);
    }

    private void appendPendingWindowUpdatesLocked(List<OutboundFrame> batch, int maxFrames, Long preferredStreamId)
            throws IOException {
        this.appendPendingWindowUpdatesLocked(batch, maxFrames, preferredStreamId, false);
    }

    void appendPendingWindowUpdatesLocked(
            List<OutboundFrame> batch,
            int maxFrames,
            Long preferredStreamId,
            boolean trackWriterHeld
    ) throws IOException {
        this.flowControlCoordinator.appendPendingWindowUpdatesLocked(batch, maxFrames, preferredStreamId, trackWriterHeld);
    }

    void afterWriteBatchLocked(List<OutboundFrame> list,
                               long batchBytes,
                               long batchStartedAtNanos,
                               long batchCompletedAtNanos) throws IOException {
        if (!list.isEmpty()) {
            noteCompletedWriteBatchLocked(list.size(), batchBytes, batchStartedAtNanos, batchCompletedAtNanos);
            releaseWriterHeldFramesLocked(list);
        }
        inflightBatch = Collections.emptyList();

        for (OutboundFrame outboundFrame : list) {
            this.sentFrames = SessionRuntime.saturatingAdd(this.sentFrames, 1L);
            if (outboundFrame.dataBytes > 0) {
                trackQueuedDataRemovedLocked(outboundFrame);
                this.sentDataBytes = SessionRuntime.saturatingAdd(this.sentDataBytes, outboundFrame.dataBytes);
                onDataFrameWrittenLocked(outboundFrame.stream, outboundFrame.dataBytes);
            }
            if (outboundFrame.stream != null) {
                outboundFrame.stream.onFrameWrittenLocked(outboundFrame.frame, outboundFrame.openingFrame);
                if (outboundFrame.openingFrame) {
                    this.markPeerVisibleLocked(outboundFrame.stream);
                }
                maybeCompactStreamLocked(outboundFrame.stream);
            }
            if (outboundFrame.frame().type() == FrameType.CLOSE) {
                finishSessionLocked(null, state == SessionState.FAILED ? SessionState.FAILED : SessionState.CLOSED);
                return;
            }
        }
    }

    void flushPendingPriorityUpdateLocked(StreamRuntime streamRuntime) throws IOException {
        this.priorityUpdateCoordinator.flushPendingPriorityUpdateLocked(streamRuntime);
    }

    void orderStreamControlLocked(OutboundFrame outboundFrame) throws IOException {
        if (outboundFrame == null) {
            return;
        }
        if (outboundFrame.stream != null
                && outboundFrame.stream.openedLocally()
                && outboundFrame.stream.openingFramePendingLocked()) {
            if (outboundFrame.frame().type() == FrameType.EXT || outboundFrame.frame().type() == FrameType.MAX_DATA) {
                this.enqueueQueuedOutboundLocked(this.dataQueue, outboundFrame);
                return;
            }
            OutboundFrame openingFrame = this.removeQueuedOpeningFrameLocked(this.dataQueue, outboundFrame.stream);
            if (openingFrame != null) {
                this.enqueueExistingOutboundLocked(this.urgentQueue, openingFrame);
                outboundFrame.stream.markOpeningFramePendingLocked();
            }
        }
        if (outboundFrame.frame().type() == FrameType.EXT) {
            this.enqueueQueuedOutboundLocked(this.dataQueue, outboundFrame);
            return;
        }
        this.enqueueQueuedOutboundLocked(this.urgentQueue, outboundFrame);
    }

    SessionOutboundQueueBookkeeping outboundQueueBookkeepingInternal() {
        return this.outboundQueueBookkeeping;
    }

    SessionPriorityUpdateCoordinator priorityUpdateCoordinatorInternal() {
        return this.priorityUpdateCoordinator;
    }

    SessionFlowControlUpdateRegistry flowControlUpdateRegistryInternal() {
        return this.flowControlUpdateRegistry;
    }

    SessionAcceptRegistry acceptRegistryInternal() {
        return this.acceptRegistry;
    }

    SessionLocalOpenTracker localOpenTrackerInternal() {
        return this.localOpenTracker;
    }

    SessionStreamBookkeeping streamBookkeepingInternal() {
        return this.streamBookkeeping;
    }

    FrameCodec.Decoder inputInternal() {
        return this.input;
    }

    InboundPayloadPool inboundPayloadPoolInternal() {
        return this.inboundPayloadPool;
    }

    void setStateInternal(SessionState state) {
        this.state = state;
    }

    IOException terminalErrorInternal() {
        return this.terminalError;
    }

    void setTerminalErrorInternal(IOException error) {
        this.terminalError = error;
    }

    void incrementReceivedFramesLocked() {
        this.receivedFrames = SessionRuntime.saturatingAdd(this.receivedFrames, 1L);
    }

    void reapExpiredHiddenControlStateLocked(long nowNanos) {
        this.terminalBookkeeping.reapExpiredHiddenControlStateLocked(nowNanos);
    }

    long localGoAwayBidiInternal() {
        return this.localGoAwayBidi;
    }

    long localGoAwayUniInternal() {
        return this.localGoAwayUni;
    }

    SessionTerminalBookkeeping.TerminalDataDisposition terminalDataDispositionForLocked(long streamId) {
        return this.terminalBookkeeping.terminalDataDispositionForLocked(streamId);
    }

    StreamRuntime createPeerOpenedStreamLocked(long streamId) {
        StreamRuntime streamRuntime = this.newPeerOpenedStreamLocked(streamId);
        boolean bidirectional = streamRuntime.bidirectional();
        this.streams.put(streamId, streamRuntime);
        this.registerGracefulCloseBlockingLocked(streamRuntime);
        this.explicitGroupTracker.trackNewStreamLocked(
                streamRuntime,
                this.peerSettings().schedulerHints(),
                this.streams.get(streamRuntime.streamIdInternal()) == streamRuntime
        );
        this.streamBookkeeping.onPeerOpenedLocked(bidirectional);
        return streamRuntime;
    }

    private StreamRuntime newPeerOpenedStreamLocked(long streamId) {
        boolean bidirectional = SessionRuntime.streamIsBidi(streamId);
        StreamRuntime streamRuntime = new StreamRuntime(this, false, bidirectional, null);
        long peerSendLimit = bidirectional
                ? this.peerSettings().initialMaxStreamDataBidiLocallyOpened()
                : 0L;
        long localReceiveLimit = bidirectional
                ? this.localSettings().initialMaxStreamDataBidiPeerOpened()
                : this.localSettings().initialMaxStreamDataUni();
        streamRuntime.initializePeerOpenedLocked(
                streamId,
                bidirectional,
                true,
                peerSendLimit,
                localReceiveLimit
        );
        return streamRuntime;
    }

    void recordAcceptedPeerStreamLocked(long streamId) {
        if (SessionRuntime.streamIsBidi(streamId)) {
            this.lastAcceptedPeerBidi = Math.max(this.lastAcceptedPeerBidi, streamId);
        } else {
            this.lastAcceptedPeerUni = Math.max(this.lastAcceptedPeerUni, streamId);
        }
    }

    boolean peerStreamWithinLimitLocked(boolean bidirectional) {
        long active = this.streamBookkeeping.activePeerCountLocked(bidirectional);
        long limit = bidirectional
                ? this.localSettings().maxIncomingStreamsBidi()
                : this.localSettings().maxIncomingStreamsUni();
        return active < limit;
    }

    void reclaimUnseenLocalStreamsLocked() {
        this.localOpenTracker.reclaimUnseenLocalStreamsLocked(this.peerGoAwayBidi, this.peerGoAwayUni);
    }

    void reclaimProvisionalsLocked() {
        this.localOpenTracker.reclaimProvisionalsLocked(
                this.nextLocalBidi,
                this.nextLocalUni,
                this.peerGoAwayBidi,
                this.peerGoAwayUni
        );
    }

    void enqueuePongLocked(byte[] payload) throws IOException {
        OutboundFrame outboundFrame = new OutboundFrame(
                new FrameCodec.Frame(FrameType.PONG, 0, 0L, payload),
                null,
                0,
                false,
                false
        );
        if (this.readLoopProtocolUrgentAdmissibleLocked(outboundFrame)) {
            this.enqueueQueuedOutboundLocked(this.urgentQueue, outboundFrame);
            return;
        }
        this.enqueueReadLoopProtocolFrameLocked(outboundFrame, true);
    }

    boolean handlePongLocked(byte[] payload, long nowNanos) {
        return this.telemetry.handlePongLocked(payload, nowNanos);
    }

    void retainHiddenAbortTombstoneLocked(long streamId, long code, String reason, long nowNanos) {
        this.terminalBookkeeping.retainHiddenAbortTombstoneLocked(streamId, code, reason, nowNanos);
    }

    long aggregateLateDataCap() {
        if (this.config.aggregateLateDataCap() > 0L) {
            return this.config.aggregateLateDataCap();
        }
        return RuntimeFlow.aggregateLateDataCap(this.localSettings().maxFramePayload());
    }

    long lateDataPerStreamCap(StreamRuntime streamRuntime) {
        if (streamRuntime == null || !streamRuntime.localReceive()) {
            return Long.MAX_VALUE;
        }
        return RuntimeFlow.lateDataPerStreamCap(streamRuntime.initialReceiveWindow(), this.localSettings().maxFramePayload());
    }

    long recvSessionReceivedBytesInternal() {
        return this.recvSessionReceivedBytes;
    }

    void setRecvSessionReceivedBytesInternal(long value) {
        this.recvSessionReceivedBytes = value;
    }

    long recvSessionAdvertisedInternal() {
        return this.recvSessionAdvertised;
    }

    void setRecvSessionAdvertisedInternal(long value) {
        this.recvSessionAdvertised = value;
    }

    long recvSessionPendingInternal() {
        return this.recvSessionPending;
    }

    void setRecvSessionPendingInternal(long value) {
        this.recvSessionPending = value;
    }

    boolean receiveReplenishRetryLocked() {
        return this.receiveReplenishRetry;
    }

    void setReceiveReplenishRetryLocked(boolean value) {
        this.receiveReplenishRetry = value;
    }

    void addReceivedDataBytesInternal(long value) {
        this.receivedDataBytes = SessionRuntime.saturatingAdd(this.receivedDataBytes, value);
    }

    long sessionSendLimitInternal() {
        return this.sessionSendLimit;
    }

    void setSessionSendLimitInternal(long value) {
        this.sessionSendLimit = value;
    }

    void clearBlockedFrameInternal(long streamId) {
        this.clearBlockedFrameLocked(streamId);
    }

    long peerGoAwayBidiInternal() {
        return this.peerGoAwayBidi;
    }

    void setPeerGoAwayBidiInternal(long value) {
        this.peerGoAwayBidi = value;
    }

    long peerGoAwayUniInternal() {
        return this.peerGoAwayUni;
    }

    void setPeerGoAwayUniInternal(long value) {
        this.peerGoAwayUni = value;
    }

    ApplicationError peerGoAwayErrorInternal() {
        return this.peerGoAwayError;
    }

    void setPeerGoAwayErrorInternal(ApplicationError error) {
        this.peerGoAwayError = error;
    }

    ApplicationError peerCloseErrorInternal() {
        return this.peerCloseError;
    }

    void setPeerCloseErrorInternal(ApplicationError error) {
        this.peerCloseError = error;
    }

    long nextPeerBidiInternal() {
        return this.nextPeerBidi;
    }

    void setNextPeerBidiInternal(long value) {
        this.nextPeerBidi = value;
    }

    long nextPeerUniInternal() {
        return this.nextPeerUni;
    }

    void setNextPeerUniInternal(long value) {
        this.nextPeerUni = value;
    }

    long bufferedReceiveBytesInternal() {
        return this.bufferedReceiveBytes;
    }

    void setBufferedReceiveBytesInternal(long value) {
        this.bufferedReceiveBytes = value;
    }

    long bufferedReceiveStorageBytesInternal() {
        return this.bufferedReceiveStorageBytes;
    }

    void setBufferedReceiveStorageBytesInternal(long value) {
        this.bufferedReceiveStorageBytes = value;
    }

    long aggregateLateDataReceivedInternal() {
        return this.aggregateLateDataReceived;
    }

    void setAggregateLateDataReceivedInternal(long value) {
        this.aggregateLateDataReceived = value;
    }

    Deque<OutboundFrame> urgentQueueInternal() {
        return this.urgentQueue;
    }

    Deque<StreamRuntime> advisoryQueueInternal() {
        return this.advisoryQueue;
    }

    Deque<OutboundFrame> dataQueueInternal() {
        return this.dataQueue;
    }

    List<OutboundFrame> inflightBatchInternal() {
        return this.inflightBatch;
    }

    void setInflightBatchInternal(List<OutboundFrame> batch) {
        this.inflightBatch = batch == null ? Collections.emptyList() : batch;
    }

    long sessionSendLimitLocked() {
        return this.sessionSendLimit;
    }

    long sessionRemainingSendCreditLocked() {
        return RuntimeFlow.windowRemaining(
                this.sessionSendLimit,
                SessionRuntime.saturatingAdd(this.sessionSentBytes, this.sessionReservedSendBytes)
        );
    }

    void reserveSessionSendBytesLocked(int bytes) {
        if (bytes <= 0) {
            return;
        }
        this.sessionReservedSendBytes = SessionRuntime.saturatingAdd(this.sessionReservedSendBytes, bytes);
    }

    void releaseSessionReservedSendBytesLocked(int bytes) {
        if (bytes <= 0) {
            return;
        }
        this.sessionReservedSendBytes = Math.max(0L, this.sessionReservedSendBytes - (long) bytes);
    }

    long sessionQueuedDataBytesLocked() {
        return this.sessionQueuedDataBytes;
    }

    SessionStatsSurfaceSnapshot statsSurfaceSnapshotLocked() {
        return new SessionStatsSurfaceSnapshot(
                this.sentFrames,
                this.receivedFrames,
                this.sentDataBytes,
                this.receivedDataBytes,
                this.streams.size(),
                this.streamBookkeeping.acceptedStreamsLocked(),
                this.retainedOpenInfoBytes,
                this.retainedPeerReasonBytes,
                this.sessionQueuedDataBytes,
                this.sessionReservedSendBytes,
                this.writerHeldRetainedBytes
        );
    }

    SessionStatsDiagnosticSnapshot statsDiagnosticSnapshotLocked() {
        return new SessionStatsDiagnosticSnapshot(
                this.droppedPriorityUpdateCount,
                this.droppedLocalPriorityUpdateCount,
                this.coalescedTerminalSignalsCount,
                this.supersededTerminalSignalsCount,
                this.lateDataAfterCloseReadBytes,
                this.lateDataAfterResetBytes,
                this.lateDataAfterAbortBytes,
                this.visibleTerminalChurnEventCount,
                this.groupRebucketEventCount,
                this.skippedCloseOnDeadIoCount,
                this.closeFrameFlushErrorCount,
                this.hiddenStreamsRefused,
                this.hiddenStreamsReaped,
                this.hiddenUnreadBytesDiscarded
        );
    }

    SessionStatsReasonSnapshot statsReasonSnapshotLocked() {
        return new SessionStatsReasonSnapshot(
                copyReasonCounts(this.resetReasonCounts),
                this.resetReasonOverflowCount,
                copyReasonCounts(this.abortReasonCounts),
                this.abortReasonOverflowCount
        );
    }

    SessionStatsReceiveSnapshot statsReceiveSnapshotLocked() {
        return new SessionStatsReceiveSnapshot(
                this.bufferedReceiveBytes,
                this.bufferedReceiveStorageBytes,
                this.recvSessionAdvertised,
                this.recvSessionReceivedBytes,
                this.recvSessionPending
        );
    }

    SessionState stateInternal() {
        return this.state;
    }

    Negotiated negotiatedInternal() {
        return this.negotiated;
    }

    boolean closeFrameQueuedInternal() {
        return this.closeFrameQueued;
    }

    void setCloseFrameQueuedInternal(boolean value) {
        this.closeFrameQueued = value;
    }

    StreamRuntime liveStreamLocked(long streamId) {
        return streamId == 0L ? null : this.streams.get(streamId);
    }

    Iterable<StreamRuntime> liveStreamsLocked() {
        return this.streams.values();
    }

    boolean gracefulCloseActiveLocked() {
        return this.gracefulCloseActive;
    }

    void setGracefulCloseActiveInternal(boolean value) {
        this.gracefulCloseActive = value;
    }

    boolean terminalCleanupAppliedInternal() {
        return this.terminalCleanupApplied;
    }

    void setTerminalCleanupAppliedInternal(boolean value) {
        this.terminalCleanupApplied = value;
    }

    void clearKeepaliveSchedulesLockedInternal() {
        this.telemetry.clearKeepaliveSchedulesLocked();
    }

    boolean hasActivePingLockedInternal() {
        return this.telemetry.hasActivePingLocked();
    }

    long nextKeepaliveWakeNanosLocked(long nowNanos) {
        return this.telemetry.nextKeepaliveWakeNanosLocked(nowNanos);
    }

    boolean processKeepaliveScheduledWorkLocked(long nowNanos) throws IOException {
        return this.telemetry.processKeepaliveScheduledWorkLocked(nowNanos);
    }

    void emitKeepaliveTimeoutClose() throws IOException {
        this.closeWithError(
                SessionTelemetryState.keepaliveTimeoutCode(),
                SessionTelemetryState.keepaliveTimeoutReason()
        );
    }

    void closeForKeepaliveTimeout() throws IOException {
        boolean timedOut;
        synchronized (this.lock) {
            timedOut = this.telemetry.armKeepaliveTimeoutCloseLocked(System.nanoTime());
        }
        if (!timedOut) {
            return;
        }
        this.emitKeepaliveTimeoutClose();
    }

    void terminatedCountDownInternal() {
        this.terminated.countDown();
    }

    boolean closedTransportInternal() {
        return this.closedTransport;
    }

    void setClosedTransportInternal(boolean value) {
        this.closedTransport = value;
    }

    long retainedOpenInfoBytesLocked() {
        return this.retainedOpenInfoBytes;
    }

    long provisionalOpenLimitedCountLocked() {
        return this.provisionalOpenLimitedCount;
    }

    long provisionalOpenExpiredCountLocked() {
        return this.provisionalOpenExpiredCount;
    }

    SessionTelemetryState telemetryInternal() {
        return this.telemetry;
    }

    long nextLocalStreamIdLocked(boolean bidirectional) {
        return bidirectional ? this.nextLocalBidi : this.nextLocalUni;
    }

    long peerGoAwayWatermarkLocked(boolean bidirectional) {
        return bidirectional ? this.peerGoAwayBidi : this.peerGoAwayUni;
    }

    void commitLocalOpenAssignmentLocked(StreamRuntime streamRuntime, long assignedStreamId) {
        if (streamRuntime == null) {
            return;
        }
        boolean bidirectional = streamRuntime.bidirectional();
        this.localOpenTracker.removeProvisionalLocked(streamRuntime);
        if (bidirectional) {
            this.nextLocalBidi = SessionRuntime.saturatingAdd(assignedStreamId, 4L);
        } else {
            this.nextLocalUni = SessionRuntime.saturatingAdd(assignedStreamId, 4L);
        }
        this.streamBookkeeping.onLocalOpenedLocked(bidirectional);
        long peerSendLimit = bidirectional
                ? this.peerSettings().initialMaxStreamDataBidiPeerOpened()
                : this.peerSettings().initialMaxStreamDataUni();
        long recvAdvertisedLimit = bidirectional
                ? this.localSettings().initialMaxStreamDataBidiLocallyOpened()
                : 0L;
        streamRuntime.initializeLocalOpenedLocked(assignedStreamId, peerSendLimit, recvAdvertisedLimit);
        this.streams.put(assignedStreamId, streamRuntime);
        this.registerGracefulCloseBlockingLocked(streamRuntime);
        this.explicitGroupTracker.trackNewStreamLocked(
                streamRuntime,
                this.peerSettings().schedulerHints(),
                this.streams.get(streamRuntime.streamIdInternal()) == streamRuntime
        );
        this.localOpenTracker.appendUnseenLocalLocked(streamRuntime);
        this.notifyOpenWaitersLocked();
    }

    void notifyLockWaitersLocked() {
        this.notifyLockWaitersLocked(LockWaitKind.GENERAL);
    }

    void notifyAcceptWaitersLocked() {
        this.notifyLockWaitersLocked(LockWaitKind.ACCEPT);
    }

    void notifyOpenWaitersLocked() {
        this.notifyLockWaitersLocked(LockWaitKind.OPEN);
    }

    void notifyStreamWaitersLocked() {
        this.notifyLockWaitersLocked(LockWaitKind.READ_STREAM, LockWaitKind.WRITE_STREAM);
    }

    void notifyStreamReadWaitersLocked() {
        this.notifyLockWaitersLocked(LockWaitKind.READ_STREAM);
    }

    void notifyStreamWriteWaitersLocked() {
        this.notifyLockWaitersLocked(LockWaitKind.WRITE_STREAM);
    }

    void notifyWriterWaitersLocked() {
        this.notifyLockWaitersLocked(LockWaitKind.WRITER);
    }

    void notifyLifecycleWaitersLocked() {
        this.notifyLockWaitersLocked(LockWaitKind.LIFECYCLE);
    }

    void notifyTelemetryWaitersLocked() {
        this.notifyLockWaitersLocked(LockWaitKind.TELEMETRY);
    }

    private void notifyLockWaitersLocked(LockWaitKind kind) {
        if (this.lockWaiters > 0 && this.hasWaitersFor(kind)) {
            this.lock.notifyAll();
        }
    }

    private void notifyLockWaitersLocked(LockWaitKind first, LockWaitKind second) {
        if (this.lockWaiters > 0 && this.hasWaitersFor(first, second)) {
            this.lock.notifyAll();
        }
    }

    private boolean hasWaitersFor(LockWaitKind kind) {
        if (this.lockWaitersByKind[LockWaitKind.GENERAL.ordinal()] > 0) {
            return true;
        }
        return this.hasSpecificWaiterFor(kind);
    }

    private boolean hasWaitersFor(LockWaitKind first, LockWaitKind second) {
        if (this.lockWaitersByKind[LockWaitKind.GENERAL.ordinal()] > 0) {
            return true;
        }
        return this.hasSpecificWaiterFor(first) || this.hasSpecificWaiterFor(second);
    }

    private boolean hasSpecificWaiterFor(LockWaitKind kind) {
        LockWaitKind effective = kind == null ? LockWaitKind.GENERAL : kind;
        return effective == LockWaitKind.GENERAL || this.lockWaitersByKind[effective.ordinal()] > 0;
    }

    private void incrementLockWaiters(LockWaitKind kind) {
        LockWaitKind effective = kind == null ? LockWaitKind.GENERAL : kind;
        this.lockWaiters++;
        this.lockWaitersByKind[effective.ordinal()]++;
    }

    private void decrementLockWaiters(LockWaitKind kind) {
        LockWaitKind effective = kind == null ? LockWaitKind.GENERAL : kind;
        this.lockWaiters--;
        this.lockWaitersByKind[effective.ordinal()]--;
    }

    void markReadyLocked(Preface remotePreface, Negotiated negotiated, long readyAtNanos) {
        this.peerPreface = remotePreface;
        this.negotiated = negotiated;
        this.nextLocalBidi = SessionRuntime.firstLocalStreamId(negotiated.localRole(), true);
        this.nextLocalUni = SessionRuntime.firstLocalStreamId(negotiated.localRole(), false);
        this.nextPeerBidi = SessionRuntime.firstPeerStreamId(negotiated.localRole(), true);
        this.nextPeerUni = SessionRuntime.firstPeerStreamId(negotiated.localRole(), false);
        this.sessionSendLimit = negotiated.peerSettings().initialMaxData();
        this.recvSessionAdvertised = this.config.settings().initialMaxData();
        this.pingNonceState = SessionRuntime.initSessionNonceState(
                (this.localPreface.tieBreakerNonce() << 1) ^ remotePreface.tieBreakerNonce()
        );
        this.telemetry.markReadyLocked(this.localPreface.tieBreakerNonce() ^ remotePreface.tieBreakerNonce(), readyAtNanos);
        this.state = SessionState.READY;
    }

    private long nextPingNonceLocked() {
        if (this.pingNonceState == 0L) {
            this.pingNonceState = SessionRuntime.initSessionNonceState(0L);
        }
        this.pingNonceState += SESSION_NONCE_GAMMA;
        long mixed = this.pingNonceState;
        mixed = (mixed ^ mixed >>> 30) * -4658895280553007687L;
        mixed = (mixed ^ mixed >>> 27) * -7723592293110705685L;
        return mixed ^ mixed >>> 31;
    }

    BufferedOutputStream outputInternal() {
        return this.output;
    }

    Runnable readerLoopTaskInternal() {
        return this::readerLoop;
    }

    Runnable writerLoopTaskInternal() {
        return this::writerLoop;
    }

    OrdinaryBatchOrderer.RetainedBias ordinaryBatchBiasInternal() {
        return this.ordinaryBatchBias;
    }

    private void establish() throws IOException {
        this.establishmentCoordinator.establish();
    }

    enum LockWaitKind {
        GENERAL,
        ACCEPT,
        OPEN,
        READ_STREAM,
        WRITE_STREAM,
        WRITER,
        LIFECYCLE,
        TELEMETRY
    }

    enum PayloadOwnership {
        BORROWED,
        OWNED
    }

    private static final class ExtPriorityUpdateParse {
        private static final ExtPriorityUpdateParse IGNORED = new ExtPriorityUpdateParse(false, false, null);
        private static final ExtPriorityUpdateParse DROPPED = new ExtPriorityUpdateParse(true, true, null);

        private final boolean priorityUpdate;
        private final boolean dropped;
        private final FrameCodec.ParsedPriorityUpdate update;

        private ExtPriorityUpdateParse(boolean priorityUpdate, boolean dropped, FrameCodec.ParsedPriorityUpdate update) {
            this.priorityUpdate = priorityUpdate;
            this.dropped = dropped;
            this.update = update;
        }

        private static ExtPriorityUpdateParse ignoredResult() {
            return IGNORED;
        }

        private static ExtPriorityUpdateParse droppedResult() {
            return DROPPED;
        }

        private static ExtPriorityUpdateParse acceptedResult(FrameCodec.ParsedPriorityUpdate update) {
            return new ExtPriorityUpdateParse(true, false, update);
        }

        private boolean priorityUpdate() {
            return priorityUpdate;
        }

        private boolean dropped() {
            return dropped;
        }

        private FrameCodec.ParsedPriorityUpdate update() {
            return update;
        }
    }

    private static final class ReadLoopProtocolTask {
        private final OutboundFrame outboundFrame;

        private ReadLoopProtocolTask(OutboundFrame outboundFrame) {
            this.outboundFrame = outboundFrame;
        }
    }

    static final class PendingPing {
        private final long startedAtNanos;
        private final byte[] payload;
        private boolean done;
        private long completedAtNanos;
        private IOException error;
        private int waiters;

        PendingPing(long startedAtNanos, byte[] payload) {
            this.startedAtNanos = startedAtNanos;
            this.payload = payload;
        }

        long startedAtNanos() {
            return this.startedAtNanos;
        }

        byte[] payload() {
            return this.payload;
        }

        synchronized boolean done() {
            return this.done;
        }

        synchronized long completedAtNanos() {
            return this.completedAtNanos;
        }

        synchronized IOException error() {
            return this.error;
        }

        synchronized void waitForCompletion(TimeoutBudget budget) throws InterruptedException {
            while (!this.done) {
                if (!budget.bounded()) {
                    this.waitUntilNotified();
                    continue;
                }
                long remainingNanos = budget.remainingNanos();
                if (remainingNanos <= 0L) {
                    return;
                }
                this.waitUntilNotifiedNanos(remainingNanos);
            }
        }

        private void waitUntilNotified() throws InterruptedException {
            this.waiters++;
            try {
                this.wait();
            } finally {
                this.waiters--;
            }
        }

        private void waitUntilNotifiedNanos(long waitNanos) throws InterruptedException {
            long boundedNanos = Math.max(1L, waitNanos);
            long millis = TimeUnit.NANOSECONDS.toMillis(boundedNanos);
            int nanos = (int) (boundedNanos - TimeUnit.MILLISECONDS.toNanos(millis));
            this.waiters++;
            try {
                this.wait(millis, nanos);
            } finally {
                this.waiters--;
            }
        }

        synchronized void complete(IOException error, long completedAtNanos) {
            this.error = error;
            this.done = true;
            this.completedAtNanos = completedAtNanos;
            if (this.waiters > 0) {
                this.notifyAll();
            }
        }
    }

    static final class ReadyBatch {
        private final List<OutboundFrame> frames;
        private final boolean ordinary;
        private final long batchCost;
        private final long costLimit;

        ReadyBatch(List<OutboundFrame> frames, boolean ordinary, long batchCost, long costLimit) {
            this.frames = frames;
            this.ordinary = ordinary;
            this.batchCost = batchCost;
            this.costLimit = costLimit;
        }

        List<OutboundFrame> frames() {
            return frames;
        }

        boolean ordinary() {
            return ordinary;
        }

        long batchCost() {
            return batchCost;
        }

        long costLimit() {
            return costLimit;
        }
    }

    private static final class TerminalControlMerge {
        private final boolean coalesced;
        private final boolean superseded;
        private final long replacedBytes;

        private TerminalControlMerge(boolean coalesced, boolean superseded, long replacedBytes) {
            this.coalesced = coalesced;
            this.superseded = superseded;
            this.replacedBytes = replacedBytes;
        }

        private static TerminalControlMerge none() {
            return new TerminalControlMerge(false, false, 0L);
        }

        private boolean coalesced() {
            return coalesced;
        }

        private boolean superseded() {
            return superseded;
        }

        private long replacedBytes() {
            return replacedBytes;
        }
    }

    static final class OutboundFrame {
        private final FrameCodec.Frame frame;
        private final StreamRuntime stream;
        private final int dataBytes;
        private final boolean openingFrame;
        private final boolean preserveAfterSendClose;
        private final byte[] payloadPrefix;
        private final byte[] payloadBytes;
        private final int payloadOffset;
        private final int payloadLength;
        private final byte[][] payloadParts;
        private final int payloadPartIndex;
        private final int payloadPartOffset;

        OutboundFrame(FrameCodec.Frame frame,
                      StreamRuntime stream,
                      int dataBytes,
                      boolean openingFrame,
                      boolean preserveAfterSendClose,
                      byte[] payloadPrefix,
                      byte[] payloadBytes,
                      int payloadOffset,
                      int payloadLength,
                      byte[][] payloadParts,
                      int payloadPartIndex,
                      int payloadPartOffset) {
            this.frame = frame;
            this.stream = stream;
            this.dataBytes = dataBytes;
            this.openingFrame = openingFrame;
            this.preserveAfterSendClose = preserveAfterSendClose;
            this.payloadPrefix = payloadPrefix;
            this.payloadBytes = payloadBytes;
            this.payloadOffset = payloadOffset;
            this.payloadLength = payloadLength;
            this.payloadParts = payloadParts;
            this.payloadPartIndex = payloadPartIndex;
            this.payloadPartOffset = payloadPartOffset;
        }

        OutboundFrame(FrameCodec.Frame frame, StreamRuntime stream, int dataBytes, boolean openingFrame, boolean preserveAfterSendClose) {
            this(frame, stream, dataBytes, openingFrame, preserveAfterSendClose, null, frame.payload(), 0, frame.payload().length, null, 0, 0);
        }

        FrameCodec.Frame frame() {
            return frame;
        }

        StreamRuntime stream() {
            return stream;
        }

        int dataBytes() {
            return dataBytes;
        }

        boolean openingFrame() {
            return openingFrame;
        }

        boolean preserveAfterSendClose() {
            return preserveAfterSendClose;
        }

        byte[] payloadPrefix() {
            return payloadPrefix;
        }

        byte[] payloadBytes() {
            return payloadBytes;
        }

        int payloadOffset() {
            return payloadOffset;
        }

        int payloadLength() {
            return payloadLength;
        }

        byte[][] payloadParts() {
            return payloadParts;
        }

        int payloadPartIndex() {
            return payloadPartIndex;
        }

        int payloadPartOffset() {
            return payloadPartOffset;
        }

        private boolean hasPayloadParts() {
            return this.payloadParts != null && this.payloadParts.length > 0 && this.payloadLength > 0;
        }

        OutboundFrame withPreserveAfterSendClose(boolean preserveAfterSendClose) {
            if (this.preserveAfterSendClose == preserveAfterSendClose) {
                return this;
            }
            return new OutboundFrame(this.frame, this.stream, this.dataBytes, this.openingFrame, preserveAfterSendClose, this.payloadPrefix, this.payloadBytes, this.payloadOffset, this.payloadLength, this.payloadParts, this.payloadPartIndex, this.payloadPartOffset);
        }
    }

}
