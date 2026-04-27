package io.zmux.internal;

import io.zmux.*;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

final class SessionTelemetryState {
    private static final Duration DEFAULT_KEEPALIVE_TIMEOUT_MIN = Duration.ofSeconds(5L);
    private static final Duration DEFAULT_KEEPALIVE_TIMEOUT_MAX = Duration.ofSeconds(60L);
    private static final Duration RTT_ADAPTIVE_SLACK = Duration.ofMillis(50L);
    private static final int MIN_SEND_RATE_SAMPLE_BYTES = 4 << 10;
    private static final long MIN_SEND_RATE_SAMPLE_DURATION_NANOS = TimeUnit.MILLISECONDS.toNanos(25L);
    private static final long KEEPALIVE_JITTER_GAMMA = -7046029254386353131L;
    private static final int PING_NONCE_BYTES = Long.BYTES;
    private static final long PING_PAYLOAD_HASH_OFFSET = 0xcbf29ce484222325L;
    private static final long PING_PAYLOAD_HASH_PRIME = 0x100000001b3L;
    private static final AtomicLong KEEPALIVE_JITTER_COUNTER = new AtomicLong();
    private static final String KEEPALIVE_TIMEOUT_REASON = "zmux: keepalive timeout";
    private final Owner owner;
    private final ZmuxConfig config;
    private final long timeOriginNanos;
    private final Instant timeOriginInstant;
    private long closeCompletionTimeoutCount;
    private long gracefulCloseTimeoutCount;
    private long keepaliveTimeoutCount;
    private boolean closeCompletionTimeoutRecorded;
    private SessionRuntime.PendingPing activePing;
    private long lastPingRttNanos;
    private long sendRateEstimateBytesPerSecond;
    private long lastInboundFrameAtNanos;
    private long lastControlProgressAtNanos;
    private long lastTransportWriteAtNanos;
    private long lastStreamProgressAtNanos;
    private long lastApplicationProgressAtNanos;
    private long lastPingSentAtNanos;
    private long lastPongAtNanos;
    private long flushCount;
    private long lastFlushAtNanos;
    private int lastFlushFrames;
    private long lastFlushBytes;
    private long blockedWriteTotalNanos;
    private long lastOpenLatencyNanos;
    private long readIdlePingDueAtNanos;
    private long writeIdlePingDueAtNanos;
    private long maxPingDueAtNanos;
    private long keepaliveJitterState;
    private long canceledPingNonce;
    private long canceledPingHash;
    private int canceledPingLength;
    private boolean canceledPingSet;

    SessionTelemetryState(Owner owner, ZmuxConfig config, long timeOriginNanos, Instant timeOriginInstant) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.config = Objects.requireNonNull(config, "config");
        this.timeOriginNanos = timeOriginNanos;
        this.timeOriginInstant = Objects.requireNonNull(timeOriginInstant, "timeOriginInstant");
    }

    static long keepaliveTimeoutCode() {
        return ErrorCode.IDLE_TIMEOUT.code();
    }

    static String keepaliveTimeoutReason() {
        return KEEPALIVE_TIMEOUT_REASON;
    }

    private static long positiveNanos(Duration duration) {
        return SessionRuntime.durationToPositiveNanosSaturated(duration);
    }

    private static long adaptiveRttTimeout(
            long lastRttNanos,
            long baseTimeoutNanos,
            long maxTimeoutNanos,
            int multiplier,
            long slackNanos
    ) {
        return SessionRuntime.adaptiveRttTimeout(
                lastRttNanos,
                baseTimeoutNanos,
                maxTimeoutNanos,
                multiplier,
                slackNanos
        );
    }

    private static long averageFloor(long left, long right) {
        if (left <= right) {
            return left + (right - left) / 2L;
        }
        return right + (left - right) / 2L;
    }

    private static long pingPayloadNonce(byte[] payload) {
        long nonce = 0L;
        for (int i = 0; i < PING_NONCE_BYTES; i++) {
            nonce = nonce << 8 | payload[i] & 0xffL;
        }
        return nonce;
    }

    private static long pingPayloadHash(byte[] payload) {
        long hash = PING_PAYLOAD_HASH_OFFSET;
        for (byte value : payload) {
            hash ^= value & 0xffL;
            hash *= PING_PAYLOAD_HASH_PRIME;
        }
        return hash;
    }

    private static long initKeepaliveJitterState(long seed) {
        if (seed != 0L) {
            return seed;
        }
        return KEEPALIVE_JITTER_COUNTER.addAndGet(-7046029254386353131L);
    }

    private static long earliestNonZeroNanos(long first, long second, long third) {
        return earlierNonZeroNanos(earlierNonZeroNanos(first, second), third);
    }

    private static long earlierNonZeroNanos(long first, long second) {
        if (first <= 0L) {
            return Math.max(0L, second);
        }
        if (second <= 0L) {
            return first;
        }
        return Math.min(first, second);
    }

    SessionStats.KeepaliveStats keepaliveStatsLocked() {
        boolean terminal = this.owner.state().terminal();
        long keepaliveIntervalNanos = terminal ? 0L : this.keepaliveIntervalNanosLocked();
        long keepaliveMaxPingIntervalNanos = terminal ? 0L : this.keepaliveMaxPingIntervalNanosLocked();
        long keepaliveTimeoutNanos = this.effectiveKeepaliveTimeoutNanosLocked();
        boolean pingOutstanding = !terminal && this.activePing != null;
        boolean pingStalled = pingOutstanding
                && keepaliveTimeoutNanos > 0L
                && RuntimeFlow.elapsedExceeds(
                System.nanoTime(),
                this.activePing.startedAtNanos(),
                keepaliveTimeoutNanos / 2L
        );
        return new SessionStats.KeepaliveStats(
                keepaliveIntervalNanos > 0L,
                keepaliveIntervalNanos,
                keepaliveMaxPingIntervalNanos,
                keepaliveTimeoutNanos,
                pingOutstanding,
                pingStalled,
                this.lastPingRttNanos,
                this.sendRateEstimateBytesPerSecond
        );
    }

    SessionStats.ProgressStats progressStatsLocked() {
        boolean terminal = this.owner.state().terminal();
        return new SessionStats.ProgressStats(
                this.instantForNanosLocked(this.lastInboundFrameAtNanos),
                this.instantForNanosLocked(this.lastControlProgressAtNanos),
                this.instantForNanosLocked(this.lastTransportWriteAtNanos),
                this.instantForNanosLocked(this.lastStreamProgressAtNanos),
                this.instantForNanosLocked(this.lastApplicationProgressAtNanos),
                terminal ? null : this.instantForNanosLocked(this.effectiveLastPingSentAtNanosLocked()),
                terminal ? null : this.instantForNanosLocked(this.lastPongAtNanos)
        );
    }

    SessionStats.FlushStats flushStatsLocked() {
        return new SessionStats.FlushStats(
                this.flushCount,
                this.instantForNanosLocked(this.lastFlushAtNanos),
                this.lastFlushFrames,
                this.lastFlushBytes
        );
    }

    long blockedWriteTotalNanosLocked() {
        return this.blockedWriteTotalNanos;
    }

    long lastOpenLatencyNanosLocked() {
        return this.lastOpenLatencyNanos;
    }

    long closeCompletionTimeoutCountLocked() {
        return this.closeCompletionTimeoutCount;
    }

    long gracefulCloseTimeoutCountLocked() {
        return this.gracefulCloseTimeoutCount;
    }

    long keepaliveTimeoutCountLocked() {
        return this.keepaliveTimeoutCount;
    }

    long lastPingRttNanos() {
        return this.lastPingRttNanos;
    }

    boolean hasActivePingLocked() {
        return this.activePing != null;
    }

    Duration ping(byte[] payload, Duration timeout) throws IOException, InterruptedException {
        TimeoutBudget budget = TimeoutBudget.fromTimeout(timeout);
        if (budget.expired()) {
            throw new PingTimeoutException();
        }
        SessionRuntime.PendingPing pendingPing;
        synchronized (this.owner.lock()) {
            while (true) {
                if (!this.owner.allowLocalNonCloseControlLocked()) {
                    throw this.owner.sessionErrorLocked();
                }
                if (this.activePing == null) {
                    if (budget.expired()) {
                        throw new PingTimeoutException();
                    }
                    break;
                }
                if (!budget.bounded()) {
                    try {
                        this.owner.waitOnLock();
                    } catch (InterruptedException interrupted) {
                        throw SessionRuntime.interrupted(
                                "zmux: interrupted while waiting for ping slot",
                                "ping",
                                ZmuxErrorScope.SESSION,
                                ZmuxErrorDirection.BOTH,
                                interrupted
                        );
                    }
                    continue;
                }
                long remainingNanos = budget.remainingNanos();
                if (remainingNanos <= 0L) {
                    throw new PingTimeoutException();
                }
                try {
                    this.owner.waitOnLockNanos(remainingNanos);
                } catch (InterruptedException interrupted) {
                    throw SessionRuntime.interrupted(
                            "zmux: interrupted while waiting for ping slot",
                            "ping",
                            ZmuxErrorScope.SESSION,
                            ZmuxErrorDirection.BOTH,
                            interrupted
                    );
                }
            }
            byte[] pingPayload = this.owner.buildPingPayloadLocked(payload);
            long startedAtNanos = System.nanoTime();
            pendingPing = new SessionRuntime.PendingPing(startedAtNanos, pingPayload);
            this.activePing = pendingPing;
            this.notePingSentLocked(startedAtNanos);
            this.resetMaxPingDueLocked(startedAtNanos);
            this.queueActivePingLocked(pingPayload);
        }
        InterruptedException waitError = null;
        try {
            pendingPing.waitForCompletion(budget);
        } catch (InterruptedException interrupted) {
            waitError = SessionRuntime.interrupted(
                    "zmux: interrupted while waiting for ping response",
                    "ping",
                    ZmuxErrorScope.SESSION,
                    ZmuxErrorDirection.BOTH,
                    interrupted
            );
        }
        if (waitError != null) {
            this.cancelActivePingIfCurrent(pendingPing);
            throw waitError;
        }
        if (!pendingPing.done()) {
            PingTimeoutException timeoutError = new PingTimeoutException();
            this.completeActivePingIfCurrent(pendingPing, timeoutError);
            throw timeoutError;
        }
        if (pendingPing.error() != null) {
            throw pendingPing.error();
        }
        return Duration.ofNanos(RuntimeFlow.elapsedNanos(
                pendingPing.completedAtNanos(),
                pendingPing.startedAtNanos()
        ));
    }

    long outstandingPingBytesLocked() {
        SessionRuntime.PendingPing ping = this.activePing;
        if (ping == null || ping.payload() == null) {
            return 0L;
        }
        return ping.payload().length;
    }

    long effectiveLastPingSentAtNanosLocked() {
        if (this.lastPingSentAtNanos > 0L) {
            return this.lastPingSentAtNanos;
        }
        return this.activePing == null ? 0L : this.activePing.startedAtNanos();
    }

    Instant instantForNanosLocked(long eventNanos) {
        if (eventNanos == 0L) {
            return null;
        }
        long deltaNanos = eventNanos - this.timeOriginNanos;
        try {
            return this.timeOriginInstant.plusNanos(deltaNanos);
        } catch (ArithmeticException ignored) {
            return deltaNanos < 0L ? Instant.MIN : Instant.MAX;
        }
    }

    void recordCloseCompletionTimeoutLocked() {
        if (this.closeCompletionTimeoutRecorded) {
            return;
        }
        this.closeCompletionTimeoutRecorded = true;
        this.closeCompletionTimeoutCount = SessionRuntime.saturatingAdd(this.closeCompletionTimeoutCount, 1L);
    }

    void recordGracefulCloseTimeoutLocked() {
        this.gracefulCloseTimeoutCount = SessionRuntime.saturatingAdd(this.gracefulCloseTimeoutCount, 1L);
    }

    void recordKeepaliveTimeoutLocked() {
        this.keepaliveTimeoutCount = SessionRuntime.saturatingAdd(this.keepaliveTimeoutCount, 1L);
    }

    void failActivePingLocked(IOException error) {
        if (this.activePing == null) {
            return;
        }
        this.completeActivePingLocked(error, System.nanoTime());
    }

    private void completeActivePingIfCurrent(SessionRuntime.PendingPing pendingPing, IOException error) {
        synchronized (this.owner.lock()) {
            if (this.activePing != pendingPing) {
                return;
            }
            this.completeActivePingLocked(error, System.nanoTime());
        }
    }

    private void cancelActivePingIfCurrent(SessionRuntime.PendingPing pendingPing) {
        synchronized (this.owner.lock()) {
            if (this.activePing != pendingPing) {
                return;
            }
            if (this.shouldRetainCanceledPingLocked(pendingPing)) {
                this.retainCanceledPingLocked(pendingPing.payload());
            }
            this.completeActivePingLocked(null, System.nanoTime());
        }
    }

    void completeActivePingLocked(IOException error, long completedAtNanos) {
        SessionRuntime.PendingPing pendingPing = this.activePing;
        if (pendingPing == null) {
            return;
        }
        long previousTracked = this.owner.trackedSessionMemoryLocked();
        if (error != null && this.shouldRetainCanceledPingLocked(pendingPing)) {
            this.retainCanceledPingLocked(pendingPing.payload());
        }
        this.activePing = null;
        if (this.keepaliveEnabledLocked()) {
            this.resetReadIdlePingDueLocked(completedAtNanos);
            this.resetWriteIdlePingDueLocked(completedAtNanos);
        }
        pendingPing.complete(error, completedAtNanos);
        if (this.owner.sessionMemoryWakeNeededLocked(previousTracked)) {
            this.owner.notifyStreamWriteWaitersLocked();
        }
        this.owner.notifyTelemetryWaitersLocked();
        if (this.keepaliveEnabledLocked()) {
            this.owner.notifyWriterWaitersLocked();
        }
    }

    void beginKeepalivePingLocked(long nowNanos) throws IOException {
        if (!this.owner.allowLocalNonCloseControlLocked() || this.activePing != null) {
            return;
        }
        byte[] payload = this.owner.buildPingPayloadLocked(null);
        this.activePing = new SessionRuntime.PendingPing(nowNanos, payload);
        this.notePingSentLocked(nowNanos);
        this.resetMaxPingDueLocked(nowNanos);
        this.queueActivePingLocked(payload);
    }

    private void queueActivePingLocked(byte[] payload) throws IOException {
        SessionRuntime.PendingPing pendingPing = this.activePing;
        try {
            this.owner.enqueuePingLocked(payload);
        } catch (IOException error) {
            this.completeActivePingLocked(error, System.nanoTime());
            throw error;
        }
        if (pendingPing != null && pendingPing.payload() == payload) {
            pendingPing.markQueued();
        }
        this.owner.notifyWriterWaitersLocked();
    }

    boolean processKeepaliveScheduledWorkLocked(long nowNanos) throws IOException {
        if (!this.keepaliveEnabledLocked()
                || this.owner.state().terminal()
                || this.owner.state() == SessionState.CLOSING
                || this.owner.closeFrameQueued()) {
            return false;
        }
        if (this.activePing != null) {
            return this.armKeepaliveTimeoutCloseLocked(nowNanos);
        }
        this.ensureKeepaliveSchedulesLocked(nowNanos);
        long nextDueAtNanos = this.nextKeepaliveDueAtNanosLocked();
        if (nextDueAtNanos == 0L || nowNanos < nextDueAtNanos) {
            return false;
        }
        this.beginKeepalivePingLocked(nowNanos);
        return false;
    }

    long nextKeepaliveWakeNanosLocked(long nowNanos) {
        if (!this.keepaliveEnabledLocked()
                || this.owner.state().terminal()
                || this.owner.state() == SessionState.CLOSING
                || this.owner.closeFrameQueued()) {
            return 0L;
        }
        if (this.activePing != null) {
            long timeoutNanos = this.effectiveKeepaliveTimeoutNanosLocked();
            if (timeoutNanos <= 0L) {
                return Math.max(1L, this.keepaliveIntervalNanosLocked());
            }
            return TimeoutBudget.positiveRemainingNanosUntil(
                    SessionRuntime.saturatingAdd(this.activePing.startedAtNanos(), timeoutNanos),
                    nowNanos
            );
        }
        this.ensureKeepaliveSchedulesLocked(nowNanos);
        long nextDueAtNanos = this.nextKeepaliveDueAtNanosLocked();
        if (nextDueAtNanos == 0L) {
            long intervalNanos = this.keepaliveIntervalNanosLocked();
            return Math.max(0L, intervalNanos);
        }
        return TimeoutBudget.positiveRemainingNanosUntil(nextDueAtNanos, nowNanos);
    }

    boolean armKeepaliveTimeoutCloseLocked(long nowNanos) {
        if (!this.keepaliveEnabledLocked()
                || this.activePing == null
                || this.owner.state().terminal()
                || this.owner.state() == SessionState.CLOSING
                || this.owner.closeFrameQueued()) {
            return false;
        }
        long timeoutNanos = this.effectiveKeepaliveTimeoutNanosLocked();
        long timeoutDueAtNanos = SessionRuntime.saturatingAdd(this.activePing.startedAtNanos(), timeoutNanos);
        if (timeoutNanos <= 0L || TimeoutBudget.remainingNanosUntil(timeoutDueAtNanos, nowNanos) >= 0L) {
            return false;
        }
        this.recordKeepaliveTimeoutLocked();
        this.failActivePingLocked(new ApplicationError(
                SessionTelemetryState.keepaliveTimeoutCode(),
                SessionTelemetryState.keepaliveTimeoutReason(),
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.TIMEOUT
        ));
        return true;
    }

    boolean keepaliveEnabledLocked() {
        return this.keepaliveIntervalNanosLocked() > 0L;
    }

    long keepaliveIntervalNanosLocked() {
        return positiveNanos(this.config.keepaliveInterval());
    }

    long keepaliveMaxPingIntervalNanosLocked() {
        return positiveNanos(this.config.keepaliveMaxPingInterval());
    }

    long effectiveKeepaliveTimeoutNanosLocked() {
        long configuredTimeoutNanos = positiveNanos(this.config.keepaliveTimeout());
        if (configuredTimeoutNanos > 0L) {
            long adaptiveFloorNanos = SessionRuntime.adaptiveRttFloor(
                    this.lastPingRttNanos,
                    4,
                    positiveNanos(RTT_ADAPTIVE_SLACK)
            );
            return Math.max(configuredTimeoutNanos, adaptiveFloorNanos);
        }
        long intervalNanos = this.keepaliveIntervalNanosLocked();
        if (intervalNanos <= 0L) {
            return 0L;
        }
        long defaultBaseTimeoutNanos = Math.max(
                RuntimeFlow.saturatingMultiply(intervalNanos, 2L),
                positiveNanos(DEFAULT_KEEPALIVE_TIMEOUT_MIN)
        );
        return adaptiveRttTimeout(
                this.lastPingRttNanos,
                defaultBaseTimeoutNanos,
                positiveNanos(DEFAULT_KEEPALIVE_TIMEOUT_MAX),
                4,
                positiveNanos(RTT_ADAPTIVE_SLACK)
        );
    }

    void noteInboundFrameLocked(long nowNanos) {
        this.lastInboundFrameAtNanos = nowNanos;
        this.lastControlProgressAtNanos = nowNanos;
        if (this.keepaliveEnabledLocked()) {
            this.resetReadIdlePingDueLocked(nowNanos);
            this.owner.notifyWriterWaitersLocked();
        }
    }

    void noteTransportWriteCompletedLocked(long nowNanos) {
        this.lastTransportWriteAtNanos = nowNanos;
        if (!this.keepaliveEnabledLocked()) {
            return;
        }
        this.resetWriteIdlePingDueLocked(nowNanos);
        this.owner.notifyWriterWaitersLocked();
    }

    void noteStreamProgressLocked(long nowNanos) {
        this.lastStreamProgressAtNanos = nowNanos;
    }

    void noteApplicationProgressLocked(long nowNanos) {
        this.lastApplicationProgressAtNanos = nowNanos;
    }

    void notePingSentLocked(long nowNanos) {
        this.lastPingSentAtNanos = nowNanos;
    }

    long sendRateEstimateLocked() {
        return this.sendRateEstimateBytesPerSecond;
    }

    void noteCompletedWriteBatchLocked(int batchFrames, long batchBytes, long startedAtNanos, long completedAtNanos) {
        long writeDurationNanos = RuntimeFlow.elapsedNanos(completedAtNanos, startedAtNanos);
        long finishedAtNanos = writeDurationNanos == 0L ? startedAtNanos : completedAtNanos;
        this.noteTransportWriteCompletedLocked(finishedAtNanos);
        this.flushCount = SessionRuntime.saturatingAdd(this.flushCount, 1L);
        this.lastFlushAtNanos = finishedAtNanos;
        this.lastFlushFrames = Math.max(0, batchFrames);
        this.lastFlushBytes = Math.max(0L, batchBytes);
        this.noteSendRateEstimateLocked(batchBytes, writeDurationNanos);
    }

    void noteBlockedWriteLocked(long blockedNanos) {
        if (blockedNanos <= 0L) {
            return;
        }
        this.blockedWriteTotalNanos = SessionRuntime.saturatingAdd(this.blockedWriteTotalNanos, blockedNanos);
    }

    void noteOpenCommitLocked(long createdAtNanos, long nowNanos) {
        if (createdAtNanos <= 0L) {
            return;
        }
        this.lastOpenLatencyNanos = RuntimeFlow.elapsedNanos(nowNanos, createdAtNanos);
    }

    void noteSendRateEstimateLocked(long bytes, long writeDurationNanos) {
        if (bytes <= 0L || writeDurationNanos <= 0L) {
            return;
        }
        if (bytes < MIN_SEND_RATE_SAMPLE_BYTES && writeDurationNanos < MIN_SEND_RATE_SAMPLE_DURATION_NANOS) {
            return;
        }
        long sample = this.bytesPerSecondSample(bytes, writeDurationNanos);
        if (sample <= 0L) {
            sample = 1L;
        }
        if (this.sendRateEstimateBytesPerSecond == 0L) {
            this.sendRateEstimateBytesPerSecond = sample;
            return;
        }
        this.sendRateEstimateBytesPerSecond = averageFloor(this.sendRateEstimateBytesPerSecond, sample);
    }

    void markReadyLocked(long jitterSeed, long readyAtNanos) {
        this.keepaliveJitterState = initKeepaliveJitterState(jitterSeed);
        this.lastInboundFrameAtNanos = readyAtNanos;
        this.lastControlProgressAtNanos = readyAtNanos;
        this.lastTransportWriteAtNanos = readyAtNanos;
        this.resetKeepaliveSchedulesLocked(readyAtNanos);
    }

    void clearKeepaliveSchedulesLocked() {
        this.readIdlePingDueAtNanos = 0L;
        this.writeIdlePingDueAtNanos = 0L;
        this.maxPingDueAtNanos = 0L;
    }

    void clearTerminalKeepaliveStateLocked() {
        this.clearKeepaliveSchedulesLocked();
        this.clearCanceledPingLocked();
        this.lastPingSentAtNanos = 0L;
        this.lastPongAtNanos = 0L;
        this.lastPingRttNanos = 0L;
    }

    void resetKeepaliveSchedulesLocked(long nowNanos) {
        this.resetReadIdlePingDueLocked(nowNanos);
        this.resetWriteIdlePingDueLocked(nowNanos);
        this.resetMaxPingDueLocked(nowNanos);
    }

    void ensureKeepaliveSchedulesLocked(long nowNanos) {
        if (!this.keepaliveEnabledLocked()) {
            this.clearKeepaliveSchedulesLocked();
            return;
        }
        if (this.readIdlePingDueAtNanos == 0L) {
            this.resetReadIdlePingDueLocked(this.lastInboundFrameAtNanos == 0L ? nowNanos : this.lastInboundFrameAtNanos);
        }
        if (this.writeIdlePingDueAtNanos == 0L) {
            this.resetWriteIdlePingDueLocked(this.lastTransportWriteAtNanos == 0L ? nowNanos : this.lastTransportWriteAtNanos);
        }
        if (this.maxPingDueAtNanos == 0L) {
            this.resetMaxPingDueLocked(nowNanos);
        }
    }

    void resetReadIdlePingDueLocked(long nowNanos) {
        long intervalNanos = this.keepaliveIntervalNanosLocked();
        if (intervalNanos <= 0L) {
            this.readIdlePingDueAtNanos = 0L;
            return;
        }
        this.readIdlePingDueAtNanos = SessionRuntime.saturatingAdd(
                nowNanos,
                this.keepaliveLeadJitteredDelayNanos(intervalNanos)
        );
    }

    void resetWriteIdlePingDueLocked(long nowNanos) {
        long intervalNanos = this.keepaliveIntervalNanosLocked();
        if (intervalNanos <= 0L) {
            this.writeIdlePingDueAtNanos = 0L;
            return;
        }
        this.writeIdlePingDueAtNanos = SessionRuntime.saturatingAdd(
                nowNanos,
                this.keepaliveLeadJitteredDelayNanos(intervalNanos)
        );
    }

    void resetMaxPingDueLocked(long nowNanos) {
        long intervalNanos = this.keepaliveIntervalNanosLocked();
        long maxPingIntervalNanos = this.keepaliveMaxPingIntervalNanosLocked();
        if (intervalNanos <= 0L || maxPingIntervalNanos <= 0L) {
            this.maxPingDueAtNanos = 0L;
            return;
        }
        this.maxPingDueAtNanos = SessionRuntime.saturatingAdd(
                nowNanos,
                this.keepaliveLeadJitteredDelayNanos(maxPingIntervalNanos)
        );
    }

    long nextKeepaliveJitterNanos(long intervalNanos) {
        long maxJitterNanos = intervalNanos / 8L;
        if (maxJitterNanos <= 0L) {
            return 0L;
        }
        return Long.remainderUnsigned(this.nextKeepaliveJitterValue(), maxJitterNanos + 1L);
    }

    long keepaliveLeadJitteredDelayNanos(long intervalNanos) {
        if (intervalNanos <= 0L) {
            return 0L;
        }
        long jitterNanos = this.nextKeepaliveJitterNanos(intervalNanos);
        long delayNanos = intervalNanos - jitterNanos;
        return delayNanos <= 0L ? intervalNanos : delayNanos;
    }

    long nextKeepaliveJitterValue() {
        long state = this.keepaliveJitterState;
        if (state == 0L) {
            state = initKeepaliveJitterState(0L);
        }
        state += KEEPALIVE_JITTER_GAMMA;
        this.keepaliveJitterState = state;
        long mixed = state;
        mixed = (mixed ^ mixed >>> 30) * -4658895280553007687L;
        mixed = (mixed ^ mixed >>> 27) * -7723592293110705685L;
        return mixed ^ mixed >>> 31;
    }

    boolean handlePongLocked(byte[] payload, long nowNanos) {
        this.lastPongAtNanos = nowNanos;
        if (this.activePing == null || !Arrays.equals(payload, this.activePing.payload())) {
            if (!this.canceledPingMatches(payload)) {
                return false;
            }
            this.clearCanceledPingLocked();
            return true;
        }
        this.lastPingRttNanos = RuntimeFlow.elapsedNanos(nowNanos, this.activePing.startedAtNanos());
        this.completeActivePingLocked(null, nowNanos);
        return true;
    }

    private boolean shouldRetainCanceledPingLocked(SessionRuntime.PendingPing pendingPing) {
        return pendingPing.queued()
                && !this.owner.closeFrameQueued()
                && this.owner.state() != SessionState.CLOSING
                && !this.owner.state().terminal();
    }

    private void retainCanceledPingLocked(byte[] payload) {
        if (payload == null || payload.length < PING_NONCE_BYTES) {
            this.clearCanceledPingLocked();
            return;
        }
        this.canceledPingNonce = pingPayloadNonce(payload);
        this.canceledPingHash = pingPayloadHash(payload);
        this.canceledPingLength = payload.length;
        this.canceledPingSet = true;
    }

    private boolean canceledPingMatches(byte[] payload) {
        return this.canceledPingSet
                && payload != null
                && payload.length == this.canceledPingLength
                && payload.length >= PING_NONCE_BYTES
                && pingPayloadNonce(payload) == this.canceledPingNonce
                && pingPayloadHash(payload) == this.canceledPingHash;
    }

    private void clearCanceledPingLocked() {
        this.canceledPingNonce = 0L;
        this.canceledPingHash = 0L;
        this.canceledPingLength = 0;
        this.canceledPingSet = false;
    }

    private long bytesPerSecondSample(long bytes, long writeDurationNanos) {
        return RuntimeFlow.saturatingMulDivFloor(bytes, TimeUnit.SECONDS.toNanos(1L), writeDurationNanos);
    }

    private long nextKeepaliveDueAtNanosLocked() {
        return earliestNonZeroNanos(this.readIdlePingDueAtNanos, this.writeIdlePingDueAtNanos, this.maxPingDueAtNanos);
    }

    interface Owner {
        Object lock();

        SessionState state();

        boolean closeFrameQueued();

        IOException sessionErrorLocked();

        byte[] buildPingPayloadLocked(byte[] payload) throws IOException;

        void enqueuePingLocked(byte[] payload) throws IOException;

        void notifyTelemetryWaitersLocked();

        void notifyWriterWaitersLocked();

        void notifyStreamWriteWaitersLocked();

        long trackedSessionMemoryLocked();

        boolean sessionMemoryWakeNeededLocked(long previousTracked);

        void waitOnLock() throws InterruptedException;

        void waitOnLockNanos(long waitNanos) throws InterruptedException;

        boolean allowLocalNonCloseControlLocked();
    }

}
