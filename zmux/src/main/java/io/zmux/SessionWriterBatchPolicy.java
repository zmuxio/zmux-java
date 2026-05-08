package io.zmux;


import java.util.List;
import java.util.concurrent.TimeUnit;

final class SessionWriterBatchPolicy {
    private static final long LATENCY_BATCH_COST_MULTIPLIER = 2L;
    private static final long DEFAULT_BATCH_COST_MULTIPLIER = 4L;
    private static final long BULK_BATCH_COST_MULTIPLIER = 8L;
    private static final long LATENCY_SPARSE_COALESCE_NANOS = TimeUnit.MILLISECONDS.toNanos(1L);
    private static final long DEFAULT_SPARSE_COALESCE_NANOS = TimeUnit.MILLISECONDS.toNanos(1L);
    private static final long BULK_SPARSE_COALESCE_NANOS = TimeUnit.MILLISECONDS.toNanos(2L);
    private static final long LATENCY_HOT_COALESCE_NANOS = TimeUnit.MILLISECONDS.toNanos(2L);
    private static final long DEFAULT_HOT_COALESCE_NANOS = TimeUnit.MILLISECONDS.toNanos(3L);
    private static final long BULK_HOT_COALESCE_NANOS = TimeUnit.MILLISECONDS.toNanos(4L);

    private SessionWriterBatchPolicy() {
    }

    static long ordinaryBatchCostLimit(Settings peerSettings, long sendRateEstimate, int maxBatchFrames) {
        SchedulerHint hint = schedulerHint(peerSettings);
        long maxPayload = maxFramePayload(peerSettings);

        long multiplier;
        switch (hint) {
            case LATENCY:
                multiplier = LATENCY_BATCH_COST_MULTIPLIER;
                break;
            case BULK_THROUGHPUT:
                multiplier = BULK_BATCH_COST_MULTIPLIER;
                break;
            default:
                multiplier = DEFAULT_BATCH_COST_MULTIPLIER;
                break;
        }
        long baseCostLimit = saturatingMultiply(SessionRuntime.saturatingAdd(maxPayload, 1L), multiplier);
        long effectiveCostLimit = WritePolicy.rateLimitedFragmentCap(baseCostLimit, sendRateEstimate, 0L, hint);
        long frameCapCost = saturatingMultiply(SessionRuntime.saturatingAdd(maxPayload, 1L), maxBatchFrames);
        return Math.max(1L, Math.min(frameCapCost, effectiveCostLimit));
    }

    static long ordinaryBatchCoalesceNanos(List<SessionRuntime.OutboundFrame> batch,
                                           long batchCost,
                                           long costLimit,
                                           boolean urgentQueued,
                                           boolean discardStaged,
                                           Settings peerSettings,
                                           int maxBatchFrames) {
        if (batch == null || batch.isEmpty() || batch.size() >= maxBatchFrames || batchCost >= costLimit) {
            return 0L;
        }
        if (urgentQueued || discardStaged) {
            return 0L;
        }

        SchedulerHint hint = schedulerHint(peerSettings);
        long maxPayload = maxFramePayload(peerSettings);
        long sparseThreshold = Math.max(4L, SessionRuntime.saturatingAdd(maxPayload / 2L, 1L));
        boolean sparse = batch.size() == 1 && batchCost <= sparseThreshold;
        switch (hint) {
            case LATENCY:
                return sparse ? LATENCY_SPARSE_COALESCE_NANOS : LATENCY_HOT_COALESCE_NANOS;
            case BULK_THROUGHPUT:
                return sparse ? BULK_SPARSE_COALESCE_NANOS : BULK_HOT_COALESCE_NANOS;
            default:
                return sparse ? DEFAULT_SPARSE_COALESCE_NANOS : DEFAULT_HOT_COALESCE_NANOS;
        }
    }

    static long outboundBatchCost(SessionRuntime.OutboundFrame outboundFrame) {
        if (outboundFrame == null) {
            return 1L;
        }
        int prefixLength = outboundFrame.payloadPrefix() == null ? 0 : outboundFrame.payloadPrefix().length;
        long payloadBytes = (long) prefixLength + Math.max(0, outboundFrame.payloadLength());
        return 1L + payloadBytes;
    }

    static boolean urgentOutboundPrecedes(SessionRuntime.OutboundFrame candidate,
                                          SessionRuntime.OutboundFrame currentBest) {
        boolean candidateOpeningData = openingDataFrame(candidate);
        boolean currentOpeningData = openingDataFrame(currentBest);
        if (candidateOpeningData != currentOpeningData && sameNonZeroStream(candidate, currentBest)) {
            return candidateOpeningData;
        }

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

    private static boolean openingDataFrame(SessionRuntime.OutboundFrame outboundFrame) {
        return outboundFrame != null
                && outboundFrame.openingFrame()
                && outboundFrame.frame().type() == FrameType.DATA
                && outboundFrame.frame().streamId() != 0L;
    }

    private static boolean sameNonZeroStream(SessionRuntime.OutboundFrame left,
                                             SessionRuntime.OutboundFrame right) {
        if (left == null || right == null) {
            return false;
        }
        long streamId = left.frame().streamId();
        return streamId != 0L && streamId == right.frame().streamId();
    }

    private static SchedulerHint schedulerHint(Settings peerSettings) {
        SchedulerHint hint = peerSettings == null ? null : peerSettings.schedulerHints();
        return hint == null ? SchedulerHint.UNSPECIFIED_OR_BALANCED : hint;
    }

    private static long maxFramePayload(Settings peerSettings) {
        long maxPayload = peerSettings == null ? 0L : peerSettings.maxFramePayload();
        return maxPayload > 0L ? maxPayload : Settings.defaults().maxFramePayload();
    }

    private static int urgentFrameRank(SessionRuntime.OutboundFrame outboundFrame) {
        if (outboundFrame == null) {
            return Integer.MAX_VALUE;
        }
        switch (outboundFrame.frame().type()) {
            case GOAWAY:
                return 0;
            case CLOSE:
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
                return 9;
        }
    }

    private static long saturatingMultiply(long left, long right) {
        if (left <= 0L || right <= 0L) {
            return 0L;
        }
        try {
            return Math.multiplyExact(left, right);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }
}
