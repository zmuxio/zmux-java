package io.zmux;


import java.util.Map;

final class OrdinaryBatchSchedulingPolicy {
    private static final int MAX_EXPLICIT_GROUPS = 16;
    private static final long FALLBACK_GROUP_BUCKET = Long.MAX_VALUE;
    private static final int INTERACTIVE_BURST_LIMIT = 8;
    private static final int BULK_RESERVE_WINDOW = 4;
    private static final int BULK_ENTRY_MULTIPLIER = 2;
    private static final long CLASS_SCORE_SCALE = 8L;

    private OrdinaryBatchSchedulingPolicy() {
    }

    static OrdinaryBatchOrderer.GroupKey streamGroupKey(OrdinaryBatchOrderer.BatchFrame frame,
                                                        SchedulerHint hint,
                                                        Map<Long, OrdinaryBatchOrderer.GroupKey> explicitGroups,
                                                        long streamKey) {
        if (hint == SchedulerHint.GROUP_FAIR && frame.group() != null && frame.group() != 0L) {
            OrdinaryBatchOrderer.GroupKey existing = explicitGroups.get(frame.group());
            if (existing != null) {
                return existing;
            }
            OrdinaryBatchOrderer.GroupKey mapped = explicitGroups.size() < MAX_EXPLICIT_GROUPS
                    ? new OrdinaryBatchOrderer.GroupKey(1, frame.group())
                    : new OrdinaryBatchOrderer.GroupKey(1, FALLBACK_GROUP_BUCKET);
            explicitGroups.put(frame.group(), mapped);
            return mapped;
        }
        return new OrdinaryBatchOrderer.GroupKey(0, streamKey);
    }

    static long streamWeight(long priority, long queuedBytes, SchedulerHint hint, long maxPayload) {
        long base = priorityWeight(priority, hint);
        long shortWindow = schedulerQuantum(maxPayload);
        if (shortWindow <= 0L) {
            return Math.max(base, 1L);
        }

        switch (hint) {
            case LATENCY:
                if (queuedBytes <= shortWindow) {
                    base = saturatingMultiply(base, 4L);
                } else if (queuedBytes <= saturatingMultiply(shortWindow, 2L)) {
                    base = saturatingMultiply(base, 2L);
                }
                break;
            case BULK_THROUGHPUT:
                if (shortWindow > 1L && queuedBytes <= shortWindow / 2L) {
                    base = saturatingAdd(base, Math.max(base / 2L, 1L));
                }
                break;
            case BALANCED_FAIR:
            case GROUP_FAIR:
            case UNSPECIFIED_OR_BALANCED:
                if (queuedBytes <= shortWindow) {
                    base = saturatingMultiply(base, 2L);
                }
                break;
            default:
                break;
        }

        return Math.max(base, 1L);
    }

    static long groupWeight(OrdinaryBatchOrderer.GroupKey groupKey, long streamWeight, SchedulerHint hint) {
        if (groupKey.kind() != 1) {
            return Math.max(streamWeight, 1L);
        }
        switch (hint) {
            case LATENCY:
                return 32L;
            case BULK_THROUGHPUT:
                return 16L;
            default:
                return 24L;
        }
    }

    static long schedulerQuantum(long maxPayload) {
        if (maxPayload > 0L) {
            return maxPayload;
        }
        return Settings.defaults().maxFramePayload();
    }

    static long feedbackWindow(SchedulerHint hint, long maxPayload) {
        long window = schedulerQuantum(maxPayload);
        switch (hint) {
            case LATENCY:
                window = saturatingMultiply(window, 6L);
                break;
            case BULK_THROUGHPUT:
                window = saturatingMultiply(window, 2L);
                break;
            default:
                window = saturatingMultiply(window, 4L);
                break;
        }
        return Math.max(window, 1L);
    }

    static long adjustWeightForLag(long base, long lag, long window, boolean fresh) {
        long adjusted = Math.max(base, 1L);
        if (fresh) {
            adjusted = saturatingAdd(adjusted, Math.max(adjusted / 2L, 1L));
        }
        if (window <= 0L || lag == 0L) {
            return adjusted;
        }
        if (lag > 0L) {
            long boost = lagScaledWeight(adjusted, Math.min(lag, window), window);
            return saturatingAdd(adjusted, Math.max(boost, 1L));
        }
        long penalty = lagScaledWeight(adjusted, Math.min(-lag, window), saturatingMultiply(window, 2L));
        return Math.max(adjusted - penalty, 1L);
    }

    static long normalizeCost(long cost) {
        return cost <= 0L ? 1L : cost;
    }

    static long classAdjustedWeight(OrdinaryBatchOrderer.BatchStreamState stream,
                                    long baseWeight,
                                    long effectiveWeight,
                                    long interactiveQuantum,
                                    boolean ageBoost) {
        long adjusted = Math.max(effectiveWeight, 1L);
        if (stream.smallBurstBonusArmed() && stream.remainingCost() <= interactiveQuantum) {
            adjusted = saturatingAdd(adjusted, Math.max(baseWeight, 1L));
        }
        if (ageBoost) {
            long boost = Math.max(baseWeight, Math.max(adjusted / 2L, 1L));
            adjusted = saturatingAdd(adjusted, boost);
        }
        return adjusted;
    }

    static OrdinaryBatchOrderer.TrafficClass chooseClass(OrdinaryBatchOrderer.GroupKey preferredGroupHead,
                                                         SchedulerHint hint,
                                                         OrdinaryBatchOrderer.GroupCandidate interactive,
                                                         OrdinaryBatchOrderer.GroupCandidate bulk,
                                                         int interactiveStreak,
                                                         int classSelectionsSinceBulk) {
        if (interactive == null) {
            return OrdinaryBatchOrderer.TrafficClass.BULK;
        }
        if (bulk == null) {
            return OrdinaryBatchOrderer.TrafficClass.INTERACTIVE;
        }
        if (interactiveStreak >= INTERACTIVE_BURST_LIMIT || classSelectionsSinceBulk >= BULK_RESERVE_WINDOW - 1) {
            return OrdinaryBatchOrderer.TrafficClass.BULK;
        }
        return betterClassCandidate(preferredGroupHead, interactive, bulk, hint)
                ? OrdinaryBatchOrderer.TrafficClass.INTERACTIVE
                : OrdinaryBatchOrderer.TrafficClass.BULK;
    }

    static OrdinaryBatchOrderer.TrafficClass classifyStream(OrdinaryBatchOrderer.BatchStreamState stream,
                                                            SchedulerHint hint,
                                                            long interactiveQuantum,
                                                            OrdinaryBatchOrderer.TrafficClass previous) {
        if (stream == null || !stream.streamScoped()) {
            return OrdinaryBatchOrderer.TrafficClass.INTERACTIVE;
        }
        long queuedBytes = Math.max(stream.remainingCost(), 0L);
        long bulkThreshold = saturatingMultiply(interactiveQuantum, BULK_ENTRY_MULTIPLIER);
        if (queuedBytes <= interactiveQuantum) {
            return OrdinaryBatchOrderer.TrafficClass.INTERACTIVE;
        }
        if (queuedBytes > bulkThreshold) {
            return OrdinaryBatchOrderer.TrafficClass.BULK;
        }
        if (previous != null) {
            return previous;
        }
        if (hint == SchedulerHint.BULK_THROUGHPUT) {
            return OrdinaryBatchOrderer.TrafficClass.BULK;
        }
        if (hint == SchedulerHint.LATENCY || stream.priority() >= 4L) {
            return OrdinaryBatchOrderer.TrafficClass.INTERACTIVE;
        }
        return WritePolicy.writeBurstLimit(stream.priority(), hint) >= WritePolicy.DEFAULT_WRITE_BURST_FRAMES
                ? OrdinaryBatchOrderer.TrafficClass.BULK
                : OrdinaryBatchOrderer.TrafficClass.INTERACTIVE;
    }

    private static boolean betterClassCandidate(OrdinaryBatchOrderer.GroupKey preferredGroupHead,
                                                OrdinaryBatchOrderer.GroupCandidate left,
                                                OrdinaryBatchOrderer.GroupCandidate right,
                                                SchedulerHint hint) {
        if (left == null) {
            return false;
        }
        if (right == null) {
            return true;
        }
        if (left.eligible() != right.eligible()) {
            return left.eligible();
        }
        long leftPrimary = scaledClassTag(left, hint, left.eligible() ? left.groupFinish() : left.groupStart());
        long rightPrimary = scaledClassTag(right, hint, right.eligible() ? right.groupFinish() : right.groupStart());
        if (leftPrimary != rightPrimary) {
            return leftPrimary < rightPrimary;
        }
        long leftSecondary = scaledClassTag(left, hint, left.eligible() ? left.groupStart() : left.groupFinish());
        long rightSecondary = scaledClassTag(right, hint, right.eligible() ? right.groupStart() : right.groupFinish());
        if (leftSecondary != rightSecondary) {
            return leftSecondary < rightSecondary;
        }
        return OrdinaryBatchCandidateComparator.betterGroupCandidate(preferredGroupHead, left, right);
    }

    private static long scaledClassTag(OrdinaryBatchOrderer.GroupCandidate candidate, SchedulerHint hint, long tag) {
        long weight = classBiasWeight(candidate.trafficClass(), hint);
        if (weight <= 0L) {
            return tag;
        }
        return RuntimeFlow.saturatingMulDivFloor(tag, CLASS_SCORE_SCALE, weight);
    }

    private static long classBiasWeight(OrdinaryBatchOrderer.TrafficClass trafficClass, SchedulerHint hint) {
        switch (hint) {
            case LATENCY:
                return trafficClass == OrdinaryBatchOrderer.TrafficClass.INTERACTIVE ? 8L : 2L;
            case BULK_THROUGHPUT:
                return trafficClass == OrdinaryBatchOrderer.TrafficClass.INTERACTIVE ? 2L : 8L;
            default:
                return trafficClass == OrdinaryBatchOrderer.TrafficClass.INTERACTIVE ? 6L : 4L;
        }
    }

    private static long priorityWeight(long priority, SchedulerHint hint) {
        switch (hint) {
            case LATENCY:
                return bandedWeight(priority, 16L, 24L, 32L, 48L, 64L, 96L);
            case BULK_THROUGHPUT:
                return bandedWeight(priority, 16L, 18L, 20L, 24L, 28L, 32L);
            default:
                return bandedWeight(priority, 16L, 20L, 24L, 32L, 48L, 72L);
        }
    }

    private static long bandedWeight(long priority,
                                     long base,
                                     long mild,
                                     long medium,
                                     long strong,
                                     long xstrong,
                                     long saturated) {
        if (priority >= 32L) {
            return saturated;
        }
        if (priority >= 16L) {
            return xstrong;
        }
        if (priority >= 8L) {
            return strong;
        }
        if (priority >= 4L) {
            return medium;
        }
        if (priority >= 1L) {
            return mild;
        }
        return base;
    }

    private static long lagScaledWeight(long base, long magnitude, long divisor) {
        if (base <= 0L || magnitude <= 0L || divisor <= 0L) {
            return 0L;
        }
        return RuntimeFlow.saturatingMulDivFloor(base, magnitude, divisor);
    }

    static long saturatingAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    static long saturatingMultiply(long left, long right) {
        try {
            return Math.multiplyExact(left, right);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }
}
