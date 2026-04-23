package io.zmux.internal;

import io.zmux.Settings;

import java.math.BigInteger;

final class RuntimeFlow {
    private static final long REPO_DEFAULT_PER_STREAM_DATA_HWM_MIN = 256L << 10;
    private static final long REPO_DEFAULT_SESSION_DATA_HWM_MIN = 4L << 20;
    private static final long REPO_DEFAULT_URGENT_LANE_CAP_MIN = 64L << 10;
    private static final long VISIBLE_ACCEPT_BACKLOG_BYTES_MIN = 4L << 20;
    private static final long VISIBLE_ACCEPT_PER_STREAM_HWM_MIN = 256L << 10;
    private static final long VISIBLE_ACCEPT_PER_STREAM_HWM_FRAMES = 16L;
    private static final long VISIBLE_ACCEPT_SESSION_HWM_FACTOR = 4L;
    private static final long MIN_LATE_DATA_PER_STREAM_CAP = 1024L;
    private static final long MIN_AGGREGATE_LATE_DATA_CAP = 64L << 10;

    private RuntimeFlow() {
    }

    static long saturatingAdd(long left, long right) {
        if (right <= 0L) {
            return left;
        }
        if (left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    static long elapsedNanos(long laterNanos, long earlierNanos) {
        long elapsed = laterNanos - earlierNanos;
        if (elapsed >= 0L) {
            return elapsed;
        }
        return laterNanos >= earlierNanos ? Long.MAX_VALUE : 0L;
    }

    static long positiveElapsedNanos(long laterNanos, long earlierNanos) {
        long elapsed = elapsedNanos(laterNanos, earlierNanos);
        return elapsed <= 0L ? 1L : elapsed;
    }

    static boolean elapsedExceeds(long laterNanos, long earlierNanos, long thresholdNanos) {
        return elapsedNanos(laterNanos, earlierNanos) > thresholdNanos;
    }

    static long saturatingMultiply(long left, long right) {
        if (left <= 0L || right <= 0L) {
            return 0L;
        }
        if (left > Long.MAX_VALUE / right) {
            return Long.MAX_VALUE;
        }
        return left * right;
    }

    static long saturatingMulDivFloor(long value, long multiplier, long divisor) {
        if (value <= 0L || multiplier <= 0L) {
            return 0L;
        }
        if (divisor <= 0L) {
            return Long.MAX_VALUE;
        }
        long quotient = value / divisor;
        long remainder = value - quotient * divisor;
        if (quotient > 0L && multiplier > Long.MAX_VALUE / quotient) {
            return Long.MAX_VALUE;
        }
        long high = quotient * multiplier;
        long low = multiplyRemainderDivFloor(remainder, multiplier, divisor);
        if (high > Long.MAX_VALUE - low) {
            return Long.MAX_VALUE;
        }
        return high + low;
    }

    static long saturatingMulDivCeil(long value, long multiplier, long divisor) {
        if (value <= 0L || multiplier <= 0L) {
            return 0L;
        }
        if (divisor <= 0L) {
            return Long.MAX_VALUE;
        }
        try {
            long product = Math.multiplyExact(value, multiplier);
            long quotient = product / divisor;
            return product % divisor == 0L ? quotient : saturatingAdd(quotient, 1L);
        } catch (ArithmeticException overflow) {
            BigInteger[] divRem = BigInteger.valueOf(value)
                    .multiply(BigInteger.valueOf(multiplier))
                    .divideAndRemainder(BigInteger.valueOf(divisor));
            BigInteger quotient = divRem[1].signum() == 0 ? divRem[0] : divRem[0].add(BigInteger.ONE);
            return quotient.min(BigInteger.valueOf(Long.MAX_VALUE)).longValue();
        }
    }

    private static long multiplyRemainderDivFloor(long value, long multiplier, long divisor) {
        try {
            return Math.multiplyExact(value, multiplier) / divisor;
        } catch (ArithmeticException overflow) {
            return BigInteger.valueOf(value)
                    .multiply(BigInteger.valueOf(multiplier))
                    .divide(BigInteger.valueOf(divisor))
                    .min(BigInteger.valueOf(Long.MAX_VALUE))
                    .longValue();
        }
    }

    static long minNonZeroPositive(long left, long right) {
        if (left <= 0L) {
            return right;
        }
        if (right <= 0L) {
            return left;
        }
        return Math.min(left, right);
    }

    static long negotiatedFramePayload(Settings local, Settings peer) {
        long payload = minNonZeroPositive(local.maxFramePayload(), peer.maxFramePayload());
        return payload > 0L ? payload : Settings.defaults().maxFramePayload();
    }

    static long repoDefaultPerStreamDataHighWatermark(long maxFramePayload) {
        return Math.max(REPO_DEFAULT_PER_STREAM_DATA_HWM_MIN, saturatingMultiply(maxFramePayload, 16L));
    }

    static long repoDefaultSessionDataHighWatermark(long perStreamDataHighWatermark) {
        return Math.max(REPO_DEFAULT_SESSION_DATA_HWM_MIN, saturatingMultiply(perStreamDataHighWatermark, 4L));
    }

    static long repoDefaultUrgentLaneCap(long maxControlPayload) {
        return Math.max(REPO_DEFAULT_URGENT_LANE_CAP_MIN, saturatingMultiply(maxControlPayload, 8L));
    }

    static long visibleAcceptBacklogBytesHardCap(long maxFramePayload) {
        long payload = maxFramePayload > 0L ? maxFramePayload : Settings.defaults().maxFramePayload();
        long perStream = saturatingMultiply(VISIBLE_ACCEPT_PER_STREAM_HWM_FRAMES, payload);
        if (perStream < VISIBLE_ACCEPT_PER_STREAM_HWM_MIN) {
            perStream = VISIBLE_ACCEPT_PER_STREAM_HWM_MIN;
        }
        long limit = saturatingMultiply(VISIBLE_ACCEPT_SESSION_HWM_FACTOR, perStream);
        return Math.max(VISIBLE_ACCEPT_BACKLOG_BYTES_MIN, limit);
    }

    static long lateDataPerStreamCap(long initialStreamWindow, long maxFramePayload) {
        long payload = maxFramePayload > 0L ? maxFramePayload : Settings.defaults().maxFramePayload();
        long limit = saturatingMultiply(payload, 2L);
        long windowCap = initialStreamWindow / 8L;
        if (windowCap < limit) {
            limit = windowCap;
        }
        return Math.max(MIN_LATE_DATA_PER_STREAM_CAP, limit);
    }

    static long aggregateLateDataCap(long maxFramePayload) {
        long payload = maxFramePayload > 0L ? maxFramePayload : Settings.defaults().maxFramePayload();
        long limit = saturatingMultiply(payload, 4L);
        return Math.max(MIN_AGGREGATE_LATE_DATA_CAP, limit);
    }

    static long lowWatermark(long highWatermark) {
        return highWatermark / 2L;
    }

    static boolean queueWouldBlock(boolean sessionMemoryBlocked,
                                   long sessionQueued,
                                   long streamQueued,
                                   long requestedBytes,
                                   long sessionHighWatermark,
                                   long streamHighWatermark) {
        if (sessionMemoryBlocked || requestedBytes == 0L) {
            return sessionMemoryBlocked;
        }
        if (saturatingAdd(sessionQueued, requestedBytes) > sessionHighWatermark) {
            return true;
        }
        return saturatingAdd(streamQueued, requestedBytes) > streamHighWatermark;
    }

    static boolean crossedLowWatermark(long previous, long next, long lowWatermark) {
        return previous > lowWatermark && next <= lowWatermark;
    }

    static boolean gainedCredit(long previous, long next) {
        return previous <= 0L && next > 0L;
    }

    static boolean memoryWakeNeeded(long previousTracked, long nextTracked, long threshold) {
        return nextTracked < previousTracked && nextTracked < threshold;
    }

    static long windowRemaining(long advertised, long received) {
        if (received >= advertised) {
            return 0L;
        }
        return advertised - received;
    }

    static boolean receiveWindowExceeded(long received, long advertised, long requestedBytes) {
        return requestedBytes > 0L && requestedBytes > windowRemaining(advertised, received);
    }
}
