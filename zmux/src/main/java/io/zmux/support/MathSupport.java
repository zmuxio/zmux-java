package io.zmux.support;

import java.math.BigInteger;

public final class MathSupport {
    private MathSupport() {
    }

    public static long saturatingMulDivFloor(long value, long multiplier, long divisor) {
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
        return high > Long.MAX_VALUE - low ? Long.MAX_VALUE : high + low;
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
}
