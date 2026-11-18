package io.zmux.internal.spi;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The shrink order of a pool, the one place both pools take it from. A count below
 * the pool's size, members and growth dials together, cancels growth dials first, newest first, then closes members:
 * those the decision named, then the longest idle, ties broken by the fewest pipes, the least age, the highest key.
 */
public final class PoolShrink {
    private PoolShrink() {}

    /**
     * A member as the order sees it.
     *
     * @param owner what the caller closes
     * @param key the pipe ID or fate group, compared unsigned
     * @param named whether the decision named it for retirement
     */
    public record Candidate<T>(T owner, long key, boolean named, long idle, int pipes, long age) {}

    /**
     * What a shrink does.
     *
     * @param cancelDials how many growth dials to cancel, newest first (the dials are numbered 0 for the oldest)
     * @param close the members to close, in the order they close
     */
    public record Plan<T>(int cancelDials, List<T> close) {}

    private static final Comparator<Candidate<?>> ORDER = Comparator
            .<Candidate<?>>comparingInt(c -> c.named() ? 0 : 1)
            .thenComparing(Comparator.<Candidate<?>>comparingLong(Candidate::idle).reversed())
            .thenComparingInt(Candidate::pipes)
            .thenComparingLong(Candidate::age)
            .thenComparing((a, b) -> Long.compareUnsigned(b.key(), a.key()));

    /** The plan that brings members and dialing growth dials down to count, leaving a pool already within it alone. */
    public static <T> Plan<T> plan(List<Candidate<T>> members, int dialing, int count) {
        int excess = members.size() + dialing - count;
        if (excess <= 0) return new Plan<>(0, List.of());
        int cancel = Math.min(dialing, excess);
        var order = new ArrayList<>(members);
        order.sort(ORDER);
        var close = new ArrayList<T>(excess - cancel);
        for (int i = 0; i < excess - cancel; i++) close.add(order.get(i).owner());
        return new Plan<>(cancel, close);
    }
}
