package io.zmux.helper;

import java.util.concurrent.CompletionStage;

/**
 * An external limiter, budget or meter acting on the bytes zmux forwards itself: a session's datagrams and the
 * splice relays. Stream I/O is wrapped by {@link LimitedStreams}. Units are bytes.
 *
 * <p>The model is reserve, then refund: a caller takes bytes before it moves them ({@link #acquire},
 * {@link #tryAcquire}), so callers sharing one limiter never exceed it together, and gives back what it did not use
 * ({@link #refund}). {@link Permit} does that bookkeeping: what it holds unused when closed goes back.
 *
 * <p>Arguments: {@code max <= 0} takes nothing and returns 0 at once; {@code min < 1} is taken as 1;
 * {@code min > max} is taken as {@code min = max}. {@code (n, n)} is therefore an all-or-nothing request and
 * {@code (1, max)} a request for as much as possible, at least one byte.
 *
 * <p>Grant: a successful {@code acquire} or {@code tryAcquire} takes and returns n with {@code min' <= n <= max},
 * where min' is {@code min}, or less when the limiter can never allow {@code min} at once (its burst). A limiter
 * that grants {@code min} anyway and records the excess as debt is conforming. The limiter chooses n in that range;
 * callers assume neither {@code n == max} nor {@code n > min}. Atomic splice outputs use {@link Permit#acquireExact} or {@link Permit#tryAcquireExact} and require
 * the full grant. Other byte and datagram wrappers accept a short grant according to their own policy.
 * When {@code max} allows, a limiter should not grant much less than its quantum (about 20 ms of traffic at its rate,
 * at most its burst), so that a stream is not fed in slivers.
 *
 * <p>Every method is safe under any number of concurrent callers; there is no fairness guarantee. An unlimited
 * limiter grants {@code max} at once; one whose rate is zero makes {@code acquire} wait until cancelled and
 * {@code tryAcquire} return 0.
 *
 * <p>zmux calls every method off its event loops and outside every session lock, and never calls it from within a carrier callback. A splice may retain a bounded
 * transformed output during an acquisition. A limiter that fails is treated alike everywhere: in {@code acquire} (an
 * exception, a failed stage, or a grant outside {@code 1..max}) it fails the operation that asked; in
 * {@code tryAcquire} it counts as nothing taken; a failure of {@code refund} is ignored.
 */
public interface Limiter {
    /** Maximum atomic grant in bytes, Long.MAX_VALUE for unlimited, or negative when unknown. */
    default long maxBurstBytes() { return -1; }

    /**
     * Waits until it can grant, then takes and returns n bytes as the class describes. Cancelling the stage before
     * it completes takes nothing: a limiter without a native rollback gives the bytes back itself, and an
     * approximate rollback may err only toward less use. An error (a budget exhausted, say) takes nothing and fails
     * the operation that asked.
     */
    CompletionStage<Long> acquire(long min, long max);

    /**
     * Grants as {@link #acquire} does without ever blocking, or returns 0 with nothing taken when it would have to
     * wait or refuses; with {@code min == max} it is all-or-nothing. It may return 0 while others wait.
     */
    long tryAcquire(long min, long max);

    /**
     * Gives back n bytes this limiter granted and the caller did not use. Best-effort and possibly a no-op: never
     * blocks or throws, may run on any thread and from cleanup paths. A refund beyond what is outstanding is clamped,
     * and no refund lets the limiter grant more than its rate times the time plus its burst. n {@code <= 0} does
     * nothing.
     */
    void refund(long n);
}
