package io.zmux;

import java.time.Duration;
import java.util.Objects;

/**
 * The local policy of {@link CarrierChoice#auto(Candidate...)}. Start from {@link #defaults()}
 * or {@link #of}; an item not set with a {@code with} method keeps its default. Values are taken
 * as they are, zero included; a negative, NaN or infinite one is rejected, as is a timerMax
 * below timerMin.
 *
 * @param cache remembers the outcomes per server and local network across dials; null: a cache
 *     private to the dial
 * @param key names the server and the local network in the cache, such as the server's host with
 *     the name of the local network
 * @param timerFactor a candidate's timer is the round trip of opening its carrier times this (3),
 *     within timerMin (1 s) and timerMax (3 s); request carriers return before any round trip, so
 *     for them the timer is timerMin. Zero timers start the next candidate at once
 * @param settle how long a joined pipe of a preferred kind stays joined before the pipes of the
 *     kind it replaces are retired (1 s); zero: at once
 * @param dropWindow a carrier kind whose pipes (or session) fail within this long of being chosen
 *     counts as failed (30 s); zero: none does
 * @param join establishes on the most robust candidate and joins the preferred ones (true, the
 *     default); false: the candidates are tried one after another, in order
 */
public record CarrierChoicePolicy(CarrierCache cache, String key, double timerFactor, Duration timerMin, Duration timerMax,
                         Duration settle, Duration dropWindow, boolean join) {
    private static final double TIMER_FACTOR = 3;
    private static final Duration TIMER_MIN = Duration.ofSeconds(1), TIMER_MAX = Duration.ofSeconds(3),
            SETTLE = Duration.ofSeconds(1), DROP_WINDOW = Duration.ofSeconds(30);

    public CarrierChoicePolicy {
        if (cache == null) cache = new CarrierCache();
        Objects.requireNonNull(key, "key");
        if (!(timerFactor >= 0) || Double.isInfinite(timerFactor)) throw new IllegalArgumentException("timerFactor negative, NaN or infinite");
        nonNegative(timerMin, "timerMin");
        nonNegative(timerMax, "timerMax");
        if (timerMax.compareTo(timerMin) < 0) throw new IllegalArgumentException("timerMax below timerMin");
        nonNegative(settle, "settle");
        nonNegative(dropWindow, "dropWindow");
    }

    private static void nonNegative(Duration d, String what) {
        if (Objects.requireNonNull(d, what).isNegative()) throw new IllegalArgumentException("negative " + what);
    }

    /** The defaults, with a cache private to the dial. */
    public static CarrierChoicePolicy defaults() { return of(null, ""); }

    /** The defaults with cache under key. */
    public static CarrierChoicePolicy of(CarrierCache cache, String key) {
        return new CarrierChoicePolicy(cache, key, TIMER_FACTOR, TIMER_MIN, TIMER_MAX, SETTLE, DROP_WINDOW, true);
    }

    public CarrierChoicePolicy withTimer(double factor, Duration min, Duration max) { return new CarrierChoicePolicy(cache, key, factor, min, max, settle, dropWindow, join); }
    public CarrierChoicePolicy withSettle(Duration d) { return new CarrierChoicePolicy(cache, key, timerFactor, timerMin, timerMax, d, dropWindow, join); }
    public CarrierChoicePolicy withDropWindow(Duration d) { return new CarrierChoicePolicy(cache, key, timerFactor, timerMin, timerMax, settle, d, join); }
    public CarrierChoicePolicy withJoin(boolean on) { return new CarrierChoicePolicy(cache, key, timerFactor, timerMin, timerMax, settle, dropWindow, on); }

    /** A candidate's timer for the round trip of opening its carrier. */
    Duration timer(Duration rtt) {
        double n = rtt.toNanos() * timerFactor;
        Duration t = n >= Long.MAX_VALUE ? timerMax : Duration.ofNanos((long) n);
        if (t.compareTo(timerMin) < 0) return timerMin;
        return t.compareTo(timerMax) > 0 ? timerMax : t;
    }
}
