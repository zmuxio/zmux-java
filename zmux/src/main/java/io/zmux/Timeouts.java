package io.zmux;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Session timers, immutable. {@link #defaults()} holds the defaults; each {@code with} method sets a
 * duration and each {@code disable} method chooses the setting that is not a duration. A duration
 * beyond the clock's range never fires.
 *
 * <ul>
 *   <li>handshake: how long establishment, joins and lanes may take, default 10 s, any non-negative
 *       duration; {@link #disableHandshakeTimeout()}: no timeout</li>
 *   <li>idle: the advertised idle timeout, default 30 s, at least 1 ms (the unit of the transport
 *       parameter, whose 0 means none); {@link #disableIdleTimeout()}: no idle timeout, and none is
 *       announced</li>
 *   <li>keepAlive: the keep-alive period, default 15 s, at least 1 s; {@link #disableKeepAlive()} turns
 *       keep-alive off, which makes the idle timeout a limit on application inactivity. The period in
 *       effect is capped at half the effective idle timeout, and each wait is drawn from [k x (1 - j), k]
 *       with j the session's {@link MultipathConfig#probeJitter() probe jitter}</li>
 * </ul>
 */
public final class Timeouts {
    /** The default handshake timeout. */
    public static final Duration DEFAULT_HANDSHAKE = Duration.ofSeconds(10);
    /** The default idle timeout. */
    public static final Duration DEFAULT_IDLE = Duration.ofSeconds(30);
    /** The default keep-alive period. */
    public static final Duration DEFAULT_KEEP_ALIVE = Duration.ofSeconds(15);
    private static final Timeouts DEFAULTS = new Timeouts(DEFAULT_HANDSHAKE, DEFAULT_IDLE, DEFAULT_KEEP_ALIVE);

    // null: the setting that is not a duration (no timeout, off).
    private final Duration handshake, idle, keepAlive;

    private Timeouts(Duration handshake, Duration idle, Duration keepAlive) {
        this.handshake = handshake;
        this.idle = idle;
        this.keepAlive = keepAlive;
    }

    static Duration nonNegative(Duration d, String what) {
        if (Objects.requireNonNull(d, what).isNegative()) throw new IllegalArgumentException(what + " must not be negative");
        return d;
    }

    static Duration atLeast(Duration d, Duration min, String what) {
        if (Objects.requireNonNull(d, what).compareTo(min) < 0) throw new IllegalArgumentException(what + " below " + min);
        return d;
    }

    public static Timeouts defaults() { return DEFAULTS; }

    /** The handshake timeout; empty: none. */
    public Optional<Duration> handshake() { return Optional.ofNullable(handshake); }
    /** The idle timeout; empty: none. */
    public Optional<Duration> idle() { return Optional.ofNullable(idle); }
    /** The keep-alive period; empty: off. */
    public Optional<Duration> keepAlive() { return Optional.ofNullable(keepAlive); }

    public Timeouts withHandshake(Duration d) { return new Timeouts(nonNegative(d, "handshake"), idle, keepAlive); }
    /** No handshake timeout. */
    public Timeouts disableHandshakeTimeout() { return new Timeouts(null, idle, keepAlive); }
    public Timeouts withIdle(Duration d) { return new Timeouts(handshake, atLeast(d, Duration.ofMillis(1), "idle"), keepAlive); }
    /** No idle timeout; none is announced. */
    public Timeouts disableIdleTimeout() { return new Timeouts(handshake, null, keepAlive); }
    /** The keep-alive period, at least 1 s (a period of 0 has no meaning). */
    public Timeouts withKeepAlive(Duration d) { return new Timeouts(handshake, idle, atLeast(d, Duration.ofSeconds(1), "keepAlive")); }
    /** Keep-alive off. */
    public Timeouts disableKeepAlive() { return new Timeouts(handshake, idle, null); }

    @Override public boolean equals(Object o) {
        return o instanceof Timeouts t && Objects.equals(handshake, t.handshake) && Objects.equals(idle, t.idle)
                && Objects.equals(keepAlive, t.keepAlive);
    }
    @Override public int hashCode() { return Objects.hash(handshake, idle, keepAlive); }
    @Override public String toString() {
        return "Timeouts[handshake=" + handshake() + ", idle=" + idle() + ", keepAlive=" + keepAlive() + "]";
    }
}
