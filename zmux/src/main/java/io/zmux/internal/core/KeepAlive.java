package io.zmux.internal.core;

import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameEncoder;
import io.zmux.internal.wire.TransportError;
import io.zmux.carrier.VarInt;
import io.zmux.internal.wire.WireException;

/**
 * Liveness of one session: QX_PING requests and responses (SPEC.md Section 4.5), the idle
 * timeout and keep-alive (Section 6.4), and the no-effect frame budget (Section 9.4).
 * Owned by the session's loop; effects on other components go through {@link Port}.
 */
public final class KeepAlive {
    /** What KeepAlive asks of the session. */
    interface Port {
        /** A frame of this component wants to be written. */
        void wantWrite();

        /** The no-effect budget is exhausted (SPEC.md Section 9.4.2). */
        void budgetExhausted();

        /** A QX_PING request completed with rtt (before its waiters wake). */
        void pingMeasured(long rtt);

        /** The response covered the GOAWAY fence (EXTENSIONS.md Section 3.3). */
        void fenceReached();
    }

    static final long BUDGET_CAP = 4096;
    static final long BUDGET_REFILL = 32;

    private final Port port;
    private final Outbox outbox;
    /** The configured keep-alive period, {@link CoreConfig#OFF}: off. */
    private final long keepAlive;
    private final double jitter;
    private final java.util.random.RandomGenerator random;
    /** The period in effect: the configured one, capped at half the effective idle timeout. */
    private long period;
    /** The current keep-alive wait and the timer start it was drawn for. */
    private long wait;
    private long waitRef = Long.MIN_VALUE;

    // QX_PING
    private long next;
    private long outstanding;

    /** When the outstanding QX_PING request was sent; Long.MIN_VALUE: none. */
    long outstandingSentAt() { return outstanding != 0 ? sentAt : Long.MIN_VALUE; }
    private long sentAt;
    private long expires;
    private boolean want;
    private boolean requestFirst;
    private boolean owe;
    private long oweSeq;
    private long reqSeen;
    private long respSeen;
    private long maxSent;
    private final WaitList waiters = new WaitList();
    private long completed;
    private long lastRtt;
    private long fence;
    private long pipe;

    // idle and keep-alive
    private long idle;
    private long lastActivity;
    private long lastKeepRef;

    // no-effect budget
    private long budgetTokens = BUDGET_CAP;
    private long budgetLast;

    KeepAlive(CoreConfig cfg, Port port, Outbox outbox, long now) {
        this.port = port;
        this.outbox = outbox;
        this.keepAlive = cfg.keepAlive();
        this.jitter = cfg.probeJitter();
        this.random = cfg.random();
        this.period = keepAlive;
        this.next = cfg.test().pingSeqStart();
        this.budgetLast = now;
    }

    /** Establishment: the idle timeout is negotiated (nanoseconds, 0 = none) and activity starts now. */
    void start(long idleTimeout, long now) {
        idle = idleTimeout;
        lastActivity = now;
        lastKeepRef = -1;
        period = effectivePeriod(keepAlive, idleTimeout);
        waitRef = Long.MIN_VALUE;
    }

    /** The keep-alive period in effect: k capped at half the idle timeout when there is one (SPEC.md 6.4). */
    static long effectivePeriod(long k, long idle) {
        return k >= 0 && idle > 0 ? Math.min(k, Math.max(idle / 2, 1)) : k;
    }

    /** The keep-alive period in effect, {@link CoreConfig#OFF}: off. */
    long period() {
        return period;
    }

    /** The current wait, drawn from [k(1 - j), k] each time the keep-alive timer restarts. */
    private long keepWait() {
        if (waitRef != lastActivity) {
            waitRef = lastActivity;
            wait = jitter > 0 ? PathHealth.probeWait(period, jitter, random.nextDouble()) : period;
        }
        return wait;
    }

    /** The effective idle timeout (SPEC.md Section 6.4), 0 when none. */
    long idleTimeout() {
        return idle;
    }

    /** Evidence of peer activity for the idle timer. */
    void activity(long now) {
        lastActivity = now;
    }

    // ---- API ----

    /** Asks for a QX_PING request, or joins the outstanding one; returns a ticket. */
    long ping(Waiter w) {
        if (outstanding == 0) {
            want = true;
            port.wantWrite();
        }
        if (w != null) waiters.add(w);
        return completed + 1;
    }

    /** The RTT of a completed ticket, or -1 while it is pending. */
    long result(long ticket) {
        return completed >= ticket ? lastRtt : -1;
    }

    public void cancelWait(Waiter w) {
        waiters.remove(w);
    }

    /** Wakes and withdraws every ping waiter (close). */
    void wakeAll() {
        waiters.wakeAll(outbox);
    }

    /** The latest QX_PING RTT, 0 before the first. */
    long lastRtt() {
        return lastRtt;
    }

    /** Asks for a measurement right after the transport parameters, unless one is in flight. */
    void requestFirst() {
        if (outstanding == 0) requestFirst = true;
    }

    /** Requests a ping whose response fences GOAWAY phase 1 (EXTENSIONS.md Section 3.3). */
    void startFence() {
        want = true;
        fence = next + 1;
    }

    void clearFence() {
        fence = 0;
    }

    boolean wants() {
        return owe || want && outstanding == 0 || requestFirst;
    }

    // ---- writing ----

    void write(RecordWriter w, long now, long srtt) {
        writeResponse(w);
        writeRequest(w, now, srtt);
    }

    void writeResponse(RecordWriter w) {
        if (!owe) return;
        long seq = oweSeq;
        if (!w.frame(8 + VarInt.length(seq), true, bb -> FrameEncoder.ping(bb, true, seq))) return;
        owe = false;
        w.wroteActivity();
    }

    private void writeRequest(RecordWriter w, long now, long srtt) {
        if ((want || requestFirst) && outstanding == 0) {
            long seq = next;
            if (!w.frame(8 + VarInt.length(seq), true, bb -> FrameEncoder.ping(bb, false, seq))) return;
            next++;
            pipe = w.b().pipe();
            outstanding = seq + 1;
            maxSent = seq + 1;
            sentAt = now;
            expires = Time.add(now, Math.max(Time.SECOND, Time.mul(srtt, 3)));
            want = false;
            requestFirst = false;
            w.wrotePingRequest();
        }
    }

    // ---- input ----

    /** A QX_PING frame; stale frames are tolerated on multipath sessions (MULTIPATH.md 6.4). */
    void onPing(Frame f, boolean multipath, long now) throws WireException {
        if (!f.response) {
            if (f.value + 1 <= reqSeen) {
                if (!multipath) {
                    throw WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "QX_PING request not increasing", "value", f.value);
                }
                return;
            }
            reqSeen = f.value + 1;
            if (owe) charge(now);
            owe = true;
            oweSeq = f.value;
            port.wantWrite();
            return;
        }
        if (f.value + 1 > maxSent) {
            throw WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "QX_PING response above the largest request", "value", f.value);
        }
        if (f.value + 1 <= respSeen) {
            if (!multipath) charge(now);
            return;
        }
        respSeen = f.value + 1;
        if (outstanding != 0 && f.value + 1 >= outstanding) {
            long rtt = Time.sub(now, sentAt);
            lastRtt = rtt;
            outstanding = 0;
            completed++;
            port.pingMeasured(rtt);
            waiters.wakeAll(outbox);
            if (fence != 0 && f.value + 1 >= fence) port.fenceReached();
        }
    }

    /** The outbound pipe that carried the outstanding request failed (MULTIPATH.md 7.2). */
    void pipeFailed(long pipeId) {
        if (outstanding != 0 && pipe == pipeId) expired();
    }

    private void expired() {
        outstanding = 0;
        if (!waiters.isEmpty() || fence != 0) {
            want = true;
            if (fence != 0) fence = next + 1;
            port.wantWrite();
        }
    }

    // ---- budget ----

    /** Subtracts one token for a chargeable frame (SPEC.md Section 9.4.2). */
    void charge(long now) {
        long step = Time.SECOND / BUDGET_REFILL;
        long el = Time.sub(now, budgetLast);
        if (el >= step) {
            long add = el / step;
            budgetTokens = Math.min(BUDGET_CAP, budgetTokens + add);
            budgetLast = Time.add(budgetLast, add * step);
            if (budgetTokens == BUDGET_CAP) budgetLast = now;
        }
        budgetTokens--;
        if (budgetTokens < 0) port.budgetExhausted();
    }

    /** The current tokens of the no-effect budget (statistic): 0 to its capacity, 0 once a charge exhausted it. */
    public long budgetTokens() {
        return Math.max(budgetTokens, 0);
    }

    // ---- timers ----

    /** The earliest deadline; live: established and not suspended (idle and keep-alive run). */
    long deadline(boolean live) {
        long t = Time.NEVER;
        if (live) {
            if (idle > 0) t = Time.add(lastActivity, idle);
            if (period >= 0 && outstanding == 0 && lastKeepRef != lastActivity) {
                t = Math.min(t, Time.add(lastActivity, keepWait()));
            }
        }
        if (outstanding != 0) t = Math.min(t, expires);
        return t;
    }

    /** Runs the timers due at t; true when the idle timeout expired. */
    boolean onTimeout(long t, boolean live) {
        if (outstanding != 0 && t >= expires) expired();
        if (!live) return false;
        if (idle > 0 && t >= Time.add(lastActivity, idle)) return true;
        if (period >= 0 && outstanding == 0 && lastKeepRef != lastActivity && t >= Time.add(lastActivity, keepWait())) {
            lastKeepRef = lastActivity;
            want = true;
            port.wantWrite();
        }
        return false;
    }
}
