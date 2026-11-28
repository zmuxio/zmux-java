package io.zmux.internal.core;

import java.util.function.ToLongFunction;

/** The session owner's index of protocol deadline sources. Sources update independently. */
final class ProtocolTimeline {
    enum Reason {
        LIFE(0), LANE(1), RESUME(2), KEEPALIVE(3), ACK(4), PACING(5), QUALITY(6),
        HEALTH(7), JOIN_BACKOFF(8), EPOCH(9), ACK_COPY(10), PING(11), CUSTOM_PICK(12),
        PROBATION(13), HEALTH_PROJECTION(14), TIER(15), PREFIX_REPAIR(16),
        SHAPING(17), DATAGRAM_HOLD(18), DATAGRAM_SEND(19), DATAGRAM_FEEDBACK(20);

        private final int slot;

        Reason(int slot) { this.slot = slot; }

        long bit() { return 1L << slot; }
    }

    private static final Reason[] REASONS = reasonsBySlot();

    private static Reason[] reasonsBySlot() {
        Reason[] reasons = Reason.values();
        Reason[] indexed = new Reason[reasons.length];
        for (Reason reason : reasons) indexed[reason.slot] = reason;
        return indexed;
    }
    static final long ALL = (1L << REASONS.length) - 1;
    static final long PATH = Reason.SHAPING.bit() - Reason.HEALTH.bit();
    static final long DATAGRAM = Reason.DATAGRAM_HOLD.bit() | Reason.DATAGRAM_SEND.bit() | Reason.DATAGRAM_FEEDBACK.bit();
    static final long RECORD = PATH | DATAGRAM | Reason.RESUME.bit() | Reason.KEEPALIVE.bit()
            | Reason.ACK.bit() | Reason.PACING.bit() | Reason.QUALITY.bit() | Reason.SHAPING.bit();
    static final long DRAIN = Reason.LANE.bit() | Reason.ACK.bit() | Reason.PACING.bit() | PATH;
    private final TimerHeap index = new TimerHeap();
    private final TimerHeap.Entry[] keys = new TimerHeap.Entry[REASONS.length];
    private final ToLongFunction<Reason> deadline;
    private long enabled = ALL;
    private long dirty = ALL;
    private long visits;
    private final long[] processed = new long[REASONS.length];
    private final long[] stalled = new long[REASONS.length];

    ProtocolTimeline(ToLongFunction<Reason> deadline) {
        this.deadline = deadline;
        java.util.Arrays.fill(stalled, Time.NEVER);
        for (Reason reason : REASONS) keys[reason.slot] = new TimerHeap.Entry();
    }

    void changed(long mask) { dirty |= mask; }

    long earliest() { return index.earliest(); }

    void refresh(long active) {
        dirty |= enabled ^ active;
        enabled = active;
        if (dirty == 0) return;
        long refresh = dirty;
        dirty = 0;
        for (long rest = refresh; rest != 0; rest &= rest - 1) {
            int i = Long.numberOfTrailingZeros(rest);
            long at = (enabled & (1L << i)) == 0 ? Time.NEVER : deadline.applyAsLong(REASONS[i]);
            visits++;
            if (at != stalled[i]) stalled[i] = Time.NEVER;
            index.schedule(keys[i], at == stalled[i] ? Time.NEVER : at);
        }
    }

    long dueReasons(long now) {
        long due = 0;
        for (Reason reason : REASONS) {
            long at = keys[reason.slot].deadline();
            if (at != Time.NEVER && at <= now) due |= reason.bit();
        }
        return due;
    }

    void beginDispatch() { java.util.Arrays.fill(processed, Time.NEVER); }

    /** Drops a stalled key until its source supplies a different deadline. Other keys can still chain. */
    long recordDispatch(long due) {
        long stopped = 0;
        for (long rest = due; rest != 0; rest &= rest - 1) {
            int i = Long.numberOfTrailingZeros(rest);
            long at = keys[i].deadline();
            if (processed[i] != Time.NEVER && at <= processed[i]) {
                stalled[i] = at;
                index.schedule(keys[i], Time.NEVER);
                stopped |= 1L << i;
            } else {
                processed[i] = at;
            }
        }
        return stopped;
    }

    static Reason reasonAt(int index) { return REASONS[index]; }

    long visits() { return visits; }
}
