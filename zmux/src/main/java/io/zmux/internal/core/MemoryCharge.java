package io.zmux.internal.core;

/**
 * What one session holds against its memory budget (SPEC.md Section 8.3 rule 4): its own
 * allowance, or its charge on a listener's shared {@link SharedMemory}.
 */
public final class MemoryCharge {
    private final CoreConfig cfg;
    private final ConnFlow conn;
    private final Datagrams dg;
    private final long initialWindow;
    private long charged;
    private long carrierMemory;
    private boolean released;

    MemoryCharge(CoreConfig cfg, ConnFlow conn, Datagrams dg) {
        this.cfg = cfg;
        this.conn = conn;
        this.dg = dg;
        this.initialWindow = conn.window();
    }

    /** The session closed: it holds nothing. */
    void release() {
        released = true;
    }

    /** What the session holds against a budget. */
    public long usage() {
        if (released) return 0;
        return receiveUsage() + conn.sendMemory();
    }

    /** Receive-side memory: buffered data, window growth beyond the initial window, datagrams, carriers. */
    private long receiveUsage() {
        long w = conn.window();
        return conn.buffered() + w - Math.min(w, initialWindow) + dg.receivedBytes() + carrierMemory;
    }

    /** Listener memory pressure: the session behaves as at its buffer cap and refuses new peer streams. */
    boolean pressure() {
        return cfg.budget() != null && cfg.budget().pressure();
    }

    /** Standalone carrier memory is part of the session's own allowance. */
    public void adjustCarrier(long delta) {
        carrierMemory += delta;
    }

    /** Brings the charge on a shared budget up to date (end of every round). */
    public void charge() {
        SharedMemory b = cfg.budget();
        if (b == null) return;
        long u = usage();
        if (u != charged) {
            b.add(u - charged);
            charged = u;
        }
    }

    /** The largest session window the budget allows; retained: multipath retention held besides. */
    long windowAllowance(long retained) {
        long w = conn.window();
        if (cfg.budget() != null) {
            if (cfg.budget().limit() == CoreConfig.UNLIMITED) return Long.MAX_VALUE;
            long u = cfg.budget().used();
            return u < cfg.budget().limit() ? w + (cfg.budget().limit() - u) : w;
        }
        long b = cfg.memoryBudget();
        if (b == CoreConfig.UNLIMITED) return Long.MAX_VALUE;
        long used = receiveUsage() + retained;
        return used >= b ? w : w + (b - used);
    }

    /** Absolute send-buffer max, from the memory account including this round's uncharged growth. */
    long sendBufferLimit() {
        if (cfg.sendBufferMax() > 0) return Math.max(cfg.sendBufferMax(), cfg.sendBuffer());
        long left = Long.MAX_VALUE;
        long used = usage();
        if (cfg.budget() != null) {
            long limit = cfg.budget().limit();
            if (limit != CoreConfig.UNLIMITED) {
                left = limit - Math.min(limit, cfg.budget().used());
                long growth = used - Math.min(used, charged);
                left -= Math.min(left, growth);
            }
        } else if (cfg.memoryBudget() != CoreConfig.UNLIMITED) {
            left = cfg.memoryBudget() - Math.min(cfg.memoryBudget(), used);
        }
        // sendHeld is already charged; add it back to get an absolute cap.
        long held = conn.sendHeld();
        long cap = left >= Long.MAX_VALUE - held ? Long.MAX_VALUE : held + left;
        return Math.max(Math.max(64L << 20, cap), cfg.sendBuffer());
    }
}
