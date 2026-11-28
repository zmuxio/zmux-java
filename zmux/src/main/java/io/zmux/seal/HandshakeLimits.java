package io.zmux.seal;

/** Bounds concurrent Establish reply construction. Share an instance when entries share a signer or executor. */
public final class HandshakeLimits {
    private final int totalLimit, anonymousLimit;
    private int total, anonymous;

    /** Anonymous work may use at most anonymousLimit permits, with at least one permit reserved for PSK. */
    public HandshakeLimits(int totalLimit, int anonymousLimit) {
        if (anonymousLimit < 0 || anonymousLimit >= totalLimit) {
            throw new IllegalArgumentException("require 0 <= anonymousLimit < totalLimit");
        }
        this.totalLimit = totalLimit;
        this.anonymousLimit = anonymousLimit;
    }

    static HandshakeLimits defaults() {
        return defaults(Runtime.getRuntime().availableProcessors());
    }

    static HandshakeLimits defaults(int processors) {
        int total = Math.max(2, processors);
        return new HandshakeLimits(total, total / 2 + total % 2);
    }

    boolean allowsAnonymous() { return anonymousLimit != 0; }

    synchronized boolean tryAcquire(boolean anonymousHello) {
        if (total == totalLimit || (anonymousHello && anonymous == anonymousLimit)) return false;
        total++;
        if (anonymousHello) anonymous++;
        return true;
    }

    synchronized void release(boolean anonymousHello) {
        total--;
        if (anonymousHello) anonymous--;
    }
}
