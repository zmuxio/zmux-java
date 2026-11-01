package io.zmux.helper;

import java.util.concurrent.CompletionStage;

/**
 * An application budget or meter. Readiness does not reserve bytes; charge only what was forwarded.
 * Calls can run concurrently when shared by streams or byte/datagram operations; implementations
 * synchronize their own budget and keep tryReady/charge nonblocking.
 */
public interface Limiter {
    /** Allows between 1 and max bytes, waiting for min or the limiter's smaller burst. Cancellation reserves nothing. */
    CompletionStage<Integer> ready(int min, int max);
    /** Immediate allowance, 0 through max; never waits. */
    int tryReady(int max);
    /** Accounts for bytes actually forwarded. */
    void charge(int bytes);
}
