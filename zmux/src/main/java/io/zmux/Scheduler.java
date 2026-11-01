package io.zmux;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/** A live scheduling policy. Custom pickers run on the owning loop and must never block. */
public sealed interface Scheduler permits Scheduler.Builtin, Scheduler.Custom {
    Scheduler AFFINITY = Builtin.AFFINITY;
    Scheduler SESSION_AFFINITY = Builtin.SESSION_AFFINITY;
    Scheduler SPREAD_ETA = Builtin.SPREAD_ETA;
    Scheduler SPREAD_ROUND_ROBIN = Builtin.SPREAD_ROUND_ROBIN;
    Scheduler SPREAD_RATE_WEIGHTED = Builtin.SPREAD_RATE_WEIGHTED;

    /** The built-in policies. */
    enum Builtin implements Scheduler {
        AFFINITY, SESSION_AFFINITY, SPREAD_ETA, SPREAD_ROUND_ROBIN, SPREAD_RATE_WEIGHTED
    }

    /** One stream direction waiting for a decision; byte counts exclude already committed data. */
    record Request(long streamId, long bytes, long eligibleTier, boolean hasEligibleTier,
                   long lastPipe, boolean hasLastPipe, int urgency) {}

    /** Values only: no mutable session or carrier handle is exposed. Rates are bytes/s. */
    record Pipe(long id, PipeState.Outbound state, Duration smoothedRtt, Duration rttVariation, long rate,
                long unsentBytes, long bytesInFlight, long priority, long weight, boolean writable,
                boolean standby, boolean saturated, boolean probation, boolean eligible, boolean shaped,
                double shapingWeight) {}

    /** An index into the pipe view, and a byte allowance (0: until invalidated), or wait for input. */
    record Answer(int pipeIndex, long bytes, boolean waitForInput) {
        public Answer {
            if (bytes < 0) throw new IllegalArgumentException("negative byte allowance");
        }
        public static Answer pipe(int index, long bytes) { return new Answer(index, bytes, false); }
        public static Answer waitForInputChange() { return new Answer(-1, 0, true); }
    }

    /**
     * One answer per request. A failed, missing or invalid answer falls back to affinity.
     *
     * <p>The picker runs synchronously on the session's event loop, between its I/O
     * (IMPLEMENTATION.md Section 2.5.2), so it <b>must be fast and must never block</b>: no
     * I/O, locks that other threads hold for long, sleeping or waiting. Blocking zmux calls
     * fail there at once. Answers are sticky, so a policy that binds streams is asked about
     * once per binding and one that spreads should return an allowance of a few milliseconds
     * of the pipe's rate rather than one chunk.
     */
    @FunctionalInterface
    interface Picker {
        List<Answer> pick(List<Request> requests, List<Pipe> pipes);
    }

    /** Creates a custom policy; blocking zmux calls from the picker are rejected. */
    static Scheduler custom(Picker picker) { return new Custom(Objects.requireNonNull(picker)); }

    /** A policy supplied by the application. */
    record Custom(Picker picker) implements Scheduler {
        public Custom { Objects.requireNonNull(picker); }
    }

}
