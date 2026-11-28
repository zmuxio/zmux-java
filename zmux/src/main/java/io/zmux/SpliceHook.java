package io.zmux;

import java.util.List;

/**
 * A synchronous policy callback for one admitted splice unit. Each step sees the entire current
 * output group. Callbacks must return promptly and must not operate on the splice's streams or
 * wait for its control commands.
 */
@FunctionalInterface
public interface SpliceHook {
    Decision apply(SpliceUnit unit) throws Exception;

    /** A decision for the current group. */
    sealed interface Decision permits Pass, Replace, Drop, Reset {}

    /** Keeps the current group, with an optional wait before submission. */
    record Pass(SpliceWait waitFor) implements Decision {
        public Pass() { this(null); }
    }

    /** Replaces the group with the ordered outputs created by this callback. An empty list drops it. */
    record Replace(List<SpliceUnit.Output> outputs, SpliceWait waitFor) implements Decision {
        public Replace { outputs = List.copyOf(outputs); }
        public Replace(List<SpliceUnit.Output> outputs) { this(outputs, null); }
    }

    /** Drops the current group. */
    record Drop() implements Decision {}

    /** Stops this direction and propagates the supplied application code. */
    record Reset(long code) implements Decision {
        public Reset {
            if (code < 0 || code > io.zmux.internal.wire.VarInt.MAX)
                throw new IllegalArgumentException("invalid reset code");
        }
    }
}
