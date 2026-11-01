package io.zmux.internal.diag;

import io.zmux.EventLoops;
import io.zmux.internal.loop.EventLoopGroup;
import java.util.function.Function;

/** Resource counters of event loops for leak checks in tests and benchmarks; not public API. */
public final class Diagnostics {
    private static volatile Function<EventLoops, EventLoopGroup> groups;

    private Diagnostics() {}

    /** Called once by {@link EventLoops} when it is initialised. */
    public static void install(Function<EventLoops, EventLoopGroup> access) {
        if (groups == null) groups = access;
    }

    /** Read buffers taken from the loops' pools and not yet returned. */
    public static int outstandingBuffers(EventLoops loops) {
        return groups.apply(loops).outstandingBuffers();
    }

    /** Input bytes the loops' transports copied into read buffers. */
    public static long copiedInputBytes(EventLoops loops) {
        return groups.apply(loops).sum(io.zmux.internal.loop.SessionHost::copiedInputBytes);
    }

    /** Input bytes the loops' sessions parsed where their carriers put them. */
    public static long inPlaceInputBytes(EventLoops loops) {
        return groups.apply(loops).sum(io.zmux.internal.loop.SessionHost::inPlaceInputBytes);
    }

    /** Bytes of owned input buffers the loops' sessions still reference. */
    public static long retainedInputBytes(EventLoops loops) {
        return groups.apply(loops).sum(io.zmux.internal.loop.SessionHost::retainedInputBytes);
    }

    /** Live loop threads of the group. */
    public static int liveThreads(EventLoops loops) {
        return groups.apply(loops).liveThreads();
    }
}
