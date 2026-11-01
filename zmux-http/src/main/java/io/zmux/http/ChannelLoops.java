package io.zmux.http;

import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * A channel's event loop, borrowed: the loop belongs to its group, whose owner shuts it down.
 * These helpers hand it on only as the narrow capability a caller needs.
 */
final class ChannelLoops {
    private ChannelLoops() {}

    /** Runs tasks on ch's loop. */
    static Executor executor(Channel ch) {
        return ch.eventLoop();
    }

    /** Whether the calling thread is ch's loop. */
    static boolean inLoop(Channel ch) {
        return inLoop(ch.eventLoop());
    }

    /** Whether the calling thread is loop. */
    static boolean inLoop(EventExecutor loop) {
        return loop.inEventLoop();
    }

    /** Runs task on ch's loop after delay. */
    static ScheduledFuture<?> schedule(Channel ch, Runnable task, long delay, TimeUnit unit) {
        return schedule(ch.eventLoop(), task, delay, unit);
    }

    private static ScheduledFuture<?> schedule(EventExecutor loop, Runnable task, long delay, TimeUnit unit) {
        return loop.schedule(task, delay, unit);
    }
}
