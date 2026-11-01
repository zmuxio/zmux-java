package io.zmux.internal.loop;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/** The executor that completes futures and runs callbacks, never a loop thread (I 2.5.3). */
final class Callbacks {
    static final Executor EXECUTOR = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("zmux-callback").factory());

    private Callbacks() {}
}
