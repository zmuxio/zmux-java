package io.zmux.nettytest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Fails a test class when Netty's leak detector reported a leaked buffer, zmux's reported a dropped
 * chunk, or an exception reached the tail of a pipeline, while it ran. Registered for every
 * test of a module that depends on this one, by JUnit's extension autodetection (this jar's resources name it), and installed
 * before the first buffer class loads so that every detector reports here. Leaks are only noticed after
 * the garbage collector found the buffer, so the end of each class collects and allocates until the
 * detector has polled.
 */
public final class LeakCheck implements AfterAllCallback {
    private static final List<String> LEAKS = new CopyOnWriteArrayList<>();
    private static final List<String> UNHANDLED = new CopyOnWriteArrayList<>();
    private static final java.util.logging.Logger TAIL = java.util.logging.Logger.getLogger("io.netty.channel.DefaultChannelPipeline");
    private static final java.util.logging.Logger CHUNKS = java.util.logging.Logger.getLogger("io.zmux.Chunk");

    static {
        // The build runs with zmux.leakDetection=paranoid: every chunk dropped without close or transfer is logged.
        CHUNKS.addHandler(new java.util.logging.Handler() {
            @Override public void publish(java.util.logging.LogRecord r) {
                if (r.getLevel().intValue() >= java.util.logging.Level.WARNING.intValue()) LEAKS.add("zmux Chunk: " + r.getMessage() + "\n" + r.getThrown());
            }

            @Override public void flush() {}

            @Override public void close() {}
        });
        // An exception that reached the tail of a pipeline is one no handler took: Netty warns, and so would a production server.
        TAIL.addHandler(new java.util.logging.Handler() {
            @Override public void publish(java.util.logging.LogRecord r) {
                if (r.getLevel().intValue() >= java.util.logging.Level.WARNING.intValue() && r.getMessage() != null && r.getMessage().contains("reached at the tail")) {
                    UNHANDLED.add(String.valueOf(r.getThrown()));
                }
            }

            @Override public void flush() {}

            @Override public void close() {}
        });
        ResourceLeakDetectorFactory.setResourceLeakDetectorFactory(new ResourceLeakDetectorFactory() {
            @Override
            @SuppressWarnings("deprecation") // the factory's only abstract method
            public <T> ResourceLeakDetector<T> newResourceLeakDetector(Class<T> resource, int samplingInterval, long maxActive) {
                return new ResourceLeakDetector<>(resource, samplingInterval) {
                    @Override protected void reportTracedLeak(String resourceType, String records) {
                        LEAKS.add(resourceType + "\n" + records);
                        super.reportTracedLeak(resourceType, records);
                    }

                    @Override protected void reportUntracedLeak(String resourceType) {
                        LEAKS.add(resourceType);
                        super.reportUntracedLeak(resourceType);
                    }
                };
            }
        });
    }

    /** Loaded by JUnit. */
    public LeakCheck() {}

    @SuppressWarnings("InterruptedExceptionSwallowed") // JUnit's signature; an interrupt fails the class, which is right here
    @Override public void afterAll(ExtensionContext context) throws Exception {
        // A leaked buffer is reported by a later allocation once it is collected: nudge both for a while (clients shut their groups down in the background).
        for (int i = 0; i < 24; i++) {
            System.gc();
            Thread.sleep(50);
            ByteBuf b = ByteBufAllocator.DEFAULT.buffer(16);
            b.release();
        }
        if (!UNHANDLED.isEmpty()) {
            String all = String.join("\n", UNHANDLED);
            UNHANDLED.clear();
            throw new AssertionError("An exception reached the tail of a pipeline during " + context.getDisplayName() + ":\n" + all);
        }
        if (LEAKS.isEmpty()) return;
        String all = String.join("\n---\n", LEAKS);
        LEAKS.clear();
        throw new AssertionError("Netty reported leaked buffers during " + context.getDisplayName() + ":\n" + all);
    }
}
