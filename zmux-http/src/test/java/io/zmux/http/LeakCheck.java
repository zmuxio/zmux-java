package io.zmux.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Fails a test class when Netty's leak detector reported a leaked buffer while it ran. Registered for every
 * test of the module by JUnit's extension autodetection (the module's resources name it), and installed
 * before the first buffer class loads so that every detector reports here. Leaks are only noticed after
 * the garbage collector found the buffer, so the end of each class collects and allocates until the
 * detector has polled.
 */
@SuppressWarnings("exports") // test-only; JUnit loads it by service name, so it is public
public final class LeakCheck implements AfterAllCallback {
    private static final List<String> LEAKS = new CopyOnWriteArrayList<>();

    static {
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

    @Override public void afterAll(ExtensionContext context) throws Exception {
        // A leaked buffer is reported by a later allocation once it is collected: nudge both for a while (clients shut their groups down in the background).
        for (int i = 0; i < 24; i++) {
            System.gc();
            Thread.sleep(50);
            ByteBuf b = ByteBufAllocator.DEFAULT.buffer(16);
            b.release();
        }
        if (LEAKS.isEmpty()) return;
        String all = String.join("\n---\n", LEAKS);
        LEAKS.clear();
        throw new AssertionError("Netty reported leaked buffers during " + context.getDisplayName() + ":\n" + all);
    }
}
