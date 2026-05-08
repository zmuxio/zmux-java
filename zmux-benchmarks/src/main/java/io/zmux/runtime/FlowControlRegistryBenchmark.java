package io.zmux.runtime;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class FlowControlRegistryBenchmark {
    @Benchmark
    public void queueAndDrainSessionWindowUpdates(RegistryState state, Blackhole blackhole) {
        state.registry.queueSessionMaxDataLocked(65_536L);
        state.registry.queueBlockedFrameLocked(0L, 32_768L);
        state.registry.takePendingWindowUpdatesLocked((type, streamId, value) -> {
            blackhole.consume(type);
            blackhole.consume(streamId);
            blackhole.consume(value);
            return true;
        }, 8, null);
        blackhole.consume(state.owner.pendingControlBytes);
    }

    @State(Scope.Thread)
    public static class RegistryState {
        FakeOwner owner;
        SessionFlowControlUpdateRegistry registry;

        @Setup(Level.Invocation)
        public void setup() {
            owner = new FakeOwner();
            registry = new SessionFlowControlUpdateRegistry(owner);
        }
    }

    private static final class FakeOwner implements SessionFlowControlUpdateRegistry.Owner {
        private long pendingControlBytes;

        private static int varintLength(long value) {
            if (value <= 63L) {
                return 1;
            }
            if (value <= 16_383L) {
                return 2;
            }
            if (value <= 1_073_741_823L) {
                return 4;
            }
            return 8;
        }

        @Override
        public long pendingControlFrameBytesLocked(long streamId, long value) {
            return varintLength(streamId) + varintLength(value);
        }

        @Override
        public boolean replacePendingControlBytesLocked(long oldBytes, long newBytes) {
            pendingControlBytes = Math.max(0L, pendingControlBytes - Math.max(0L, oldBytes));
            pendingControlBytes = SessionRuntime.saturatingAdd(pendingControlBytes, newBytes);
            return true;
        }

        @Override
        public void releasePendingControlBytesLocked(long bytes) {
            pendingControlBytes = Math.max(0L, pendingControlBytes - Math.max(0L, bytes));
        }

        @Override
        public void releasePendingControlBytesForHandoffLocked(long bytes) {
            releasePendingControlBytesLocked(bytes);
        }

        @Override
        public boolean allowLocalNonCloseControlLocked() {
            return true;
        }

        @Override
        public StreamRuntime liveStreamLocked(long streamId) {
            return null;
        }
    }
}
