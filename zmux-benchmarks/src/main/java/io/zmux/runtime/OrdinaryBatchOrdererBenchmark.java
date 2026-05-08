package io.zmux.runtime;

import io.zmux.SchedulerHint;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
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
public class OrdinaryBatchOrdererBenchmark {
    private static void consumeOrder(OrdinaryBatchOrderer.OrderView order, Blackhole blackhole) {
        int checksum = 0;
        for (int index = 0; index < order.size(); ++index) {
            checksum = 31 * checksum + order.indexAt(index);
        }
        blackhole.consume(checksum);
    }

    @Benchmark
    public void balancedOrderView(BatchState state, Blackhole blackhole) {
        OrdinaryBatchOrderer.OrderView order = OrdinaryBatchOrderer.orderView(
                state.balancedBatch,
                SchedulerHint.UNSPECIFIED_OR_BALANCED,
                16_384L,
                state.retainedBias,
                state.workspace
        );
        consumeOrder(order, blackhole);
    }

    @Benchmark
    public void groupFairOrderView(BatchState state, Blackhole blackhole) {
        OrdinaryBatchOrderer.OrderView order = OrdinaryBatchOrderer.orderView(
                state.groupFairBatch,
                SchedulerHint.GROUP_FAIR,
                16_384L,
                state.retainedBias,
                state.workspace
        );
        consumeOrder(order, blackhole);
    }

    @State(Scope.Thread)
    public static class BatchState {
        @Param({"32", "256"})
        int batchSize;

        ArrayList<OrdinaryBatchOrderer.BatchFrame> balancedBatch;
        ArrayList<OrdinaryBatchOrderer.BatchFrame> groupFairBatch;
        OrdinaryBatchOrderer.Workspace workspace;
        OrdinaryBatchOrderer.RetainedBias retainedBias;

        @Setup(Level.Trial)
        public void setup() {
            balancedBatch = new ArrayList<>(batchSize);
            groupFairBatch = new ArrayList<>(batchSize);
            for (int index = 0; index < batchSize; ++index) {
                long streamId = 4L + (long) index * 4L;
                long priority = (index % 8L) * 8L;
                long cost = 256L + (index % 5L) * 1024L;
                Long group = (long) (index % 16);
                balancedBatch.add(new OrdinaryBatchOrderer.BatchFrame(
                        streamId,
                        true,
                        index % 17 == 0,
                        false,
                        cost,
                        priority,
                        group
                ));
                groupFairBatch.add(new OrdinaryBatchOrderer.BatchFrame(
                        streamId,
                        true,
                        index % 19 == 0,
                        false,
                        cost,
                        priority,
                        group
                ));
            }
            workspace = new OrdinaryBatchOrderer.Workspace();
            retainedBias = new OrdinaryBatchOrderer.RetainedBias();
        }
    }
}
