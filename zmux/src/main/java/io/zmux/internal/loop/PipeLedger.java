package io.zmux.internal.loop;

import io.zmux.AccountingPolicy;
import io.zmux.PipeAccounting;
import io.zmux.PipeId;
import io.zmux.carrier.CarrierHalfSource;
import io.zmux.internal.core.Action;
import io.zmux.internal.core.Time;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.SequencedMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

final class PipeLedger implements PipeAccounting.Ticket {
    private record Bucket(CarrierHalfSource source, boolean overflow) {}

    private static final class Totals {
        private long active, retired, records, partial;
        private boolean imprecise;

        PipeAccounting.Half snapshot(Bucket bucket) {
            return new PipeAccounting.Half(bucket.source(), active, retired, records, partial, imprecise, bucket.overflow());
        }
    }

    final class Segment {
        private final Totals totals;
        private long active;
        private boolean retired;

        private Segment(Totals totals) { this.totals = totals; }

        void add(long n, int count, long tail, boolean uncertain) {
            synchronized (lock) {
                if (retired) totals.retired = Time.add(totals.retired, n);
                else {
                    active = Time.add(active, n);
                    totals.active = Time.add(totals.active, n);
                }
                totals.records = Time.add(totals.records, count);
                totals.partial = Time.add(totals.partial, tail);
                totals.imprecise |= uncertain;
            }
        }

        void retire() {
            synchronized (lock) {
                if (retired) return;
                retired = true;
                totals.active -= active;
                totals.retired = Time.add(totals.retired, active);
            }
        }
    }

    private final Object lock = new Object();
    private final PipeId pipe;
    private final AccountingPolicy policy;
    private final Set<CarrierHalfSource> sources = new HashSet<>();
    private final SequencedMap<Bucket, Totals> send = new LinkedHashMap<>(), receive = new LinkedHashMap<>();
    private final CompletableFuture<PipeAccounting> done = new CompletableFuture<>();

    PipeLedger(long pipe, AccountingPolicy policy) {
        this.pipe = PipeId.of(pipe);
        this.policy = policy;
    }

    boolean admits(Transport carrier, int directions) {
        synchronized (lock) {
            if (!policy.strict()) return true;
            var read = (directions & Action.DIR_IN) == 0 ? null : carrier.receiveSource();
            var write = (directions & Action.DIR_OUT) == 0 ? null : carrier.sendSource();
            int extra = read != null && !sources.contains(read) ? 1 : 0;
            if (write != null && !write.equals(read) && !sources.contains(write)) extra++;
            return extra <= policy.sourceLimit() - sources.size();
        }
    }

    Segment half(CarrierHalfSource source, boolean sending) {
        synchronized (lock) {
            boolean overflow = source != null && !sources.contains(source) && sources.size() >= policy.sourceLimit();
            if (!overflow && source != null) sources.add(source);
            var bucket = new Bucket(overflow ? null : source, overflow);
            return new Segment((sending ? send : receive).computeIfAbsent(bucket, ignored -> new Totals()));
        }
    }

    void finishAfter(CompletionStage<Void> release) {
        release.whenCompleteAsync((value, failure) -> done.complete(snapshot()), Callbacks.EXECUTOR);
    }

    @Override public PipeAccounting snapshot() {
        synchronized (lock) {
            return new PipeAccounting(pipe, snapshot(send), snapshot(receive));
        }
    }

    private static List<PipeAccounting.Half> snapshot(SequencedMap<Bucket, Totals> halves) {
        return halves.entrySet().stream().map(e -> e.getValue().snapshot(e.getKey())).toList();
    }

    @Override public CompletableFuture<PipeAccounting> completion() { return done.copy(); }
}
