package io.zmux;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/** A separately bounded queue of immutable input or output samples. */
public final class SpliceMirror implements AutoCloseable {
    /** The boundary at which the sample was collected. */
    public enum Phase { INPUT, OUTPUT }
    /** Whether the sample copies or leases its backing. */
    public enum Ownership { COPY, LEASE }

    /** A bounded view of a unit at the selected boundary. */
    public record Sample(SpliceUnit.Metadata metadata, Phase phase, int index, List<ByteBuffer> payload, int length) {
        public Sample { payload = payload.stream().map(ByteBuffer::asReadOnlyBuffer).toList(); }
        @Override public List<ByteBuffer> payload() { return payload.stream().map(ByteBuffer::duplicate).toList(); }
    }

    private record Entry(Sample sample, IdentityHashMap<Object, SpliceRoots.Size> roots) {}

    private final Set<Phase> phases;
    private final Ownership ownership;
    private final int maxSamples, maxSampleBytes;
    private final long maxBytes;
    private final ArrayDeque<Entry> queue = new ArrayDeque<>();
    private final SpliceRoots held = new SpliceRoots();
    private final AtomicLong misses = new AtomicLong();
    private final ReentrantLock lock = new ReentrantLock();
    private boolean closed;

    public SpliceMirror(int maxSamples, int maxBytes, int maxSampleBytes) {
        this(EnumSet.allOf(Phase.class), Ownership.COPY, maxSamples, maxBytes, maxSampleBytes);
    }

    public SpliceMirror(Set<Phase> phases, Ownership ownership, int maxSamples, long maxBytes, int maxSampleBytes) {
        if (maxSamples <= 0 || maxBytes < 0 || maxSampleBytes < 0 || maxSampleBytes > maxBytes)
            throw new IllegalArgumentException("invalid mirror limits");
        this.phases = Set.copyOf(phases);
        this.ownership = Objects.requireNonNull(ownership);
        this.maxSamples = maxSamples;
        this.maxBytes = maxBytes;
        this.maxSampleBytes = maxSampleBytes;
    }

    /** Returns the number of bytes copied, or -1 for an omitted sample. */
    int publish(SpliceUnit.Metadata metadata, Phase phase, int index, SplicePayload input) {
        if (!phases.contains(phase)) return 0;
        if (!lock.tryLock()) return miss();
        try {
            if (closed || queue.size() >= maxSamples) return miss();
            var roots = new IdentityHashMap<Object, SpliceRoots.Size>();
            var views = new ArrayList<ByteBuffer>();
            int n = Math.min(input.length(), maxSampleBytes);
            if (ownership == Ownership.COPY) {
                if (n > maxBytes - held.bytes()) return miss();
                ByteBuffer copy = ByteBuffer.allocate(n);
                input.copyTo(0, copy, n);
                views.add(copy.flip());
                roots.put(copy, new SpliceRoots.Size(n));
            } else if (!input.mirrorRanges(n, views, roots)) return miss();
            if (!held.retain(roots, maxBytes)) return miss();
            queue.addLast(new Entry(new Sample(metadata, phase, index, views, input.length()), roots));
            return ownership == Ownership.COPY ? n : 0;
        } finally {
            lock.unlock();
        }
    }

    private int miss() { misses.incrementAndGet(); return -1; }

    /** Takes the next sample, or null when there is none. */
    public Sample poll() {
        lock.lock();
        try {
            Entry entry = queue.pollFirst();
            if (entry == null) return null;
            entry.roots().keySet().forEach(held::release);
            return entry.sample();
        } finally { lock.unlock(); }
    }

    public long misses() { return misses.get(); }

    @Override public void close() {
        lock.lock();
        try {
            closed = true;
            for (Entry entry : queue) entry.roots().keySet().forEach(held::release);
            queue.clear();
        } finally { lock.unlock(); }
    }
}
