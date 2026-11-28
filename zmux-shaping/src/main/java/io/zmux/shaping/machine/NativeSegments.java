package io.zmux.shaping.machine;

import io.zmux.shaping.*;
import java.time.Duration;
import java.util.List;
import java.util.Set;

/** Versioned table over the unpadded, encoded native message length. */
public final class NativeSegments implements ShaperFactory {
    private static final int MAX_UNIT = 65535 + Long.BYTES;

    /** The plan for one half-open range of encoded native message lengths. */
    public record Range(int lo, int hi, int padTo, int padMultiple, int pad, Duration delay) {}

    private final List<Range> ranges;
    private final ProfileLimits limits;

    public NativeSegments(int version, ProfileLimits limits, List<Range> ranges) {
        if (version != 1) throw new DefinitionException(DefinitionException.Category.SCHEMA_VERSION, "segment version");
        this.limits = java.util.Objects.requireNonNull(limits);
        this.ranges = List.copyOf(ranges);
        int previous = 0;
        for (Range range : this.ranges) {
            if (range.lo < previous || range.hi <= range.lo || range.hi > MAX_UNIT + 1
                    || invalid(range.pad) || invalid(range.padTo) || invalid(range.padMultiple)
                    || range.delay.isNegative() || range.delay.compareTo(Duration.ofSeconds(1_000_000_000L)) > 0) {
                throw new DefinitionException(DefinitionException.Category.RANGE, "invalid or overlapping native segment");
            }
            previous = range.hi;
        }
    }

    private static boolean invalid(int length) { return length < 0 || length > MAX_UNIT; }

    public ShapingBudget budget() { return new ShapingBudget(null, limits, false); }

    private Range lookup(int length) {
        int low = 0, high = ranges.size();
        while (low < high) {
            int middle = low + (high - low) / 2;
            if (ranges.get(middle).hi <= length) low = middle + 1;
            else high = middle;
        }
        return low < ranges.size() && ranges.get(low).lo <= length ? ranges.get(low) : null;
    }

    @Override public Shaper create(ShapedPipe pipe) {
        return new Shaper() {
            @Override public Set<Capability> capabilities() { return Set.of(Capability.DATAGRAM_PLAN); }
            @Override public void plan(RecordView view, RecordPlan plan) {}
            @Override public void onEvent(Event event, Effects effects) {}
            @Override public void planDatagram(DatagramView view, UnitPlan plan, Effects effects) {
                Range range = lookup(view.length());
                if (range != null) plan.padTo(range.padTo).padMultiple(range.padMultiple).pad(range.pad)
                        .delayNanos(range.delay.toNanos());
            }
        };
    }
}
