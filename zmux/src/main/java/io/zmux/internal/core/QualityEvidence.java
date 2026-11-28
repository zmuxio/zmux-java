package io.zmux.internal.core;

import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/** Retains observed sample identities until their own expiry or withdrawal. */
final class QualityEvidence {
    private record FieldSamples(Object value, PathEvidence.Sample current, LinkedHashSet<PathEvidence.Sample> live) {}

    private final EnumMap<PathEvidence.Field, FieldSamples> fields = new EnumMap<>(PathEvidence.Field.class);
    private long generation, identity;

    boolean observe(PathEvidence quality, PathEvidence.Loss loss, long now) {
        long before = identity;
        if (generation != quality.generation()) {
            fields.clear();
            generation = quality.generation();
        }
        observeTiming(PathEvidence.Field.CURRENT_RTT, quality.currentRtt().orElse(null), now);
        observeTiming(PathEvidence.Field.BASELINE_RTT, quality.baselineRtt().orElse(null), now);
        var delivery = quality.delivery().orElse(null);
        observe(PathEvidence.Field.DELIVERY, delivery, delivery == null ? 0 : delivery.end(),
                delivery == null ? 0 : delivery.validUntil(), null, 0, now);
        observe(PathEvidence.Field.LOSS, loss, loss == null ? 0 : loss.sampledAt(), loss == null ? 0 : loss.validUntil(),
                loss == null ? null : loss.connection(), loss == null ? 0 : loss.counterGeneration(), now);
        return identity != before;
    }

    private void observeTiming(PathEvidence.Field field, PathEvidence.Timing sample, long now) {
        observe(field, sample, sample == null ? 0 : sample.sampledAt(), sample == null ? 0 : sample.validUntil(), null, 0, now);
    }

    private void observe(PathEvidence.Field field, Object value, long at, long expires, Object connection,
                         long counterGeneration, long now) {
        if (value == null) {
            if (fields.remove(field) != null) identity = Math.incrementExact(identity);
            return;
        }
        var previous = fields.get(field);
        var live = previous == null || !Objects.equals(previous.current().connection(), connection)
                || previous.current().counterGeneration() != counterGeneration
                ? new LinkedHashSet<PathEvidence.Sample>() : previous.live();
        // One field publishes in chronological order with a fixed validity duration.
        while (!live.isEmpty() && live.getFirst().validUntil() <= now) live.removeFirst();
        if (previous != null && previous.value().equals(value)) return;
        var next = new PathEvidence.Sample(field, Math.incrementExact(identity), at, expires, connection, counterGeneration);
        identity = next.identity();
        live.add(next);
        fields.put(field, new FieldSamples(value, next, live));
    }

    List<PathEvidence.Sample> latest() {
        return fields.values().stream().map(FieldSamples::current).toList();
    }

    boolean contains(PathEvidence.Sample sample, long now) {
        var field = fields.get(sample.field());
        return field != null && sample.validUntil() > now && field.live().contains(sample);
    }
}
