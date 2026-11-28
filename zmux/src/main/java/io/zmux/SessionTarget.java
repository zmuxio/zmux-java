package io.zmux;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Versioned member quotas and replacement intents for a managed session. */
public record SessionTarget(long revision, List<Group> groups, List<Replacement> replace) {
    public SessionTarget {
        groups = List.copyOf(groups);
        replace = List.copyOf(replace);
    }

    public static SessionTarget empty() { return new SessionTarget(0, List.of(), List.of()); }

    /** A named pool that reconciles one logical member at a time. */
    public record Group(String id, List<Quota> quotas, Rotation rotation) {
        public Group {
            if (Objects.requireNonNull(id).isEmpty()) throw new IllegalArgumentException("empty target group");
            quotas = List.copyOf(quotas);
            Objects.requireNonNull(rotation);
        }
    }

    /** Limits counting from confirmation or completed record I/O. Zero leaves a limit unset. */
    public record Rotation(Duration maxAge, long sendBytes, long receiveBytes) {
        public static final Rotation NONE = new Rotation(Duration.ZERO, 0, 0);

        public Rotation {
            if (Objects.requireNonNull(maxAge).isNegative() || sendBytes < 0 || receiveBytes < 0) {
                throw new IllegalArgumentException("negative rotation limit");
            }
        }
    }

    /** Scheduling settings for one local direction. */
    public record PathPreference(long tier, boolean standby, long weight) {
        public PathPreference {
            if (tier < 0 || tier > io.zmux.internal.wire.VarInt.MAX || weight < 0) {
                throw new IllegalArgumentException("invalid path preference");
            }
        }
    }

    /** Overrides a candidate's send and receive settings independently. */
    public record Preferences(PathPreference send, PathPreference receive) {}

    /** A count of complete members covering the given directions from the listed candidates. */
    public record Quota(String id, int count, Directions directions, List<String> candidates,
                        Map<String, Preferences> preferences) {
        public Quota(String id, int count, Directions directions, List<String> candidates) {
            this(id, count, directions, candidates, Map.of());
        }

        public Quota {
            if (Objects.requireNonNull(id).isEmpty() || count < 0 || Objects.requireNonNull(directions) == Directions.NONE) {
                throw new IllegalArgumentException("invalid member quota");
            }
            candidates = List.copyOf(candidates);
            preferences = Map.copyOf(preferences);
            if (!preferences.isEmpty() && !new java.util.HashSet<>(candidates).containsAll(preferences.keySet())) throw new IllegalArgumentException("preference for an unlisted candidate");
            if (count > 0 && candidates.isEmpty()) throw new IllegalArgumentException("quota has no candidate");
        }
    }

    /** A stable intent to replace the specified members, optionally after advancing the Join epoch. */
    public record Replacement(String id, Set<MemberId> members, long minEpoch, List<QualityInput> evidence) {
        public Replacement(String id, Set<MemberId> members, long minEpoch) { this(id, members, minEpoch, List.of()); }
        public Replacement {
            if (Objects.requireNonNull(id).isEmpty() || minEpoch < 0) throw new IllegalArgumentException("invalid replacement intent");
            members = Set.copyOf(members);
            evidence = List.copyOf(evidence);
            for (var input : evidence) if (!members.contains(input.member())) throw new IllegalArgumentException("unrelated quality input");
        }
    }

    /** Original observation relied on by a quality-driven replacement. */
    public record QualityInput(long binding, long targetRevision, MemberId member, PipeId pipe, Direction direction,
                               long generation, Field field, long sampleIdentity, long sampledAt, long validUntil,
                               CarrierId connection, long counterGeneration) {
        /** The directional observation used to make a replacement decision. */
        public enum Field { HEALTH, CURRENT_RTT, BASELINE_RTT, DELIVERY, LOSS }

        /** Whether this input still refers to the same directional health episode. */
        public boolean matches(PipeHealth pipeHealth) {
            return io.zmux.internal.core.PathEvidence.matches(pipeHealth, direction, generation);
        }

        /** The precise field observation and original validity selected from the member view. */
        public CapacitySnapshot.QualitySample observation() {
            return new CapacitySnapshot.QualitySample(field, sampleIdentity, sampledAt, validUntil, connection, counterGeneration);
        }

    }

    boolean evidenceValid(Set<MemberId> draining, java.util.function.Predicate<QualityInput> valid) {
        return replace.stream().flatMap(r -> r.evidence().stream()).filter(input -> draining.contains(input.member())).allMatch(valid);
    }
}
