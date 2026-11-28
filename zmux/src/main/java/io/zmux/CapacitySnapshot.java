package io.zmux;

import java.util.List;

/** A session owner's view of direction coverage, members and occupied capacity. */
public record CapacitySnapshot(long observed, long policyRevision, long authGeneration, long epoch, Directions required, Directions covered,
                               Directions stable, List<Member> members, int envelope, int localAttachments, int peerAttachments,
                               int reserved, int pendingPeerJoins, int localCarriers, int authorizationExecutions,
                               CapacityLimits limits, java.util.Map<String, Directions> topology) {
    public CapacitySnapshot {
        members = List.copyOf(members);
        topology = java.util.Map.copyOf(topology);
    }

    /** An attachment's health, current outbound measurements, and their sample identities. */
    public record Path(PipeId pipe, PipeHealth health, java.util.Optional<PathQuality> outbound,
                       java.util.Optional<ObservedLoss> outboundLoss, List<QualitySample> samples) {
        public Path { samples = List.copyOf(samples); }
    }

    /** Identity and original expiry of a still-valid observation from this path. */
    public record QualitySample(SessionTarget.QualityInput.Field field, long identity, long sampledAt, long validUntil,
                                CarrierId connection, long counterGeneration) {}

    /** The shared native interval and its observation and expiry in the session clock domain. */
    public record ObservedLoss(CarrierLoss interval, long sampledAt, long validUntil) {}

    /** An attachment group and its current contribution to coverage. */
    public record Member(MemberId id, String candidate, List<PipeId> pipes, Directions expected, Directions covered,
                         Directions stable, boolean complete, boolean draining, long joinedAt, boolean rotationDue, List<Path> paths) {
        public Member { pipes = List.copyOf(pipes); paths = List.copyOf(paths); }
    }
}
