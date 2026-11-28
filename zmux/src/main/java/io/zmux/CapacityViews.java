package io.zmux;

import io.zmux.internal.core.PathEvidence;
import io.zmux.internal.core.SessionCapacity;
import io.zmux.internal.loop.SessionRuntime;
import java.util.List;
import java.util.Optional;

/** Converts owner observations and controller inputs at the API boundary. */
final class CapacityViews {
    private CapacityViews() {}

    static CapacitySnapshot snapshot(SessionCapacity.Snapshot view) {
        return new CapacitySnapshot(view.observed(), view.policyRevision(), view.authGeneration(), view.epoch(),
                view.required(), view.covered(), view.stable(), view.members().stream().map(CapacityViews::member).toList(),
                view.envelope(), view.localAttachments(), view.peerAttachments(), view.reserved(), view.pendingPeerJoins(),
                view.localCarriers(), view.authorizationExecutions(), view.limits(), view.topology());
    }

    private static CapacitySnapshot.Member member(SessionCapacity.MemberView view) {
        return new CapacitySnapshot.Member(view.id(), view.candidate(), view.pipes(), view.expected(), view.covered(),
                view.stable(), view.complete(), view.draining(), view.joinedAt(), view.rotationDue(),
                view.paths().stream().map(CapacityViews::path).toList());
    }

    private static CapacitySnapshot.Path path(SessionCapacity.Path view) {
        return new CapacitySnapshot.Path(view.pipe(), view.health(), Optional.ofNullable(view.outbound()).map(CapacityViews::quality),
                Optional.ofNullable((SessionRuntime.LossEvidence) view.loss()).map(SessionRuntime.LossEvidence::observed),
                view.samples().stream().map(CapacityViews::sample).toList());
    }

    private static PathQuality quality(PathEvidence evidence) {
        return new PathQuality(evidence.generation(), evidence.currentRtt().map(CapacityViews::timing),
                evidence.baselineRtt().map(CapacityViews::timing), evidence.delivery().map(CapacityViews::delivery));
    }

    private static PathQuality.Timing timing(PathEvidence.Timing sample) {
        return new PathQuality.Timing(sample.sampledAt(), sample.validUntil(), sample.rtt());
    }

    private static PathQuality.Delivery delivery(PathEvidence.Delivery sample) {
        var load = switch (sample.load()) {
            case BACKLOGGED -> PathQuality.DeliveryLoad.BACKLOGGED;
            case LIMITED -> PathQuality.DeliveryLoad.LIMITED;
            case UNKNOWN -> PathQuality.DeliveryLoad.UNKNOWN;
        };
        return new PathQuality.Delivery(sample.start(), sample.end(), sample.bytes(), sample.bytesPerSecond(),
                sample.validUntil(), load);
    }

    private static CapacitySnapshot.QualitySample sample(PathEvidence.Sample sample) {
        return new CapacitySnapshot.QualitySample(SessionTarget.QualityInput.Field.valueOf(sample.field().name()),
                sample.identity(), sample.sampledAt(), sample.validUntil(), (CarrierId) sample.connection(), sample.counterGeneration());
    }

    static PathEvidence.Sample sample(SessionTarget.QualityInput input) {
        return new PathEvidence.Sample(PathEvidence.Field.valueOf(input.field().name()), input.sampleIdentity(),
                input.sampledAt(), input.validUntil(), input.connection(), input.counterGeneration());
    }

    static SessionCapacity.CandidateIdentity identity(CarrierCache.Key key, List<Directions> layout) {
        return new SessionCapacity.CandidateIdentity(key.key(), key.kind(), key.variant(), layout);
    }
}
