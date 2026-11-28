package io.zmux.internal.core;

import io.zmux.CapacityLimits;
import io.zmux.Direction;
import io.zmux.PipeHealth;
import io.zmux.Directions;
import io.zmux.MemberId;
import io.zmux.PipeId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Projects attachment ownership and commits planned drains on the session owner. */
public final class SessionCapacity {

    /** The owner’s membership, coverage and reservations at one observation. */
    public record Snapshot(long observed, long policyRevision, long authGeneration, long epoch, Directions required,
                           Directions covered, Directions stable, List<MemberView> members, int envelope,
                           int localAttachments, int peerAttachments, int reserved, int pendingPeerJoins,
                           int localCarriers, int authorizationExecutions, CapacityLimits limits, Map<String, Directions> topology) {
        public Snapshot { members = List.copyOf(members); }
    }

    /** One member’s attachments and contribution to coverage. */
    public record MemberView(MemberId id, String candidate, List<PipeId> pipes, Directions expected, Directions covered,
                             Directions stable, boolean complete, boolean draining, long joinedAt, boolean rotationDue,
                             List<Path> paths) {
        public MemberView { pipes = List.copyOf(pipes); paths = List.copyOf(paths); }
    }

    /** One attachment’s health and current outbound evidence. */
    public record Path(PipeId pipe, PipeHealth health, PathEvidence outbound, PathEvidence.Loss loss,
                       List<PathEvidence.Sample> samples) {}

    /** The carrier registration and layout attributed to a live member. */
    public record CandidateIdentity(String key, io.zmux.CarrierKind kind, String variant, List<Directions> layout) {
        public CandidateIdentity {
            Objects.requireNonNull(key);
            Objects.requireNonNull(kind);
            Objects.requireNonNull(variant);
            layout = List.copyOf(layout);
        }
    }

    private final Session session;
    private long observed;
    private java.util.function.Consumer<MemberHealth> feedback;

    void feedback(java.util.function.Consumer<MemberHealth> observer) { feedback = observer; }

    /** One owner-committed change to a tracked member’s coverage or fault state. */
    public record MemberHealth(MemberId id, String candidate, Directions expected, Directions covered,
                               Directions stable, long at, boolean failed, boolean retired, io.zmux.carrier.CarrierFaultException.Episode episode) {}

    void healthChanged(PathSession path, Pipe changed, io.zmux.PipeHealth before, io.zmux.PipeHealth after) {
        if (feedback == null) return;
        Member member = changed.member();
        if (member.candidate.isEmpty()) return;
        int covered = 0, stable = 0;
        boolean retired = true;
        for (PipeId id : member.pipes) {
            Pipe pipe = session.pipes.get(id.value());
            if (pipe == null || pipe.mp().retired()) continue;
            retired = false;
            int directions = path.joins.confirmedCoverage(pipe);
            covered |= directions;
            var health = pipe.mp().projection.snapshot();
            if (health == null) continue;
            if (health.send().settled()) stable |= directions & Action.DIR_OUT;
            if (health.receive().settled()) stable |= directions & Action.DIR_IN;
        }
        boolean endedStall = before != null && before.send().state() == io.zmux.DirectionHealth.State.UNAVAILABLE
                && (before.send().reason() == io.zmux.DirectionHealth.Reason.ACK_STALL
                    || before.send().reason() == io.zmux.DirectionHealth.Reason.WRITE_STALL)
                && after.send().state() == io.zmux.DirectionHealth.State.ENDED;
        boolean failed = before != null && !endedStall
                && (failed(before.send(), after.send()) || failed(before.receive(), after.receive()));
        if (failed) member.faultReported = true;
        var fault = failed ? io.zmux.carrier.CarrierFaultException.episodeOf(after.failure()) : null;
        var episode = fault != null && fault.connection() == changed.conn() ? fault : null;
        feedback.accept(new MemberHealth(member.id, member.candidate, directions(member.expected), directions(covered),
                directions(stable), session.now(), failed, retired, episode));
    }

    private static boolean failed(io.zmux.DirectionHealth before, io.zmux.DirectionHealth after) {
        if (!before.serves() || (after.state() != io.zmux.DirectionHealth.State.ENDED
                && after.state() != io.zmux.DirectionHealth.State.UNAVAILABLE)) return false;
        return switch (after.reason()) {
            case ACK_STALL, WRITE_STALL, INBOUND_SILENCE, CARRIER_ERROR, UNEXPECTED_EOF -> true;
            default -> false;
        };
    }
    private final WaitList watchers = new WaitList();
    private java.util.Map<MemberId, DrainClaim> drainClaims;

    SessionCapacity(Session session) { this.session = session; }

    long observed() { return observed; }

    void changed() {
        if (observed != -1L) observed++;
        if (!watchers.isEmpty()) watchers.wakeEvery(session.outbox);
    }

    boolean waitForChange(long since, Waiter waiter) {
        if (observed != since) return true;
        watchers.add(waiter);
        return false;
    }

    void withdraw(Waiter waiter) { watchers.remove(waiter); }

    /** Ownership of the members selected by one uncommitted change. */
    public static final class DrainClaim {
        private final Set<MemberId> members;

        private DrainClaim(Set<MemberId> members) { this.members = Set.copyOf(members); }
    }

    boolean drainClaimAvailable(Set<MemberId> members) {
        return drainClaims == null || members.stream().noneMatch(drainClaims::containsKey);
    }

    DrainClaim claimDrain(Set<MemberId> members) {
        if (drainClaims == null) drainClaims = new java.util.HashMap<>();
        var claim = new DrainClaim(members);
        for (var id : members) drainClaims.put(id, claim);
        changed();
        return claim;
    }

    void releaseDrain(DrainClaim claim) {
        boolean released = false;
        for (var id : claim.members) released |= drainClaims.remove(id, claim);
        if (released) changed();
    }

    void invalidateDrains() {
        if (drainClaims != null) drainClaims.clear();
    }

    /** The session's side of a member. Its directions are fixed when its first carrier is handed over. */
    public static final class Member {
        private final MemberId id;
        private String candidate;
        private CandidateIdentity identity;
        private boolean faultReported;
        private final int expected;
        private final int expectedAttachments;
        private final List<PipeId> pipes = new ArrayList<>();
        private Tiers.PipeSettings preferences;
        private long joinedAt = Time.NEVER;
        private long sent, received, sendLimit, receiveLimit;

        boolean rotationDue() { return (sendLimit > 0 && sent >= sendLimit) || (receiveLimit > 0 && received >= receiveLimit); }

        boolean rotating() { return sendLimit > 0 || receiveLimit > 0; }

        void rotation(long send, long receive) {
            if (send != sendLimit && sendLimit == 0) sent = 0;
            if (receive != receiveLimit && receiveLimit == 0) received = 0;
            sendLimit = send;
            receiveLimit = receive;
        }

        boolean record(boolean sending, long bytes) {
            if (!rotating()) return false;
            boolean before = rotationDue();
            if (sending) sent = Time.add(sent, bytes);
            else received = Time.add(received, bytes);
            return !before && rotationDue();
        }

        void confirmed(long now) {
            if (joinedAt == Time.NEVER) joinedAt = now;
        }

        Member(MemberId id, String candidate, int expected, int expectedAttachments) {
            this.id = id;
            this.candidate = candidate;
            this.expected = expected;
            this.expectedAttachments = expectedAttachments;
        }

        /** The stable ID allocated by the session owner. */
        public MemberId id() { return id; }

        public void attribute(CandidateIdentity identity) { this.identity = identity; }

        void checkIdentity(String candidate, CandidateIdentity next) {
            if (this.candidate.equals(candidate) && identity != null && !identity.equals(next)) {
                throw new IllegalArgumentException("live candidate identity changed");
            }
        }

        public boolean faultReported() { return faultReported; }

        String candidate() { return candidate; }
        int expected() { return expected; }

        void joined(PipeId pipe) { pipes.add(pipe); }

        boolean bind(String source) {
            if (candidate.equals(source)) return false;
            if (!candidate.isEmpty()) throw new IllegalArgumentException("member already belongs to candidate " + candidate);
            candidate = source;
            return true;
        }
    }

    private static final class Coverage {
        private int covered;
        private int stable;
        private final List<Path> paths = new ArrayList<>();
        private int fullAttachments;
        private boolean draining;

        void include(int service, int settled) {
            covered |= service;
            stable |= settled;
        }
    }

    Snapshot snapshot(int carriers, int authorizationExecutions) {
        var members = new LinkedHashMap<Member, Coverage>();
        var total = new Coverage();
        PathSession path = session.path() instanceof PathSession multipath ? multipath : null;
        int local = 0, peer = 0;
        for (Pipe pipe : session.pipes.all()) {
            Attach attach = pipe.mp();
            if (attach == null ? !session.established() : attach.retired()) continue;
            if (attach == null ? session.initiator() : attach.joinerLocal) {
                if (attach == null || !attach.joining.held()) local++;
            } else peer++;
            var health = session.pipeHealth(pipe);
            int covered = 0, stable = 0;
            if (health.peerAttached()) {
                if (path != null) covered = path.joins.confirmedCoverage(pipe);
                else {
                    if (health.send().serves()) covered |= Action.DIR_OUT;
                    if (health.receive().serves()) covered |= Action.DIR_IN;
                }
                if (health.send().settled()) stable |= Action.DIR_OUT;
                if (health.receive().settled()) stable |= Action.DIR_IN;
            }
            var member = members.computeIfAbsent(pipe.member(), key -> new Coverage());
            var quality = pipe.out() == null ? null : pipe.out().snd.quality(health.send(), session.now());
            var loss = quality == null ? null : session.publishedLoss(pipe.id);
            if (quality != null) pipe.out().snd.qualityEvidence.observe(quality, loss, session.now());
            member.paths.add(new Path(PipeId.of(pipe.id), health, quality, loss, quality == null ? List.of() : pipe.out().snd.qualityEvidence.latest()));
            member.include(covered, stable);
            member.draining |= attach != null && attach.health.draining();
            if (covered == configuredDirections(pipe)) member.fullAttachments++;
            total.include(covered, stable);
        }
        List<MemberView> views = new ArrayList<>(members.size());
        members.forEach((member, coverage) -> views.add(new MemberView(member.id, member.candidate, member.pipes,
                directions(member.expected), directions(coverage.covered), directions(coverage.stable),
                coverage.fullAttachments == member.expectedAttachments && coverage.covered == member.expected, coverage.draining,
                member.joinedAt, member.rotationDue(), coverage.paths)));
        long epoch = 0;
        int pending = 0, reserved = 0;
        if (path != null) {
            epoch = path.joins.epoch();
            pending = path.joins.pendingPeerJoins();
            reserved = path.joins.reservedJoins();
        }
        var policy = session.sessionPolicy();
        return new Snapshot(observed, policy.revision(), policy.authGeneration(), epoch, Directions.BOTH,
                directions(total.covered), directions(total.stable), views, session.config().sessionPolicy().limits().attachmentLimit(),
                local, peer, reserved, pending, carriers, authorizationExecutions, policy.limits(), policy.topology());
    }

    boolean validQualityInput(MemberId member, PipeId id, Direction direction, long generation, PathEvidence.Sample sample) {
        var pipe = session.pipes.get(id.value());
        if (pipe == null || (pipe.mp() != null && pipe.mp().retired()) || !pipe.member().id().equals(member)) return false;
        var health = session.pipeHealth(pipe);
        if (!PathEvidence.matches(health, direction, generation)) return false;
        if (sample.field() == PathEvidence.Field.HEALTH) return true;
        long now = session.now();
        if (pipe.out() == null || direction != Direction.OUTBOUND || now < sample.sampledAt()) return false;
        var evidence = pipe.out().snd.qualityEvidence;
        evidence.observe(pipe.out().snd.quality(health.send(), now), session.publishedLoss(pipe.id), now);
        return evidence.contains(sample, now);
    }

    void setMemberPolicy(PathSession path, MemberId id, Tiers.PipeSettings next, long sendLimit, long receiveLimit) {
        Member member = null;
        for (Pipe pipe : session.pipes.all()) {
            if (pipe.member().id.equals(id)) { member = pipe.member(); break; }
        }
        if (member == null) return;
        member.rotation(sendLimit, receiveLimit);
        if (next.equals(member.preferences)) return;
        member.preferences = next;
        for (PipeId pipeId : member.pipes) {
            Pipe pipe = session.pipes.get(pipeId.value());
            if (pipe == null) continue;
            if (pipe.out() == null) {
                path.tiers.setPipe(pipe.id, next.toBuilder().sendPriority(null).sendStandby(null).weight(null).build());
            } else if (pipe.in() == null) {
                path.tiers.setPipe(pipe.id, next.toBuilder().recvPriority(null).recvStandby(null).build());
            } else path.tiers.setPipe(pipe.id, next);
        }
    }

    boolean drain(PathSession path, Set<MemberId> ids, long minimumEpoch,
                  java.util.function.Predicate<List<PipeId>> beforeDrain) {
        if (path.joins.epoch() < minimumEpoch) return false;
        var selected = new ArrayList<Pipe>();
        var found = new java.util.HashSet<MemberId>();
        int remaining = 0;
        for (Pipe pipe : session.pipes.all()) {
            if (pipe.mp().retired()) continue;
            MemberId member = pipe.member().id();
            if (ids.contains(member)) {
                found.add(member);
                if (!pipe.mp().health.draining()) selected.add(pipe);
            } else {
                remaining |= path.joins.confirmedCoverage(pipe);
            }
        }
        if (!found.equals(ids) || remaining != Action.DIR_BIDI) return false;
        if (beforeDrain != null && !beforeDrain.test(selected.stream().map(pipe -> new PipeId(pipe.id)).toList())) return false;
        for (Pipe pipe : selected) path.health.removePipe(pipe.id, true);
        return true;
    }

    boolean drainPipe(PathSession path, long id) {
        Pipe selected = session.pipes.get(id);
        if (selected == null || selected.mp().retired()) return false;
        if (selected.mp().health.draining()) return true;
        if (drainClaims != null && drainClaims.containsKey(selected.member().id())) return false;
        int remaining = 0;
        for (Pipe pipe : session.pipes.all()) {
            if (pipe != selected) remaining |= path.joins.confirmedCoverage(pipe);
            if (remaining == Action.DIR_BIDI) return path.health.removePipe(id, true);
        }
        return false;
    }

    boolean rollback(PathSession path, MemberId member) {
        int remaining = 0;
        var selected = new ArrayList<Pipe>();
        for (Pipe pipe : session.pipes.all()) {
            if (pipe.mp().retired()) continue;
            if (pipe.member().id().equals(member)) selected.add(pipe);
            else remaining |= path.joins.confirmedCoverage(pipe);
        }
        // Preserve just the confirmed attachments that still supply unique service.
        boolean complete = true;
        for (Pipe pipe : selected) {
            int coverage = path.joins.confirmedCoverage(pipe);
            if ((coverage & ~remaining) != 0) {
                remaining |= coverage;
                complete = false;
            } else {
                path.health.removePipe(pipe.id, coverage != 0);
            }
        }
        return complete;
    }

    private static Directions directions(int mask) {
        return switch (mask) {
            case Action.DIR_IN -> Directions.RECEIVE;
            case Action.DIR_OUT -> Directions.SEND;
            case Action.DIR_BIDI -> Directions.BOTH;
            default -> Directions.NONE;
        };
    }

    static int configuredDirections(Pipe pipe) {
        return (pipe.in() == null ? 0 : Action.DIR_IN) | (pipe.out() == null ? 0 : Action.DIR_OUT);
    }
}
