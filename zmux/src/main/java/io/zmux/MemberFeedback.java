package io.zmux;

import io.zmux.internal.core.SessionCapacity.MemberHealth;
import io.zmux.internal.core.Time;
import java.util.HashMap;
import java.util.Map;

/** Reduces confirmed members' service edges on the session owner. */
final class MemberFeedback {
    private static final long UNSET = Time.NEVER;
    private static final Runnable INACTIVE = () -> {};

    private static final class Member {
        SessionController.Candidate candidate;
        boolean stable, failedRound;
        long window, exposure = UNSET;

        Member(SessionController.Candidate candidate, long window, boolean stable) {
            this.candidate = candidate;
            this.window = window;
            this.stable = stable;
        }
    }

    private final SessionController controller;
    private final ZmuxSession session;
    private final Map<MemberId, Member> known = new HashMap<>();
    private Runnable cancel = INACTIVE;
    private long scheduled = UNSET;

    MemberFeedback(SessionController controller, ZmuxSession session) {
        this.controller = controller;
        this.session = session;
    }

    void start() {
        session.core.memberFeedback(this::observe);
        session.core.performanceFeedback((id, at, validUntil, rtt, rate) -> {
            var member = known.get(id);
            if (member == null) return;
            var c = member.candidate;
            controller.cache.report(c.cacheKey(), c.kind(), c.variant(), CarrierCache.Outcome.PERFORMANCE,
                    new CarrierCache.Observation(controller.clock().wallTime(at), java.time.Duration.ZERO,
                            rtt, rate, java.time.Duration.ofNanos(validUntil - at), CarrierCache.MeasurementSource.PROTOCOL));
        });
        long now = session.core.now();
        for (var member : session.core.capacity().members()) {
            if (known.containsKey(member.id())) continue;
            observe(new MemberHealth(member.id(), member.candidate(), member.expected(), member.covered(), member.stable(),
                    now, false, false, null));
        }
    }

    void takeOver(MemberFeedback previous) {
        long at = session.core.now();
        previous.cancel.run();
        for (var row : previous.known.entrySet()) {
            var member = row.getValue();
            previous.advance(member, at);
            var candidate = controller.candidate(member.candidate.id());
            if (previous.controller.cache == controller.cache && candidate != null
                    && SessionController.key(member.candidate).equals(SessionController.key(candidate))) {
                member.candidate = candidate;
                known.put(row.getKey(), member);
                continue;
            }
            previous.closeExposure(member, at);
            previous.health(member, null);
            if (candidate != null) {
                var next = new Member(candidate, member.window, member.stable);
                next.failedRound = member.failedRound;
                if (member.stable) {
                    if (next.window == UNSET) next.exposure = at;
                    health(next, at);
                }
                known.put(row.getKey(), next);
            }
        }
        previous.known.clear();
        start();
        arm();
    }

    void observe(MemberHealth edge) {
        Member member = known.get(edge.id());
        boolean full = edge.covered() == edge.expected();
        boolean stable = full && edge.stable() == edge.expected();
        long at = edge.at();
        if (member == null) {
            var candidate = controller.candidate(edge.candidate());
            if (candidate == null || (!edge.failed() && (edge.covered() == Directions.NONE || edge.retired()))) return;
            member = new Member(candidate, Time.add(at, Time.nanos(controller.cache.config().dropWindow())), stable);
            known.put(edge.id(), member);
            if (stable) health(member, at);
        }
        advance(member, at);
        if (edge.failed() && !member.failedRound) {
            member.failedRound = true;
            var outcome = member.window == UNSET ? CarrierCache.Outcome.LATE_DROP : CarrierCache.Outcome.SHORT_DROP;
            controller.reportFault(member.candidate, outcome, at, edge.episode());
            member.window = UNSET;
        }
        if (member.stable && (!stable || edge.retired())) {
            closeExposure(member, at);
            health(member, null);
        } else if (!member.stable && stable && !edge.retired()) {
            if (member.window == UNSET) member.exposure = at;
            health(member, at);
        }
        if (full && !edge.failed()) member.failedRound = false;
        member.stable = stable;
        if (edge.retired()) known.remove(edge.id());
        arm();
    }

    private void advance(Member member, long at) {
        if (member.window != UNSET && at >= member.window) {
            if (member.stable) {
                controller.report(member.candidate, CarrierCache.Outcome.SURVIVED, member.window, 0);
                member.exposure = member.window;
            }
            member.window = UNSET;
        }
    }

    private void health(Member member, Long since) {
        var candidate = member.candidate;
        controller.cache.memberHealth(new CarrierCache.Key(candidate.cacheKey(), candidate.kind(), candidate.variant()),
                member, since);
    }

    private void closeExposure(Member member, long at) {
        if (at > member.exposure) {
            controller.report(member.candidate, CarrierCache.Outcome.LIFETIME, at, at - member.exposure);
        }
        member.exposure = UNSET;
    }

    private void arm() {
        long earliest = UNSET;
        for (Member member : known.values()) earliest = Math.min(earliest, member.window);
        if (earliest == scheduled) return;
        cancel.run();
        scheduled = earliest;
        cancel = earliest == UNSET ? INACTIVE : session.driver.capacityDeadline(earliest, () -> {
            scheduled = UNSET;
            long now = session.driver.now();
            for (Member member : known.values()) advance(member, now);
            arm();
        });
    }

    void close() {
        session.core.memberFeedback(null);
        session.core.performanceFeedback(null);
        cancel.run();
        long at = session.driver.now();
        for (Member member : known.values()) {
            advance(member, at);
            health(member, null);
            closeExposure(member, at);
        }
        known.clear();
    }
}
