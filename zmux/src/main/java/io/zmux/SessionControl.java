package io.zmux;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import io.zmux.internal.core.Time;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/** The target binding of one session. The session retains its attached carriers when the binding stops. */
public final class SessionControl implements AutoCloseable {
    /** The stage preventing further progress. */
    public enum Blocking {
        NONE, CAPACITY, EXECUTION, CLEANUP, QUEUE_FULL, NO_ELIGIBLE_CANDIDATE, CONFIRMATION, STABILITY, COVERAGE, EPOCH,
        /** The local session policy excludes every applicable recovery candidate's requested layout. */
        TOPOLOGY_EXCLUDED,
        /** The host requires a later start for the candidate. */
        DIAL_AFTER,
        /** No time remains for a new attempt. */
        DEADLINE,
        /** The controller no longer accepts work. */
        STALE_DECISION, OWNER_CLOSED
    }

    /** The last committed controller observation. */
    public record Snapshot(long generation, Long predecessor, long targetRevision, CapacitySnapshot capacity, Blocking blocked, List<Long> attempts, Long nextRetry, SessionController.Occupancy occupancy) {
        public Snapshot { attempts = List.copyOf(attempts); }
    }

    private record Target(SessionTarget value, CompletableFuture<Void> changed) {}
    private record Slot(SessionTarget.Group group, SessionTarget.Quota quota) {}
    private record Provision(SessionController.Candidate candidate, SessionTarget.Quota quota) {}
    private record Recovery(Provision provision, CapacityChange change, Attempt attempt) {
        void closeChange() { change.close(); }
        void cancelUnneeded() { change.cancelUnneeded(); }
    }
    private record RecoveryStart(Recovery recovery, Blocking blocked) {}
    private record RecoveryPlan(List<Provision> available, Long retry, boolean topologyExcluded, boolean dialBlocked,
                                List<SessionController.BlockedStart> suppressed) {}
    private record MemberPolicy(SessionTarget.Preferences preferences, SessionTarget.Rotation rotation) {}
    private record Plan(List<Provision> add, Set<MemberId> drain, long epoch,
                        Blocking blocked, Long retry, String pool) {}

    private final long generation;
    private final Long predecessor;
    private final SessionController controller;
    private final SessionController.Owner owner;
    private final ZmuxSession session;
    private final java.util.concurrent.ConcurrentHashMap<String, Long> dialAfter;
    private final CompletableFuture<Void> done = new CompletableFuture<>();
    private final CompletableFuture<Void> stop = new CompletableFuture<>();
    private final Thread worker;
    private final MemberFeedback feedback;
    private volatile Target target;
    private volatile Snapshot snapshot;
    private CompletableFuture<Snapshot> nextSnapshot;
    private Throwable snapshotFailure;
    // Only the session owner mutates the registered batches.
    private final Set<CapacityChange> changes = new LinkedHashSet<>();
    private String lastPool;
    private long epochRequested;
    private boolean requestedEpoch;
    private Map<MemberId, MemberPolicy> installed = Map.of();

    SessionControl(SessionController controller, SessionController.Owner owner, ZmuxSession session, SessionTarget target, CapacitySnapshot initial, long generation, Long predecessor) {
        this.generation = generation;
        this.predecessor = predecessor;
        this.controller = controller;
        this.owner = owner;
        this.session = session;
        dialAfter = new java.util.concurrent.ConcurrentHashMap<>(controller.initialDialAfter());
        this.target = new Target(target, new CompletableFuture<>());
        snapshot = new Snapshot(generation, predecessor, target.revision(), initial, Blocking.NONE, List.of(), null, owner.occupancy());
        feedback = new MemberFeedback(controller, session);
        worker = Thread.ofVirtual().name("zmux-session-control").unstarted(this::run);
    }

    ZmuxSession session() { return session; }
    SessionController controller() { return controller; }
    long generation() { return generation; }

    long revision() { return target.value().revision(); }

    final class Fence {
        private final Target desired;

        private Fence(Target desired) { this.desired = desired; }

        long generation() { return generation; }
        long revision() { return desired.value().revision(); }

        boolean current() {
            return session.control.get() == SessionControl.this && target == desired && !stop.isDone();
        }

        Object submit(io.zmux.internal.loop.Operation.Step step, Object stale) throws IOException {
            return session.run(op -> current() ? step.attempt(op) : stale);
        }

        void commit(Runnable step) {
            if (current()) step.run();
        }

        void track(CapacityChange change) { changes.add(change); }
        void untrack(CapacityChange change) { changes.remove(change); }

        void demand() {
            var capacity = session.capacityOnOwner();
            owner.recoveryDemand(wanted(capacity, missing(capacity)));
        }
    }

    Fence fence() { return new Fence(target); }

    // One transaction owns the staggered alternatives and their absolute deadline.
    final class RecoveryBatch {
        private final Fence fence;
        private final Set<CapacityChange> active = new LinkedHashSet<>();
        private long id;
        private long deadline = Time.NEVER;
        private boolean adopted, closed;
        private CapacityChange.Status lastRollback;

        private RecoveryBatch(Fence fence) { this.fence = fence; }

        long bound(long end) { return Math.min(deadline, end); }

        long reserve(CapacityChange change, long end, int attachments) {
            active.add(change);
            if (id == 0) {
                id = session.nextCapacityBatch();
                deadline = end;
                session.driver.controlEvent(new SessionEvent.BatchReserved(generation, fence.revision(), id,
                        session.core.now(), deadline, attachments));
            }
            return id;
        }

        void tighten(long cutoff) {
            if (cutoff >= deadline) return;
            deadline = cutoff;
            for (var change : active) change.tightenDeadline(cutoff);
        }

        void remove(CapacityChange change, CapacityChange.Status status) {
            active.remove(change);
            if (status != CapacityChange.Status.COMMITTED) lastRollback = status;
        }

        Directions need(CapacitySnapshot capacity) {
            var need = capacity.required();
            for (var member : capacity.members()) {
                if (active.stream().noneMatch(change -> change.owns(member.id()))) need = difference(need, member.covered());
            }
            return need;
        }

        void confirmed(CapacityChange change) {
            adopted = true;
            change.terminateOnOwner(CapacityChange.Status.COMMITTED, null);
            var remaining = need(session.capacityOnOwner());
            for (var other : List.copyOf(active)) {
                if (!intersects(remaining, other.supplies())) other.terminateOnOwner(CapacityChange.Status.CANCELLED,
                        Attempt.Disposition.NO_LONGER_NEEDED);
            }
            fence.demand();
        }

        void close(CapacityChange.Status reason) {
            if (closed) return;
            closed = true;
            boolean rolledBack = !adopted || !active.isEmpty();
            for (var change : List.copyOf(active)) change.terminateOnOwner(reason, null);
            if (id != 0 && rolledBack) session.driver.controlEvent(new SessionEvent.BatchRolledBack(
                    generation, fence.revision(), id, session.core.now(), lastRollback == null ? reason : lastRollback));
            if (recoveryBatch == this) recoveryBatch = null;
        }
    }

    private RecoveryBatch recoveryBatch;

    private void terminateChanges(CapacityChange.Status reason) {
        if (recoveryBatch != null) recoveryBatch.close(reason);
        for (var change : List.copyOf(changes)) change.terminateOnOwner(reason, null);
    }

    void install(SessionControl previous) {
        fence().demand();
        if (previous == null) feedback.start();
        else {
            feedback.takeOver(previous.feedback);
            owner.inheritOpportunities(previous.owner, targetCandidates(target.value()));
            previous.revoke();
        }
    }

    private Set<SessionController.Candidate> targetCandidates(SessionTarget value) {
        var result = new LinkedHashSet<SessionController.Candidate>();
        for (var group : value.groups()) for (var quota : group.quotas()) {
            if (quota.count() > 0) for (var id : quota.candidates()) result.add(controller.candidate(id));
        }
        return result;
    }

    private Set<SessionController.Candidate> wanted(CapacitySnapshot capacity, Directions need) {
        var result = new LinkedHashSet<SessionController.Candidate>();
        for (var c : controller.candidates()) {
            if (!c.attachments().isEmpty() && intersects(need, c.member().directions()) && allowed(capacity, c)) result.add(c);
        }
        return result;
    }

    void detach() {
        revoke();
        feedback.close();
    }

    private void revoke() {
        fence().commit(() -> terminateChanges(CapacityChange.Status.CANCELLED));
        stop.complete(null);
        owner.recoveryDemand(Set.of());
        owner.withdraw();
        session.driver.publishCompletion(() -> done.complete(null));
    }

    void start() { worker.start(); }

    /** Validates and publishes an entire new target revision. */
    public synchronized void setTarget(SessionTarget next) throws IOException {
        if (!fence().current()) throw new SessionController.StaleBindingException();
        controller.validate(Objects.requireNonNull(next));
        controller.checkEnvelope(next, session.capacity());
        var current = target;
        if (next.equals(current.value())) return;
        if (Long.compareUnsigned(next.revision(), current.value().revision()) <= 0) {
            throw new IllegalArgumentException("stale or conflicting target revision " + Long.toUnsignedString(current.value().revision()));
        }
        new Fence(current).submit(op -> {
            for (var replacement : next.replace()) for (var input : replacement.evidence()) {
                if (input.binding() != generation || input.targetRevision() != current.value().revision()
                        || !session.validQualityInput(input)) {
                    return new SessionController.StaleDecisionException();
                }
            }
            try { owner.enqueue(); } catch (IllegalStateException unavailable) { return unavailable; }
            terminateChanges(CapacityChange.Status.STALE_TARGET);
            target = new Target(next, new CompletableFuture<>());
            fence().demand();
            owner.retainOpportunities(targetCandidates(next));
            session.core.targetChanged(generation, next.revision(), false, null);
            return null;
        }, new SessionController.StaleBindingException());
        current.changed().complete(null);
    }

    /** Sets this session's earliest start for a candidate in the controller clock domain. Zero clears it. */
    public synchronized void setDialAfter(String candidate, long earliestStart) throws IOException {
        fence().submit(op -> {
            if (controller.candidate(candidate) == null) return new IllegalArgumentException("unknown DialAfter candidate");
            if (earliestStart == 0) dialAfter.remove(candidate);
            else dialAfter.put(candidate, earliestStart);
            controller.scheduleChanged();
            return null;
        }, new SessionController.StaleBindingException());
    }

    private long after(SessionController.Candidate candidate) { return dialAfter.getOrDefault(candidate.id(), 0L); }

    private Long dialBlock(SessionController.Candidate candidate, long now) {
        long after = after(candidate);
        return now < after ? after : null;
    }

    public Snapshot snapshot() { return snapshot; }

    /** Waits for a publication after the observed snapshot, or for the binding to close. */
    synchronized CompletableFuture<Snapshot> awaitSnapshotChange(Snapshot observed) {
        if (snapshot != observed) return CompletableFuture.completedFuture(snapshot);
        if (snapshotFailure != null) return CompletableFuture.failedFuture(snapshotFailure);
        if (nextSnapshot == null) nextSnapshot = new CompletableFuture<>();
        return nextSnapshot.copy();
    }

    public CompletableFuture<Void> done() { return done.copy(); }

    synchronized SessionControl replaceWith(SessionController replacement, SessionTarget target, String bootstrapCandidate) throws IOException {
        return replacement.bind(session, target, bootstrapCandidate, false, replacement.new Owner(), this);
    }

    /** Releases this binding if it still owns the session. */
    public synchronized void release() throws IOException {
        fence().submit(op -> {
            detach();
            session.control.set(null);
            return null;
        }, new SessionController.StaleBindingException());
    }

    @Override public void close() {
        try { release(); }
        catch (SessionController.StaleBindingException | SessionClosedException ended) { /* Already released. */ }
        catch (IOException failure) { throw new java.io.UncheckedIOException(failure); }
    }

    private void run() {
        Throwable failure = null;
        try {
            while (!stop.isDone() && !session.isClosed()) {
                Target desired = target;
                var available = controller.available();
                var capacity = session.capacity();
                Directions need = missing(capacity);
                new Fence(desired).submit(op -> {
                    fence().demand();
                    return null;
                }, null);
                var recovery = need == Directions.NONE ? null : recoveryPlan(desired.value(), capacity, need, Set.of());
                if (recovery != null && !recovery.available().isEmpty()) {
                    Long retry = recover(desired);
                    if (retry != null && desired == target) {
                        var latest = session.capacity();
                        if (missing(latest) != Directions.NONE)
                            await(desired, latest.observed(), available, retry);
                    }
                    continue;
                }
                var suppressed = new ArrayList<SessionController.BlockedStart>();
                Plan plan = recovery == null ? plan(desired, capacity, suppressed)
                        : new Plan(List.of(), Set.of(), 0, Blocking.NO_ELIGIBLE_CANDIDATE, recovery.retry(), null);
                if (recovery != null) suppress(desired, recovery.suppressed());
                else if (plan.add().isEmpty() && plan.epoch() <= capacity.epoch()) suppress(desired, suppressed);
                var blocked = plan.blocked();
                Long retry = earliest(plan.retry(), recovery == null ? null : recovery.retry());
                if (blocked == Blocking.NO_ELIGIBLE_CANDIDATE && recovery != null) {
                    if (recovery.topologyExcluded()) blocked = Blocking.TOPOLOGY_EXCLUDED;
                    else if (recovery.dialBlocked() && java.util.Objects.equals(retry, recovery.retry())) blocked = Blocking.DIAL_AFTER;
                }
                if (plan.add().isEmpty() && plan.drain().isEmpty() && blocked != Blocking.NONE) {
                    blocked = pendingBlock(desired, blocked);
                }
                publish(desired, capacity, blocked, List.of(), retry);
                if (plan.epoch() > capacity.epoch()) {
                    if (!requestedEpoch || epochRequested != capacity.epoch()) {
                        new Fence(desired).submit(op -> {
                            var current = session.capacityOnOwner();
                            if (current.epoch() == capacity.epoch() && missing(current) == Directions.NONE
                                    && desired.value().evidenceValid(plan.drain(), session::validQualityInput)) {
                                session.core.advanceEpoch();
                                requestedEpoch = true;
                                epochRequested = capacity.epoch();
                            }
                            return null;
                        }, null);
                    }
                    publish(desired, capacity, Blocking.EPOCH, List.of());
                } else if (!plan.add().isEmpty() || !plan.drain().isEmpty()) {
                    if (execute(desired, capacity, plan)) continue;
                } else {
                    if (blocked == Blocking.NONE) new Fence(desired).submit(op -> {
                        owner.withdraw();
                        owner.endOpportunities();
                        return null;
                    }, null);
                }
                var current = session.capacity();
                if (current.policyRevision() != capacity.policyRevision() || current.covered() != capacity.covered()
                        || current.stable() != capacity.stable() || current.localCarriers() < capacity.localCarriers()
                        || current.localAttachments() < capacity.localAttachments()) continue;
                await(desired, current.observed(), available, retry);
            }
        } catch (SessionClosedException closed) {
            // Closing the session also stops its binding.
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            failure = interrupted;
        } catch (ExecutionException error) {
            if (!(error.getCause() instanceof SessionClosedException)) failure = error;
        } catch (IOException | RuntimeException error) {
            failure = error;
        } finally {
            controller.unbind(session, this);
            var closed = failure == null ? new IllegalStateException("control binding stopped") : failure;
            CompletableFuture<Snapshot> unpublished;
            synchronized (this) {
                unpublished = nextSnapshot;
                nextSnapshot = null;
                snapshotFailure = closed;
            }
            if (unpublished != null) unpublished.completeExceptionally(closed);
            if (failure == null) done.complete(null);
            else done.completeExceptionally(failure);
        }
    }

    private void publish(Target desired, CapacitySnapshot capacity, Blocking blocked, List<Long> attempts) {
        publish(desired, capacity, blocked, attempts, null);
    }

    private void publish(Target desired, CapacitySnapshot capacity, Blocking blocked, List<Long> attempts, Long retry) {
        CompletableFuture<Snapshot> published;
        Snapshot value;
        synchronized (this) {
            if (!new Fence(desired).current()) return;
            value = new Snapshot(generation, predecessor, desired.value().revision(), capacity, blocked, attempts, retry, owner.occupancy());
            snapshot = value;
            published = nextSnapshot;
            nextSnapshot = null;
        }
        if (published != null) published.complete(value);
    }

    private void suppress(Target desired, List<SessionController.BlockedStart> starts) throws IOException {
        if (starts.isEmpty()) return;
        new Fence(desired).submit(op -> {
            starts.forEach(owner::suppressDue);
            return null;
        }, null);
    }

    private Plan plan(Target targetAtPlan, CapacitySnapshot capacity, List<SessionController.BlockedStart> suppressed) throws IOException {
        SessionTarget desired = targetAtPlan.value();
        boolean recovering = missing(capacity) != Directions.NONE;
        List<CapacitySnapshot.Member> members = capacity.members();
        var retire = new LinkedHashSet<MemberId>();
        long epoch = 0;
        boolean stale = false;
        for (var replacement : desired.replace()) {
            if (!replacement.evidence().isEmpty() && !Boolean.TRUE.equals(new Fence(targetAtPlan).submit(
                    op -> replacement.evidence().stream().allMatch(session::validQualityInput), false))) {
                stale = true;
                continue;
            }
            for (var member : members) if (!member.draining() && replacement.members().contains(member.id())) {
                retire.add(member.id());
                epoch = Math.max(epoch, replacement.minEpoch());
            }
        }
        var slots = new ArrayList<Slot>();
        desired.groups().stream().sorted(Comparator.comparing(SessionTarget.Group::id)).forEach(group ->
            group.quotas().stream().sorted(Comparator.comparing(SessionTarget.Quota::id)).forEach(quota -> {
                for (int i = 0; i < Math.min(quota.count(), members.size() + 1L); i++) slots.add(new Slot(group, quota));
            }));
        int[] matches = new int[members.size()];
        java.util.Arrays.fill(matches, -1);
        for (int i = 0; i < slots.size(); i++) match(i, slots, members, retire, matches, new boolean[matches.length]);
        var fulfilled = new boolean[slots.size()];
        var memberForSlot = new int[slots.size()];
        var preferences = new LinkedHashMap<MemberId, MemberPolicy>();
        Long retry = null, hostRetry = null;
        long now = controller.clock().now();
        for (int i = 0; i < matches.length; i++) {
            var member = members.get(i);
            if (matches[i] >= 0) {
                var slot = slots.get(matches[i]);
                memberForSlot[matches[i]] = i;
                var candidate = controller.candidate(member.candidate());
                var rotation = slot.group().rotation();
                preferences.put(member.id(), new MemberPolicy(candidate.preferences(slot.quota()), rotation));
                long ageDue = rotation.maxAge().isZero() ? Time.NEVER : Time.add(member.joinedAt(), Time.nanos(rotation.maxAge()));
                if (member.rotationDue() || ageDue <= now) {
                    if (!recovering) retire.add(member.id());
                    else fulfilled[matches[i]] = true;
                } else {
                    fulfilled[matches[i]] = true;
                    if (ageDue != Time.NEVER) retry = earliest(retry, ageDue);
                }
            } else if (!desired.groups().isEmpty() && !member.draining() && !member.candidate().isEmpty()) retire.add(member.id());
        }
        applyMemberPolicies(targetAtPlan, preferences);
        int pivot = 0;
        if (lastPool != null) {
            while (pivot < slots.size() && slots.get(pivot).group().id().compareTo(lastPool) <= 0) pivot++;
        }
        boolean deficit = false;
        for (boolean matched : fulfilled) deficit |= !matched;
        if (!deficit) {
            if (!recovering && retire.isEmpty()) {
                for (int offset = 0; offset < slots.size(); offset++) {
                    int i = (pivot + offset) % slots.size();
                    var member = members.get(memberForSlot[i]);
                    if (member.stable() != member.expected()) continue;
                    var slot = slots.get(i);
                    var quota = slot.quota();
                    for (var upgrade : controller.upgrades(controller.candidate(member.candidate()), quota, capacity)) {
                        Long after = dialBlock(upgrade, now);
                        if (after == null) {
                            return new Plan(List.of(new Provision(upgrade, quota)), Set.of(member.id()), 0, Blocking.NONE, retry, slot.group().id());
                        }
                        suppressed.add(new SessionController.BlockedStart(upgrade, CarrierCache.Suppression.DIAL_AFTER, after));
                        hostRetry = earliest(hostRetry, after);
                    }
                }
            }
            var blocked = stale ? Blocking.STALE_DECISION : recovering ? Blocking.NO_ELIGIBLE_CANDIDATE : Blocking.NONE;
            if (hostRetry != null && (retry == null || hostRetry <= retry)) blocked = Blocking.DIAL_AFTER;
            return new Plan(List.of(), recovering || retire.isEmpty() ? Set.of() : Set.of(retire.getFirst()),
                    epoch, blocked, earliest(retry, hostRetry), null);
        }
        for (int offset = 0; offset < slots.size(); offset++) {
            int i = (pivot + offset) % slots.size();
            if (fulfilled[i]) continue;
            Slot slot = slots.get(i);
            for (var candidate : controller.ranked(slot.quota().candidates())) {
                if (!allowed(capacity, candidate)) {
                    suppressed.add(new SessionController.BlockedStart(candidate, CarrierCache.Suppression.UNAUTHORIZED, null));
                    continue;
                }
                Long after = dialBlock(candidate, now);
                if (after != null) {
                    suppressed.add(new SessionController.BlockedStart(candidate, CarrierCache.Suppression.DIAL_AFTER, after));
                    hostRetry = earliest(hostRetry, after);
                    continue;
                }
                long delay = controller.retryDelay(candidate);
                if (delay == 0) {
                    var drains = recovering || retire.isEmpty() ? Set.<MemberId>of() : Set.of(retire.getFirst());
                    return new Plan(List.of(new Provision(candidate, slot.quota())), drains, epoch,
                            Blocking.NONE, earliest(retry, hostRetry), slot.group().id());
                }
                var reason = controller.admission(candidate, false);
                if (reason != null) suppressed.add(new SessionController.BlockedStart(candidate, reason, Time.add(now, delay)));
                retry = earliest(retry, Time.add(now, delay));
            }
        }
        Blocking blocked = hostRetry != null && (retry == null || hostRetry <= retry)
                ? Blocking.DIAL_AFTER : Blocking.NO_ELIGIBLE_CANDIDATE;
        return new Plan(List.of(), Set.of(), epoch, blocked, earliest(retry, hostRetry), null);
    }

    static Directions missing(CapacitySnapshot capacity) {
        return difference(capacity.required(), capacity.covered());
    }

    private static Directions union(Directions first, Directions second) {
        if (first == Directions.NONE || first == second) return second;
        return second == Directions.NONE ? first : Directions.BOTH;
    }

    private static Directions difference(Directions required, Directions covered) {
        boolean send = required.sends() && !covered.sends();
        boolean receive = required.receives() && !covered.receives();
        if (send) return receive ? Directions.BOTH : Directions.SEND;
        return receive ? Directions.RECEIVE : Directions.NONE;
    }

    static boolean intersects(Directions first, Directions second) {
        return (first.sends() && second.sends()) || (first.receives() && second.receives());
    }

    private RecoveryPlan recoveryPlan(SessionTarget desired, CapacitySnapshot capacity, Directions need, Set<String> launched) {
        var ids = controller.candidates().stream().map(SessionController.Candidate::id).toList();
        var available = new ArrayList<Provision>();
        var suppressed = new ArrayList<SessionController.BlockedStart>();
        Long retry = null, hostRetry = null;
        long now = controller.clock().now();
        boolean excluded = false, permitted = false;
        for (var candidate : controller.ranked(ids, true, now)) {
            if (candidate.attachments().isEmpty() || !intersects(need, candidate.member().directions())) continue;
            if (!allowed(capacity, candidate)) {
                excluded = true;
                continue;
            }
            permitted = true;
            if (launched.contains(candidate.id())) continue;
            long delay = controller.recoveryDelay(candidate);
            Long after = dialBlock(candidate, now);
            var reason = controller.admission(candidate, true);
            if (after != null || reason != null) {
                Long next = delay > 0 ? Time.add(now, delay) : null;
                if (after != null) {
                    next = next == null ? after : Math.max(next, after);
                    hostRetry = earliest(hostRetry, next);
                    reason = CarrierCache.Suppression.DIAL_AFTER;
                }
                retry = earliest(retry, next);
                Long due = controller.cache.retryMetrics(candidate.cacheKey(), candidate.kind(), candidate.variant()).recoveryRetryAt();
                if (due != null && due > now) continue;
                suppressed.add(new SessionController.BlockedStart(candidate, reason, next));
                continue;
            }
            available.add(new Provision(candidate, quotaFor(desired, candidate)));
        }
        return new RecoveryPlan(available, earliest(retry, hostRetry), excluded && !permitted,
                hostRetry != null && (retry == null || hostRetry <= retry), suppressed);
    }

    /** Required service still missing after adopting the confirmed alternatives. */
    static final class RecoveryNeed {
        private Directions missing;

        RecoveryNeed(Directions missing) { this.missing = missing; }

        Directions missing() { return missing; }
        boolean includes(Directions supplied) { return intersects(missing, supplied); }
        void confirmed(Directions supplied) { missing = difference(missing, supplied); }
    }

    private static Directions recoveryNeed(CapacitySnapshot capacity, List<Recovery> running) {
        if (running.isEmpty()) return missing(capacity);
        var need = capacity.required();
        for (var member : capacity.members()) {
            if (running.stream().noneMatch(work -> work.attempt().member().equals(member.id()))) {
                need = difference(need, member.covered());
            }
        }
        return need;
    }

    private Long recover(Target desired) throws IOException, InterruptedException, ExecutionException {
        var running = new ArrayList<Recovery>();
        var launched = new LinkedHashSet<String>();
        var fence = new Fence(desired);
        var batch = new RecoveryBatch(fence);
        fence.submit(op -> { recoveryBatch = batch; return null; }, null);
        boolean retryRound = false;
        long stagger = 0;
        Attempt awaitingStart = null;
        try {
            while (desired == target && !stop.isDone()) {
                var available = controller.available();
                var capacity = session.capacity();
                var need = new RecoveryNeed(recoveryNeed(capacity, running));
                if (awaitingStart != null) {
                    Long started = awaitingStart.openerStartedAt();
                    if (started != null) {
                        stagger = Time.add(started, BootstrapRace.STAGGER_NANOS);
                        awaitingStart = null;
                    } else if (awaitingStart.result() != null) awaitingStart = null;
                }
                var finishedAt = new LinkedHashMap<Recovery, Long>();
                for (var work : running) finishedAt.put(work, work.attempt().finishedAt());
                running.sort(Comparator.comparing(finishedAt::get, Comparator.nullsLast(Long::compare)));
                for (var it = running.iterator(); it.hasNext();) {
                    var recovery = it.next();
                    var result = recovery.attempt().result();
                    if (result == null && need.includes(recovery.provision().candidate().member().directions())) continue;
                    if (result != null && result.disposition() == Attempt.Disposition.CONFIRMED) {
                        var covered = session.capacity().members().stream().filter(member -> member.id().equals(result.member()))
                                .map(CapacitySnapshot.Member::covered).findFirst().orElse(Directions.NONE);
                        need.confirmed(covered);
                    } else if (result != null) {
                        if (result.disposition() == Attempt.Disposition.DIAL_AFTER) launched.remove(recovery.provision().candidate().id());
                        retryRound |= candidateFailed(result);
                        Directions failed = recovery.provision().candidate().member().directions();
                        Directions remaining = Directions.NONE;
                        for (var other : running) {
                            if (other == recovery || other.attempt().result() != null) continue;
                            var supplied = other.provision().candidate().member().directions();
                            remaining = union(remaining, supplied);
                        }
                        if (intersects(difference(need.missing(), remaining), failed)) stagger = 0;
                    } else recovery.cancelUnneeded();
                    if (awaitingStart == recovery.attempt()) awaitingStart = null;
                    recovery.closeChange();
                    it.remove();
                }
                if (need.missing() == Directions.NONE) return null;
                long now = controller.clock().now();
                var plan = recoveryPlan(desired.value(), capacity, need.missing(), launched);
                Long retry = plan.retry();
                Blocking blocked = plan.topologyExcluded() ? Blocking.TOPOLOGY_EXCLUDED
                        : plan.dialBlocked() ? Blocking.DIAL_AFTER : Blocking.NO_ELIGIBLE_CANDIDATE;
                if (awaitingStart == null && now >= stagger) {
                    if (plan.available().isEmpty()) suppress(desired, plan.suppressed());
                    for (var provision : plan.available()) {
                        var started = startRecovery(desired, capacity, need.missing(), provision, batch);
                        if (started.blocked() != Blocking.NONE) {
                            blocked = started.blocked();
                            if (blocked == Blocking.CLEANUP || blocked == Blocking.EXECUTION || blocked == Blocking.QUEUE_FULL) break;
                            continue;
                        }
                        Recovery recovery = started.recovery();
                        if (recovery == null) continue;
                        running.add(recovery);
                        launched.add(provision.candidate().id());
                        awaitingStart = recovery.attempt();
                        break;
                    }
                }
                if (stagger > now) {
                    if (!plan.available().isEmpty()) retry = stagger;
                    else if (retry != null) retry = Math.max(retry, stagger);
                }
                if (running.isEmpty()) {
                    if (launched.isEmpty()) {
                        publish(desired, capacity, blocked, List.of());
                        await(desired, session.capacity().observed(), available, retry);
                        continue;
                    }
                    if (!retryRound) {
                        var current = session.capacity();
                        if (missing(current) == need.missing()) {
                            publish(desired, current, blocked, List.of(), retry);
                            await(desired, current.observed(), available, retry);
                        }
                    }
                    return retry;
                }
                var ids = running.stream().map(r -> r.attempt().id()).filter(id -> id != 0).toList();
                publish(desired, capacity, Blocking.CONFIRMATION, ids, retry);
                var changed = new ArrayList<CompletableFuture<?>>();
                changed.add(available);
                if (awaitingStart != null) changed.add(awaitingStart.opening());
                for (var work : running) changed.add(work.attempt().decided());
                await(desired, capacity.observed(), CompletableFuture.anyOf(changed.toArray(CompletableFuture[]::new)), retry);
            }
            return null;
        } finally {
            session.driver.post(() -> fence.commit(() -> batch.close(CapacityChange.Status.CANCELLED)));
        }
    }

    private RecoveryStart startRecovery(Target desired, CapacitySnapshot capacity, Directions need, Provision provision, RecoveryBatch batch) throws IOException {
        var request = new ChangeRequest(capacity.policyRevision(), capacity.observed(), controller.timeout(),
                List.of(provision.candidate().member(provision.quota())), Set.of(), 0);
        var admitted = admit(desired, request, provision.candidate(), batch);
        // A successful start transfers the change to the recovery tracked by the owner.
        //noinspection resource
        var change = admitted.change();
        if (change == null) return new RecoveryStart(null, admitted.blocked());
        try {
            if (need == Directions.BOTH) need = provision.candidate().member().directions();
            return new RecoveryStart(new Recovery(provision, change, startAttempt(change,
                    0, provision.candidate(), need)), Blocking.NONE);
        } catch (CapacityChangeException blocked) {
            change.cancel();
            suppress(desired, List.of(new SessionController.BlockedStart(provision.candidate(), suppression(blocked), null)));
            return new RecoveryStart(null, blocked.reason() == CapacityChangeException.Reason.EXPIRED
                    ? Blocking.DEADLINE : Blocking.CAPACITY);
        } catch (IOException | RuntimeException | Error failure) {
            change.cancel();
            throw failure;
        }
    }

    private CapacityChange.ControlledChange admit(Target desired, ChangeRequest request,
                                                  SessionController.Candidate candidate, RecoveryBatch recovery) throws IOException {
        try {
            return CapacityChange.begin(session, request, owner, candidate, recovery != null,
                    () -> after(candidate), new Fence(desired), recovery);
        } catch (SessionController.QueueFullException full) {
            return new CapacityChange.ControlledChange(null, Blocking.QUEUE_FULL);
        } catch (CapacityChangeException blocked) {
            new Fence(desired).submit(op -> {
                owner.blocked();
                if (candidate != null) owner.suppressDue(new SessionController.BlockedStart(candidate, suppression(blocked), null));
                return null;
            }, null);
            return new CapacityChange.ControlledChange(null, blocked.reason() == CapacityChangeException.Reason.EXPIRED
                    ? Blocking.DEADLINE : Blocking.CAPACITY);
        }
    }

    private Blocking pendingBlock(Target desired, Blocking blocked) throws IOException {
        return (Blocking) new Fence(desired).submit(op -> {
            try {
                owner.enqueue();
                owner.blocked();
                return blocked;
            } catch (SessionController.QueueFullException full) {
                return Blocking.QUEUE_FULL;
            } catch (IllegalStateException closed) {
                return Blocking.OWNER_CLOSED;
            }
        }, blocked);
    }

    private void applyMemberPolicies(Target targetAtPlan, Map<MemberId, MemberPolicy> policies) throws IOException {
        if (stop.isDone() || target != targetAtPlan || installed.equals(policies)) return;
        new Fence(targetAtPlan).submit(op -> {
            for (var entry : policies.entrySet()) {
                var policy = entry.getValue();
                var send = policy.preferences().send();
                var receive = policy.preferences().receive();
                var settings = io.zmux.internal.core.Tiers.PipeSettings.builder()
                        .sendPriority(send.tier()).sendStandby(send.standby()).weight(send.weight())
                        .recvPriority(receive.tier()).recvStandby(receive.standby()).build();
                session.core.setMemberPolicy(entry.getKey(), settings, policy.rotation().sendBytes(), policy.rotation().receiveBytes());
            }
            installed = Map.copyOf(policies);
            return null;
        }, null);
    }

    private static Long earliest(Long first, Long second) {
        return first == null || (second != null && second < first) ? second : first;
    }

    private static CarrierCache.Suppression suppression(CapacityChangeException blocked) {
        return switch (blocked.reason()) {
            case EXPIRED -> CarrierCache.Suppression.DEADLINE;
            case STALE_POLICY -> CarrierCache.Suppression.UNAUTHORIZED;
            case BLOCKED_CAPACITY, MEMBER_BUSY -> CarrierCache.Suppression.CAPACITY;
            case UNKNOWN_MEMBER -> CarrierCache.Suppression.STALE;
        };
    }

    private static SessionTarget.Quota quotaFor(SessionTarget target, SessionController.Candidate candidate) {
        return target.groups().stream().flatMap(group -> group.quotas().stream())
                .filter(quota -> quota.count() > 0 && quota.candidates().contains(candidate.id())).findFirst().orElse(null);
    }

    private static boolean allowed(CapacitySnapshot capacity, SessionController.Candidate candidate) {
        return SessionController.includes(capacity.topology().getOrDefault(candidate.id(), Directions.BOTH),
                candidate.member().directions());
    }

    private static boolean match(int index, List<Slot> slots, List<CapacitySnapshot.Member> members, Set<MemberId> excluded,
                                 int[] matches, boolean[] seen) {
        var quota = slots.get(index).quota();
        for (int i = 0; i < members.size(); i++) {
            var member = members.get(i);
            if (seen[i] || excluded.contains(member.id()) || !quota.candidates().contains(member.candidate())
                    || !SessionController.includes(member.expected(), quota.directions()) || !member.complete()) continue;
            seen[i] = true;
            if (matches[i] < 0 || match(matches[i], slots, members, excluded, matches, seen)) {
                matches[i] = index;
                return true;
            }
        }
        return false;
    }

    private boolean execute(Target desired, CapacitySnapshot capacity, Plan plan)
            throws IOException, InterruptedException, ExecutionException {
        var additions = plan.add().stream().map(provision -> provision.candidate().member(provision.quota())).toList();
        var request = new ChangeRequest(capacity.policyRevision(), capacity.observed(), controller.timeout(), additions, plan.drain(), plan.epoch());
        var candidate = plan.add().isEmpty() ? null : plan.add().getFirst().candidate();
        var admitted = admit(desired, request, candidate, null);
        try (var change = admitted.change()) {
            if (change == null) {
                Long retry = null;
                if (admitted.blocked() == Blocking.DIAL_AFTER) {
                    assert candidate != null : "only an addition can be delayed before dialing";
                    retry = after(candidate);
                }
                publish(desired, capacity, admitted.blocked(), List.of(), retry);
                return false;
            }
            List<Long> ids = List.of();
            if (candidate != null) {
                lastPool = plan.pool();
                var attempt = startAttempt(change, 0, candidate, null);
                CompletableFuture.anyOf(attempt.opening(), attempt.decided(), desired.changed(), stop).get();
                if (attempt.id() != 0) ids = List.of(attempt.id());
                publish(desired, session.capacity(), Blocking.CONFIRMATION, ids);
                CompletableFuture.anyOf(attempt.decided(), desired.changed(), stop).get();
                if (desired.changed().isDone()) change.staleTarget();
                if (desired.changed().isDone() || stop.isDone()) return true;
                var outcome = attempt.result();
                assert outcome != null : "the attempt publishes its result before deciding";
                if (outcome.disposition() != Attempt.Disposition.CONFIRMED) return true;
            }
            while (!stop.isDone() && !desired.changed().isDone()) {
                var available = controller.available();
                var result = commit(desired, change);
                if (result == null) return true;
                switch (result.status()) {
                    case COMMITTED, EXPIRED, STALE_POLICY, STALE_TARGET, STALE_DECISION, ATTEMPT_FAILED, CANCELLED, SESSION_CLOSED -> { return true; }
                    case BLOCKED_CLEANUP, QUEUE_FULL -> {
                        publish(desired, result.capacity(), result.status() == CapacityChange.Status.QUEUE_FULL
                                ? Blocking.QUEUE_FULL : Blocking.CLEANUP, ids);
                        await(desired, result.capacity().observed(), CompletableFuture.anyOf(available, change.done()), null);
                    }
                    case BLOCKED_COVERAGE -> {
                        publish(desired, result.capacity(), Blocking.COVERAGE, ids);
                        return false;
                    }
                    case WAITING_STABILITY, WAITING_CONFIRMATION -> {
                        publish(desired, result.capacity(), Blocking.STABILITY, ids);
                        await(desired, result.capacity().observed(), CompletableFuture.anyOf(controller.available(), change.done()), null);
                    }
                    case WAITING_EPOCH -> {
                        publish(desired, result.capacity(), Blocking.EPOCH, ids);
                        await(desired, result.capacity().observed(), change.done(), null);
                    }
                }
            }
            if (desired.changed().isDone()) change.staleTarget();
            return true;
        } catch (CapacityChangeException blocked) {
            suppress(desired, plan.add().stream().map(provision ->
                    new SessionController.BlockedStart(provision.candidate(), suppression(blocked), null)).toList());
            publish(desired, capacity, blocked.reason() == CapacityChangeException.Reason.EXPIRED
                    ? Blocking.DEADLINE : Blocking.CAPACITY, List.of());
            return false;
        }
    }

    private static Attempt startAttempt(CapacityChange change, int index, SessionController.Candidate candidate,
                                 Directions need) throws IOException {
        CarrierOpener opener = (setup, open) -> {
            var result = candidate.opener().open(setup, open);
            return new CarrierOpener.Opened(result.carriers().stream()
                    .map(io -> io.withCarrier(candidate.kind()).withVariant(candidate.variant())).toList(), result.failure());
        };
        return change.startControllerMember(index, opener, need);
    }

    private CapacityChange.Result commit(Target desired, CapacityChange change) throws IOException {
        return desired == target && !stop.isDone() ? change.commit(draining ->
                desired.value().evidenceValid(draining, session::validQualityInput)) : null;
    }

    static boolean candidateFailed(Attempt.Result result) {
        return switch (result.disposition()) {
            case DIAL_FAILED -> true;
            case CONFIRMATION_FAILED, JOIN_REFUSED, DEADLINE -> {
                boolean transport = false;
                for (Throwable cause = result.error(); cause != null; cause = cause.getCause()) {
                    if (cause instanceof io.zmux.carrier.CarrierFaultException || cause instanceof java.io.EOFException
                            || cause instanceof java.net.SocketException
                            || (cause instanceof java.net.SocketTimeoutException && !(cause instanceof JoinTimeoutException))) {
                        transport = true;
                        break;
                    }
                }
                yield transport;
            }
            default -> false;
        };
    }

    private void await(Target desired, long observed, CompletableFuture<?> available, Long retry)
            throws InterruptedException, ExecutionException {
        var change = session.awaitCapacityChange(observed);
        var timer = new CompletableFuture<Void>();
        Runnable cancel = retry == null ? () -> {} : controller.clock().schedule(
                retry, () -> timer.complete(null));
        try {
            if (retry != null && controller.clock().now() >= retry) timer.complete(null);
            CompletableFuture.anyOf(change, desired.changed(), stop, available, timer).get();
        } finally {
            cancel.run();
            change.cancel(false);
        }
    }
}
