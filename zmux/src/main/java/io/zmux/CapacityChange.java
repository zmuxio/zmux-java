package io.zmux;

import io.zmux.carrier.PipeIo;
import io.zmux.internal.core.Action;
import io.zmux.internal.core.Joins;
import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.SessionCapacity;
import io.zmux.internal.loop.Bootstrap;
import io.zmux.internal.core.Time;
import io.zmux.internal.loop.SessionRuntime;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** The preparation and atomic drain of a session capacity change. */
public final class CapacityChange implements AutoCloseable {
    /** The status of committing the batch. */
    public enum Status { COMMITTED, WAITING_CONFIRMATION, WAITING_STABILITY, WAITING_EPOCH, BLOCKED_COVERAGE, BLOCKED_CLEANUP, QUEUE_FULL,
        ATTEMPT_FAILED, STALE_POLICY, STALE_TARGET, STALE_DECISION, EXPIRED, CANCELLED, SESSION_CLOSED }

    /** The observation and member IDs from the same owner turn as the commit. */
    public record Result(Status status, CapacitySnapshot capacity, Set<MemberId> draining) {
        public Result { draining = Set.copyOf(draining); }
    }

    private final ZmuxSession session;
    private final ChangeRequest request;
    private final SessionController.Owner controllerOwner;
    private final SessionControl.Fence fence;
    private final SessionControl.RecoveryBatch recovery;
    private final long batch;
    private final SessionController.Lease controllerLease;
    private final Joins.JoinReservation reservation;
    private final SessionCapacity.DrainClaim drain;
    private volatile long ownerDeadline;
    private final Attempt[] attempts;
    private final SessionCapacity.Member[] members;
    private Runnable removeDeadline = () -> {};
    private final Runnable removeSuspensionObserver;
    private final Runnable removeCloseObserver;
    private final CompletableFuture<Void> done = new CompletableFuture<>();
    private Status terminal;

    private record ControllerAttempt(Directions need) {}

    private CapacityChange(ZmuxSession session, ChangeRequest request, SessionController.Owner controllerOwner,
                           SessionController.Lease controllerLease, Joins.JoinReservation reservation,
                           SessionCapacity.DrainClaim drain, long deadline, SessionControl.Fence fence,
                           SessionControl.RecoveryBatch recovery) {
        this.session = session;
        this.request = request;
        this.controllerOwner = controllerOwner;
        this.controllerLease = controllerLease;
        this.reservation = reservation;
        this.drain = drain;
        this.fence = fence;
        this.recovery = recovery;
        int attachments = request.add().stream().mapToInt(m -> m.attachments().size()).sum();
        batch = recovery != null ? recovery.reserve(this, deadline, attachments)
                : fence != null ? session.nextCapacityBatch() : 0;
        ownerDeadline = deadline;
        attempts = new Attempt[request.add().size()];
        members = new SessionCapacity.Member[attempts.length];
        removeCloseObserver = session.driver.watchEnding(error -> terminateOnOwner(Status.SESSION_CLOSED, null));
        removeSuspensionObserver = controllerLease == null ? () -> {}
                : session.core.watchSuspension(grace -> onCurrent(() -> {
                    long cutoff = Time.sub(grace, 1);
                    if (recovery == null) tightenDeadline(cutoff);
                    else recovery.tighten(cutoff);
                }));
        armDeadline();
        if (fence != null) {
            fence.track(this);
            if (recovery == null) session.driver.controlEvent(new SessionEvent.BatchReserved(fence.generation(),
                    fence.revision(), batch, session.core.now(), deadline, attachments));
        }
    }

    void tightenDeadline(long cutoff) {
        if (terminal == null && cutoff < ownerDeadline) {
            ownerDeadline = cutoff;
            armDeadline();
        }
    }

    private void armDeadline() {
        removeDeadline.run();
        removeDeadline = session.driver.capacityDeadline(ownerDeadline, () -> onCurrent(() -> terminateOnOwner(Status.EXPIRED, null)));
    }

    record ControlledChange(CapacityChange change, SessionControl.Blocking blocked) {}

    static ControlledChange begin(ZmuxSession session, ChangeRequest request, SessionController.Owner owner,
                                  SessionController.Candidate candidate, boolean recovery,
                                  java.util.function.LongSupplier dialAfter, SessionControl.Fence fence,
                                  SessionControl.RecoveryBatch race) throws IOException {
        Objects.requireNonNull(request);
        long operationDeadline = owner == null ? Time.add(session.driver.now(), Time.nanos(request.timeout())) : Time.NEVER;
        int count = 0, mask = 0;
        for (MemberSpec member : request.add()) {
            count = Math.addExact(count, member.attachments().size());
            mask |= directions(member.directions());
        }
        int attachments = count;
        int directions = mask;
        io.zmux.internal.loop.Operation.Step reserve = op -> {
            if (session.core.error() != null) return CoreException.session(session.core.error());
            var snapshot = session.capacityOnOwner();
            if (owner != null) {
                try { owner.enqueue(); }
                catch (SessionController.QueueFullException full) {
                    return new ControlledChange(null, SessionControl.Blocking.QUEUE_FULL);
                } catch (IllegalStateException closed) {
                    return new ControlledChange(null, SessionControl.Blocking.OWNER_CLOSED);
                }
            }
            if (snapshot.policyRevision() != request.policyRevision()) {
                return new CapacityChangeException(CapacityChangeException.Reason.STALE_POLICY);
            }
            long now = session.driver.now();
            long deadline = owner == null ? operationDeadline : Time.add(now, Time.nanos(request.timeout()));
            if (race != null) deadline = race.bound(deadline);
            if (owner != null && attachments != 0) deadline = session.core.boundAttemptDeadline(deadline);
            if (now >= deadline || deadline == Time.NEVER) {
                return new CapacityChangeException(CapacityChangeException.Reason.EXPIRED);
            }
            if (request.drain().stream().anyMatch(id -> snapshot.members().stream().noneMatch(member -> member.id().equals(id)))) {
                return new CapacityChangeException(CapacityChangeException.Reason.UNKNOWN_MEMBER);
            }
            if (!session.core.drainClaimAvailable(request.drain())) {
                return new CapacityChangeException(CapacityChangeException.Reason.MEMBER_BUSY);
            }
            var granted = new SessionController.Admission[1];
            java.util.function.BooleanSupplier grant = owner == null || candidate == null ? null : () -> {
                if (!session.core.sessionPolicy().allows(candidate.id(), directions)) {
                    granted[0] = new SessionController.Admission(null, SessionControl.Blocking.TOPOLOGY_EXCLUDED);
                } else if (attachments > snapshot.limits().localCarriers() - snapshot.localCarriers()) {
                    granted[0] = new SessionController.Admission(null, SessionControl.Blocking.CAPACITY);
                } else {
                    granted[0] = owner.acquire(candidate, recovery, attachments, false, dialAfter);
                }
                // The successful grant passes its lease to the capacity change below.
                //noinspection resource
                return granted[0].lease() != null;
            };
            Joins.JoinReservation reservation = attachments == 0 ? null
                    : session.core.reserveJoins(attachments, !request.drain().isEmpty(), directions, grant);
            var admitted = granted[0];
            if (attachments != 0 && reservation == null) {
                if (admitted != null) {
                    var blocked = Objects.requireNonNull(admitted.blocked());
                    if (owner != null && (blocked == SessionControl.Blocking.CAPACITY || blocked == SessionControl.Blocking.TOPOLOGY_EXCLUDED)) owner.blocked();
                    return new ControlledChange(null, blocked);
                }
                return new CapacityChangeException(CapacityChangeException.Reason.BLOCKED_CAPACITY);
            }
            var lease = admitted == null ? null : admitted.lease();
            try {
                var drain = request.drain().isEmpty() ? null : session.core.claimDrain(request.drain());
                return new ControlledChange(new CapacityChange(session, request, owner, lease, reservation, drain, deadline, fence, race),
                        SessionControl.Blocking.NONE);
            } catch (RuntimeException | Error failure) {
                if (reservation != null) session.core.releaseJoins(reservation);
                if (lease != null) lease.close();
                throw failure;
            }
        };
        return (ControlledChange) (fence == null ? session.run(reserve) : fence.submit(reserve,
                new ControlledChange(null, SessionControl.Blocking.NONE)));
    }

    /** Starts an opener for one previously unclaimed index. */
    public Attempt startMember(int index, CarrierOpener opener) throws IOException {
        return startMember(index, opener, null);
    }

    SessionController.Lease controllerLease() { return controllerLease; }

    Attempt startControllerMember(int index, CarrierOpener opener, Directions need) throws IOException {
        return startMember(index, opener, new ControllerAttempt(need));
    }

    private Attempt startMember(int index, CarrierOpener opener, ControllerAttempt controlled) throws IOException {
        Objects.requireNonNull(opener);
        return (Attempt) submit(op -> {
            if (terminal != null || expired()) return new CapacityChangeException(CapacityChangeException.Reason.EXPIRED);
            if (attempts[index] != null) return new IllegalStateException("member index already claimed");
            if (session.core.sessionPolicy().revision() != request.policyRevision()) {
                return new CapacityChangeException(CapacityChangeException.Reason.STALE_POLICY);
            }
            var spec = request.add().get(index);
            var member = session.core.newMember(spec.candidate(), directions(spec.directions()), spec.attachments().size());
            if (controllerLease != null) member.attribute(CapacityViews.identity(controllerLease.key(),
                    spec.attachments().stream().map(AttachmentSpec::directions).toList()));
            var need = controlled == null || controlled.need() == null ? spec.directions() : controlled.need();
            var attempt = new Attempt(spec.candidate(), need, member.id(), session.driver.clockDomain(),
                    fence == null ? 0 : fence.generation(), batch, session.driver::publishCompletion,
                    () -> session.driver.post(() -> onCurrent(() -> cancelAttemptOnOwner(index))));
            attempt.expectReplies(spec.attachments().size());
            attempts[index] = attempt;
            members[index] = member;
            Thread.ofVirtual().name("zmux-capacity-attempt").start(() -> execute(attempt, member, spec, opener, controlled));
            return attempt;
        }, new CapacityChangeException(CapacityChangeException.Reason.EXPIRED));
    }

    private Object submit(io.zmux.internal.loop.Operation.Step step, Object stale) throws IOException {
        return fence == null ? session.run(step) : fence.submit(step, stale);
    }

    private void onCurrent(Runnable action) {
        if (fence == null) action.run();
        else fence.commit(action);
    }

    private void cancelAttemptOnOwner(int index) {
        if (terminal != null) return;
        session.core.setNow(session.driver.now());
        finishOnOwner(attempts[index], members[index], Attempt.Disposition.CANCELLED, null);
    }

    boolean owns(MemberId member) {
        return members[0] != null && members[0].id().equals(member);
    }

    Directions supplies() { return request.add().getFirst().directions(); }

    private Directions need(CapacitySnapshot capacity) {
        return recovery == null ? SessionControl.missing(capacity) : recovery.need(capacity);
    }

    private void execute(Attempt attempt, SessionCapacity.Member member, MemberSpec spec, CarrierOpener opener,
                         ControllerAttempt controlled) {
        var lease = controllerLease;
        var opened = new ArrayList<PipeIo>();
        var joins = new ArrayList<SessionRuntime.JoinAttempt>();
        var lifetime = new Cancellation();
        var disposition = Attempt.Disposition.HOOK_FAILED;
        // The attempt owns setup; it closes after the cleanup below, as before.
        try (var setup = attempt.setup()) {
            try {
                CarrierOpener.Opened result;
                try {
                    if (!startOpener(attempt, spec, controlled, lease)) {
                        long after = lease == null ? 0 : lease.blockedUntil();
                        if (after != 0) finish(attempt, member, Attempt.Disposition.DIAL_AFTER, new DialAfterException(spec.candidate(), after));
                        else finish(attempt, member, expired() ? Attempt.Disposition.DEADLINE : Attempt.Disposition.NO_LONGER_NEEDED, null);
                        return;
                    }
                    if (attempt.result() != null) return;
                    if (lease != null) lease.openerStarted();
                    result = Objects.requireNonNull(opener.open(setup, new CarrierOpener.Request(attempt.id(),
                            CarrierOpener.Stage.JOIN, attempt.snapshot().need(), spec.attachments(), session.credential(),
                            Duration.ofNanos(Math.max(0, Time.sub(ownerDeadline, session.driver.now()))), lifetime)));
                } finally {
                    if (lease != null) lease.hostReturned();
                }
                opened.addAll(result.carriers());
                if (result.failure() != null) {
                    finish(attempt, member, Attempt.Disposition.DIAL_FAILED, result.failure());
                    return;
                }
                if (opened.size() != spec.attachments().size()
                        || opened.stream().anyMatch(io -> io.reader() == null || io.writer() == null)) {
                    throw new IllegalArgumentException("carrier layout differs from requested Join layout");
                }
                disposition = Attempt.Disposition.CONFIRMATION_FAILED;
                try (var _ = setup.onCancel(() -> {
                    synchronized (joins) {
                        for (var join : joins) session.driver.cancelJoinSetup(join, new JoinTimeoutException());
                    }
                })) {
                    var admit = controlled == null ? null : controllerAdmission(controlled, attempt);
                    synchronized (joins) {
                        if (attempt.result() != null) return;
                        for (int i = 0; i < opened.size(); i++) {
                            var io = opened.set(i, null);
                            var options = spec.attachments().get(i).joinOptions();
                            joins.add(session.driver.addPipe(io, options.join(), reservation, member, admit, at -> {
                                if (attempt.verifiedReply() && lease != null) lease.report(CarrierCache.Outcome.USABLE, at);
                            }));
                        }
                    }
                    attempt.handoff(joins, session.driver);
                    for (int i = 0; i < joins.size(); i++) {
                        var options = spec.attachments().get(i).joinOptions();
                        // The owner deadline cancels these waits in the session's clock domain.
                        session.joinCarrier(joins.get(i), options, Time.NEVER, null);
                    }
                }
                finish(attempt, member, Attempt.Disposition.CONFIRMED, null);
            } catch (IOException | RuntimeException | Error failure) {
                if (failure instanceof JoinRefusedException || failure instanceof PipeRefusedException) disposition = Attempt.Disposition.JOIN_REFUSED;
                if (failure instanceof SessionRuntime.ObsoleteJoin) disposition = Attempt.Disposition.NO_LONGER_NEEDED;
                if (failure instanceof SessionClosedException) disposition = Attempt.Disposition.SESSION_CLOSED;
                for (var join : joins) session.driver.cancelJoinSetup(join, failure);
                finish(attempt, member, disposition, failure);
            } finally {
                Throwable failure = new java.util.concurrent.CancellationException("carrier was not handed off");
                var discarded = opened.stream().filter(Objects::nonNull).map(io -> Bootstrap.discard(io, failure)).toList();
                var released = CompletableFuture.allOf(discarded.toArray(CompletableFuture[]::new));
                attempt.releaseAfter(joins, released, lifetime, session.driver);
                if (lease != null) {
                    joins.forEach(join -> join.released().whenComplete((_, error) -> lease.cleanup.releaseOne()));
                    int unused = spec.attachments().size() - joins.size();
                    if (discarded.size() > unused) {
                        released.whenComplete((unusedResult, error) -> {
                            for (int i = 0; i < unused; i++) lease.cleanup.releaseOne();
                        });
                    } else {
                        discarded.forEach(carrier -> carrier.whenComplete((unusedResult, error) -> lease.cleanup.releaseOne()));
                        for (int i = discarded.size(); i < unused; i++) lease.cleanup.releaseOne();
                    }
                }
            }
        }
    }

    private boolean startOpener(Attempt attempt, MemberSpec spec, ControllerAttempt controlled,
                                SessionController.Lease lease) throws IOException {
        return (boolean) submit(op -> {
            if (terminal != null) return suppress(lease, terminal == Status.EXPIRED
                    ? CarrierCache.Suppression.DEADLINE : CarrierCache.Suppression.STALE);
            var capacity = session.capacityOnOwner();
            if (expired()) return suppress(lease, CarrierCache.Suppression.DEADLINE);
            if (session.core.joinBackedOff()) return suppress(lease, CarrierCache.Suppression.RETRY_AFTER);
            if (capacity.policyRevision() != request.policyRevision()
                    || !SessionController.includes(capacity.topology().getOrDefault(spec.candidate(), Directions.BOTH),
                            spec.directions())) return suppress(lease, CarrierCache.Suppression.UNAUTHORIZED);
            if (controlled != null) {
                if (controlled.need() != null && !SessionControl.intersects(controlled.need(), need(capacity)))
                    return suppress(lease, CarrierCache.Suppression.COVERAGE);
            }
            boolean started = attempt.beginOpener(() -> lease == null || lease.start(),
                    () -> suppress(lease, CarrierCache.Suppression.STALE), session::nextCapacityAttempt);
            if (started) {
                if (fence != null) session.driver.controlEvent(new SessionEvent.AttemptStarted(fence.generation(),
                        fence.revision(), batch, attempt.snapshot(), attempt.openerStartedAt()));
                attempt.started();
            }
            return started;
        }, false);
    }

    private static boolean suppress(SessionController.Lease lease, CarrierCache.Suppression reason) {
        if (lease != null) lease.suppress(reason);
        return false;
    }

    private java.util.function.BooleanSupplier controllerAdmission(ControllerAttempt controlled, Attempt attempt) {
        // All calls are on the owner. Check demand before the first half of a split member.
        return new java.util.function.BooleanSupplier() {
            private boolean admitted;

            @Override public boolean getAsBoolean() {
                if (attempt.result() != null || expired() || (fence != null && !fence.current())
                        || session.core.sessionPolicy().revision() != request.policyRevision()) return false;
                if (!admitted) admitted = controlled.need() == null
                        || SessionControl.intersects(controlled.need(), need(session.capacityOnOwner()));
                return admitted;
            }
        };
    }

    private void finish(Attempt attempt, SessionCapacity.Member member,
                        Attempt.Disposition disposition, Throwable error) {
        session.driver.post(() -> onCurrent(() -> {
            if (terminal != null) return;
            session.core.setNow(session.driver.now());
            if (expired()) {
                terminateOnOwner(Status.EXPIRED, null);
                return;
            }
            if (fence != null && session.core.sessionPolicy().revision() != request.policyRevision()) {
                terminateOnOwner(Status.STALE_POLICY, null);
                return;
            }
            if (recovery != null && disposition == Attempt.Disposition.CONFIRMED) {
                var capacity = session.capacityOnOwner();
                var current = capacity.members().stream().filter(m -> m.id().equals(member.id())).findFirst().orElse(null);
                if (current == null || !current.complete() || !SessionControl.intersects(need(capacity), current.covered())) {
                    terminateOnOwner(Status.CANCELLED, Attempt.Disposition.NO_LONGER_NEEDED);
                    return;
                }
                if (finishOnOwner(attempt, member, disposition, error)) recovery.confirmed(this);
            } else if (finishOnOwner(attempt, member, disposition, error) && fence != null
                    && disposition != Attempt.Disposition.CONFIRMED) {
                terminateOnOwner(Status.ATTEMPT_FAILED, null);
            }
        }));
    }

    private boolean finishOnOwner(Attempt attempt, SessionCapacity.Member member,
                                  Attempt.Disposition disposition, Throwable error) {
        long finishedAt = session.core.now();
        if (!attempt.finish(disposition, error, finishedAt)) return false;
        if (disposition != Attempt.Disposition.CONFIRMED) session.core.rollbackMember(member.id());
        if (fence != null) {
            session.driver.controlEvent(new SessionEvent.AttemptEnded(fence.generation(), fence.revision(),
                    batch, attempt.result(), finishedAt));
        }
        if (controllerLease != null) {
            if (fence != null && disposition != Attempt.Disposition.CONFIRMED) fence.demand();
            controllerLease.settle(attempt.result(), attempt.hasVerifiedReplies(), member.faultReported(), finishedAt);
            if (disposition == Attempt.Disposition.CONFIRMED) controllerLease.cleanup.close();
        }
        attempt.publishResult();
        return true;
    }

    /** Commits the drain after one owner turn has checked policy, confirmation, stability and coverage. */
    public Result commit() throws IOException { return commit(draining -> true); }

    Result commit(java.util.function.Predicate<Set<MemberId>> evidenceValid) throws IOException {
        return (Result) submit(op -> {
            var snapshot = session.capacityOnOwner();
            if (terminal != null) return new Result(terminal, snapshot, terminal == Status.COMMITTED ? request.drain() : Set.of());
            if (!request.drain().isEmpty() && !evidenceValid.test(request.drain()))
                return rejected(Status.STALE_DECISION, snapshot);
            if (snapshot.policyRevision() != request.policyRevision()) return rejected(Status.STALE_POLICY, snapshot);
            if (session.core.now() >= ownerDeadline) return rejected(Status.EXPIRED, snapshot);
            if (snapshot.epoch() < request.minEpoch()) return new Result(Status.WAITING_EPOCH, snapshot, Set.of());
            boolean pending = false;
            for (Attempt attempt : attempts) {
                var outcome = attempt == null ? null : attempt.result();
                if (outcome == null) pending = true;
                else if (outcome.disposition() != Attempt.Disposition.CONFIRMED) {
                    return rejected(Status.ATTEMPT_FAILED, snapshot);
                }
            }
            if (pending) return new Result(Status.WAITING_CONFIRMATION, snapshot, Set.of());
            if (!request.drain().isEmpty()) {
                boolean settled = true;
                for (SessionCapacity.Member member : members) {
                    var current = snapshot.members().stream().filter(m -> m.id().equals(member.id())).findFirst().orElse(null);
                    if (current != null && current.complete() && current.stable() == current.expected()) continue;
                    settled = false;
                    if (current != null) for (var pipe : current.pipes()) session.core.observeStability(pipe.value());
                }
                if (!settled) return new Result(Status.WAITING_STABILITY, snapshot, Set.of());
                var cleanup = new Retirement(controllerOwner);
                boolean committed = false;
                try {
                    committed = session.core.drainMembers(request.drain(), request.minEpoch(), request.policyRevision(),
                            controllerOwner == null ? null : cleanup::reserve);
                    if (!committed) return new Result(cleanup.blocked, snapshot, Set.of());
                } finally {
                    cleanup.complete(committed);
                }
            }
            terminateOnOwner(Status.COMMITTED, null);
            return new Result(Status.COMMITTED, session.capacityOnOwner(), request.drain());
        }, null);
    }

    private Result rejected(Status status, CapacitySnapshot snapshot) {
        terminateOnOwner(status, null);
        return new Result(status, snapshot, Set.of());
    }

    private final class Retirement {
        private final SessionController.Owner owner;
        private Status blocked = Status.BLOCKED_COVERAGE;
        private List<PipeId> retiring;
        private SessionController.Cleanup cleanup;

        Retirement(SessionController.Owner owner) { this.owner = owner; }

        boolean reserve(List<PipeId> pipes) {
            if (pipes.isEmpty()) {
                owner.withdraw();
                return true;
            }
            var admission = owner.acquire(null, false, pipes.size(), false, () -> 0);
            // This admission transfers its cleanup lease to the retirement tracker below.
            //noinspection resource
            if (admission.lease() == null) {
                blocked = admission.blocked() == SessionControl.Blocking.QUEUE_FULL ? Status.QUEUE_FULL : Status.BLOCKED_CLEANUP;
                return false;
            }
            retiring = pipes;
            // Cleanup stays reserved until the retiring pipes finish closing or the change is rolled back.
            //noinspection resource
            cleanup = admission.lease().cleanup;
            return true;
        }

        void complete(boolean committed) {
            if (cleanup == null) return;
            if (committed) {
                retiring.forEach(pipe -> new ZmuxPipe(session, pipe.value()).closed()
                        .whenComplete((_, error) -> cleanup.releaseOne()));
            } else cleanup.close();
        }
    }

    /** Completes when the change can no longer take new work. */
    public CompletableFuture<Void> done() { return done.copy(); }

    public void cancel() { terminate(Status.CANCELLED, null); }

    void cancelUnneeded() { terminate(Status.CANCELLED, Attempt.Disposition.NO_LONGER_NEEDED); }

    void staleTarget() { terminate(Status.STALE_TARGET, null); }

    @Override public void close() { cancel(); }

    private void terminate(Status status, Attempt.Disposition disposition) {
        // Commands preserve order with a subsequent reservation on this session.
        session.driver.post(() -> onCurrent(() -> terminateOnOwner(status, disposition)));
    }

    void terminateOnOwner(Status status, Attempt.Disposition cancelledAs) {
        if (terminal != null) return;
        session.core.setNow(session.driver.now());
        terminal = status;
        if (fence != null) fence.untrack(this);
        if (recovery != null) recovery.remove(this, status);
        removeCloseObserver.run();
        removeSuspensionObserver.run();
        removeDeadline.run();
        if (reservation != null) session.core.releaseJoins(reservation);
        if (drain != null) session.core.releaseDrain(drain);
        if (controllerLease != null && attempts[0] == null) controllerLease.close();
        if (status != Status.COMMITTED) {
            Attempt.Disposition disposition = switch (status) {
                case EXPIRED -> Attempt.Disposition.DEADLINE;
                case SESSION_CLOSED -> Attempt.Disposition.SESSION_CLOSED;
                case STALE_TARGET, STALE_POLICY, STALE_DECISION, ATTEMPT_FAILED -> Attempt.Disposition.NO_LONGER_NEEDED;
                default -> cancelledAs == null ? Attempt.Disposition.CANCELLED : cancelledAs;
            };
            for (int i = 0; i < attempts.length; i++) {
                var attempt = attempts[i];
                if (attempt == null) continue;
                if (finishOnOwner(attempt, members[i], disposition, null)) continue;
                var result = attempt.result();
                if (result != null && result.disposition() == Attempt.Disposition.CONFIRMED) {
                    session.core.rollbackMember(members[i].id());
                }
            }
            if (fence != null && recovery == null) session.driver.controlEvent(new SessionEvent.BatchRolledBack(
                    fence.generation(), fence.revision(), batch, session.core.now(), status));
        } else if (fence != null && !request.drain().isEmpty()) {
            session.driver.controlEvent(new SessionEvent.DrainCommitted(fence.generation(), fence.revision(),
                    batch, session.core.now(), request.drain()));
        }
        session.driver.publishCompletion(() -> done.complete(null));
    }

    private boolean expired() { return session.driver.now() >= ownerDeadline; }

    private static int directions(Directions directions) {
        return (directions.sends() ? Action.DIR_OUT : 0) | (directions.receives() ? Action.DIR_IN : 0);
    }
}
