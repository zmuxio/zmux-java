package io.zmux;

import io.zmux.internal.loop.SessionRuntime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** A carrier-opening attempt. Cancelling an observer leaves the attempt running. */
public final class Attempt {
    /** The reason a carrier-opening attempt finished. */
    public enum Disposition { CONFIRMED, DIAL_FAILED, HOOK_FAILED, CANCELLED, NO_LONGER_NEEDED, SESSION_CLOSED, DEADLINE, DIAL_AFTER,
        JOIN_REFUSED, CONFIRMATION_FAILED }

    /** The outcome committed for this attempt. */
    public record Result(long id, long generation, long batch, Disposition disposition, MemberId member, List<PipeId> pipes, Throwable error) {
        public Result { pipes = List.copyOf(pipes); }
    }

    /** The current protocol stages of the attachments from one opener. */
    public record Attachment(PipeId pipe, boolean replied, boolean confirmed) {}

    /** An attempt's purpose, protocol phases and final result if it has finished. */
    public record Snapshot(long id, long generation, long batch, String candidate, MemberId member, Directions need, long startedAt, Long finishedAt,
                           List<Attachment> attachments, Result result) {
        public Snapshot { attachments = List.copyOf(attachments); }
    }

    private record Terminal(Result result, long at) {}

    private volatile long id;
    private final String candidate;
    private final Directions need;
    private final MemberId member;
    private final ClockDomain clock;
    private final long reservedAt;
    private final long generation, batch;
    private final java.util.concurrent.Executor completionExecutor;
    private final Runnable cancel;
    private volatile Long openerStartedAt;
    private final Cancellation setup = new Cancellation();
    private final CompletableFuture<Void> opening = new CompletableFuture<>();
    private final CompletableFuture<Void> decided = new CompletableFuture<>();
    private final CompletableFuture<Result> outcome = new CompletableFuture<>();
    private final CompletableFuture<List<PipeId>> replies = new CompletableFuture<>();
    private final CompletableFuture<Void> released = new CompletableFuture<>();
    private volatile int expectedReplies;
    private final java.util.concurrent.atomic.AtomicInteger verifiedReplies = new java.util.concurrent.atomic.AtomicInteger();
    private volatile Terminal terminal;
    private volatile List<SessionRuntime.JoinAttempt> attachments = List.of();

    Attempt(String candidate, Directions need, MemberId member, ClockDomain clock, long generation, long batch,
            java.util.concurrent.Executor completionExecutor, Runnable cancel) {
        this.candidate = candidate;
        this.need = need;
        this.member = member;
        this.clock = clock;
        reservedAt = clock.now();
        this.generation = generation;
        this.batch = batch;
        this.completionExecutor = completionExecutor;
        this.cancel = cancel;
    }

    /** The attempt ID, or zero before the opener starts. */
    public long id() { return id; }

    public CompletableFuture<Result> completion() { return outcome.copy(); }

    /** Observes the authenticated Replies for the whole member. */
    public CompletableFuture<List<PipeId>> replies() { return replies.copy(); }

    /** Observes the end of the opener and of I/O ownership for all returned carriers. */
    public CompletableFuture<Void> released() { return released.copy(); }

    public Snapshot snapshot() {
        var end = terminal;
        var stages = attachments.stream().map(join -> new Attachment(join.assignedPipe(), replied(join), confirmed(join))).toList();
        Long started = openerStartedAt;
        return new Snapshot(id, generation, batch, candidate, member, need, started == null ? reservedAt : started, end == null ? null : end.at(), stages,
                end == null ? null : end.result());
    }

    private static boolean replied(SessionRuntime.JoinAttempt join) { return join.verifiedReply().isDone() && !join.verifiedReply().isCompletedExceptionally(); }

    private static boolean confirmed(SessionRuntime.JoinAttempt join) {
        var confirmation = join.confirmation();
        return confirmation.isDone() && !confirmation.isCompletedExceptionally();
    }

    public void cancel() { cancel.run(); }

    Cancellation setup() { return setup; }

    CompletableFuture<Void> opening() { return opening; }
    CompletableFuture<Void> decided() { return decided; }

    void started() { opening.complete(null); }

    Long openerStartedAt() { return openerStartedAt; }

    boolean beginOpener(java.util.function.BooleanSupplier admission, Runnable stale,
                                     java.util.function.LongSupplier allocateId) {
        if (terminal != null || openerStartedAt != null || setup.isCancelled()) {
            stale.run();
            return false;
        }
        if (!admission.getAsBoolean()) return false;
        id = allocateId.getAsLong();
        openerStartedAt = clock.now();
        return true;
    }

    MemberId member() { return member; }

    Long finishedAt() {
        var end = terminal;
        return end == null ? null : end.at();
    }

    Result result() {
        var end = terminal;
        return end == null ? null : end.result();
    }

    void handoff(List<SessionRuntime.JoinAttempt> joins, SessionRuntime runtime) {
        List<SessionRuntime.JoinAttempt> group = List.copyOf(joins);
        attachments = group;
        CompletableFuture.allOf(group.stream().map(SessionRuntime.JoinAttempt::verifiedReply).toArray(CompletableFuture[]::new)).whenCompleteAsync((value, error) -> {
            if (error != null) replies.completeExceptionally(error);
            else replies.complete(group.stream().map(SessionRuntime.JoinAttempt::assignedPipe).toList());
        }, runtime::publishCompletion);
    }

    void releaseAfter(List<SessionRuntime.JoinAttempt> joins, CompletableFuture<Void> discarded,
                      Cancellation lifetime, SessionRuntime runtime) {
        var pending = java.util.stream.Stream.concat(joins.stream().map(join -> join.released().toCompletableFuture()),
                java.util.stream.Stream.of(discarded)).toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(pending).whenCompleteAsync((value, error) -> {
            try { lifetime.close(); }
            finally { released.complete(null); }
        }, runtime::publishCompletion);
    }

    void expectReplies(int expected) { expectedReplies = expected; }

    boolean verifiedReply() { return verifiedReplies.incrementAndGet() == expectedReplies; }

    boolean hasVerifiedReplies() { return verifiedReplies.get() == expectedReplies; }

    boolean finish(Disposition disposition, Throwable error, long at) {
        if (terminal != null) return false;
        var pipes = attachments.stream().map(SessionRuntime.JoinAttempt::assignedPipe)
                .filter(pipe -> !pipe.equals(PipeId.UNASSIGNED)).toList();
        var result = new Result(id, generation, batch, disposition, pipes.isEmpty() ? null : member, pipes, error);
        terminal = new Terminal(result, at);
        return true;
    }

    void publishResult() {
        var result = terminal.result();
        var disposition = result.disposition();
        var error = result.error();
        if (disposition != Disposition.CONFIRMED) {
            Thread.ofVirtual().name("zmux-attempt-cancellation").start(setup::cancel);
        }
        decided.complete(null);
        completionExecutor.execute(() -> {
            if (disposition == Disposition.CONFIRMED) replies.complete(result.pipes());
            else if (!hasVerifiedReplies()) replies.completeExceptionally(error == null
                    ? new java.util.concurrent.CancellationException() : error);
            outcome.complete(result);
        });
    }
}
