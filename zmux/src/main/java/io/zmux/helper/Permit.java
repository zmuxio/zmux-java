package io.zmux.helper;

import io.zmux.CallbackFailedException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/**
 * Bytes taken from a {@link Limiter}: what is not {@linkplain #consume consumed} goes back when it is closed.
 * Not thread-safe: one owner uses and closes it; closing twice gives back nothing more.
 *
 * <p>The static calls apply the limiter contract for zmux and for applications alike: arguments are clamped as
 * {@link Limiter} says before the limiter sees them ({@code max <= 0} does not call it), a failing {@code acquire}
 * fails the stage, a failing {@code tryAcquire} counts as nothing taken, and a failing {@code refund} is ignored.
 */
public final class Permit implements AutoCloseable {
    private final Limiter limiter;
    private long left;

    private Permit(Limiter limiter, long left) { this.limiter = limiter; this.left = left; }

    /**
     * Waits for a grant of min through max bytes ({@link Limiter#acquire}); with {@code max <= 0} an empty permit at
     * once, without calling the limiter. Cancel-safe: cancelling the returned stage cancels the limiter's wait, and a
     * grant that arrives anyway goes back. The stage fails with {@link CallbackFailedException} when the limiter
     * throws, fails its stage or grants outside {@code 1..max} (a positive invalid grant is refunded).
     */
    public static CompletionStage<Permit> acquire(Limiter limiter, long min, long max) {
        Objects.requireNonNull(limiter);
        if (max <= 0) return CompletableFuture.completedFuture(new Permit(limiter, 0));
        long lo = Math.clamp(min, 1, max);
        CompletableFuture<Long> wait;
        try {
            wait = Objects.requireNonNull(limiter.acquire(lo, max), "limiter returned null").toCompletableFuture();
        } catch (RuntimeException | AssertionError e) {
            return CompletableFuture.failedFuture(new CallbackFailedException("limiter failed", e));
        }
        CompletableFuture<Permit> out = new CompletableFuture<>();
        wait.whenComplete((got, error) -> {
            if (error != null) {
                Throwable cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
                out.completeExceptionally(cause instanceof CancellationException ? cause : new CallbackFailedException("limiter failed", cause));
                return;
            }
            if (got == null || got < 1 || got > max) {
                if (got != null && got > 0) refund(limiter, got);
                out.completeExceptionally(new CallbackFailedException("invalid limiter grant " + got + " for max " + max));
                return;
            }
            Permit p = new Permit(limiter, got);
            if (!out.complete(p)) p.close(); // cancelled meanwhile: the grant goes back
        });
        out.whenComplete((p, error) -> { if (error instanceof CancellationException) wait.cancel(true); });
        return out;
    }

    /**
     * Abandons a stage of {@link #acquire}: cancels it while it waits, and closes the permit it already produced,
     * so the grant goes back either way.
     */
    public static void abandon(CompletionStage<Permit> pending) {
        CompletableFuture<Permit> f = pending.toCompletableFuture();
        if (!f.cancel(true) && f.isDone() && !f.isCompletedExceptionally()) f.join().close();
    }

    /**
     * A grant of min through max bytes available now ({@link Limiter#tryAcquire}); empty when the limiter grants 0,
     * when {@code max <= 0} (the limiter is not called), and when it throws or grants outside {@code 0..max}, which
     * counts as nothing taken (a positive invalid grant is refunded).
     */
    public static Permit tryAcquire(Limiter limiter, long min, long max) {
        Objects.requireNonNull(limiter);
        if (max <= 0) return new Permit(limiter, 0);
        long got;
        try { got = limiter.tryAcquire(Math.clamp(min, 1, max), max); }
        catch (RuntimeException | AssertionError e) { return new Permit(limiter, 0); }
        if (got > max) { refund(limiter, got); got = 0; }
        return new Permit(limiter, Math.max(got, 0));
    }

    /**
     * Polices one atomic unit (a datagram) of n bytes with {@code tryAcquire(n, n)}: true when granted, the grant
     * then staying taken (the whole unit may be sent even when fewer bytes were granted); false with nothing taken
     * otherwise, a failing limiter included. An empty unit passes without a call.
     */
    public static boolean police(Limiter limiter, long n) {
        if (n <= 0) return true;
        //noinspection resource: a granted unit stays taken; an empty permit holds nothing
        Permit p = tryAcquire(limiter, n, n);
        if (p.isEmpty()) return false;
        p.left = 0;
        return true;
    }

    /** Gives n bytes back ({@link Limiter#refund}), ignoring a failure of the limiter; nothing for n {@code <= 0} or no limiter. */
    public static void refund(Limiter limiter, long n) {
        if (limiter == null || n <= 0) return;
        try { limiter.refund(n); }
        catch (RuntimeException | AssertionError ignored) { /* a failing refund is ignored */ }
    }

    /** The bytes it holds unused. */
    public long size() { return left; }

    /** Whether it holds nothing. */
    public boolean isEmpty() { return left == 0; }

    /** Uses n of its bytes (at most what it holds): they stay taken. */
    public void consume(long n) {
        if (n < 0) throw new IllegalArgumentException("negative");
        left -= Math.min(n, left);
    }

    /** Gives back what it holds unused. */
    @Override public void close() {
        long n = left; left = 0;
        refund(limiter, n);
    }

    @Override public String toString() { return "Permit[left=" + left + "]"; }
}
