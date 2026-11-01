package io.zmux;

import io.zmux.carrier.PipeIo;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A mode's choice of carrier kind for {@link Zmux#dial(CarrierMode, ZmuxConfig, ModePolicy)} and
 * the members of a {@link PipePool#mode mode pool}: the candidates by index of preference, read
 * against and recorded in the policy's cache. Immutable; the cache is safe for concurrent use.
 */
final class ModeSource {
    private final List<Candidate> cands;
    private final boolean auto;
    private final ModePolicy policy;

    ModeSource(CarrierMode mode, ModePolicy policy) {
        this.cands = mode.candidates();
        this.auto = mode.isAuto();
        this.policy = policy == null ? ModePolicy.defaults() : policy;
    }

    List<Candidate> candidates() { return cands; }

    ModePolicy policy() { return policy; }

    private CarrierCache cache() { return policy.cache(); }

    private boolean failed(int i, long now) {
        return cache().failUntil(policy.key(), cands.get(i).kind(), now) != Long.MIN_VALUE;
    }

    /** The candidates not held as failed, in order of preference; all of them when every one is. */
    List<Integer> usable(long now) {
        var order = new ArrayList<Integer>();
        for (int i = 0; i < cands.size(); i++) if (!failed(i, now)) order.add(i);
        if (order.isEmpty()) for (int i = 0; i < cands.size(); i++) order.add(i);
        return order;
    }

    /** The most robust of order: a kind the cache holds as usable, else the exchange carrier, else the last. */
    int base(List<Integer> order, long now) {
        for (int i : order) if (cache().good(policy.key(), cands.get(i).kind(), now)) return i;
        for (int i : order) if (cands.get(i).kind() == CarrierKind.EXCHANGE) return i;
        return order.getLast();
    }

    /** The kind a new member is dialled over. */
    int pick() {
        if (!auto) return 0;
        long now = System.nanoTime();
        return base(usable(now), now);
    }

    /** The most preferred kind before cur not held as failed; -1: none. */
    int better(int cur) {
        if (!auto) return -1;
        long now = System.nanoTime();
        for (int i = 0; i < cur; i++) if (!failed(i, now)) return i;
        return -1;
    }

    Duration settle() { return policy.settle(); }

    void record(int i, boolean usable) {
        if (auto) cache().record(policy.key(), cands.get(i).kind(), usable, System.nanoTime());
    }

    /** Candidate i serves session: remembered as usable, and reported as its kind when it changed. */
    void established(ZmuxSession session, int i) {
        record(i, true);
        CarrierKind kind = cands.get(i).kind();
        if (session.carrierKind() != kind) session.reportCarrier(List.of(), kind);
    }

    /** A pipe of candidate i, joined at joinedAt (System.nanoTime), ended or failed: a failure within the drop window. */
    void dropped(int i, long joinedAt) {
        if (System.nanoTime() - joinedAt < policy.dropWindow().toNanos()) record(i, false);
    }

    /**
     * Joins carriers of candidate i to session with options within the candidate's timer, and
     * reports the try. A failure is recorded in the cache and thrown.
     */
    List<ZmuxPipe> join(ZmuxSession session, int i, PipeOptions options) throws IOException {
        Candidate c = cands.get(i);
        List<ZmuxPipe> pipes;
        try {
            pipes = joinCandidate(session, c, options);
        } catch (InterruptedIOException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            session.reportCarrier(List.of(new CandidateResult(c.kind(), CarrierSelection.outcomeOf(e), true, e)), null);
            record(i, false);
            throw e instanceof IOException io ? io : new IOException(c.kind() + " join failed", e);
        }
        session.reportCarrier(List.of(new CandidateResult(c.kind(), CandidateOutcome.USABLE, true, null)), null);
        return pipes;
    }

    private List<ZmuxPipe> joinCandidate(ZmuxSession s, Candidate c, PipeOptions options) throws IOException {
        Credential cred = s.credential();
        if (c.joiner() != null) {
            List<ZmuxPipe> pipes = c.joiner().join(s, options, cred, policy.timerMin());
            if (pipes == null || pipes.isEmpty()) throw new IOException(c.kind() + " joined no pipe");
            return List.copyOf(pipes);
        }
        long t0 = System.nanoTime();
        PipeIo io = c.dialer().dial(cred, Duration.ofNanos(s.core.handshakeTimeout())).withCarrier(c.kind());
        return List.of(addWithin(s, io, options, policy.timer(Duration.ofNanos(System.nanoTime() - t0))));
    }

    /** Joins io as a pipe of s, aborting it when the join is not confirmed within timer. */
    private static ZmuxPipe addWithin(ZmuxSession s, PipeIo io, PipeOptions options, Duration timer) throws IOException {
        CompletableFuture<ZmuxPipe> f = new CompletableFuture<>();
        Thread.ofVirtual().name("zmux-carrier-join").start(() -> {
            try { f.complete(s.paths().add(io, options)); } catch (Throwable e) { f.completeExceptionally(e); }
        });
        try {
            return f.get(timer.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            // Confirmed late: not usable all the same.
            f.thenAccept(ModeSource::abort);
            throw new SocketTimeoutException("join not confirmed within the timer");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            f.thenAccept(ModeSource::abort);
            throw new InterruptedIOException("interrupted");
        } catch (ExecutionException e) {
            throw e.getCause() instanceof IOException io2 ? io2 : new IOException(e.getCause());
        }
    }

    private static void abort(ZmuxPipe pipe) {
        try { pipe.abort(); } catch (IOException ignored) { /* Gone. */ }
    }
}
