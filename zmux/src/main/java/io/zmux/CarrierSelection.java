package io.zmux;

import io.zmux.carrier.PipeIo;
import io.zmux.internal.core.CoreConfig;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/** {@link Zmux#dial(CarrierChoice, ZmuxConfig, CarrierChoicePolicy)}: carrier selection. */
final class CarrierSelection {
    private CarrierSelection() {}

    static ZmuxSession dial(CarrierChoice choice, ZmuxConfig cfg, CarrierChoicePolicy policy) throws IOException {
        CarrierCredential cred = cfg.carrierCredential;
        var src = new CarrierChoiceSource(choice, policy);
        List<Candidate> cands = src.candidates();
        if (!choice.isAuto()) {
            Candidate c = cands.getFirst();
            Started st = start(c, cfg, cred, null);
            if (st.session == null) throw new CarrierChoiceException(List.of(st.result));
            st.session.reportCarrier(List.of(st.result), c.kind(), c.variant());
            return st.session;
        }
        CarrierChoicePolicy p = src.policy();
        ZmuxConfig acfg = cfg.withoutEarlyData();
        long now = System.nanoTime();
        List<Integer> order = src.usable(now);
        boolean join = p.join() && cfg.maxPipes() > 1;
        if (join) {
            int base = src.base(order, now);
            var seq = new ArrayList<Integer>();
            seq.add(base);
            for (int i : order) if (i != base) seq.add(i);
            order = seq;
        }
        List<CandidateResult> results = new ArrayList<>();
        for (int i : order) {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedIOException("interrupted");
            Candidate c = cands.get(i);
            src.prime(i);
            Started st = start(c, acfg, cred, p);
            results.add(st.result);
            src.record(i, st.session != null);
            if (st.session == null) continue;
            ZmuxSession s = st.session;
            s.reportCarrier(results, c.kind(), c.variant());
            long chosen = System.nanoTime();
            ZmuxPipe pipe0 = join && s.info().multipath() ? s.pipes().stream().filter(x -> x.rawId() == 0).findFirst().orElse(null) : null;
            if (pipe0 != null) {
                // Every later upgrade, fallback and retry is this pool's.
                try {
                    PipeMaintenance.startWith(s, PipePool.join(new PipePool.ChoiceJoin(src)), pipe0, i, chosen);
                } catch (IOException | RuntimeException e) {
                    s.close();
                    throw e;
                }
            } else {
                int kind = i;
                s.closed().thenAccept(e -> { if (!(e instanceof SessionError.ApplicationClosed)) src.dropped(kind, chosen); });
            }
            return s;
        }
        throw new CarrierChoiceException(results);
    }

    private record Started(ZmuxSession session, CandidateResult result) {}

    /**
     * Opens a carrier of c and starts a session on it. With p (an auto choice) the peer's parameters
     * must arrive within the candidate's timer; a session that misses it is abandoned, which
     * sends nothing.
     */
    private static Started start(Candidate c, ZmuxConfig cfg, CarrierCredential cred, CarrierChoicePolicy p) throws InterruptedIOException {
        Duration bound = cfg.handshakeBound();
        long t0 = System.nanoTime();
        PipeIo io;
        try {
            io = c.dialer().dial(cred, bound).withCarrier(c.kind()).withVariant(c.variant());
        } catch (InterruptedIOException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            return failed(c, e);
        }
        if (p == null) {
            try {
                return new Started(Zmux.open(io, Role.CLIENT, cfg), usable(c));
            } catch (InterruptedIOException e) {
                throw e;
            } catch (IOException | RuntimeException e) {
                return failed(c, e);
            }
        }
        long deadline = System.nanoTime() + p.timer(Duration.ofNanos(System.nanoTime() - t0)).toNanos();
        ZmuxSession s;
        try {
            CoreConfig.Role r = Zmux.core(Role.CLIENT);
            s = Zmux.started(Zmux.startExternal(io, cfg, r, r), cfg.carrierCredential);
        } catch (InterruptedIOException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            return failed(c, e);
        }
        Started usable = new Started(s, usable(c));
        try {
            s.awaitEstablished(deadline);
            return usable;
        } catch (SocketTimeoutException e) {
            boolean abandoned;
            try {
                abandoned = s.abandonHandshake();
            } catch (IOException again) {
                abandoned = true;
            }
            if (!abandoned) return usable; // established meanwhile
            var err = new SocketTimeoutException(c.kind() + " carrier: no transport parameters within the timer");
            return new Started(null, new CandidateResult(c.kind(), CandidateOutcome.TIMED_OUT, false, err, c.variant()));
        } catch (InterruptedIOException e) {
            s.close();
            throw e;
        } catch (IOException e) {
            s.close();
            return failed(c, e);
        }
    }

    /** The result of a candidate that started a session. */
    private static CandidateResult usable(Candidate c) {
        return new CandidateResult(c.kind(), CandidateOutcome.USABLE, false, null, c.variant());
    }

    /** No session from a candidate that failed with e. */
    private static Started failed(Candidate c, Throwable e) {
        return new Started(null, new CandidateResult(c.kind(), outcomeOf(e), false, e, c.variant()));
    }

    /** Classifies the failure of a try. */
    static CandidateOutcome outcomeOf(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof HttpStatusException || t instanceof HttpRedirectException || t instanceof java.net.ConnectException) {
                return CandidateOutcome.REFUSED;
            }
            if (t instanceof SocketTimeoutException || t instanceof TimeoutException) return CandidateOutcome.TIMED_OUT;
            if (t instanceof SessionClosedException sc && sc.error() instanceof SessionError.HandshakeTimeout) return CandidateOutcome.TIMED_OUT;
            // A carrier that opened at once and failed later (an HTTP response refusing it): its pipe's error.
            if (t instanceof SessionClosedException sc && sc.error() instanceof SessionError.PipesLost(Throwable last) && last != null) {
                return outcomeOf(last);
            }
        }
        return CandidateOutcome.FAILED;
    }
}
