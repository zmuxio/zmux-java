package io.zmux;

import io.zmux.carrier.PipeIo;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

/** Immutable maintenance policy for one route. Scheduling and attachment limits stay in the session. */
public record PipePool(Join join, int count, PipeOptions options, Rotation rotation, Backoff backoff,
                       Duration inspectionInterval, Duration attemptTimeout, PoolPolicy policy) {
    /**
     * Creates and joins one fresh pipe using the supplied options. Must cooperate with
     * cancellation and interruption: an attempt that does not is abandoned rather than awaited
     * and may keep running after its maintenance run ended (see {@link PipeMaintenance}).
     */
    @FunctionalInterface public interface Dial {
        ZmuxPipe open(Attempt attempt) throws IOException, InterruptedException;
    }
    /**
     * Joins one member of several pipes, such as a split carrier's uplink and downlink
     * ({@code HttpCarrierClient.splitJoin}), each with {@link Attempt#options()} and its own direction.
     * The member is usable while all of its pipes are, counts once against the pool's count, and is
     * redialled, rotated (make before break) and retired as a whole. The pipes belong to
     * {@link Attempt#session()}; a join that fails aborts those it made, and the pool aborts an
     * abandoned attempt's pipes when it returns.
     */
    @FunctionalInterface public interface Join {
        java.util.List<ZmuxPipe> open(Attempt attempt) throws IOException, InterruptedException;
    }
    /** Opens a carrier and transfers its ownership on return. */
    @FunctionalInterface public interface CarrierDial {
        PipeIo open(Cancellation cancellation) throws IOException, InterruptedException;
    }
    public record Rotation(long maxBytes, Duration maxAge, double jitter) {
        public Rotation {
            if (maxBytes < 0 || jitter < 0 || jitter > 1 || !Double.isFinite(jitter)) throw new IllegalArgumentException("rotation");
            Objects.requireNonNull(maxAge); nanos(maxAge, true);
        }
        public static Rotation disabled() { return new Rotation(0, Duration.ZERO, 0); }
    }
    public record Backoff(Duration minimum, Duration maximum, double jitter) {
        public Backoff {
            if (nanos(minimum, false) > nanos(maximum, false) || jitter < 0 || jitter > 1 || !Double.isFinite(jitter))
                throw new IllegalArgumentException("backoff");
        }
        /** The default maximum: the longest wait between failed dials. */
        public static final Duration DEFAULT_MAXIMUM = Duration.ofSeconds(60);
        public static Backoff defaults() { return new Backoff(Duration.ofSeconds(1), DEFAULT_MAXIMUM, .2); }
    }
    public PipePool {
        Objects.requireNonNull(join); Objects.requireNonNull(options); Objects.requireNonNull(rotation); Objects.requireNonNull(backoff);
        if (count < 1) throw new IllegalArgumentException("count must be positive");
        if (join instanceof InBandDial && options.direction == PipeOptions.Direction.RECEIVE_ONLY)
            throw new IllegalArgumentException("receive-only pools need a Dial using Attempt.reserve");
        nanos(inspectionInterval, false); if (attemptTimeout != null) nanos(attemptTimeout, false);
    }
    static long nanos(Duration duration, boolean zero) {
        Objects.requireNonNull(duration);
        try {
            long n = duration.toNanos();
            if (n < 0 || !zero && n == 0) throw new IllegalArgumentException("duration must be positive");
            return n;
        } catch (ArithmeticException error) { throw new IllegalArgumentException("duration exceeds the monotonic clock range", error); }
    }
    public static PipePool of(Dial dial) {
        Objects.requireNonNull(dial);
        return join(new SingleDial(dial));
    }
    /** A pool whose members are groups of pipes joined by join. */
    public static PipePool join(Join join) {
        return new PipePool(join, 1, PipeOptions.defaults(), Rotation.disabled(), Backoff.defaults(), Duration.ofMillis(250), null, null);
    }
    /** A single attempt's creation scope; completed resources are committed by maintenance. */
    public static final class Attempt {
        private final ZmuxSession session;
        private final PipeOptions options;
        private final Cancellation cancellation;
        /** The candidate a mode pool's attempt dials; -1 outside mode pools. */
        final int kind;
        private final Thread owner;
        private PipeReservation reservation;
        private ZmuxPipe made;
        /** The pipes the attempt produced, aborted when the pool rejects them. */
        private java.util.List<ZmuxPipe> group = java.util.List.of();
        private boolean started;
        Attempt(ZmuxSession session, PipeOptions options, Cancellation cancellation, int kind) {
            this.session = session; this.options = options; this.cancellation = cancellation; this.kind = kind; owner = Thread.currentThread();
        }
        public PipeOptions options() { return options; }
        /** The session a {@link Join} adds its pipes to. */
        public ZmuxSession session() { return session; }
        public Cancellation cancellation() { return cancellation; }
        private void begin() {
            if (Thread.currentThread() != owner || started) throw new IllegalStateException("one creation per dial attempt on its worker");
            started = true;
        }
        public ZmuxPipe join(PipeIo io) throws IOException { begin(); return made = session.addPipe(io, options); }
        public ZmuxPipe join(java.net.InetSocketAddress address) throws IOException { begin(); return made = session.addPipe(address, options); }
        public ZmuxPipe joinTls(java.net.InetSocketAddress address, TlsOptions tls) throws IOException {
            begin(); return made = session.addPipeTls(address, tls, options);
        }
        public Reserved reserve() throws IOException { begin(); reservation = session.reservePipe(options); return new Reserved(); }
        public final class Reserved implements AutoCloseable {
            private Reserved() {}
            public long id() { return reservation.id(); }
            public String header() { return reservation.header(); }
            public ZmuxPipe attach(PipeIo io) throws IOException {
                if (Thread.currentThread() != owner) throw new IllegalStateException("reservation belongs to the dial worker");
                return made = reservation.attach(io);
            }
            @Override public void close() { reservation.close(); }
        }
        /**
         * Takes the attempt's result: every pipe belongs to the session and, when the attempt created one
         * itself, the result is that pipe.
         */
        boolean adopt(java.util.List<ZmuxPipe> pipes) {
            var g = java.util.List.copyOf(pipes);
            if (g.isEmpty()) return false;
            for (ZmuxPipe p : g) if (!p.belongsTo(session)) return false;
            if (made != null && !(g.size() == 1 && g.getFirst().id() == made.id())) return false;
            group = g; // only the attempt's own pipes are ever aborted
            return true;
        }
        void finish(boolean accepted) {
            if (reservation != null) reservation.close();
            if (accepted) return;
            if (made != null && !group.contains(made)) made.abortQueued();
            for (ZmuxPipe p : group) p.abortQueued();
        }
    }
    private record SingleDial(Dial dial) implements Join {
        @Override public java.util.List<ZmuxPipe> open(Attempt attempt) throws IOException, InterruptedException {
            return java.util.List.of(Objects.requireNonNull(dial.open(attempt), "dial returned null"));
        }
    }
    private record InBandDial(CarrierDial carrier) implements Join {
        @Override public java.util.List<ZmuxPipe> open(Attempt attempt) throws IOException, InterruptedException {
            return java.util.List.of(dialOne(attempt));
        }
        private ZmuxPipe dialOne(Attempt attempt) throws IOException, InterruptedException {
            PipeIo io = Objects.requireNonNull(carrier.open(attempt.cancellation()), "carrier factory returned null");
            if (attempt.cancellation.isCancelled() || Thread.currentThread().isInterrupted()) {
                try { if (io.reader() != null) io.reader().close(); }
                finally { if (io.writer() != null) io.writer().abort(new IOException("dial cancelled")); }
                throw new InterruptedException("dial cancelled");
            }
            return attempt.join(io);
        }
    }
    /** A mode pool's member source: the candidate the run chose for the attempt. */
    record ModeJoin(ModeSource source) implements Join {
        @Override public java.util.List<ZmuxPipe> open(Attempt attempt) throws IOException {
            return source.join(attempt.session(), attempt.kind, attempt.options());
        }
    }
    /**
     * A pool whose members come from mode (API.md Section 4.8): each member is dialled over the
     * kind the policy's cache holds most robust, the outcome is recorded in the cache, and a
     * member not of the most preferred kind is upgraded, make before break, to a more preferred
     * kind not backing off: the new member replaces it once it has been usable for the policy's
     * settle time; if its dial fails the old member stays. At most one upgrade per pool is under
     * way at a time, and it needs a free attachment. Several mode pools may share a cache.
     *
     * @param policy null: {@link ModePolicy#defaults()}
     */
    public static PipePool mode(CarrierMode mode, ModePolicy policy) {
        return join(new ModeJoin(new ModeSource(Objects.requireNonNull(mode), policy)));
    }
    /** The mode of a {@link #mode} pool; null otherwise. */
    ModeSource modeSource() { return join instanceof ModeJoin(ModeSource source) ? source : null; }
    /** Adapts a raw carrier factory through the session's existing in-band join operation. */
    public static PipePool carriers(CarrierDial dial) { return join(new InBandDial(Objects.requireNonNull(dial))); }
    public PipePool withCount(int value) { return new PipePool(join, value, options, rotation, backoff, inspectionInterval, attemptTimeout, policy); }
    public PipePool withOptions(PipeOptions value) { return new PipePool(join, count, value, rotation, backoff, inspectionInterval, attemptTimeout, policy); }
    public PipePool withRotation(Rotation value) { return new PipePool(join, count, options, value, backoff, inspectionInterval, attemptTimeout, policy); }
    public PipePool withBackoff(Backoff value) { return new PipePool(join, count, options, rotation, value, inspectionInterval, attemptTimeout, policy); }
    public PipePool withInspectionInterval(Duration value) { return new PipePool(join, count, options, rotation, backoff, value, attemptTimeout, policy); }
    /** Consulted on every pass of the maintenance run; null: none. */
    public PipePool withPolicy(PoolPolicy value) { return new PipePool(join, count, options, rotation, backoff, inspectionInterval, attemptTimeout, value); }
    public PipePool withAttemptTimeout(Duration value) { return new PipePool(join, count, options, rotation, backoff, inspectionInterval, value, policy); }
}
