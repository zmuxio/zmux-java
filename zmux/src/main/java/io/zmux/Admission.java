package io.zmux;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * The decision on every new carrier of a {@link ZmuxListener}, configured once on its
 * {@link ListenerConfig#admission()} and run by every entry of that listener: its socket and
 * TLS carriers, those given to {@link io.zmux.adapter.ListenerAdapter} (the HTTP, WebSocket and exchange
 * carriers of an HTTP endpoint, the connections of a QUIC server), joins and resumptions
 * included. A carrier first takes a slot of its source ({@link
 * ListenerConfig#maxPendingPerSource()}) and one of the listener ({@link
 * ListenerConfig#maxPending()}), then completes its outer handshake (TLS), then the
 * listener reads up to {@code peek} bytes of a byte stream, or the first message of a
 * message-framed carrier (at most {@link #maxMessage()} bytes), never more, and asks the
 * decider; nothing is sent before the decision and nothing is consumed except what an accept
 * skips. An HTTP request carrier is decided on its request head ({@link Stage#HEAD}); on the
 * head of an HTTP upgrade {@link AdmissionVerdict#DEFER} decides again on the carrier's first
 * message once the upgrade completed.
 *
 * <p>Built with {@link #builder(int, Decider)}; every setting not called keeps its default.
 */
public final class Admission {
    /** The longest prefix: it is read into the carrier's read buffer, which holds a first record of at most this size. */
    public static final int MAX_PEEK = 65535;

    /** The longest first message of a message-framed carrier: the largest record. */
    public static final int MAX_MESSAGE = 65535;

    private final int peek, maxPeek, maxMessage;
    private final Duration peekTimeout, stallMin, stallMax;
    private final boolean fallbackOnPeekTimeout;
    private final Decider decider;
    private final AdmissionVerdict refusal;

    private Admission(Builder b) {
        peek = b.peek; maxPeek = b.maxPeek; maxMessage = b.maxMessage == 0 ? MAX_MESSAGE : b.maxMessage; peekTimeout = b.peekTimeout; fallbackOnPeekTimeout = b.fallbackOnPeekTimeout;
        stallMin = b.stallMin; stallMax = b.stallMax; decider = b.decider; refusal = b.refusal;
    }

    /**
     * An admission that reads peek bytes (0: none), at most {@link #MAX_PEEK}, of a byte stream before it asks decider;
     * fewer when the carrier ends first; a carrier without a receiving half, or one decided
     * on its HTTP request head, is decided at once. On a message-framed carrier any peek above 0, and every decision
     * deferred from an upgrade's head, reads the first message whole as the prefix, bounded by {@link #maxMessage()}
     * and, for a positive peek, the larger of peek and maxPeek. An oversized message counts in
     * {@link AdmissionStats#tooLarge()} after deferral or when only the smaller adapter limit
     * is known to be exceeded. Exceeding the prefix bound in ordinary admission counts in
     * {@link AdmissionStats#closed()}. On acceptance, skip is 0 (replayed) or the message length.
     */
    public static Builder builder(int peek, Decider decider) {
        Objects.requireNonNull(decider, "decider");
        if (peek < 0 || peek > MAX_PEEK) throw new IllegalArgumentException("peek outside [0, " + MAX_PEEK + "]");
        return new Builder(peek, decider);
    }

    /** The entry's fixed authentication refusal, or null for a general classification decision. */
    public AdmissionVerdict refusal() { return refusal; }

    /** The bytes read before the decision. */
    public int peek() { return peek; }

    /** The bound of {@link Decision#need()} on a byte stream; 0: peek, so that {@link AdmissionVerdict#MORE} is invalid. */
    public int maxPeek() { return maxPeek; }

    /** The bound of the first message of a message-framed carrier, the prefix of its decision. */
    public int maxMessage() { return maxMessage; }

    /** The peek timeout; zero: drawn once per admission between 5 s and 30 s. */
    public Duration peekTimeout() { return peekTimeout; }

    /** See {@link Builder#fallbackOnPeekTimeout(boolean)}. */
    public boolean isFallbackOnPeekTimeout() { return fallbackOnPeekTimeout; }

    /** The lower bound of a {@link AdmissionVerdict#STALL}'s duration; zero with {@link #stallMax()}: drawn. */
    public Duration stallMin() { return stallMin; }

    /** The upper bound of a {@link AdmissionVerdict#STALL}'s duration; zero: drawn. */
    public Duration stallMax() { return stallMax; }

    /** The decision function. */
    public Decider decider() { return decider; }

    /** Builds an {@link Admission}; every method checks its argument. */
    public static final class Builder {
        private final int peek;
        private final Decider decider;
        private AdmissionVerdict refusal;
        private int maxPeek, maxMessage;
        private Duration peekTimeout = Duration.ZERO, stallMin = Duration.ZERO, stallMax = Duration.ZERO;
        private boolean fallbackOnPeekTimeout;

        private Builder(int peek, Decider decider) { this.peek = peek; this.decider = decider; }

        /**
         * Bounds {@link Decision#need()} on a byte stream, at most {@link #MAX_PEEK}; not called: peek, so that MORE
         * is invalid. For a positive peek this also bounds the first message together with {@link #maxMessage(int)}.
         */
        public Builder maxPeek(int n) {
            if (n != 0 && (n < peek || n > MAX_PEEK)) throw new IllegalArgumentException("maxPeek must be zero or between peek and " + MAX_PEEK);
            maxPeek = n;
            return this;
        }

        /**
         * Bounds the first message of a message-framed carrier (WebSocket), the prefix of its decision, at most
         * {@link #MAX_MESSAGE}; 0 or not called: {@link #MAX_MESSAGE}. A longer first message closes the carrier
         * without a decision. See the counting rules in {@link Admission#builder(int, Decider)}.
         */
        public Builder maxMessage(int n) {
            if (n < 0 || n > MAX_MESSAGE) throw new IllegalArgumentException("maxMessage outside [0, " + MAX_MESSAGE + "]");
            maxMessage = n;
            return this;
        }

        /**
         * A positive bound on the reading of the prefix and the decision together, from the end of the outer
         * handshake; not called: drawn once per admission from a secure random source between 5 s and 30 s.
         */
        public Builder peekTimeout(Duration d) {
            if (Objects.requireNonNull(d, "d").isNegative() || d.isZero()) throw new IllegalArgumentException("peekTimeout not positive");
            peekTimeout = d;
            return this;
        }

        /**
         * A byte-stream carrier whose prefix has not arrived when the peek timeout ends, in the first read or in a
         * {@link AdmissionVerdict#MORE} round, takes {@link AdmissionVerdict#FALLBACK} with every byte read replayed
         * instead of being closed. It counts once according to the fallback path's outcome, including Stall or
         * Stall overflow without a target. Native TCP, TLS and QUIC streams can resume the read. An external
         * reader without read deadlines, a message or request carrier closes at expiry, as does a
         * carrier whose decider has not returned in time. Default false.
         */
        public Builder fallbackOnPeekTimeout(boolean on) {
            fallbackOnPeekTimeout = on;
            return this;
        }

        /**
         * The bounds of a {@link AdmissionVerdict#STALL}'s duration, min at most max and max positive; not called:
         * max is drawn once between 30 s and 120 s and min between a quarter and three quarters of it.
         */
        public Builder stall(Duration min, Duration max) {
            Objects.requireNonNull(min, "min");
            Objects.requireNonNull(max, "max");
            if (min.isNegative() || max.isNegative() || max.isZero()) throw new IllegalArgumentException("stall bounds: min >= 0, max > 0");
            if (min.compareTo(max) > 0) throw new IllegalArgumentException("stall min above max");
            stallMin = min;
            stallMax = max;
            return this;
        }

        /**
         * Applies a single refusal policy through authentication. FALLBACK requires a host
         * handler and a request head or a byte stream. A bounded first message cannot
         * always be replayed, so that stage closes.
         */
        public Builder refusal(AdmissionVerdict verdict) {
            if (verdict != AdmissionVerdict.CLOSE && verdict != AdmissionVerdict.STALL && verdict != AdmissionVerdict.FALLBACK) {
                throw new IllegalArgumentException("invalid admission refusal");
            }
            refusal = verdict;
            return this;
        }

        public Admission build() { return new Admission(this); }
    }

    /**
     * Decides on a carrier, once per carrier, once more per {@link AdmissionVerdict#MORE} and once more after a
     * {@link AdmissionVerdict#DEFER}, and for different carriers concurrently, outside
     * every lock of zmux, on the listener's event loop: it must not block, and returns a stage
     * that completes later when the decision takes time. prefix is read-only and valid until
     * the stage completes; info is each call's own (a deferred decision gets the head's
     * metadata at {@link Stage#CARRIER}). The decision is due by deadline (what is left of the peek
     * timeout): a stage not complete by then is cancelled ({@code toCompletableFuture().cancel(false)})
     * and the site's configured refusal applied. A decider that throws, a stage that fails,
     * a null decision and a skip outside the prefix take the same refusal. Without a
     * {@link Builder#refusal} the carrier closes. Stopping the listener closes pending carriers.
     */
    @FunctionalInterface
    public interface Decider {
        CompletionStage<Decision> decide(CarrierInfo info, ByteBuffer prefix, Deadline deadline);

        /** A decider that decides at once, on the event loop, with f. */
        static Decider immediate(java.util.function.BiFunction<CarrierInfo, ByteBuffer, Decision> f) {
            Objects.requireNonNull(f, "f");
            return (info, prefix, deadline) -> java.util.concurrent.CompletableFuture.completedFuture(f.apply(info, prefix));
        }
    }

    /** When a decision is due, on the {@link System#nanoTime()} clock. */
    public record Deadline(long nanoTime, java.util.function.BooleanSupplier cancellation) {
        public Deadline(long nanoTime) { this(nanoTime, () -> false); }

        public Deadline {
            Objects.requireNonNull(cancellation, "cancellation");
        }

        /** Whether the listener withdrew the carrier. */
        public boolean cancelled() { return cancellation.getAsBoolean(); }

        /** The time left, zero once passed. */
        public Duration remaining() {
            long left = nanoTime - System.nanoTime();
            return left > 0 ? Duration.ofNanos(left) : Duration.ZERO;
        }

        /** Whether the deadline passed. */
        public boolean expired() { return nanoTime - System.nanoTime() <= 0; }
    }


    /**
     * A verdict; skip applies to {@link AdmissionVerdict#ACCEPT} only, value to {@link AdmissionVerdict#ACCEPT} and
     * {@link AdmissionVerdict#FALLBACK}. A skip outside the prefix is {@link AdmissionVerdict#CLOSE}.
     *
     * @param value the host's value for an accepted carrier, or null: zmux hands it on without
     *     looking at it to the {@link InboundTransform} ({@link InboundTransform.Context#admitted()})
     *     and to the session ({@link ZmuxSession#admitted()}) or pipe ({@link ZmuxPipe#admitted()})
     *     the carrier makes, and holds it until their state is released. Whether a join comes
     *     from the session's client is the host's to decide: zmux compares no values. On a
     *     {@link AdmissionVerdict#FALLBACK} it is the host's routing of the carrier, {@link Takeover#value()}
     * @param need with {@link AdmissionVerdict#MORE}, the total prefix length asked for
     * @param variant a short label of the carrier for any verdict, one of a few constants of the
     *     host: counted in {@link ZmuxListener#variantStats()}, and on acceptance the pipe input's
     *     variant unless a transform's result has its own
     */
    public record Decision(AdmissionVerdict verdict, int skip, Object value, int need, String variant) {
        public static final Decision TAKEOVER = new Decision(AdmissionVerdict.TAKEOVER, 0, null);
        public static final Decision FALLBACK = new Decision(AdmissionVerdict.FALLBACK, 0, null);
        public static final Decision STALL = new Decision(AdmissionVerdict.STALL, 0, null);
        public static final Decision CLOSE = new Decision(AdmissionVerdict.CLOSE, 0, null);
        public Decision {
            Objects.requireNonNull(verdict);
            if (variant == null) variant = "";
        }
        /** A verdict without a value. */
        public Decision(AdmissionVerdict verdict, int skip) { this(verdict, skip, null); }
        /** A verdict with a value and without a variant. */
        public Decision(AdmissionVerdict verdict, int skip, Object value) { this(verdict, skip, value, 0, ""); }
        public static Decision accept(int skip) { return new Decision(AdmissionVerdict.ACCEPT, skip, null); }
        /** An accept carrying the host's value (null: none). */
        public static Decision accept(int skip, Object value) { return new Decision(AdmissionVerdict.ACCEPT, skip, value); }
        /** Hands the carrier and the routing value to the listener's byte-stream handler. */
        public static Decision takeover(Object value) { return new Decision(AdmissionVerdict.TAKEOVER, 0, value); }
        public static Decision fallback(Object value) { return new Decision(AdmissionVerdict.FALLBACK, 0, value); }
        /** Asks for a prefix of need bytes. */
        public static Decision more(int need) { return new Decision(AdmissionVerdict.MORE, 0, null, need, ""); }
        /** This decision labelled v. */
        public Decision withVariant(String v) { return new Decision(verdict, skip, value, need, v); }
    }

    /** The counts of one {@link Decision#variant()} ({@link ZmuxListener#variantStats()}). */
    public record VariantStat(String name, boolean overflow, long accepted, long fallback, long takeover, long stalled, long closed) {
        /** A variant snapshot with no carrier takeovers. */
        public VariantStat(String name, boolean overflow, long accepted, long fallback, long stalled, long closed) {
            this(name, overflow, accepted, fallback, 0, stalled, closed);
        }
    }

    /** How many labels are counted apart; further ones share one bucket, listed last. */
    public static final int MAX_VARIANTS = 64;

    /**
     * The host's byte-stream handler. TAKEOVER selects this handler for every carrier kind.
     * FALLBACK selects the normal handler of its transport, including the HTTP request handler
     * for HTTP before an upgrade. A streaming HTTP takeover first sends the acceptance response
     * head. Its request and response bodies become the two carrier halves.
     *
     * <p>Runs off the event loops. Normal completion (including a null stage) succeeds and
     * closes the carrier unless detached. Failure and withdrawal also reclaim an undetached
     * carrier. Withdrawal leaves the host's stage running.
     */
    @FunctionalInterface
    public interface Fallback {
        java.util.concurrent.CompletionStage<?> serve(Takeover takeover);
    }

    /** What a decision is taken on ({@link CarrierInfo#stage()}). */
    public enum Stage {
        /** The carrier's start: the prefix is its first bytes ({@code peek}, {@link AdmissionVerdict#MORE}) or its first message. */
        CARRIER,
        /**
         * An HTTP request's head, before any body byte is read or the upgrade is answered: an HTTP upgrade
         * (WebSocket; {@link CarrierInfo#upgrade()}), an exchange carrier, or an HTTP request carrier without a body,
         * such as a downlink GET. There is no prefix, and a credential travels in the request header field
         * {@link PrefixAuth#HEADER_FIELD}.
         */
        HEAD
    }

    /** Evidence supplied by the adapter for the actual carrier carrying the presentation. */
    public enum OuterSecurity {
        CLEAR, ENCRYPTED, TLS_1_3, QUIC, EARLY_DATA;

        public boolean encrypted() { return this == ENCRYPTED || permitsBinding(); }

        public boolean permitsBinding() { return this == TLS_1_3 || this == QUIC; }

        /** The negotiated protocol of a completed TLS connection. */
        public static OuterSecurity tls(String protocol) {
            if ("TLSv1.3".equals(protocol)) return TLS_1_3;
            return protocol != null && protocol.startsWith("TLSv") ? ENCRYPTED : CLEAR;
        }
    }

    /**
     * Immutable metadata supplied by the carrier adapter. Addresses are optional.
     * The request is {@link RequestHead#NONE} for carriers without an HTTP request.
     */
    public static final class CarrierInfo {
        static { io.zmux.internal.exchange.ExchangeAccess.installInfo(CarrierInfo::verifiedExchange); }

        private final InetSocketAddress source, peer;
        private final String applicationProtocol, serverName;
        private final io.zmux.carrier.PipeIo.Exporter exporter;
        private final RequestHead request;
        private final boolean terminatedTls, upgrade, authenticatedEnvelope;
        private final Stage stage;
        private final io.zmux.carrier.Framing framing;
        private final OuterSecurity outerSecurity;

        /** A byte stream decided on its start, using {@link OuterSecurity#CLEAR} initially. */
        public CarrierInfo(InetSocketAddress source, InetSocketAddress peer, String applicationProtocol, String serverName,
                           io.zmux.carrier.PipeIo.Exporter exporter, RequestHead request) {
            this(source, peer, applicationProtocol, serverName, exporter, request, exporter != null,
                    Stage.CARRIER, io.zmux.carrier.Framing.BYTE_STREAM, false, OuterSecurity.CLEAR);
        }

        /** Creates metadata for a carrier adapter. */
        public CarrierInfo(InetSocketAddress source, InetSocketAddress peer, String applicationProtocol, String serverName,
                           io.zmux.carrier.PipeIo.Exporter exporter, RequestHead request, boolean terminatedTls,
                           Stage stage, io.zmux.carrier.Framing framing, boolean upgrade, OuterSecurity outerSecurity) {
            this(source, peer, applicationProtocol, serverName, exporter, request, terminatedTls, stage, framing,
                    upgrade, outerSecurity, false);
        }

        private CarrierInfo(InetSocketAddress source, InetSocketAddress peer, String applicationProtocol, String serverName,
                            io.zmux.carrier.PipeIo.Exporter exporter, RequestHead request, boolean terminatedTls,
                            Stage stage, io.zmux.carrier.Framing framing, boolean upgrade, OuterSecurity outerSecurity,
                            boolean authenticatedEnvelope) {
            this.source = source;
            this.peer = peer;
            this.applicationProtocol = Objects.requireNonNull(applicationProtocol, "applicationProtocol");
            this.serverName = Objects.requireNonNull(serverName, "serverName");
            this.exporter = exporter;
            this.request = Objects.requireNonNull(request, "request");
            this.terminatedTls = terminatedTls;
            this.stage = Objects.requireNonNull(stage, "stage");
            this.framing = Objects.requireNonNull(framing, "framing");
            this.upgrade = upgrade;
            this.outerSecurity = Objects.requireNonNull(outerSecurity, "outerSecurity");
            this.authenticatedEnvelope = authenticatedEnvelope;
        }

        /** Source used for per-source limits, which may be reported by a trusted intermediary. */
        public InetSocketAddress source() { return source; }
        /** The address of the directly connected peer. */
        public InetSocketAddress peer() { return peer; }
        /** The negotiated ALPN, or an empty string without one. */
        public String applicationProtocol() { return applicationProtocol; }
        /** The server name sent by the client, or an empty string without one. */
        public String serverName() { return serverName; }
        /** The exporter of the completed handshake on the connection carrying this input. */
        public io.zmux.carrier.PipeIo.Exporter exporter() { return exporter; }
        /** The HTTP request, or {@link RequestHead#NONE}. */
        public RequestHead request() { return request; }
        /** Whether this process terminated the carrier's TLS or QUIC handshake. */
        public boolean terminatedTls() { return terminatedTls; }
        /** The current admission stage. */
        public Stage stage() { return stage; }
        /** The carrier framing, used to interpret the prefix at {@link Stage#CARRIER}. */
        public io.zmux.carrier.Framing framing() { return framing; }
        /** Whether the carrier belongs to an HTTP upgrade. */
        public boolean upgrade() { return upgrade; }
        /** The adapter's evidence of encryption, version and early data. */
        public OuterSecurity outerSecurity() { return outerSecurity; }

        boolean authenticatedEnvelope() { return authenticatedEnvelope; }

        private CarrierInfo verifiedExchange() {
            return new CarrierInfo(source, peer, applicationProtocol, serverName, exporter, request, terminatedTls, stage, framing,
                    upgrade, outerSecurity, true);
        }

        /** This metadata at stage, of a carrier of framing, upgrade as {@link #upgrade()}. */
        public CarrierInfo at(Stage stage, io.zmux.carrier.Framing framing, boolean upgrade) {
            return new CarrierInfo(source, peer, applicationProtocol, serverName, exporter, request, terminatedTls, stage, framing,
                    upgrade, outerSecurity);
        }

        /** This metadata with explicit evidence from the adapter's actual outer connection. */
        public CarrierInfo withOuterSecurity(OuterSecurity security) {
            return new CarrierInfo(source, peer, applicationProtocol, serverName, exporter, request, terminatedTls, stage, framing,
                    upgrade, security);
        }

        /** Whether this process terminated the carrier's completed TLS or QUIC handshake. */
        public boolean tls() { return terminatedTls; }

        /** Whether the adapter or a forwarding intermediary reported early data. */
        public boolean earlyData() {
            return outerSecurity == OuterSecurity.EARLY_DATA
                    || header("Early-Data").stream().anyMatch(value -> "1".equals(value.strip()));
        }

        /** length bytes of the exporter for label and context; null without an exporter or when it fails. */
        public byte[] exportKeyingMaterial(String label, byte[] context, int length) {
            return exporter == null ? null : exporter.export(label, context, length);
        }

        /** The values of the request header field name, compared without regard to case; empty when absent and for carriers that are not HTTP requests. */
        public java.util.List<String> header(String name) { return request.header(name); }
    }
}
