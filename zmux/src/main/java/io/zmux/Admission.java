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

    private Admission(Builder b) {
        peek = b.peek; maxPeek = b.maxPeek; maxMessage = b.maxMessage == 0 ? MAX_MESSAGE : b.maxMessage; peekTimeout = b.peekTimeout; fallbackOnPeekTimeout = b.fallbackOnPeekTimeout;
        stallMin = b.stallMin; stallMax = b.stallMax; decider = b.decider;
    }

    /**
     * An admission that reads peek bytes (0: none), at most {@link #MAX_PEEK}, of a byte stream before it asks decider;
     * fewer when the carrier ends or the peek timeout passes first; a carrier without a receiving half, or one decided
     * on its HTTP request head, is decided at once. On a message-framed carrier any peek above 0, and every decision
     * deferred from an upgrade's head, reads the first message whole as the prefix, of at most {@link #maxMessage()}
     * bytes: a longer one is closed without a decision ({@link AdmissionStats#tooLarge()}), and so is a carrier that
     * ends, fails or sends a message that is not binary first ({@link AdmissionStats#closed()}); skip is then 0
     * (replayed) or its length.
     */
    public static Builder builder(int peek, Decider decider) {
        Objects.requireNonNull(decider, "decider");
        if (peek < 0 || peek > MAX_PEEK) throw new IllegalArgumentException("peek outside [0, " + MAX_PEEK + "]");
        return new Builder(peek, decider);
    }

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
        private int maxPeek, maxMessage;
        private Duration peekTimeout = Duration.ZERO, stallMin = Duration.ZERO, stallMax = Duration.ZERO;
        private boolean fallbackOnPeekTimeout;

        private Builder(int peek, Decider decider) { this.peek = peek; this.decider = decider; }

        /**
         * Bounds {@link Decision#need()} on a byte stream, at most {@link #MAX_PEEK}; not called: peek, so that MORE
         * is invalid. It does not bound a first message ({@link #maxMessage(int)}).
         */
        public Builder maxPeek(int n) {
            if (n < 0 || n > MAX_PEEK) throw new IllegalArgumentException("maxPeek outside [0, " + MAX_PEEK + "]");
            maxPeek = n;
            return this;
        }

        /**
         * Bounds the first message of a message-framed carrier (WebSocket), the prefix of its decision, at most
         * {@link #MAX_MESSAGE}; 0 or not called: {@link #MAX_MESSAGE}. A longer first message closes the carrier
         * without a decision, counted in {@link AdmissionStats#tooLarge()}.
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
         * instead of being closed (without a fallback target it is stalled), counted in both the peek-timeout and
         * fallback counters; a message-framed carrier and an HTTP request carrier are closed as before, and so is a
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
     * and the carrier closed; so is it when the listener stops taking carriers. A decider
     * that throws, a stage that fails, a null decision and a skip outside the prefix close
     * the carrier.
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
    public record Deadline(long nanoTime) {
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
        /** A fallback carrying the host's value ({@link Takeover#value()}; null: none). */
        public static Decision fallback(Object value) { return new Decision(AdmissionVerdict.FALLBACK, 0, value); }
        /** Asks for a prefix of need bytes. */
        public static Decision more(int need) { return new Decision(AdmissionVerdict.MORE, 0, null, need, ""); }
        /** This decision labelled v. */
        public Decision withVariant(String v) { return new Decision(verdict, skip, value, need, v); }
    }

    /** The counts of one {@link Decision#variant()} ({@link ZmuxListener#variantStats()}). */
    public record VariantStat(String name, boolean overflow, long accepted, long fallback, long stalled, long closed) {}

    /** How many labels are counted apart; further ones share one bucket, listed last. */
    public static final int MAX_VARIANTS = 64;

    /**
     * The host's target of the carriers the admission hands back ({@link AdmissionVerdict#FALLBACK}, a prefix
     * {@link Builder#fallbackOnPeekTimeout} gave up on): {@link ListenerConfig#fallback()} takes every such carrier,
     * byte streams of the listener's sockets and of other entries, message-framed carriers (WebSocket after a
     * deferred decision), QUIC streams and HTTP request carriers decided on the start of their body alike. serve runs
     * off the event loops, once per carrier, and returns at once with a stage: the carrier is the host's until that
     * stage completes (null: until serve returns), then zmux closes it unless the host called {@link Takeover#detach()}.
     * A serve that throws, and a stage that fails, close the carrier at once unless it was detached; neither reaches
     * the listener. That scope is the same at every entry point; nothing else cancels the stage. What an HTTP request
     * is depends on the stage it is decided at: on its head ({@link Stage#HEAD}: an upgrade's head, an exchange
     * request, a request without a body) it is a request, answered by its HTTP adapter (see {@link
     * io.zmux.adapter.ListenerAdapter#handleRequest}); on the start of its body (a streaming or uplink request
     * carrier) it is a carrier, taken over here as the full-duplex byte stream of its request body (in) and response
     * body (out), the response head written by the adapter (its adapter answers it as a request only when the
     * listener has no fallback).
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

    /**
     * The metadata of a carrier, immutable, as far as its adapter has it: an admission decides with it (each call
     * gets its own), a {@link Takeover} carries it, and a client's {@link CarrierCredential} is given it. Fields
     * that do not apply are empty.
     *
     * @param source the carrier's source, the one authority for every per-source limit: the
     *     client address a trusted intermediary reports (port 0 when it reports none), or what
     *     the host resolved and gave the listener, else the peer; for a client, the server's
     *     address; null when unknown
     * @param peer the address the carrier arrived from directly (the TCP or QUIC peer, an HTTP
     *     request's remote address); null when unknown
     * @param applicationProtocol the application protocol the TLS or QUIC handshake negotiated; empty without one
     * @param serverName the server name (SNI) the client sent in its TLS handshake; empty when it sent none
     * @param exporter the exporter of the carrier's completed TLS or QUIC handshake (RFC 8446
     *     Section 7.5), for a carrier decided on its HTTP request head that of the connection
     *     carrying the request; null without one
     * @param request the HTTP request of the carrier; {@link RequestHead#NONE} for other carriers
     * @param terminatedTls whether this process terminated the carrier's completed TLS or QUIC
     *     handshake; no proof of an end-to-end handshake: an intermediary may have opened it
     * @param stage what the decision is taken on: the carrier's start or an HTTP request head
     * @param framing the carrier's framing, as {@link FirstBytes#classify} takes it: at {@link Stage#CARRIER} the
     *     prefix is the start of a byte stream or, with {@link io.zmux.carrier.Framing#MESSAGES}, a first message
     * @param upgrade the carrier of an HTTP upgrade (WebSocket), on its head and on the decision a
     *     {@link AdmissionVerdict#DEFER} on that head takes on the carrier
     */
    public record CarrierInfo(InetSocketAddress source, InetSocketAddress peer, String applicationProtocol, String serverName,
                              io.zmux.carrier.PipeIo.Exporter exporter, RequestHead request, boolean terminatedTls,
                              Stage stage, io.zmux.carrier.Framing framing, boolean upgrade) {
        /**
         * A byte stream decided on its start, the carrier of a completed handshake exactly when it has an exporter;
         * {@link #at} sets the stage.
         */
        public CarrierInfo(InetSocketAddress source, InetSocketAddress peer, String applicationProtocol, String serverName,
                           io.zmux.carrier.PipeIo.Exporter exporter, RequestHead request) {
            this(source, peer, applicationProtocol, serverName, exporter, request, exporter != null,
                    Stage.CARRIER, io.zmux.carrier.Framing.BYTE_STREAM, false);
        }

        public CarrierInfo {
            Objects.requireNonNull(applicationProtocol, "applicationProtocol");
            Objects.requireNonNull(serverName, "serverName");
            Objects.requireNonNull(request, "request");
            Objects.requireNonNull(stage, "stage");
            Objects.requireNonNull(framing, "framing");
        }

        /** This metadata at stage, of a carrier of framing, upgrade as {@link #upgrade()}. */
        public CarrierInfo at(Stage stage, io.zmux.carrier.Framing framing, boolean upgrade) {
            return new CarrierInfo(source, peer, applicationProtocol, serverName, exporter, request, terminatedTls, stage, framing, upgrade);
        }

        /** Whether this process terminated the carrier's completed TLS or QUIC handshake. */
        public boolean tls() { return terminatedTls; }

        /** length bytes of the exporter for label and context; null without an exporter or when it fails. */
        public byte[] exportKeyingMaterial(String label, byte[] context, int length) {
            return exporter == null ? null : exporter.export(label, context, length);
        }

        /** The values of the request header field name, compared without regard to case; empty when absent and for carriers that are not HTTP requests. */
        public java.util.List<String> header(String name) { return request.header(name); }
    }
}
