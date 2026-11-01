package io.zmux;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * The decision on every new carrier of a {@link ZmuxListener}, configured once on its
 * {@link ListenerConfig#admission()} and run by every entry of that listener: its socket and
 * TLS carriers, those given to {@link ZmuxListener#handle} (the HTTP, WebSocket and exchange
 * carriers of an HTTP endpoint, the connections of a QUIC server), joins and resumptions
 * included. A carrier first takes a slot of its source ({@link
 * ListenerConfig#maxPendingPerSource()}), then completes its outer handshake (TLS), then the
 * listener reads up to {@code peek} bytes, never more, and asks the decider; nothing is sent
 * before the decision and nothing is consumed except what an accept skips.
 *
 * @param peek bytes read before the decision (0: none), at most {@link #MAX_PEEK}; fewer when
 *     the carrier ends or the peek timeout passes first; a carrier without a receiving half,
 *     or one decided on its HTTP request head, is decided at once
 * @param peekTimeout runs from the end of the outer handshake and bounds the reading of the
 *     prefix and the decision together; zero: drawn once per admission from a secure random
 *     source between 5 s and 30 s
 * @param stallMin lower bound of a {@link Verdict#STALL}'s duration
 * @param stallMax upper bound of a {@link Verdict#STALL}'s duration; when both bounds are
 *     zero (a stall of no duration is {@link Verdict#CLOSE}), stallMax is drawn once between
 *     30 s and 120 s and stallMin between a quarter and three quarters of it
 * @param decider the decision function
 */
public record Admission(int peek, Duration peekTimeout, Duration stallMin, Duration stallMax, Decider decider) {
    /** The longest prefix: it is read into the carrier's read buffer, which holds a first record of at most this size. */
    public static final int MAX_PEEK = 65535;

    public Admission {
        Objects.requireNonNull(decider, "decider");
        if (peek < 0 || peek > MAX_PEEK) throw new IllegalArgumentException("peek outside [0, " + MAX_PEEK + "]");
        if (Objects.requireNonNull(peekTimeout).isNegative() || Objects.requireNonNull(stallMin).isNegative()
                || Objects.requireNonNull(stallMax).isNegative()) throw new IllegalArgumentException("negative duration");
        if (stallMin.compareTo(stallMax) > 0) throw new IllegalArgumentException("stallMin above stallMax");
        if (stallMax.isZero() && !stallMin.isZero()) throw new IllegalArgumentException("stallMin without stallMax");
    }

    /** An admission with drawn timeouts and stall bounds. */
    public static Admission of(int peek, Decider decider) {
        return new Admission(peek, Duration.ZERO, Duration.ZERO, Duration.ZERO, decider);
    }

    public Admission withPeekTimeout(Duration d) { return new Admission(peek, d, stallMin, stallMax, decider); }
    public Admission withStall(Duration min, Duration max) { return new Admission(peek, peekTimeout, min, max, decider); }

    /**
     * Decides on a carrier, once per carrier and for different carriers concurrently, outside
     * every lock of zmux, on the listener's event loop: it must not block, and returns a stage
     * that completes later when the decision takes time. prefix is read-only and valid until
     * the stage completes. The decision is due by deadline (what is left of the peek
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

    /** The outcome of an admission. */
    public enum Verdict {
        /** The carrier goes to the listener without the first {@link Decision#skip()} prefix bytes. */
        ACCEPT,
        /**
         * The carrier goes, with every byte read replayed, to the fallback target of the entry
         * that took it ({@link ListenerConfig#fallback()} for the listener's sockets, the target
         * given to {@link ZmuxListener#handle}: an HTTP endpoint's host handler, a QUIC
         * server's fallback); without one it is {@link #STALL}.
         */
        FALLBACK,
        /**
         * Reads and discards what arrives, sends nothing, and closes the carrier gracefully when
         * the peer ends it or after a duration that depends on its source alone. A stalled carrier
         * keeps its slot of {@link ListenerConfig#maxPendingPerSource()} until closed; a source
         * has at most one fewer stalled carriers than that, a further stall is closed at once.
         */
        STALL,
        /** Closes the carrier at once. */
        CLOSE
    }

    /**
     * A verdict; skip and value apply to {@link Verdict#ACCEPT} only. A skip outside the prefix
     * is {@link Verdict#CLOSE}.
     *
     * @param value the host's value for an accepted carrier, or null: zmux hands it on without
     *     looking at it to the {@link InboundTransform} ({@link InboundTransform.Context#admitted()})
     *     and to the session ({@link ZmuxSession#admitted()}) or pipe ({@link ZmuxPipe#admitted()})
     *     the carrier makes, and holds it until their state is released. Whether a join comes
     *     from the session's client is the host's to decide: zmux compares no values
     */
    public record Decision(Verdict verdict, int skip, Object value) {
        public static final Decision FALLBACK = new Decision(Verdict.FALLBACK, 0, null);
        public static final Decision STALL = new Decision(Verdict.STALL, 0, null);
        public static final Decision CLOSE = new Decision(Verdict.CLOSE, 0, null);
        public Decision { Objects.requireNonNull(verdict); }
        /** A verdict without a value. */
        public Decision(Verdict verdict, int skip) { this(verdict, skip, null); }
        public static Decision accept(int skip) { return new Decision(Verdict.ACCEPT, skip, null); }
        /** An accept carrying the host's value (null: none). */
        public static Decision accept(int skip, Object value) { return new Decision(Verdict.ACCEPT, skip, value); }
    }

    /**
     * The target of a carrier that was not accepted. serve runs off the event loop, once per
     * carrier. carrier is a byte-stream pipe input with every byte read before the decision
     * in front of its input; the target uses it as zmux uses a pipe input: it starts the
     * reader once, has at most one write in flight, and closes (or aborts) each half once,
     * which closes the carrier. Its sink and write completions are called on the listener's
     * event loop and must not block. The application protocol of a TLS carrier is kept.
     */
    @FunctionalInterface
    public interface Fallback {
        void serve(io.zmux.carrier.PipeIo carrier, CarrierInfo info);
    }

    /**
     * The metadata of a carrier, read-only, as far as its adapter has it: an admission decides with it, and a client's {@link Credential} is given it.
     * Fields that do not apply are empty.
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
     */
    public record CarrierInfo(InetSocketAddress source, InetSocketAddress peer, String applicationProtocol, String serverName,
                              io.zmux.carrier.PipeIo.Exporter exporter, RequestHead request) {
        public CarrierInfo {
            Objects.requireNonNull(applicationProtocol, "applicationProtocol");
            Objects.requireNonNull(serverName, "serverName");
            Objects.requireNonNull(request, "request");
        }

        /** Whether the carrier has a TLS (or QUIC) exporter; a carrier given to the listener has one when its pipe input offers it. */
        public boolean tls() { return exporter != null; }

        /** length bytes of the exporter for label and context; null without an exporter or when it fails. */
        public byte[] exportKeyingMaterial(String label, byte[] context, int length) {
            return exporter == null ? null : exporter.export(label, context, length);
        }

        /**
         * Whether the carrier is decided on its HTTP request head, without a prefix: an HTTP upgrade (WebSocket, WebTransport) or an HTTP request carrier whose
         * request has no body. A credential travels in the request header field {@link PrefixAuth#HEADER_FIELD}.
         */
        public boolean head() { return request.head(); }

        /** The values of the request header field name, compared without regard to case; empty when absent and for carriers that are not HTTP requests. */
        public java.util.List<String> header(String name) { return request.header(name); }
    }
}
