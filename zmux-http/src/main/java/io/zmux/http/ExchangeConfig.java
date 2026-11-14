package io.zmux.http;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Configures both ends of an HTTP exchange carrier: one bidirectional
 * carrier made of a sequence of bounded HTTP POST exchanges, for paths where a long-lived request
 * does not work. The PSK is the parameter of {@link #builder(byte[])}; every other field
 * has a default. The PSK and base64 must be the same on both ends; the other
 * fields are local to one end. The server's hold time must stay below the response timeout of
 * every intermediary on the path, and the client's request timeout above the hold time.
 *
 * <p>The MACs authenticate and bind responses to requests, but nothing is encrypted: when TLS is
 * terminated by a proxy or CDN, that intermediary can read the carrier.
 */
public final class ExchangeConfig {
    private final byte[] psk;
    private final boolean base64, cacheBust;
    private final Map<String, List<String>> headers;
    private final String downUrl, instance, nonceParam;
    private final int downRequests, upRequests, upPieceSize, downPieceSize, downWindow, upStoreLimit, nonceCacheSize;
    private final io.zmux.Backoff backoff;
    private final Duration requestTimeout, deadline, holdTime, coalesce, idleTimeout, goneMemory, maxClockSkew;

    private ExchangeConfig(Builder b) {
        psk = b.psk;
        base64 = b.base64;
        cacheBust = b.cacheBust;
        headers = Map.copyOf(b.headers);
        downUrl = b.downUrl;
        instance = b.instance;
        nonceParam = b.nonceParam.isEmpty() ? "n" : b.nonceParam;
        downRequests = pos(b.downRequests, 2);
        upRequests = pos(b.upRequests, 2);
        upPieceSize = pos(b.upPieceSize, defaultPieceSize(base64));
        downPieceSize = pos(b.downPieceSize, defaultPieceSize(base64));
        downWindow = pos(b.downWindow, 32);
        nonceCacheSize = pos(b.nonceCacheSize, 1 << 18);
        requestTimeout = dur(b.requestTimeout, Duration.ofSeconds(25));
        backoff = b.backoff;
        deadline = dur(b.deadline, Duration.ofSeconds(30));
        holdTime = b.holdTime == null ? Duration.ofSeconds(20) : b.holdTime;
        coalesce = b.coalesce == null ? Duration.ofMillis(2) : b.coalesce;
        idleTimeout = dur(b.idleTimeout, Duration.ofSeconds(60));
        goneMemory = b.goneMemory == null ? Duration.ofSeconds(60) : b.goneMemory;
        maxClockSkew = dur(b.maxClockSkew, Duration.ofSeconds(60));
        if (b.upStoreLimit > 0 && b.upStoreLimit < 2L * upLimit()) {
            throw new IllegalArgumentException("upStoreLimit below twice the largest UP piece accepted");
        }
        upStoreLimit = b.upStoreLimit > 0 ? b.upStoreLimit : (int) Math.max(4 << 20, 2L * upLimit());
    }

    // A field left unset (0 or null here, which no setter accepts) takes its default.
    private static int pos(int v, int d) { return v > 0 ? v : d; }

    private static Duration dur(Duration v, Duration d) { return v != null ? v : d; }

    private static int count(int n, String what) {
        if (n < 1) throw new IllegalArgumentException(what + " below 1");
        return n;
    }

    private static Duration positive(Duration d, String what) {
        if (!Objects.requireNonNull(d, what).isPositive()) throw new IllegalArgumentException(what + " must be positive");
        return d;
    }

    private static Duration time(Duration d, String what) {
        if (Objects.requireNonNull(d, what).isNegative()) throw new IllegalArgumentException("negative " + what);
        return d;
    }

    /**
     * The default UP and DOWN piece size: the largest whose bodies, with the authentication header
     * and Data MAC and after the body encoding (base64 or not), stay at most 1,000,000 bytes.
     */
    public static int defaultPieceSize(boolean base64) {
        return base64 ? Exchange.BODY_MAX / 4 * 3 - Exchange.REQ_OVERHEAD : Exchange.BODY_MAX - Exchange.REQ_OVERHEAD;
    }

    /**
     * A builder for a carrier authenticated with psk, the pre-shared key both ends hold: the MACs
     * authenticate both ends with it.
     *
     * @throws IllegalArgumentException when psk is empty
     */
    public static Builder builder(byte[] psk) { return new Builder(psk); }

    public byte[] psk() { return psk.clone(); }
    byte[] pskRef() { return psk; }
    /** Bodies are base64-encoded (standard alphabet, padded). */
    public boolean isBase64() { return base64; }
    /** A URL without {nonce} gets the nonce query parameter (client). */
    public boolean isCacheBust() { return cacheBust; }
    /** Fields added to every request (client). */
    public Map<String, List<String>> headers() { return headers; }
    /** The target template of DOWN requests (client); empty: the URL UP requests use. */
    public String downUrl() { return downUrl; }
    /** Replaces {instance} in the URL (client); empty: a random token per carrier. */
    public String instance() { return instance; }
    /** The query parameter of the per-request random value (client). */
    public String nonceParam() { return nonceParam; }
    public int downRequests() { return downRequests; }
    public int upRequests() { return upRequests; }
    public int upPieceSize() { return upPieceSize; }
    public int downPieceSize() { return downPieceSize; }
    public int downWindow() { return downWindow; }
    public int upStoreLimit() { return upStoreLimit; }
    public int nonceCacheSize() { return nonceCacheSize; }
    public Duration requestTimeout() { return requestTimeout; }
    /** The randomized, doubling delay before a request without a result is repeated (client). */
    public io.zmux.Backoff backoff() { return backoff; }
    public Duration deadline() { return deadline; }
    public Duration holdTime() { return holdTime; }
    public Duration coalesce() { return coalesce; }
    public Duration idleTimeout() { return idleTimeout; }
    public Duration goneMemory() { return goneMemory; }
    public Duration maxClockSkew() { return maxClockSkew; }

    /** The largest UP piece the server accepts: the client's piece size, at least the default limit of 1 MiB. */
    int upLimit() { return Math.max(upPieceSize, Exchange.LIMIT); }

    /** The largest DOWN piece the client accepts. */
    int downLimit() { return Math.max(downPieceSize, Exchange.LIMIT); }

    /**
     * Builds an {@link ExchangeConfig}; a value not set keeps its default. A value outside a
     * setting's range is rejected: counts are at least 1, the timeouts, the deadline and the clock
     * skew positive; zero is a value for {@link #holdTime()}, {@link #coalesce()} and {@link #goneMemory()}.
     */
    public static final class Builder {
        private final byte[] psk;
        private boolean base64, cacheBust = true;
        private Map<String, List<String>> headers = Map.of();
        private io.zmux.Backoff backoff = new io.zmux.Backoff(Duration.ofMillis(20), Duration.ofSeconds(2), 0.5);
        private String downUrl = "", instance = "", nonceParam = "";
        private int downRequests, upRequests, upPieceSize, downPieceSize, downWindow, upStoreLimit, nonceCacheSize;
        private Duration requestTimeout, deadline, holdTime, coalesce, idleTimeout, goneMemory, maxClockSkew;

        private Builder(byte[] key) {
            if (Objects.requireNonNull(key, "psk").length == 0) throw new IllegalArgumentException("an exchange carrier needs a PSK");
            psk = key.clone();
        }

        /** Sends every body base64-encoded (standard alphabet, padded). */
        public Builder base64(boolean on) { base64 = on; return this; }
        /**
         * Whether a URL without {nonce} gets the nonce query parameter (client; default true).
         * False suits a server or intermediary that refuses unknown query parameters when nothing
         * on the path caches POST responses.
         */
        public Builder cacheBust(boolean on) { cacheBust = on; return this; }
        /** Fields added to every request (client). */
        public Builder headers(Map<String, List<String>> h) { headers = Map.copyOf(h); return this; }
        /** The target template of DOWN requests (client); empty: the URL UP and CLOSE requests use. */
        public Builder downUrl(String url) { downUrl = Objects.requireNonNull(url); return this; }
        /** Replaces {instance} in the URL (client); empty: a random token per carrier. */
        public Builder instance(String s) { instance = Objects.requireNonNull(s); return this; }
        /** The query parameter of the per-request random value when the URL has no {nonce} ("n"). */
        public Builder nonceParam(String s) { nonceParam = Objects.requireNonNull(s); return this; }
        /** DOWN requests the client keeps outstanding (2); lowered while the server answers BUSY. */
        public Builder downRequests(int n) { downRequests = count(n, "downRequests"); return this; }
        /** UP requests outstanding at once (2). */
        public Builder upRequests(int n) { upRequests = count(n, "upRequests"); return this; }
        /** The largest UP piece the client sends, and the server's limit above 1 MiB. */
        public Builder upPieceSize(int n) { upPieceSize = count(n, "upPieceSize"); return this; }
        /** The largest DOWN piece the server sends, and the client's limit above 1 MiB. */
        public Builder downPieceSize(int n) { downPieceSize = count(n, "downPieceSize"); return this; }
        /** Downlink pieces the server assigns ahead of the client's acknowledgement (32). */
        public Builder downWindow(int n) { downWindow = count(n, "downWindow"); return this; }
        /** Uplink bytes the server holds per carrier (4 MiB, or twice the largest UP piece accepted if more); less than twice that piece is an error. */
        public Builder upStoreLimit(int n) { upStoreLimit = count(n, "upStoreLimit"); return this; }
        /** (Carrier ID, Nonce) pairs the server records within the time window (262144). */
        public Builder nonceCacheSize(int n) { nonceCacheSize = count(n, "nonceCacheSize"); return this; }
        /** Bounds one attempt of one request (25 s); must exceed the server's hold time. */
        public Builder requestTimeout(Duration d) { requestTimeout = positive(d, "requestTimeout"); return this; }
        /**
         * The randomized, doubling delay before a request without a result is repeated: its
         * minimum and maximum (20 ms, 2 s) and its jitter j (0.5): each wait is drawn from [d(1 - j), d], d doubling from the minimum to the maximum.
         */
        public Builder backoff(io.zmux.Backoff b) { backoff = Objects.requireNonNull(b, "backoff"); return this; }
        /** Fails the carrier when a request has had no result for this long (30 s). */
        public Builder deadline(Duration d) { deadline = positive(d, "deadline"); return this; }
        /** How long the server holds a DOWN request without data (20 s); zero: it answers at once. */
        public Builder holdTime(Duration d) { holdTime = time(d, "holdTime"); return this; }
        /** How long a piece waits for more bytes after its first byte while a request can take it (2 ms unless set; 0: no wait). */
        public Builder coalesce(Duration d) {
            coalesce = time(d, "coalesce");
            return this;
        }
        /** Fails a server carrier without requests (60 s). */
        public Builder idleTimeout(Duration d) { idleTimeout = positive(d, "idleTimeout"); return this; }
        /** How long the server remembers an ended carrier to answer its late requests with GONE (60 s); zero: not at all. */
        public Builder goneMemory(Duration d) { goneMemory = time(d, "goneMemory"); return this; }
        /** The largest difference between a request's Time and the server's clock accepted (60 s). */
        public Builder maxClockSkew(Duration d) { maxClockSkew = positive(d, "maxClockSkew"); return this; }

        public ExchangeConfig build() { return new ExchangeConfig(this); }
    }
}
