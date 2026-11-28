package io.zmux.http;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.ReferenceCountUtil;
import io.zmux.Candidate;
import io.zmux.CarrierClientClosedException;
import io.zmux.CarrierKind;
import io.zmux.CarrierCredential;
import io.zmux.HttpStatusException;
import io.zmux.HttpTarget;
import io.zmux.JoinHint;
import io.zmux.PipeOptions;
import io.zmux.ZmuxFuture;
import io.zmux.carrier.PipeIo;
import io.zmux.http.internal.HttpTransport;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.TrustManagerFactory;

/**
 * Opens HTTP carriers on Netty: streaming requests ({@link #pipe}, {@link #splitPipe}), the
 * exchange carrier ({@link #exchangePipe}) and WebSockets ({@link #webSocketPipe}), each also as
 * a {@link Candidate} for {@link io.zmux.SessionController}.
 * HTTPS uses HTTP/2 when the server's ALPN offers it and HTTP/1.1 otherwise; cleartext HTTP uses
 * HTTP/1.1, or HTTP/2 by prior knowledge ({@link Builder#http2()}). Streaming carriers over
 * HTTP/1.1 get a connection of their own, which closes with them. HTTP/2 and explicit
 * HTTP/3 (with the HTTP/3 adapter) open streams on shared connections. Safe for concurrent use; {@link #close} closes every connection
 * it opened.
 */
public final class HttpCarrierClient implements AutoCloseable {
    private static final SecureRandom RANDOM = new SecureRandom();

    static {
        HttpTransport.installClientAccess(new HttpTransport.ClientAccess() {
            @Override public Builder select(Builder builder, HttpTransport.Client transport) {
                return builder.requestTransport(transport);
            }

            @Override public CompletableFuture<Channel> open(HttpCarrierClient client, URI target) {
                return client.conns.open(target, false, false);
            }

            @Override public HttpTransport.Client transport(HttpCarrierClient client) { return client.conns.h3; }

            @Override public int openConnections(HttpCarrierClient client) { return client.conns.open(); }
        });
    }
    private final EventLoopGroup group;
    private final boolean ownGroup;
    final Connections conns;
    final Duration timeout;
    private final String pairField;
    /** The exchange carriers not yet ended. */
    final java.util.Set<ExchangeClient> exchanges = java.util.concurrent.ConcurrentHashMap.newKeySet();
    volatile boolean closed;
    /** The close once started ({@link #closeAsync}); null: open. */
    private final java.util.concurrent.atomic.AtomicReference<CompletableFuture<Void>> closing = new java.util.concurrent.atomic.AtomicReference<>();

    private HttpCarrierClient(Builder b) throws IOException {
        pairField = JoinHint.pairField(b.pairField);
        SslContextBuilder tls = b.tls == null ? SslContextBuilder.forClient().trustManager(defaultTrust()) : b.tls;
        SslContext both = alpn(tls, ApplicationProtocolNames.HTTP_2, ApplicationProtocolNames.HTTP_1_1);
        SslContext h1 = alpn(tls, ApplicationProtocolNames.HTTP_1_1);
        ownGroup = b.group == null;
        group = ownGroup ? new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory()) : b.group;
        timeout = b.timeout;
        conns = new Connections(group, both, h1, b.version, b.timeout, b.requestTransport);
    }

    private static TrustManagerFactory defaultTrust() {
        try {
            var tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init((java.security.KeyStore) null);
            return tmf;
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException("the default TLS trust store is unusable", e);
        }
    }

    private static SslContext alpn(SslContextBuilder b, String... protocols) throws javax.net.ssl.SSLException {
        return b.applicationProtocolConfig(new ApplicationProtocolConfig(ApplicationProtocolConfig.Protocol.ALPN,
                ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT, protocols)).build();
    }

    public static Builder builder() { return new Builder(); }

    // ---- streaming carriers ----

    /**
     * Opens a carrier over one HTTP request to url: {@link PipeOptions.Direction#BIDIRECTIONAL}
     * sends the request body up and reads the response body (HTTP/2, or HTTP/1.1 where every hop
     * forwards both bodies at once); {@code SEND_ONLY} is a POST whose body is the uplink;
     * {@code RECEIVE_ONLY} is a GET whose response body is the downlink. headers are added to the
     * request. The pipe input
     * has the halves of dir only. A response status other than 2xx fails the pipe with an
     * {@link HttpStatusException}.
     *
     * <p>A redirect is followed as {@link Redirects#check} allows, up to
     * {@link Redirects#DEFAULT_MAX} hops; a refused one fails the pipe with an
     * {@link io.zmux.HttpRedirectException}. The request is repeated whole at the new URL, with a
     * fresh credential and every uplink byte written so far: until the first response that is
     * not a redirect, the uplink is kept, up to {@link Redirects#MAX_REPLAY} bytes, past which a
     * later redirect is refused, so that no byte is lost or sent twice.
     *
     * <p>credential, when not null, is presented for the server's admission: its prefix starts a
     * request body; a GET, which has none, carries its header form. The connection is not known
     * when the request is built, so a credential bound to TLS is an error.
     */
    public PipeIo pipe(URI url, Map<String, List<String>> headers, PipeOptions.Direction dir, CarrierCredential credential) throws IOException {
        return StreamingRequest.open(this, url, headers, Objects.requireNonNull(dir), credential, null);
    }

    /**
     * Opens a split bootstrap over two requests, a POST uplink and a GET downlink, paired by a
     * random pairing token in {@link JoinHint#PAIR_HEADER} (or {@link Builder#pairField}), each presenting
     * credential as {@link #pipe} does. The carriers must be confidential end to end, or up to a
     * trusted TLS-terminating intermediary.
     */
    public PipeIo splitPipe(URI url, Map<String, List<String>> headers, CarrierCredential credential) throws IOException {
        return splitPipe(url, headers, credential, null);
    }

    private PipeIo splitPipe(URI url, Map<String, List<String>> headers, CarrierCredential credential, HttpTarget target) throws IOException {
        byte[] token = new byte[16];
        RANDOM.nextBytes(token);
        Map<String, List<String>> h = new HashMap<>(headers);
        h.put(pairField, List.of(Base64.getUrlEncoder().withoutPadding().encodeToString(token)));
        PipeIo up = StreamingRequest.open(this, url, h, PipeOptions.Direction.SEND_ONLY, credential, target);
        PipeIo down;
        try {
            down = StreamingRequest.open(this, url, h, PipeOptions.Direction.RECEIVE_ONLY, credential, target);
        } catch (IOException | RuntimeException e) {
            up.writer().abort(e);
            throw e;
        }
        return PipeIo.split(up, down);
    }

    /**
     * A bidirectional streaming request ({@link #pipe}) to url as a candidate; its carriers start
     * at the URL redirects last led it to ({@link Candidate#target()}).
     */
    public Candidate streamingCandidate(URI url, Map<String, List<String>> headers) {
        HttpTarget target = new HttpTarget(url.toString());
        return Candidate.of(conns.http3() ? CarrierKind.HTTP3 : CarrierKind.STREAMING,
                (cred, t) -> StreamingRequest.open(this, url, headers, PipeOptions.Direction.BIDIRECTIONAL, cred, target)).withTarget(target);
    }

    /** Opens a paired POST and GET for either Bootstrap or Join. */
    public io.zmux.CarrierOpener splitOpener(URI url, Map<String, List<String>> headers) {
        HttpTarget target = new HttpTarget(url.toString());
        return (setup, request) -> {
            var opened = new java.util.ArrayList<PipeIo>();
            try {
                for (int i = 0; i < request.layout().size(); i++) {
                    if (setup.isCancelled()) throw new java.io.InterruptedIOException("carrier setup cancelled");
                    opened.add(splitPipe(url, headers, request.credential(), target));
                }
                return new io.zmux.CarrierOpener.Opened(opened);
            } catch (IOException | RuntimeException failure) {
                return new io.zmux.CarrierOpener.Opened(opened, failure);
            }
        };
    }

    /** A split carrier with a shared redirect target. */
    public Candidate splitCandidate(URI url, Map<String, List<String>> headers) {
        HttpTarget target = new HttpTarget(url.toString());
        return Candidate.of(CarrierKind.SPLIT, (cred, t) -> splitPipe(url, headers, cred, target)).withTarget(target);
    }

    // ---- WebSocket carriers ----

    /**
     * Opens a WebSocket to url (ws, wss, or http and https for the same) over a connection of its
     * own (HTTP/1.1, the form intermediaries forward) and returns it as a pipe with message
     * framing: each binary message is one record's Frames field. protocols are the subprotocols
     * offered, by preference (empty: {@link io.zmux.ZmuxConfig#DEFAULT_ALPN}), followed by their
     * draft-01 aliases; {@link HttpEndpoint.Builder#NO_SUBPROTOCOL} alone offers none (with other entries it is an
     * {@link IllegalArgumentException}); the selected one is the pipe's application protocol. headers are added to
     * the upgrade request.
     * credential, when not null, is presented in its header form, bound to the TLS connection of
     * a wss URL. A refused upgrade fails with an {@link HttpStatusException}. A redirect of the
     * upgrade is followed as {@link Redirects#check} allows, up to {@link Redirects#DEFAULT_MAX}
     * hops, each on a new connection with the credential presented anew; a refused one fails
     * with an {@link io.zmux.HttpRedirectException}.
     */
    public PipeIo webSocketPipe(URI url, Map<String, List<String>> headers, List<String> protocols, CarrierCredential credential) throws IOException {
        return WebSocketClient.open(this, url, headers, protocols, credential).io();
    }

    /**
     * A WebSocket ({@link #webSocketPipe}) to url as a candidate, presenting the session's
     * credential; its carriers start at the URL redirects last led it to ({@link Candidate#target()}).
     */
    public Candidate webSocketCandidate(URI url, Map<String, List<String>> headers, List<String> protocols) {
        List<String> p = List.copyOf(protocols);
        HttpTarget target = new HttpTarget(url.toString());
        return Candidate.of(CarrierKind.WEBSOCKET, (cred, t) -> {
            var o = WebSocketClient.open(this, URI.create(target.url()), headers, p, cred);
            target.redirected(o.url().toString());
            return o.io();
        }).withTarget(target);
    }

    // ---- the exchange carrier ----

    /**
     * Opens an exchange carrier to url, a template that may hold {instance} and {nonce} anywhere
     * in its path or query (see {@link ExchangeConfig}), and returns it as a bidirectional pipe.
     * Requests go out on pooled connections of this client: HTTP/1.1 connections kept for reuse,
     * or streams of a shared HTTP/2 or HTTP/3 connection. The carrier keeps DOWN requests outstanding from the
     * start, and ends when zmux closes both halves or the carrier fails. It authenticates with
     * the config's PSK; no credential is presented.
     *
     * <p>Every request follows a 307 or 308 that {@link Redirects#check} allows, up to
     * {@link Redirects#DEFAULT_MAX} hops, with its body unchanged; a 301, 302 or 303, or another
     * refused redirect, fails the carrier with an {@link io.zmux.HttpRedirectException}. A pure
     * scheme upgrade is kept for the later requests; another redirect is followed per request.
     *
     * @throws IllegalArgumentException when url (or the config's DOWN URL) is not an http or
     *     https URL once its placeholders are replaced, or uses http in explicit HTTP/3 mode
     * @throws CarrierClientClosedException when the client is closed, as every dial of a closed client fails
     */
    public PipeIo exchangePipe(String url, ExchangeConfig config) throws CarrierClientClosedException {
        if (closed) throw closedFailure();
        return new ExchangeClient(this, Objects.requireNonNull(url), Objects.requireNonNull(config)).pipe();
    }

    /** An exchange carrier ({@link #exchangePipe}) to url as a candidate; the session's credential is not presented on it. */
    public Candidate exchangeCandidate(String url, ExchangeConfig config) {
        var target = new ExchangeClient.Target(url, config.nonceParam(), "", false);
        if (conns.http3()) conns.requireRequestScheme(target.build(new byte[Exchange.NONCE]));
        return Candidate.of(CarrierKind.EXCHANGE, (cred, t) -> exchangePipe(url, config));
    }

    static String target(URI u) {
        String p = u.getRawPath() == null || u.getRawPath().isEmpty() ? "/" : u.getRawPath();
        return u.getRawQuery() == null ? p : p + "?" + u.getRawQuery();
    }

    static String authority(URI u) {
        return u.getPort() > 0 ? u.getHost() + ":" + u.getPort() : u.getHost();
    }

    <T> T await(CompletableFuture<T> f, String what) throws IOException {
        if (!f.isDone() && conns.onBorrowedLoop()) {
            abandon(f);
            throw new IllegalStateException("blocking HTTP request on the connection loop");
        }
        try {
            return f.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            abandon(f);
            throw new InterruptedIOException("interrupted");
        } catch (TimeoutException e) {
            abandon(f);
            throw new SocketTimeoutException(what + " timed out");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException r) throw r;
            throw e.getCause() instanceof io.zmux.ZmuxException z ? z : new io.zmux.CarrierFailedException(what + " failed", e.getCause());
        }
    }

    private static void abandon(CompletableFuture<?> opening) {
        // H3 owns and can cancel an individual request, including its pending stream-credit wait.
        if (opening instanceof Connections.RequestOpening && opening.cancel(false)) return;
        opening.thenAccept(value -> { if (value instanceof Channel channel) HttpTransport.abort(channel); });
    }

    /** What a streaming request does with the head of its response; called on the channel's loop. */
    interface ResponseHook {
        /** A redirect: true when the request handled it (followed it, or failed the carrier with its reason). */
        boolean redirect(int status, String location);

        /** A response that is not a redirect; ok: 2xx. */
        void response(boolean ok);
    }

    /** The response of a streaming carrier: its body is the downlink; a status other than 2xx fails the carrier. */
    static final class ResponseReader extends ChannelInboundHandlerAdapter {
        private final BodyCarrier c;
        private final boolean reads;
        private final ResponseHook hook;
        private boolean ok, redirected;

        ResponseReader(BodyCarrier c, boolean reads, ResponseHook hook) { this.c = c; this.reads = reads; this.hook = hook; }

        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
            try {
                if (redirected) return; // the rest of a redirect's response
                if (msg instanceof HttpResponse r) {
                    int s = r.status().code();
                    if (Redirects.isRedirect(s) && hook.redirect(s, r.headers().get(HttpHeaderNames.LOCATION))) {
                        redirected = true;
                        return;
                    }
                    ok = s / 100 == 2;
                    hook.response(ok);
                    if (!ok) {
                        c.lost(new HttpStatusException(s, r.status().reasonPhrase()));
                        ctx.close();
                    }
                }
                if (msg instanceof HttpContent h && ok) {
                    if (reads) c.in(h.content().retain());
                    if (h instanceof LastHttpContent) {
                        if (reads) c.inEnd();
                        else c.lost(new ClosedChannelException()); // an uplink's response ends the carrier
                    }
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            if (!redirected) c.lost(new ClosedChannelException());
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (!redirected) c.lost(cause);
            ctx.close();
        }
    }

    EventLoopGroup group() { return group; }

    /**
     * Counts x as running, for {@link #close} to fail; false once the client is closed. The
     * set is updated before closed is read, and close sets closed before it reads the set, so
     * one of the two always sees the other.
     */
    boolean register(ExchangeClient x) {
        exchanges.add(x);
        if (!closed) return true;
        exchanges.remove(x);
        return false;
    }

    /**
     * Closes the client without waiting: fails the exchange carriers still running (their readers
     * and writers see the client's close as the cause), then closes every connection of the client
     * (those of carriers in use and those still connecting too), then shuts down the client's own
     * group. The future completes once that is done; the carriers' failing step is bounded by the
     * client's timeout. Calls after the first return the same future. Cancelling the future
     * changes nothing of the close.
     *
     * <p>With a group of the caller's ({@link Builder#group}), close the client first, then shut
     * the group down. A group shut down first leaves the client usable only for close: zmux's
     * calls on its carriers then fail at once, and the group's shutdown closes the connections.
     */
    public ZmuxFuture<Void> closeAsync() {
        CompletableFuture<Void> done = new CompletableFuture<>();
        CompletableFuture<Void> running = closing.compareAndExchange(null, done);
        if (running != null) return io.zmux.internal.spi.Spi.future(running, null);
        closed = true;
        Throwable cause = new io.zmux.CarrierFailedException("HTTP carrier client closed"); // the running carriers' failure
        List<CompletableFuture<Void>> failing = new java.util.ArrayList<>();
        for (ExchangeClient x : exchanges) failing.add(x.shutdown(cause));
        // Bounded: what has not failed by then fails on its loop.
        CompletableFuture.allOf(failing.toArray(CompletableFuture[]::new)).orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS)
                .whenComplete((v, e) -> conns.close().addListener(f -> {
                    if (!ownGroup) done.complete(null);
                    else group.shutdownGracefully(0, 1, TimeUnit.SECONDS).addListener(t -> done.complete(null));
                }));
        return io.zmux.internal.spi.Spi.future(done, null);
    }

    /**
     * {@link #closeAsync()}, then waits for it, bounded by the client's timeout. Called on one of
     * the group's event loops it does not wait: the close then completes after it returns.
     */
    @Override public void close() {
        var f = closeAsync();
        if (onGroup()) return;
        try {
            f.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException e) {
            // Bounded: the rest completes on the group.
        }
    }

    /** The failure of every dial once the client is closed. */
    static CarrierClientClosedException closedFailure() {
        return new CarrierClientClosedException("HTTP carrier client closed");
    }

    /** Whether the calling thread is one of the group's event loops, which must not wait for the group. */
    private boolean onGroup() {
        if (conns.onBorrowedLoop()) return true;
        for (io.netty.util.concurrent.EventExecutor e : group) if (e.inEventLoop()) return true;
        return false;
    }

    /** Configures an {@link HttpCarrierClient}. */
    public static final class Builder {
        private EventLoopGroup group;
        private SslContextBuilder tls;
        private HttpTransport.Client requestTransport;
        private Connections.Version version = Connections.Version.AUTO;
        private Duration timeout = Duration.ofSeconds(10);
        private String pairField = "";

        private Builder() {}

        /** The event loops of the client's connections (null: one of its own). */
        public Builder group(EventLoopGroup g) { group = g; return this; }

        /**
         * The TLS of https and wss URLs, as a client SslContextBuilder (null: the system's trust);
         * the client sets its ALPN protocols (h2, http/1.1).
         */
        public Builder tls(SslContextBuilder b) { tls = b; return this; }

        /**
         * The request field that pairs the two requests of a split bootstrap (empty: {@link JoinHint#PAIR_HEADER}),
         * another name only where an intermediary strips unknown fields; the server's
         * {@link HttpEndpoint.Builder#pairField(String)} must be the same.
         */
        public Builder pairField(String field) { pairField = Objects.requireNonNull(field); return this; }

        /** HTTP/1.1 only. */
        public Builder http1() { version = Connections.Version.HTTP_1_1; return this; }

        /** HTTP/2 only: over TLS by ALPN, over cleartext by prior knowledge. */
        public Builder http2() { version = Connections.Version.HTTP_2; return this; }

        /** Installs the request transport of the HTTP/3 adapter. */
        Builder requestTransport(HttpTransport.Client transport) {
            requestTransport = Objects.requireNonNull(transport);
            version = Connections.Version.HTTP_3;
            return this;
        }

        /** Bounds connecting and each opening (10 s). */
        public Builder timeout(Duration d) { timeout = Objects.requireNonNull(d); return this; }

        public HttpCarrierClient build() throws IOException { return new HttpCarrierClient(this); }
    }
}
