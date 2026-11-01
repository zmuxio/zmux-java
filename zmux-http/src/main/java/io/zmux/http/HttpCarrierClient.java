package io.zmux.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.ReferenceCountUtil;
import io.zmux.Admission;
import io.zmux.Candidate;
import io.zmux.CarrierKind;
import io.zmux.Credential;
import io.zmux.HttpStatusException;
import io.zmux.JoinHint;
import io.zmux.PipeOptions;
import io.zmux.RequestHead;
import io.zmux.ZmuxPipe;
import io.zmux.ZmuxSession;
import io.zmux.carrier.Framing;
import io.zmux.carrier.PipeIo;
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
 * a {@link Candidate} for {@link io.zmux.Zmux#dial(io.zmux.CarrierMode, io.zmux.ZmuxConfig, io.zmux.ModePolicy)}.
 * HTTPS uses HTTP/2 when the server's ALPN offers it and HTTP/1.1 otherwise; cleartext HTTP uses
 * HTTP/1.1, or HTTP/2 by prior knowledge ({@link Builder#http2()}). Streaming carriers over
 * HTTP/1.1 get a connection of their own, which closes with them; over HTTP/2 they are streams
 * of one connection per server. Safe for concurrent use; close it to close its connections.
 */
public final class HttpCarrierClient implements AutoCloseable {
    private static final SecureRandom RANDOM = new SecureRandom();
    private final EventLoopGroup group;
    private final boolean ownGroup;
    final Connections conns;
    final Duration timeout;
    /** The exchange carriers not yet ended. */
    final java.util.Set<ExchangeClient> exchanges = java.util.concurrent.ConcurrentHashMap.newKeySet();
    volatile boolean closed;

    private HttpCarrierClient(Builder b) throws IOException {
        ownGroup = b.group == null;
        group = ownGroup ? new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory()) : b.group;
        SslContextBuilder tls = b.tls;
        SslContext both, h1;
        try {
            if (tls == null) {
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init((java.security.KeyStore) null);
                tls = SslContextBuilder.forClient().trustManager(tmf);
            }
            both = alpn(tls, ApplicationProtocolNames.HTTP_2, ApplicationProtocolNames.HTTP_1_1);
            h1 = alpn(tls, ApplicationProtocolNames.HTTP_1_1);
        } catch (java.security.GeneralSecurityException e) {
            throw new IOException(e);
        }
        timeout = b.timeout;
        conns = new Connections(group, both, h1, b.version, b.timeout);
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
     * request (such as {@link JoinHint#JOIN_HEADER} with a reservation's header). The pipe input
     * has the halves of dir only. A response status other than 2xx fails the pipe with an
     * {@link HttpStatusException}.
     *
     * <p>credential, when not null, is presented for the server's admission: its prefix starts a
     * request body; a GET, which has none, carries its header form. The connection is not known
     * when the request is built, so a credential bound to TLS is an error.
     */
    public PipeIo pipe(URI url, Map<String, List<String>> headers, PipeOptions.Direction dir, Credential credential) throws IOException {
        Objects.requireNonNull(dir);
        boolean get = dir == PipeOptions.Direction.RECEIVE_ONLY;
        var info = new Admission.CarrierInfo(Connections.address(url), Connections.address(url), "", "", null, RequestHead.of(Map.of(), get));
        byte[] prefix = null;
        Credential.HeaderField field = null;
        if (credential != null) {
            if (get) field = credential.header(info);
            else prefix = credential.prefix(info);
        }
        Channel ch = await(conns.open(url, false, false), "connect");
        boolean h1 = !(ch instanceof Http2StreamChannel);
        HttpRequest req = get ? new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, target(url), Unpooled.EMPTY_BUFFER)
                : new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, target(url));
        req.headers().set(HttpHeaderNames.HOST, authority(url));
        headers.forEach((k, vs) -> { for (String v : vs) req.headers().add(k, v); });
        if (field != null) req.headers().add(field.name(), field.value());
        if (!get) {
            req.headers().set(HttpHeaderNames.CONTENT_TYPE, Requests.OCTETS);
            if (h1) HttpUtil.setTransferEncodingChunked(req, true);
        } else {
            HttpUtil.setContentLength(req, 0);
        }
        boolean reads = dir != PipeOptions.Direction.SEND_ONLY, writes = !get;
        BodyCarrier c = new BodyCarrier(ch, Framing.BYTE_STREAM, reads, writes, new BodyCarrier.Out() {
            @Override public Object content(ByteBuf data) { return new DefaultHttpContent(data); }
            @Override public Object end() { return new DefaultLastHttpContent(); }
        }, ch::close, cause -> { ch.close(); return null; });
        ch.pipeline().addLast(new ResponseReader(c, reads));
        ch.write(req);
        if (prefix != null && prefix.length > 0) ch.write(new DefaultHttpContent(Unpooled.wrappedBuffer(prefix)));
        ch.flush();
        return new PipeIo(c.reader(), c.writer(), Framing.BYTE_STREAM, "http " + dir, Requests.connection(ch))
                .withReceiveMemory(BodyCarrier.RECEIVE_MEMORY)
                .withCarrier(dir == PipeOptions.Direction.BIDIRECTIONAL ? CarrierKind.STREAMING : CarrierKind.SPLIT);
    }

    /**
     * Opens a split bootstrap over two requests, a POST uplink and a GET downlink, paired by a
     * random pairing token in {@link JoinHint#PAIR_HEADER} (SPEC.md Section 6.1.1), each presenting
     * credential as {@link #pipe} does. The carriers must be confidential end to end, or up to a
     * trusted TLS-terminating intermediary.
     */
    public PipeIo splitPipe(URI url, Map<String, List<String>> headers, Credential credential) throws IOException {
        byte[] token = new byte[16];
        RANDOM.nextBytes(token);
        Map<String, List<String>> h = new HashMap<>(headers);
        h.put(JoinHint.PAIR_HEADER, List.of(Base64.getUrlEncoder().withoutPadding().encodeToString(token)));
        PipeIo up = pipe(url, h, PipeOptions.Direction.SEND_ONLY, credential);
        PipeIo down;
        try {
            down = pipe(url, h, PipeOptions.Direction.RECEIVE_ONLY, credential);
        } catch (IOException | RuntimeException e) {
            up.writer().abort(e);
            throw e;
        }
        return PipeIo.split(up, down);
    }

    /** A bidirectional streaming request ({@link #pipe}) to url as a candidate. */
    public Candidate streamingCandidate(URI url, Map<String, List<String>> headers) {
        return Candidate.of(CarrierKind.STREAMING, (cred, t) -> pipe(url, headers, PipeOptions.Direction.BIDIRECTIONAL, cred));
    }

    private void startDownlink(io.zmux.PipeReservation r, Map<String, List<String>> h, URI url, Credential credential,
                               java.util.concurrent.CompletableFuture<ZmuxPipe> down) {
        Thread.ofVirtual().name("zmux-split-downlink").start(() -> {
            PipeIo io;
            try {
                io = pipe(url, h, PipeOptions.Direction.RECEIVE_ONLY, credential);
            } catch (IOException | RuntimeException e) {
                r.cancel();
                down.completeExceptionally(e);
                return;
            }
            try {
                down.complete(r.attach(io));
            } catch (IOException | RuntimeException e) {
                down.completeExceptionally(e);
            }
        });
    }

    /**
     * Joins a split carrier to session as further pipes: a downlink GET joined out of band and an
     * uplink POST joined in band, opened at the same time (a split bootstrap's pairing makes pipe
     * pair 0 only, so {@link #splitPipe} cannot join). Both get options with their own direction,
     * and present credential as {@link #pipe} does. When either join fails the other pipe is aborted.
     * A {@link io.zmux.PipePool#join pool} keeps such pairs as members:
     * {@code PipePool.join(a -> client.splitJoin(a.session(), url, headers, cred, a.options()))}.
     *
     * @return the downlink and the uplink
     */
    public List<ZmuxPipe> splitJoin(ZmuxSession session, URI url, Map<String, List<String>> headers, Credential credential,
                                    PipeOptions options) throws IOException {
        return splitJoin(session, url, headers, credential, options,
                session.paths().reserve(options.toBuilder().direction(PipeOptions.Direction.RECEIVE_ONLY).build()));
    }

    /** splitJoin over the downlink's reservation r, which this call owns: attached, or cancelled on failure. */
    private List<ZmuxPipe> splitJoin(ZmuxSession session, URI url, Map<String, List<String>> headers, Credential credential,
                                     PipeOptions options, io.zmux.PipeReservation r) throws IOException {
        var down = new java.util.concurrent.CompletableFuture<ZmuxPipe>();
        // The downlink thread owns the reservation once it starts; until then a failure cancels it here.
        try {
            Map<String, List<String>> h = new HashMap<>(headers);
            h.put(JoinHint.JOIN_HEADER, List.of(r.header()));
            startDownlink(r, h, url, credential, down);
        } catch (RuntimeException | Error e) {
            r.cancel();
            throw e;
        }
        ZmuxPipe pu = null;
        Throwable failure = null;
        try {
            PipeIo up = pipe(url, headers, PipeOptions.Direction.SEND_ONLY, credential);
            pu = session.paths().add(up, options.toBuilder().direction(PipeOptions.Direction.SEND_ONLY).build());
        } catch (IOException | RuntimeException e) {
            failure = e;
        }
        ZmuxPipe pd = null;
        try {
            pd = down.join();
        } catch (java.util.concurrent.CompletionException e) {
            if (failure == null) failure = e.getCause() != null ? e.getCause() : e;
        }
        if (failure == null) return List.of(Objects.requireNonNull(pd), Objects.requireNonNull(pu));
        if (pd != null) pd.abort();
        if (pu != null) pu.abort();
        if (failure instanceof IOException io) throw io;
        if (failure instanceof RuntimeException re) throw re;
        throw new IOException("split join failed", failure);
    }

    /**
     * A split bootstrap ({@link #splitPipe}) to url as a candidate; as a further pipe of a session it
     * joins with {@link #splitJoin}.
     */
    public Candidate splitCandidate(URI url, Map<String, List<String>> headers) {
        return new Candidate(CarrierKind.SPLIT, (cred, t) -> splitPipe(url, headers, cred),
                (session, options, cred, t) -> splitJoin(session, url, headers, cred, options));
    }

    // ---- WebSocket carriers ----

    /**
     * Opens a WebSocket to url (ws, wss, or http and https for the same) over a connection of its
     * own (HTTP/1.1, the form intermediaries forward) and returns it as a pipe with message
     * framing: each binary message is one record's Frames field. protocols are the subprotocols
     * offered, by preference (empty: {@link io.zmux.ZmuxConfig#DEFAULT_ALPN}), followed by their
     * draft-01 aliases; the selected one is the pipe's application protocol. headers are added to
     * the upgrade request (such as {@link JoinHint#JOIN_HEADER} with a reservation's header).
     * credential, when not null, is presented in its header form, bound to the TLS connection of
     * a wss URL. A refused upgrade fails with an {@link HttpStatusException}.
     */
    public PipeIo webSocketPipe(URI url, Map<String, List<String>> headers, List<String> protocols, Credential credential) throws IOException {
        return WebSocketClient.open(this, url, headers, protocols, credential);
    }

    /** A WebSocket ({@link #webSocketPipe}) to url as a candidate, presenting the session's credential. */
    public Candidate webSocketCandidate(URI url, Map<String, List<String>> headers, List<String> protocols) {
        List<String> p = List.copyOf(protocols);
        return Candidate.of(CarrierKind.WEBSOCKET, (cred, t) -> webSocketPipe(url, headers, p, cred));
    }

    // ---- the exchange carrier ----

    /**
     * Opens an exchange carrier to url, a template that may hold {instance} and {nonce} anywhere
     * in its path or query (see {@link ExchangeConfig}), and returns it as a bidirectional pipe.
     * Requests go out on pooled connections of this client: HTTP/1.1 connections kept for reuse,
     * or streams of one HTTP/2 connection. The carrier keeps DOWN requests outstanding from the
     * start, and ends when zmux closes both halves or the carrier fails. It authenticates with
     * the config's PSK; no credential is presented.
     *
     * @throws IllegalArgumentException when url (or the config's DOWN URL) is not an http or
     *     https URL once its placeholders are replaced
     */
    public PipeIo exchangePipe(String url, ExchangeConfig config) {
        if (closed) throw new IllegalStateException("closed");
        return new ExchangeClient(this, Objects.requireNonNull(url), Objects.requireNonNull(config)).pipe();
    }

    /** An exchange carrier ({@link #exchangePipe}) to url as a candidate; the session's credential is not presented on it. */
    public Candidate exchangeCandidate(String url, ExchangeConfig config) {
        new ExchangeClient.Target(url, config.nonceParam(), "", false); // a bad URL fails now, not at the dial
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
        try {
            return f.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            f.thenAccept(x -> { if (x instanceof Channel c) c.close(); });
            throw new InterruptedIOException("interrupted");
        } catch (TimeoutException e) {
            f.thenAccept(x -> { if (x instanceof Channel c) c.close(); });
            throw new SocketTimeoutException(what + " timed out");
        } catch (ExecutionException e) {
            throw e.getCause() instanceof IOException io ? io : new IOException(e.getCause());
        }
    }

    /** The response of a streaming carrier: its body is the downlink; a status other than 2xx fails the carrier. */
    static final class ResponseReader extends ChannelInboundHandlerAdapter {
        private final BodyCarrier c;
        private final boolean reads;
        private boolean ok;

        ResponseReader(BodyCarrier c, boolean reads) { this.c = c; this.reads = reads; }

        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
            try {
                if (msg instanceof HttpResponse r) {
                    int s = r.status().code();
                    ok = s / 100 == 2;
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
            c.lost(new ClosedChannelException());
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            c.lost(cause);
            ctx.close();
        }
    }

    EventLoopGroup group() { return group; }

    /** Fails the exchange carriers still running and closes the connections. */
    @Override public void close() {
        closed = true;
        for (ExchangeClient x : exchanges) x.shutdown();
        conns.close();
        if (ownGroup) group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }

    /** Configures an {@link HttpCarrierClient}. */
    public static final class Builder {
        private EventLoopGroup group;
        private SslContextBuilder tls;
        private Connections.Version version = Connections.Version.AUTO;
        private Duration timeout = Duration.ofSeconds(10);

        private Builder() {}

        /** The event loops of the client's connections (null: one of its own). */
        public Builder group(EventLoopGroup g) { group = g; return this; }

        /**
         * The TLS of https and wss URLs, as a client SslContextBuilder (null: the system's trust);
         * the client sets its ALPN protocols (h2, http/1.1).
         */
        public Builder tls(SslContextBuilder b) { tls = b; return this; }

        /** HTTP/1.1 only. */
        public Builder http1() { version = Connections.Version.HTTP_1_1; return this; }

        /** HTTP/2 only: over TLS by ALPN, over cleartext by prior knowledge. */
        public Builder http2() { version = Connections.Version.HTTP_2; return this; }

        /** Bounds connecting and each opening (10 s). */
        public Builder timeout(Duration d) { timeout = Objects.requireNonNull(d); return this; }

        public HttpCarrierClient build() throws IOException { return new HttpCarrierClient(this); }
    }
}
