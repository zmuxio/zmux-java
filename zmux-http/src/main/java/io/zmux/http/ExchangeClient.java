package io.zmux.http;

import io.zmux.http.internal.HttpTransport;

import io.zmux.internal.spi.Loops;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import java.util.concurrent.ScheduledFuture;
import io.zmux.CarrierKind;
import io.zmux.Chunk;
import io.zmux.HttpRedirectException;
import io.zmux.carrier.Framing;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import io.zmux.internal.spi.LentInput;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * The client end of one exchange carrier. Its pieces and bodies are heap buffers, so it holds
 * nothing to release and a loop that stops leaves nothing behind. Its state is owned by one
 * event loop: zmux's calls and the results of the HTTP attempts hop there, and a
 * scheduler there ({@link #step}) seals UP pieces, keeps DOWN requests outstanding, repeats
 * requests without a result after a backoff and fails the carrier at its deadline. Each attempt
 * is one HTTP exchange on a connection of the {@link HttpCarrierClient}.
 */
final class ExchangeClient {
    static final String GONE = "exchange carrier gone", DEADLINE = "exchange carrier deadline exceeded",
            PROTOCOL = "exchange carrier protocol violation";

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final HexFormat HEX = HexFormat.of();

    private final HttpCarrierClient http;
    private final ExchangeConfig cfg;
    private final EventLoop loop;
    private final byte[] cid = new byte[Exchange.CID];
    private final Exchange.Key key;
    private final Target upTarget, downTarget;
    /** The loop's: the scheme and authority a pure scheme upgrade led each target's requests to; null: none. */
    private URI upUpgrade, downUpgrade;

    // Every field below is the loop's.
    private Throwable err;
    /** No more requests: the carrier completed or failed. */
    private boolean ended;
    private boolean sentAny, gone, stepping, stepQueued;
    private ScheduledFuture<?> timer;
    private long timerAt;
    private final Set<Attempt> inflight = new HashSet<>();

    // Uplink.
    /** The piece being filled, in write mode; null: none. */
    private ByteBuffer open;
    private long openAt;
    private long upSeq;
    /** Sealed pieces not yet processed, by number. */
    private final List<Up> ups = new ArrayList<>();
    private int upFlight, upLimit;
    private boolean wclosed, finSeal;
    /** zmux's write being taken into pieces, and its completion. */
    private OutboundBatch wBatch;
    private PipeWriter.Completion wDone;
    private int wSeg, wOff;

    // Downlink.
    /** The smallest downlink number not received. */
    private long ack;
    /** The next piece to hand to zmux. */
    private long deliv;
    private final Map<Long, Piece> held = new HashMap<>();
    /** The lowest FIN piece; -1 while unknown. */
    private long fin = -1;
    private final Map<Long, State> downs = new HashMap<>();
    private int downLimit;
    private boolean rclosed;
    /** The downlink in number order, lent to zmux's reader within its receive memory. */
    private final LentInput input;
    private boolean delivering;
    /** Fails the carrier when its loop terminates: no call of zmux or timer of the loop ends it any more. */
    private final io.netty.util.concurrent.GenericFutureListener<io.netty.util.concurrent.Future<Object>> onTerminated = f -> terminated();

    ExchangeClient(HttpCarrierClient http, String url, ExchangeConfig cfg) throws io.zmux.CarrierClientClosedException {
        this.http = http;
        this.cfg = cfg;
        loop = http.group().next();
        RANDOM.nextBytes(cid);
        key = Exchange.key(cfg.pskRef(), cid);
        String inst = cfg.instance();
        if (inst.isEmpty()) {
            byte[] t = new byte[8];
            RANDOM.nextBytes(t);
            inst = HEX.formatHex(t);
        }
        String down = cfg.downUrl().isEmpty() ? url : cfg.downUrl();
        upTarget = new Target(url, cfg.nonceParam(), inst, cfg.isCacheBust() && !url.contains("{nonce}"));
        downTarget = new Target(down, cfg.nonceParam(), inst, cfg.isCacheBust() && !down.contains("{nonce}"));
        if (http.conns.http3()) {
            byte[] nonce = new byte[Exchange.NONCE];
            http.conns.requireRequestScheme(upTarget.build(nonce));
            http.conns.requireRequestScheme(downTarget.build(nonce));
        }
        upLimit = cfg.upRequests();
        downLimit = cfg.downRequests();
        input = new LentInput(receiveMemory(), loop, loop::inEventLoop, more -> { if (more) deliver(); });
        if (!http.register(this)) throw HttpCarrierClient.closedFailure();
        loop.terminationFuture().addListener(onTerminated);
        submit(this::step, NOTHING);
    }

    private static final Runnable NOTHING = () -> {};

    /** The client closes: the carrier fails with cause, without a CLOSE request (the connections are going). Complete once it failed. */
    java.util.concurrent.CompletableFuture<Void> shutdown(Throwable cause) {
        var failed = new java.util.concurrent.CompletableFuture<Void>();
        submit(() -> {
            gone = true;
            fail(cause);
            step(); // ends the carrier now, so its reader has the failure when this completes
            failed.complete(null);
        }, () -> failed.complete(null));
        return failed;
    }

    /**
     * Runs an outside call's task on the loop. A loop that refuses it is shut down: the carrier
     * stops counting as running, and onRejected fails what the call brought (a completion, a
     * sink). The carrier's state, all heap, goes with the loop.
     */
    private boolean submit(Runnable task, Runnable onRejected) {
        return Loops.run(loop, task, () -> {
            http.exchanges.remove(this);
            onRejected.run();
        });
    }

    PipeIo pipe() {
        return new PipeIo(new Reader(), new Writer(), Framing.BYTE_STREAM, "http exchange", this)
                .withReceiveMemory(receiveMemory()).withCarrier(CarrierKind.EXCHANGE);
    }

    private long receiveMemory() { return 2L * cfg.downLimit(); }

    /** Builds request targets from a URL template with {instance} and {nonce}. */
    record Target(String template, String param, String instance, boolean bust) {
        Target {
            try {
                URI u = new URI(build(template, param, instance, bust, new byte[Exchange.NONCE]));
                if (u.getHost() == null || !(Connections.secure(u) || "http".equalsIgnoreCase(u.getScheme()))) {
                    throw new IllegalArgumentException("exchange URL needs an http or https host: " + template);
                }
            } catch (java.net.URISyntaxException e) {
                throw new IllegalArgumentException("exchange URL: " + e.getMessage(), e);
            }
        }

        URI build(byte[] nonce) { return URI.create(build(template, param, instance, bust, nonce)); }

        private static String build(String t, String param, String instance, boolean bust, byte[] nonce) {
            String n = HEX.formatHex(nonce);
            String u = t.replace("{instance}", instance).replace("{nonce}", n);
            if (bust) u += (u.contains("?") ? "&" : "?") + param + "=" + n;
            return u;
        }
    }

    /** One request number's retry state. */
    private static class State {
        boolean inflight;
        Attempt attempt;
        long retryAt;
        /** The first attempt without a result since the last result; 0: none. */
        long since;
        long backoff;
    }

    private static final class Up extends State {
        final long seq;
        /** Null for an empty piece. */
        final ByteBuffer data;
        final boolean fin;

        Up(long seq, ByteBuffer data, boolean fin) { this.seq = seq; this.data = data; this.fin = fin; }
    }

    /** A received downlink piece; data null when empty. */
    private record Piece(ByteBuffer data, boolean fin) {}

    // ---- scheduling ----

    /** Runs the scheduler soon, once however often it is asked. */
    private void poke() {
        if (stepQueued || stepping) { stepQueued = true; return; }
        stepQueued = true;
        submit(this::step, NOTHING);
    }

    /** Fails the carrier: no piece is delivered after the failure, so the held ones go now. */
    private void fail(Throwable e) {
        if (err == null) err = e;
        dropHeld();
        if (ended) deliver(); // the scheduler is done: the reader learns of the failure here
        else poke();
    }

    private boolean complete() { return finSeal && ups.isEmpty() && fin >= 0 && ack > fin; }

    /** Advances the schedule: seals UP pieces, launches attempts, and arms the timer for when it next has to run. */
    private void step() {
        stepQueued = false;
        if (ended) return;
        stepping = true;
        try {
            long now = System.nanoTime();
            long next = Long.MAX_VALUE;
            if (err == null) {
                long dl = cfg.deadline().toNanos();
                for (Up u : ups) next = Math.min(next, check(u, now, dl));
                for (State s : downs.values()) next = Math.min(next, check(s, now, dl));
            }
            boolean complete = complete();
            if (err != null || complete || (finSeal && ups.isEmpty() && rclosed)) {
                end(complete);
                return;
            }
            seal(now);
            take();
            if (open != null && open.position() > 0 && ups.size() < upLimit) next = Math.min(next, openAt + cfg.coalesce().toNanos());
            for (Up u : ups) {
                if (upFlight >= upLimit) break;
                if (u.inflight) continue;
                if (u.retryAt - now > 0) { next = Math.min(next, u.retryAt); continue; }
                upFlight++;
                launch(Exchange.KIND_UP, u.seq, u, u.data, u.fin, now);
            }
            if (!rclosed) next = Math.min(next, downlink(now));
            arm(next, now);
        } finally {
            stepping = false;
        }
        if (stepQueued) { stepQueued = false; step(); }
    }

    private long check(State s, long now, long deadline) {
        if (s.since == 0) return Long.MAX_VALUE;
        if (now - s.since > deadline) fail(new io.zmux.CarrierFailedException(DEADLINE));
        return s.since + deadline + 1_000_000;
    }

    /** Keeps DOWN requests outstanding for consecutive numbers from Down Ack on, up to the FIN, the limit and a read-ahead bound. */
    private long downlink(long now) {
        long next = Long.MAX_VALUE;
        int want = 0;
        long last = ack;
        for (long n = ack; want < downLimit; n++) {
            if ((fin >= 0 && n > fin) || n >= deliv + 2L * cfg.downRequests()) break;
            if (held.containsKey(n)) continue;
            want++;
            last = n;
            State s = downs.get(n);
            if (s == null) {
                s = new State();
                s.backoff = cfg.backoff().min().toNanos();
                downs.put(n, s);
            }
            if (s.inflight) continue;
            if (s.retryAt - now > 0) { next = Math.min(next, s.retryAt); continue; }
            launch(Exchange.KIND_DOWN, n, s, null, false, now);
        }
        long l = last;
        // Numbers no longer wanted (the limit fell) start afresh later.
        downs.entrySet().removeIf(e -> !e.getValue().inflight && (e.getKey() > l || e.getKey() < ack));
        return next;
    }

    private void arm(long at, long now) {
        if (at == Long.MAX_VALUE) return;
        if (timer != null && !timer.isDone() && timerAt - at <= 0) return;
        if (timer != null) timer.cancel(false);
        timerAt = at;
        timer = Loops.schedule(loop, this::step, Math.max(at - now, 0), TimeUnit.NANOSECONDS, NOTHING);
    }

    /** Seals the open piece when it is due and a slot is free, and the empty FIN piece after the write side ended. */
    private void seal(long now) {
        if (ups.size() >= upLimit || finSeal) return;
        int len = open == null ? 0 : open.position();
        boolean due = wclosed || len == cfg.upPieceSize() || (len > 0 && now - (openAt + cfg.coalesce().toNanos()) >= 0);
        if (!due) return;
        Up u = new Up(upSeq++, len > 0 ? open.flip() : null, wclosed);
        open = null;
        u.backoff = cfg.backoff().min().toNanos();
        finSeal = wclosed;
        ups.add(u);
    }

    /** Takes zmux's pending write into the open piece, sealing full pieces while slots are free. */
    private void take() {
        OutboundBatch b = wBatch;
        if (b == null) return;
        if (err != null || ended) { finishWrite(err != null ? err : new ClosedChannelException()); return; }
        int piece = cfg.upPieceSize();
        while (wSeg < b.segmentCount()) {
            ByteBuffer seg = b.segment(wSeg);
            int rem = seg.remaining() - wOff;
            if (rem <= 0) { wSeg++; wOff = 0; continue; }
            if (open != null && open.position() == piece) {
                if (ups.size() >= upLimit) return; // every slot is taken: the write waits
                seal(System.nanoTime());
                continue;
            }
            if (open == null) {
                // Start at the pending bytes (at least 4 KiB), but never above a piece, which may be smaller.
                int initial = Math.max(rem, 4096);
                open = ByteBuffer.allocate(Math.min(initial, piece));
                openAt = System.nanoTime();
                poke(); // the coalescing timer
            }
            int n = Math.min(rem, piece - open.position());
            open = Exchange.append(open, seg.slice(seg.position() + wOff, n), piece);
            wOff += n;
        }
        if (open != null && open.position() == piece) seal(System.nanoTime());
        finishWrite(null);
        poke();
    }

    private void finishWrite(Throwable e) {
        PipeWriter.Completion d = wDone;
        wBatch = null;
        wDone = null;
        if (d == null) return;
        if (e == null) d.completed();
        else d.failed(e);
    }

    /**
     * Stops the carrier: cancels the attempts in flight, drops the uplink and, unless the
     * server forgot it, sends a CLOSE without waiting for its result. After a completion that
     * CLOSE carries the Down Ack past the downlink FIN, which completes the server's side too.
     */
    private void end(boolean complete) {
        ended = true;
        loop.terminationFuture().removeListener(onTerminated);
        http.exchanges.remove(this);
        if (err == null && !complete) err = new ClosedChannelException();
        if (err != null) dropHeld();
        if (timer != null) timer.cancel(false);
        for (Attempt a : List.copyOf(inflight)) a.cancel();
        inflight.clear();
        ups.clear();
        open = null;
        finishWrite(err != null ? err : new ClosedChannelException());
        if (sentAny && !gone) launch(Exchange.KIND_CLOSE, 0, new State(), null, false, System.nanoTime());
        deliver();
    }

    private void dropHeld() {
        held.clear();
    }

    // ---- attempts ----

    private void launch(int kind, long seq, State st, ByteBuffer data, boolean finFlag, long now) {
        Attempt a = new Attempt(kind, seq, st, data, finFlag ? Exchange.FLAG_FIN : 0, ack);
        st.inflight = true;
        st.attempt = a;
        if (st.since == 0) st.since = now;
        sentAny = true;
        if (kind != Exchange.KIND_CLOSE) inflight.add(a);
        a.start();
    }

    /** What an attempt brought back: none (no result), bad (a verified response that fails the carrier), or flags and data. */
    private record Result(boolean ok, boolean bad, int flags, ByteBuffer data, Throwable error) {
        static final Result NONE = new Result(false, false, 0, null, null);
        static final Result BAD = new Result(true, true, 0, null, null);
        Result(boolean ok, boolean bad, int flags, ByteBuffer data) { this(ok, bad, flags, data, null); }
        /** A refused redirect: the carrier fails. */
        static Result refused(Throwable e) { return new Result(true, true, 0, null, e); }
    }

    /** A request's URL: its target's, over the scheme and authority a pure scheme upgrade led its earlier requests to. */
    private URI requestUri(int kind, byte[] nonce) {
        URI u = (kind == Exchange.KIND_DOWN ? downTarget : upTarget).build(nonce);
        URI up = kind == Exchange.KIND_DOWN ? downUpgrade : upUpgrade;
        if (up == null) return u;
        return URI.create(up.getScheme() + "://" + up.getRawAuthority() + HttpCarrierClient.target(u));
    }

    /** One HTTP exchange of one request. */
    private final class Attempt {
        final int kind;
        final long seq;
        final State st;
        final byte[] nonce = new byte[Exchange.NONCE];
        /** The URL of the request; redirects move it, on the loop. */
        volatile URI uri;
        int hops;
        /** The encoded request body, kept until the result to repeat it at a redirect's URL. */
        ByteBuffer[] body;
        /** The loop's: the result was taken (or the attempt cancelled). */
        boolean finished;
        ScheduledFuture<?> timeout;
        volatile Channel channel;
        volatile boolean cancelled;

        Attempt(int kind, long seq, State st, ByteBuffer data, int flags, long ackSent) {
            this.kind = kind; this.seq = seq; this.st = st;
            RANDOM.nextBytes(nonce);
            ByteBuffer d = data == null ? ByteBuffer.allocate(0) : data.duplicate();
            var sealed = Exchange.seal(key, cid, kind, flags, seq, ackSent, (int) (System.currentTimeMillis() / 1000), nonce, d.duplicate());
            ByteBuffer[] b = {sealed.header(), d, sealed.dataMac()};
            if (cfg.isBase64()) {
                ByteBuffer bin = ByteBuffer.allocate(b[0].remaining() + b[1].remaining() + b[2].remaining());
                for (ByteBuffer x : b) bin.put(x.duplicate());
                b = new ByteBuffer[] {Base64.getEncoder().encode(bin.flip())};
            }
            body = b;
            uri = requestUri(kind, nonce);
        }

        void start() {
            timeout = Loops.schedule(loop, () -> finish(Result.NONE), cfg.requestTimeout().toNanos(), TimeUnit.NANOSECONDS, NOTHING);
            connect();
        }

        /**
         * Sends the request at uri on a connection; on the loop. Nothing is held while the connection
         * is being opened: its completion can be lost (the client's group shutting down), so the
         * request buffer is taken only once the connection is there, back on the loop.
         */
        private void connect() {
            http.conns.open(uri, true, false).whenComplete((ch, e) -> {
                if (e != null) { finishOnLoop(Result.NONE); return; }
                channel = ch;
                if (cancelled) { ch.close(); return; }
                Loops.run(loop, () -> dispatch(ch), ch::close); // refused: nobody is left to take the result
            });
        }

        /** Hands the request to the connection's loop, unless the attempt ended meanwhile; on the loop. */
        private void dispatch(Channel ch) {
            if (finished || body == null) { ch.close(); return; }
            ByteBuffer[] parts = new ByteBuffer[body.length];
            for (int i = 0; i < parts.length; i++) parts[i] = body[i].duplicate();
            ByteBuf b = Unpooled.wrappedBuffer(parts); // heap views: a refused send leaves nothing to release
            Loops.run(ch.eventLoop(), () -> send(ch, b), () -> {
                ch.close();
                finish(Result.NONE);
            });
        }

        /**
         * A redirect of the request: one Redirects.check allows within the origin, a 307 or 308
         * (a POST does not become a GET), is followed with the body unchanged, up to
         * Redirects.DEFAULT_MAX hops; any other fails the carrier. A pure scheme upgrade is kept
         * for the target's later requests. On the loop.
         */
        void redirected(int status, String location) {
            if (finished) return;
            Throwable refused = null;
            URI to = null;
            try {
                if (status != 307 && status != 308) throw new HttpRedirectException(status, location == null ? "" : location, "would change POST to GET");
                to = Redirects.check(uri, "POST", status, location);
                if (hops >= Redirects.DEFAULT_MAX) throw new HttpRedirectException(status, to.toString(), "too many redirects");
            } catch (HttpRedirectException e) {
                refused = e;
            }
            if (refused != null) { finish(Result.refused(refused)); return; }
            hops++;
            if (Redirects.schemeUpgradeOf(uri, to)) {
                if (kind == Exchange.KIND_DOWN) downUpgrade = to; else upUpgrade = to;
            }
            uri = to;
            channel = null;
            connect();
        }

        private void send(Channel ch, ByteBuf b) {
            FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, HttpCarrierClient.target(uri), b);
            req.headers().set(HttpHeaderNames.HOST, HttpCarrierClient.authority(uri));
            cfg.headers().forEach((k, vs) -> { for (String v : vs) req.headers().add(k, v); });
            req.headers().set(HttpHeaderNames.CONTENT_TYPE, Requests.OCTETS).set(Requests.CACHE, Requests.NO_STORE)
                    .set(HttpHeaderNames.PRAGMA, "no-cache");
            HttpUtil.setContentLength(req, b.readableBytes());
            ch.pipeline().addLast(new Collector(this, ch));
            ch.writeAndFlush(req).addListener(f -> { if (!f.isSuccess()) ch.close(); });
        }

        /** Ends the attempt without taking its result (the carrier ended or the downlink closed). */
        void cancel() {
            body = null;
            finished = true;
            cancelled = true;
            if (timeout != null) timeout.cancel(false);
            Channel ch = channel;
            if (ch != null) HttpTransport.abort(ch);
        }

        /** Runs task on the loop unless the group is gone. */
        void onLoop(Runnable task) {
            if (!loop.isShuttingDown()) Loops.run(loop, task, () -> {}); // refused: nobody is left
        }

        /** Hands the result to the loop. A connection's end can outlive the group: nobody is left to take it. */
        void finishOnLoop(Result r) {
            Loops.run(loop, () -> finish(r), NOTHING);
        }

        /** Takes the attempt's result on the loop. */
        @SuppressWarnings("ReferenceEquality") // Result.NONE is a sentinel
        void finish(Result r) {
            if (finished) return;
            finished = true;
            body = null;
            if (timeout != null) timeout.cancel(false);
            if (r == Result.NONE) { Channel ch = channel; if (ch != null) HttpTransport.abort(ch); }
            inflight.remove(this);
            if (kind == Exchange.KIND_CLOSE) return;
            apply(this, r);
            poke();
        }

        /** Verifies a complete response body (encoded); the result's data is a view of it. */
        Result result(ByteBuffer raw) {
            ByteBuffer b = cfg.isBase64() ? Exchange.decodeBase64(raw) : raw;
            if (b == null || b.remaining() < Exchange.RESP_HDR) return Result.NONE;
            byte[] mac = new byte[Exchange.MAC];
            b.get(b.position(), mac);
            int fl = b.get(b.position() + Exchange.MAC) & 0xff;
            ByteBuffer data = b.slice(b.position() + Exchange.RESP_HDR, b.remaining() - Exchange.RESP_HDR);
            byte[] want = key.responseMac(cid, kind, seq, nonce, fl, data.duplicate());
            if (!MessageDigest.isEqual(want, mac)) return Result.NONE;
            if (!Exchange.responseValid(kind, fl, data.remaining())) return Result.BAD;
            return new Result(true, false, fl, data.hasRemaining() ? data : null);
        }
    }

    /** Reads one attempt's response on its channel; a complete HTTP/1.1 exchange gives the connection back. */
    private final class Collector extends ChannelInboundHandlerAdapter {
        private final Attempt a;
        private final Channel ch;
        /** The body being read, in write mode; null: none. */
        private ByteBuffer body;
        private long length = -1;
        private boolean keepAlive, done;

        Collector(Attempt a, Channel ch) { this.a = a; this.ch = ch; }

        private int limit() {
            int n = Exchange.RESP_HDR + cfg.downLimit();
            return cfg.isBase64() ? Exchange.base64Length(n) : n;
        }

        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
            try {
                if (done) return;
                if (msg instanceof HttpResponse r) {
                    if (r.status().code() / 100 == 1) return; // an interim response
                    length = HttpUtil.getContentLength(r, -1L);
                    keepAlive = HttpUtil.isKeepAlive(r);
                    int code = r.status().code();
                    if (Redirects.isRedirect(code)) {
                        String location = r.headers().get(HttpHeaderNames.LOCATION);
                        done = true;
                        ch.pipeline().remove(this);
                        ch.close();
                        a.onLoop(() -> a.redirected(code, location));
                        return;
                    }
                    if (r.status().code() != 200 || length < 0) { result(Result.NONE, false); return; }
                    // Over the local limit: a configuration mismatch, never repeated.
                    if (length > limit()) { result(Result.BAD, false); return; }
                    body = ByteBuffer.allocate((int) length);
                }
                if (msg instanceof HttpContent c && body != null) {
                    ByteBuf in = c.content();
                    if (in.readableBytes() > body.remaining()) { result(Result.NONE, false); return; }
                    body = Exchange.append(body, in, in.readerIndex(), in.readableBytes(), (int) length);
                    if (c instanceof LastHttpContent) {
                        if (body.position() != length) { result(Result.NONE, false); return; }
                        ByteBuffer b = body.flip();
                        body = null;
                        result(a.result(b), keepAlive);
                    }
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }

        /** Hands r to the carrier; reusable: the connection may serve another request. */
        private void result(Result r, boolean reusable) {
            done = true;
            body = null;
            try {
                ch.pipeline().remove(this);
                if (reusable && !a.cancelled) http.conns.release(a.uri, ch, true);
                else ch.close();
            } finally {
                a.finishOnLoop(r); // r's data goes to the carrier whatever the connection's handling threw
            }
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            if (!done) result(Result.NONE, false);
            ctx.fireChannelInactive();
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (!done) result(Result.NONE, false);
        }
    }

    /** Applies an attempt's result. */
    private void apply(Attempt a, Result r) {
        State st = a.st;
        st.inflight = false;
        st.attempt = null;
        if (a.kind == Exchange.KIND_UP) upFlight--;
        if (ended) return;
        long now = System.nanoTime();
        if (r.error != null) { fail(r.error); return; }
        if (!r.ok) { retry(st, now); return; }
        if (r.bad) { fail(new io.zmux.CarrierFailedException(PROTOCOL)); return; }
        if (r.flags == Exchange.FLAG_GONE) { gone = true; fail(new io.zmux.CarrierFailedException(GONE)); return; }
        st.since = 0;
        if (r.flags == Exchange.FLAG_BUSY) {
            retry(st, now);
            if (a.kind == Exchange.KIND_UP) upLimit = Math.max(1, upLimit - 1);
            else downLimit = Math.max(1, downLimit - 1);
            return;
        }
        st.backoff = cfg.backoff().min().toNanos();
        if (a.kind == Exchange.KIND_UP) {
            upLimit = Math.min(cfg.upRequests(), upLimit + 1);
            for (Iterator<Up> it = ups.iterator(); it.hasNext(); ) {
                Up u = it.next();
                if (u.seq == a.seq) {
                    it.remove();
                    break;
                }
            }
            take();
            return;
        }
        downLimit = Math.min(cfg.downRequests(), downLimit + 1);
        receive(a.seq, r.data, (r.flags & Exchange.FLAG_FIN) != 0);
    }

    private void retry(State st, long now) {
        if (wclosed && rclosed) {
            // zmux closed both halves and waits for nothing more of the carrier: what is left ends at the first
            // attempt without a result instead of being repeated up to the deadline (or forever without one).
            fail(new ClosedChannelException());
            return;
        }
        long spread = (long) (st.backoff * cfg.backoff().jitter());
        st.retryAt = now + st.backoff - spread + ThreadLocalRandom.current().nextLong(spread + 1);
        st.backoff = Math.min(2 * st.backoff, cfg.backoff().max().toNanos());
    }

    /** Takes downlink piece n. */
    private void receive(long n, ByteBuffer data, boolean isFin) {
        downs.remove(n);
        if (err != null || held.containsKey(n) || n < ack || (fin >= 0 && n > fin) || rclosed) return;
        if (isFin && (fin < 0 || n < fin)) {
            fin = n;
            held.keySet().removeIf(k -> k > n);
        }
        held.put(n, new Piece(data, isFin));
        while (held.containsKey(ack) && !(fin >= 0 && ack > fin)) ack++;
        deliver();
    }

    /** Hands the pieces in number order to zmux, within its receive memory; then the end or the failure. */
    private void deliver() {
        if (err != null) { dropHeld(); input.fail(err); return; }
        if (delivering) return; // the input asked for more while taking a piece: the loop below goes on
        delivering = true;
        try {
            while (input.taking()) {
                if (fin >= 0 && deliv > fin) { input.end(); return; }
                Piece p = held.get(deliv);
                if (p == null) return;
                int n = p.data == null ? 0 : p.data.remaining();
                if (n > 0 && !input.hasRoom(n)) return;
                held.remove(deliv);
                deliv++;
                poke(); // the read-ahead bound moved
                if (n > 0) input.offer(Chunk.of(p.data), n);
            }
        } finally {
            delivering = false;
        }
    }

    /** The loop terminated: nothing of it runs any more, so the carrier fails here, once. */
    private void terminated() {
        if (ended) return;
        ended = true;
        if (err == null) err = new ClosedChannelException();
        http.exchanges.remove(this);
        dropHeld();
        input.fail(err);
        finishWrite(err);
    }

    // ---- zmux's halves ----

    private final class Reader implements PipeReader {
        @Override public void start(Sink s) {
            submit(() -> {
                if (rclosed) return;
                input.start(s);
                deliver();
            }, () -> s.onError(new ClosedChannelException()));
        }

        /** Stops the downlink; the carrier ends once the uplink is done. */
        @Override public void close() {
            submit(() -> {
                if (rclosed) return;
                rclosed = true;
                input.close();
                dropHeld();
                for (State s : downs.values()) {
                    if (s.attempt != null) { Attempt a = s.attempt; a.cancel(); inflight.remove(a); s.inflight = false; s.attempt = null; }
                }
                downs.clear();
                poke();
            }, NOTHING);
        }
    }

    private final class Writer implements PipeWriter {
        @Override public void write(OutboundBatch batch, Completion done) {
            submit(() -> {
                if (err != null || wclosed || ended) { done.failed(err != null ? err : new ClosedChannelException()); return; }
                wBatch = batch;
                wDone = done;
                wSeg = 0;
                wOff = 0;
                take();
            }, () -> done.failed(new ClosedChannelException()));
        }

        /** Ends the uplink gracefully: the pieces written are sent, the last one with FIN. */
        @Override public void close() {
            submit(() -> { wclosed = true; poke(); }, NOTHING);
        }

        /** Fails the carrier; the server is told with a CLOSE request. */
        @Override public void abort(Throwable cause) {
            submit(() -> fail(cause == null ? new ClosedChannelException() : cause), NOTHING);
        }
    }
}
