package io.zmux.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
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
import io.netty.util.concurrent.ScheduledFuture;
import io.zmux.CarrierKind;
import io.zmux.Chunk;
import io.zmux.carrier.Framing;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import io.zmux.carrier.VarInt;
import java.io.IOException;
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
 * The client end of one exchange carrier carrier. Its state is
 * owned by one event loop: zmux's calls and the results of the HTTP attempts hop there, and a
 * scheduler there ({@link #step}) seals UP pieces, keeps DOWN requests outstanding, repeats
 * requests without a result after a backoff and fails the carrier at its deadline. Each attempt
 * is one HTTP exchange on a connection of the {@link HttpCarrierClient}.
 */
final class ExchangeClient {
    /** Failures zmux sees on a failed carrier. */
    static final class ExchangeException extends IOException {
        ExchangeException(String m) { super(m); }
    }

    static final String GONE = "exchange carrier gone", DEADLINE = "exchange carrier deadline exceeded",
            PROTOCOL = "exchange carrier protocol violation";

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final HexFormat HEX = HexFormat.of();

    private final HttpCarrierClient http;
    private final ExchangeConfig cfg;
    private final EventLoop loop;
    private final ByteBufAllocator alloc;
    private final byte[] cid = new byte[Exchange.CID];
    private final Exchange.Key key;
    private final Target upTarget, downTarget;

    // Every field below is the loop's.
    private Throwable err;
    /** No more requests: the carrier completed or failed. */
    private boolean ended;
    private boolean sentAny, gone, stepping, stepQueued;
    private ScheduledFuture<?> timer;
    private long timerAt;
    private final Set<Attempt> inflight = new HashSet<>();

    // Uplink.
    private ByteBuf open;
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
    private PipeReader.Sink sink;
    /** Bytes handed to zmux and not yet consumed. */
    private long outstanding;

    ExchangeClient(HttpCarrierClient http, String url, ExchangeConfig cfg) {
        this.http = http;
        this.cfg = cfg;
        loop = http.group().next();
        alloc = ByteBufAllocator.DEFAULT;
        RANDOM.nextBytes(cid);
        key = Exchange.key(cfg.pskRef(), cid);
        String inst = cfg.instance();
        if (inst.isEmpty()) {
            byte[] t = new byte[8];
            RANDOM.nextBytes(t);
            inst = HEX.formatHex(t);
        }
        String down = cfg.downUrl().isEmpty() ? url : cfg.downUrl();
        upTarget = new Target(url, cfg.nonceParam(), inst, cfg.cacheBust() && !url.contains("{nonce}"));
        downTarget = new Target(down, cfg.nonceParam(), inst, cfg.cacheBust() && !down.contains("{nonce}"));
        upLimit = cfg.upRequests();
        downLimit = cfg.downRequests();
        http.exchanges.add(this);
        loop.execute(this::step);
    }

    /** The client closes: the carrier fails without a CLOSE request (the connections are going). */
    void shutdown() {
        loop.execute(() -> { gone = true; fail(new ClosedChannelException()); });
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
        final ByteBuf data;
        final boolean fin;

        Up(long seq, ByteBuf data, boolean fin) { this.seq = seq; this.data = data; this.fin = fin; }
    }

    /** A received downlink piece; data null when empty. */
    private record Piece(ByteBuf data, boolean fin) {}

    // ---- scheduling ----

    /** Runs the scheduler soon, once however often it is asked. */
    private void poke() {
        if (stepQueued || stepping) { stepQueued = true; return; }
        stepQueued = true;
        loop.execute(this::step);
    }

    private void fail(Throwable e) {
        if (err == null) err = e;
        poke();
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
            if (err != null || complete || finSeal && ups.isEmpty() && rclosed) {
                end(complete);
                return;
            }
            seal(now);
            take();
            if (open != null && open.isReadable() && ups.size() < upLimit) next = Math.min(next, openAt + cfg.coalesce().toNanos());
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
        if (now - s.since > deadline) fail(new ExchangeException(DEADLINE));
        return s.since + deadline + 1_000_000;
    }

    /** Keeps DOWN requests outstanding for consecutive numbers from Down Ack on, up to the FIN, the limit and a read-ahead bound. */
    private long downlink(long now) {
        long next = Long.MAX_VALUE;
        int want = 0;
        long last = ack;
        for (long n = ack; want < downLimit; n++) {
            if (fin >= 0 && n > fin || n >= deliv + 2L * cfg.downRequests()) break;
            if (held.containsKey(n)) continue;
            want++;
            last = n;
            State s = downs.get(n);
            if (s == null) {
                s = new State();
                s.backoff = cfg.backoffMin().toNanos();
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
        timer = loop.schedule(this::step, Math.max(at - now, 0), TimeUnit.NANOSECONDS);
    }

    /** Seals the open piece when it is due and a slot is free, and the empty FIN piece after the write side ended. */
    private void seal(long now) {
        if (ups.size() >= upLimit || finSeal) return;
        int len = open == null ? 0 : open.readableBytes();
        boolean due = wclosed || len == cfg.upPieceSize() || len > 0 && now - (openAt + cfg.coalesce().toNanos()) >= 0;
        if (!due) return;
        Up u = new Up(upSeq++, len > 0 ? open : null, wclosed);
        if (len == 0 && open != null) open.release();
        open = null;
        u.backoff = cfg.backoffMin().toNanos();
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
            if (open != null && open.readableBytes() == piece) {
                if (ups.size() >= upLimit) return; // every slot is taken: the write waits
                seal(System.nanoTime());
                continue;
            }
            if (open == null) {
                // Start at the pending bytes (at least 4 KiB), but never above a piece, which may be smaller.
                int initial = Math.max(rem, 4096);
                open = alloc.buffer(Math.min(initial, piece), piece);
                openAt = System.nanoTime();
                poke(); // the coalescing timer
            }
            int n = Math.min(rem, piece - open.readableBytes());
            open.writeBytes(seg.slice(seg.position() + wOff, n));
            wOff += n;
        }
        if (open != null && open.readableBytes() == piece) seal(System.nanoTime());
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
     * Stops the carrier: cancels the attempts in flight, releases the uplink and, unless the
     * server forgot it, sends a CLOSE without waiting for its result. After a completion that
     * CLOSE carries the Down Ack past the downlink FIN, which completes the server's side too.
     */
    private void end(boolean complete) {
        ended = true;
        http.exchanges.remove(this);
        if (err == null && !complete) err = new ClosedChannelException();
        if (timer != null) timer.cancel(false);
        for (Attempt a : List.copyOf(inflight)) a.cancel();
        inflight.clear();
        for (Up u : ups) if (u.data != null) u.data.release();
        ups.clear();
        if (open != null) { open.release(); open = null; }
        if (!complete) dropHeld();
        finishWrite(err != null ? err : new ClosedChannelException());
        if (sentAny && !gone) launch(Exchange.KIND_CLOSE, 0, new State(), null, false, System.nanoTime());
        deliver();
    }

    private void dropHeld() {
        for (Piece p : held.values()) if (p.data != null) p.data.release();
        held.clear();
    }

    // ---- attempts ----

    private void launch(int kind, long seq, State st, ByteBuf data, boolean finFlag, long now) {
        Attempt a = new Attempt(kind, seq, st, data, finFlag ? Exchange.FLAG_FIN : 0, ack);
        st.inflight = true;
        st.attempt = a;
        if (st.since == 0) st.since = now;
        sentAny = true;
        if (kind != Exchange.KIND_CLOSE) inflight.add(a);
        a.start();
    }

    /** What an attempt brought back: none (no result), bad (a verified response that fails the carrier), or flags and data. */
    private record Result(boolean ok, boolean bad, int flags, ByteBuf data) {
        static final Result NONE = new Result(false, false, 0, null);
        static final Result BAD = new Result(true, true, 0, null);
    }

    /** One HTTP exchange of one request. */
    private final class Attempt {
        final int kind;
        final long seq;
        final State st;
        final byte[] nonce = new byte[Exchange.NONCE];
        final ByteBuffer seqRaw;
        final URI uri;
        ByteBuf body;
        /** The loop's: the result was taken (or the attempt cancelled). */
        boolean finished;
        ScheduledFuture<?> timeout;
        volatile Channel channel;
        volatile boolean cancelled;

        Attempt(int kind, long seq, State st, ByteBuf data, int flags, long ackSent) {
            this.kind = kind; this.seq = seq; this.st = st;
            RANDOM.nextBytes(nonce);
            ByteBuffer d = data == null ? ByteBuffer.allocate(0) : data.nioBuffer();
            ByteBuffer hdr = Exchange.requestHeader(key, cid, kind, flags, seq, ackSent, (int) (System.currentTimeMillis() / 1000), nonce, d);
            seqRaw = hdr.slice(Exchange.CID + 2, VarInt.length(seq));
            ByteBuf b = data == null ? Unpooled.wrappedBuffer(hdr) : Unpooled.wrappedBuffer(Unpooled.wrappedBuffer(hdr), data.retainedDuplicate());
            if (cfg.base64()) {
                ByteBuffer bin = ByteBuffer.allocate(b.readableBytes());
                b.getBytes(b.readerIndex(), bin);
                b.release();
                b = Unpooled.wrappedBuffer(Base64.getEncoder().encode(bin.flip()));
            }
            body = b;
            uri = (kind == Exchange.KIND_DOWN ? downTarget : upTarget).build(nonce);
        }

        void start() {
            timeout = loop.schedule(() -> finish(Result.NONE), cfg.requestTimeout().toNanos(), TimeUnit.NANOSECONDS);
            http.conns.open(uri, true, false).whenComplete((ch, e) -> {
                ByteBuf b = body;
                body = null;
                if (e != null) { b.release(); loop.execute(() -> finish(Result.NONE)); return; }
                channel = ch;
                if (cancelled) { b.release(); ch.close(); return; }
                ChannelLoops.executor(ch).execute(() -> send(ch, b));
            });
        }

        private void send(Channel ch, ByteBuf b) {
            FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, HttpCarrierClient.target(uri), b);
            req.headers().set(HttpHeaderNames.HOST, HttpCarrierClient.authority(uri));
            cfg.header().forEach((k, vs) -> { for (String v : vs) req.headers().add(k, v); });
            req.headers().set(HttpHeaderNames.CONTENT_TYPE, Requests.OCTETS).set(Requests.CACHE, Requests.NO_STORE)
                    .set(HttpHeaderNames.PRAGMA, "no-cache");
            HttpUtil.setContentLength(req, b.readableBytes());
            ch.pipeline().addLast(new Collector(this, ch));
            ch.writeAndFlush(req).addListener(f -> { if (!f.isSuccess()) ch.close(); });
        }

        /** Ends the attempt without taking its result (the carrier ended or the downlink closed). */
        void cancel() {
            finished = true;
            cancelled = true;
            if (timeout != null) timeout.cancel(false);
            Channel ch = channel;
            if (ch != null) ch.close();
        }

        /** Takes the attempt's result on the loop. */
        void finish(Result r) {
            if (finished) { if (r.data != null) r.data.release(); return; }
            finished = true;
            if (timeout != null) timeout.cancel(false);
            if (r == Result.NONE) { Channel ch = channel; if (ch != null) ch.close(); }
            inflight.remove(this);
            if (kind == Exchange.KIND_CLOSE) { if (r.data != null) r.data.release(); return; }
            apply(this, r);
            poke();
        }

        /** Verifies a complete response body (encoded); its data stays in body. */
        Result result(ByteBuf raw) {
            ByteBuf b = raw;
            if (cfg.base64()) {
                ByteBuffer dec = Exchange.decodeBase64(raw.nioBuffer());
                raw.release();
                if (dec == null) return Result.NONE;
                b = Unpooled.wrappedBuffer(dec);
            }
            if (b.readableBytes() < Exchange.RESP_HDR) { b.release(); return Result.NONE; }
            byte[] mac = new byte[Exchange.MAC];
            b.getBytes(b.readerIndex(), mac);
            int fl = b.getUnsignedByte(b.readerIndex() + Exchange.MAC);
            ByteBuf data = b.slice(b.readerIndex() + Exchange.RESP_HDR, b.readableBytes() - Exchange.RESP_HDR);
            byte[] want = key.responseMac(cid, kind, seqRaw, nonce, fl, data.nioBuffer());
            if (!MessageDigest.isEqual(want, mac)) { b.release(); return Result.NONE; }
            if (!Exchange.responseValid(kind, fl, data.readableBytes())) { b.release(); return Result.BAD; }
            if (!data.isReadable()) { b.release(); return new Result(true, false, fl, null); }
            return new Result(true, false, fl, data);
        }
    }

    /** Reads one attempt's response on its channel; a complete HTTP/1.1 exchange gives the connection back. */
    private final class Collector extends ChannelInboundHandlerAdapter {
        private final Attempt a;
        private final Channel ch;
        private ByteBuf body;
        private long length = -1;
        private boolean keepAlive, done;

        Collector(Attempt a, Channel ch) { this.a = a; this.ch = ch; }

        private int limit() {
            int n = Exchange.RESP_HDR + cfg.downLimit();
            return cfg.base64() ? Exchange.base64Length(n) : n;
        }

        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
            try {
                if (done) return;
                if (msg instanceof HttpResponse r) {
                    if (r.status().code() / 100 == 1) return; // an interim response
                    length = HttpUtil.getContentLength(r, -1L);
                    keepAlive = HttpUtil.isKeepAlive(r);
                    if (r.status().code() != 200 || length < 0) { result(Result.NONE, false); return; }
                    // Over the local limit: a configuration mismatch, never repeated.
                    if (length > limit()) { result(Result.BAD, false); return; }
                    body = ctx.alloc().buffer((int) length, (int) length);
                }
                if (msg instanceof HttpContent c && body != null) {
                    if (c.content().readableBytes() > body.writableBytes()) { result(Result.NONE, false); return; }
                    body.writeBytes(c.content());
                    if (c instanceof LastHttpContent) {
                        if (body.readableBytes() != length) { result(Result.NONE, false); return; }
                        ByteBuf b = body;
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
            if (body != null) { body.release(); body = null; }
            ch.pipeline().remove(this);
            if (reusable && !a.cancelled) http.conns.release(a.uri, ch, true);
            else ch.close();
            loop.execute(() -> a.finish(r));
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
        if (ended) { if (r.data != null) r.data.release(); return; }
        long now = System.nanoTime();
        if (!r.ok) { retry(st, now); return; }
        if (r.bad) { fail(new ExchangeException(PROTOCOL)); return; }
        if (r.flags == Exchange.FLAG_GONE) { gone = true; fail(new ExchangeException(GONE)); return; }
        st.since = 0;
        if (r.flags == Exchange.FLAG_BUSY) {
            retry(st, now);
            if (a.kind == Exchange.KIND_UP) upLimit = Math.max(1, upLimit - 1);
            else downLimit = Math.max(1, downLimit - 1);
            return;
        }
        st.backoff = cfg.backoffMin().toNanos();
        if (a.kind == Exchange.KIND_UP) {
            upLimit = Math.min(cfg.upRequests(), upLimit + 1);
            for (Iterator<Up> it = ups.iterator(); it.hasNext(); ) {
                Up u = it.next();
                if (u.seq == a.seq) {
                    if (u.data != null) u.data.release();
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
        long half = st.backoff / 2;
        st.retryAt = now + half + ThreadLocalRandom.current().nextLong(half + 1);
        st.backoff = Math.min(2 * st.backoff, cfg.backoffMax().toNanos());
    }

    /** Takes downlink piece n. */
    private void receive(long n, ByteBuf data, boolean isFin) {
        downs.remove(n);
        if (held.containsKey(n) || n < ack || fin >= 0 && n > fin || rclosed) {
            if (data != null) data.release();
            return;
        }
        if (isFin && (fin < 0 || n < fin)) {
            fin = n;
            for (Iterator<Map.Entry<Long, Piece>> it = held.entrySet().iterator(); it.hasNext(); ) {
                var e = it.next();
                if (e.getKey() > n) { if (e.getValue().data != null) e.getValue().data.release(); it.remove(); }
            }
        }
        held.put(n, new Piece(data, isFin));
        while (held.containsKey(ack) && !(fin >= 0 && ack > fin)) ack++;
        deliver();
    }

    /** Hands the pieces in number order to zmux, within its receive memory; then the end or the failure. */
    private void deliver() {
        while (sink != null) {
            if (err != null) {
                PipeReader.Sink s = sink;
                sink = null;
                s.onError(err);
                return;
            }
            if (fin >= 0 && deliv > fin) {
                PipeReader.Sink s = sink;
                sink = null;
                s.onEnd();
                return;
            }
            Piece p = held.get(deliv);
            if (p == null) return;
            int n = p.data == null ? 0 : p.data.readableBytes();
            if (n > 0 && outstanding > 0 && outstanding + n > receiveMemory()) return;
            held.remove(deliv);
            deliv++;
            poke(); // the read-ahead bound moved
            if (n == 0) continue;
            outstanding += n;
            ByteBuf b = p.data;
            sink.onChunk(Chunk.of(b.nioBuffer(), b::release), () -> loop.execute(() -> { outstanding -= n; deliver(); }));
        }
    }

    // ---- zmux's halves ----

    private final class Reader implements PipeReader {
        @Override public void start(Sink s) {
            loop.execute(() -> {
                if (rclosed) return;
                sink = s;
                deliver();
            });
        }

        /** Stops the downlink; the carrier ends once the uplink is done. */
        @Override public void close() {
            loop.execute(() -> {
                if (rclosed) return;
                rclosed = true;
                sink = null;
                dropHeld();
                for (State s : downs.values()) {
                    if (s.attempt != null) { Attempt a = s.attempt; a.cancel(); inflight.remove(a); s.inflight = false; s.attempt = null; }
                }
                downs.clear();
                poke();
            });
        }
    }

    private final class Writer implements PipeWriter {
        @Override public void write(OutboundBatch batch, Completion done) {
            loop.execute(() -> {
                if (err != null || wclosed || ended) { done.failed(err != null ? err : new ClosedChannelException()); return; }
                wBatch = batch;
                wDone = done;
                wSeg = 0;
                wOff = 0;
                take();
            });
        }

        /** Ends the uplink gracefully: the pieces written are sent, the last one with FIN. */
        @Override public void close() {
            loop.execute(() -> { wclosed = true; poke(); });
        }

        /** Fails the carrier; the server is told with a CLOSE request. */
        @Override public void abort(Throwable cause) {
            loop.execute(() -> fail(cause == null ? new ClosedChannelException() : cause));
        }
    }
}
