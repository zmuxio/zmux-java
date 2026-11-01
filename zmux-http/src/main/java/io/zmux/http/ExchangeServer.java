package io.zmux.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.concurrent.ScheduledFuture;
import io.zmux.CarrierKind;
import io.zmux.Chunk;
import io.zmux.JoinHint;
import io.zmux.carrier.Framing;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import io.zmux.helper.ReplayRecord;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The server of exchange carriers: authenticates each
 * request, creates a carrier for an unknown Carrier ID and hands it to the listener, and answers
 * UP, DOWN and CLOSE requests. Each carrier's state is owned by one event loop; requests arriving
 * on other channels hop there after their authentication. A request that is not authenticated
 * goes to the host's handler with its body replayed and changes nothing.
 */
final class ExchangeServer {
    /** What an out-of-order UP piece counts against the uplink store limit besides its bytes; bounds how many are held. */
    static final int PIECE_COST = 64;
    private final HttpEndpoint ep;
    final ExchangeConfig cfg;
    private final ReplayRecord nonces;
    private final ConcurrentHashMap<ByteBuffer, Carrier> carriers = new ConcurrentHashMap<>();
    /** Ended carriers remembered to answer GONE: key and expiry (System.nanoTime). */
    private final ConcurrentHashMap<ByteBuffer, Gone> gone = new ConcurrentHashMap<>();
    private volatile boolean closed;
    /** The clock of the time window, in seconds since 1970 (tests set it). */
    volatile java.util.function.LongSupplier seconds = () -> System.currentTimeMillis() / 1000;

    private record Gone(Exchange.Key key, long until) {}

    ExchangeServer(HttpEndpoint ep, ExchangeConfig cfg) {
        this.ep = ep;
        this.cfg = cfg;
        nonces = new ReplayRecord(cfg.nonceCacheSize());
    }

    /** The encoded body limit of a request. */
    private int limit() {
        int n = Exchange.REQ_HDR_MAX + cfg.upLimit();
        return cfg.base64() ? Exchange.base64Length(n) : n;
    }

    /**
     * Starts collecting the body of a request that {@link HttpClass#classify} found an exchange
     * request; null when its body exceeds the limit (the host's then).
     */
    Collect collect(Router router, HttpRequest req) {
        long cl = HttpUtil.getContentLength(req, -1L);
        if (cl > limit()) return null;
        return new Collect(router, req, (int) cl);
    }

    /** One exchange request's body being read, on its channel's event loop. */
    final class Collect {
        private final Router router;
        private final HttpRequest req;
        private final int length;
        /** The body read so far, allocated with its first bytes. */
        private ByteBuf body;

        Collect(Router router, HttpRequest req, int length) {
            this.router = router;
            this.req = req;
            this.length = length;
        }

        private ByteBuf body() {
            if (body == null) body = router.context().alloc().buffer(length, length);
            return body;
        }

        /** Adds body bytes; false when the body is longer than its Content-Length (b is then not taken). */
        boolean add(ByteBuf b) {
            if (b.readableBytes() > body().writableBytes()) return false;
            body.writeBytes(b, b.readerIndex(), b.readableBytes());
            return true;
        }

        /** The body ended (ended) or overflowed with extra, which goes to the host after it. */
        void complete(boolean ended, ByteBuf extra) {
            if (extra != null || !ended || body().readableBytes() != length) {
                router.toHost(req, extra == null ? List.of(body) : List.of(body, extra), ended);
                return;
            }
            serve(this);
        }

        void abandon() { if (body != null) body.release(); }

        /** The host's: the request with the body bytes read. */
        void fallback() {
            router.toHost(req, List.of(body()), true);
        }
    }

    /** Answers a complete request body. */
    private void serve(Collect c) {
        ByteBuf raw = c.body();
        ByteBuffer bin = raw.nioBuffer();
        if (cfg.base64()) {
            bin = Exchange.decodeBase64(bin);
            if (bin == null) { c.fallback(); return; }
        }
        Exchange.Request q = Exchange.parseRequest(bin);
        if (q == null) { c.fallback(); return; }
        ByteBuffer cid = ByteBuffer.wrap(q.cid());
        Carrier car = carriers.get(cid);
        Gone g = car == null ? gone(cid) : null;
        Exchange.Key key = car != null ? car.key : g != null ? g.key : Exchange.key(cfg.pskRef(), q.cid());
        if (!authentic(key, q)) { c.fallback(); return; }
        ChannelHandlerContext ctx = c.router.context();
        // The request's fields are views of the body: it lives until the answer was written.
        ByteBuf hold = cfg.base64() ? Unpooled.wrappedBuffer(bin) : raw;
        if (cfg.base64()) raw.release();
        Answer a = new Answer(ctx, c.req, key, q, hold);
        if (!q.valid()) {
            if (car != null) car.loop.execute(() -> car.end(new IOException("exchange carrier protocol violation")));
            a.send(Exchange.FLAG_GONE, null);
            return;
        }
        if (car != null) { car.loop.execute(() -> car.serve(a)); return; }
        if (g != null) { a.send(Exchange.FLAG_GONE, null); return; }
        if (q.kind() == Exchange.KIND_CLOSE) {
            // Creates none, and remembers the carrier as gone.
            remember(cid, key);
            a.send(0, null);
            return;
        }
        if (closed || ep.listener.isClosed()) { a.send(Exchange.FLAG_GONE, null); return; }
        Carrier nc = new Carrier(ctx.channel().eventLoop(), ctx.alloc(), q.cid(), key);
        Carrier old = carriers.putIfAbsent(cid, nc);
        if (old != null) { old.loop.execute(() -> old.serve(a)); return; } // created concurrently
        if (closed || gone(cid) != null) {
            // Ended (or the server closed) between the lookup and the creation.
            carriers.remove(cid, nc);
            a.send(Exchange.FLAG_GONE, null);
            return;
        }
        var slot = ep.listener.reserve(Requests.peer(ctx.channel()), Requests.source(ctx.channel(), c.req, ep.trusted));
        if (slot == null) {
            // Beyond the source's share of pending carriers: no state is kept and the carrier is
            // not remembered as gone; the request consumed nothing.
            carriers.remove(cid, nc);
            a.send(Exchange.FLAG_BUSY, null);
            return;
        }
        PipeIo io = new PipeIo(nc.new Reader(), nc.new Writer(), Framing.BYTE_STREAM, "http exchange", nc)
                .withReceiveMemory(cfg.upStoreLimit()).withCarrier(CarrierKind.EXCHANGE);
        slot.handle(io, JoinHint.inBand(slot.peer()), null);
        nc.idle();
        nc.loop.execute(() -> nc.serve(a));
    }

    /** Authenticates a request: the MAC, the time window, the nonce record. */
    private boolean authentic(Exchange.Key key, Exchange.Request q) {
        if (!Exchange.verify(key, q)) return false;
        long now = seconds.getAsLong();
        long skew = cfg.maxClockSkew().toSeconds();
        long d = q.time() - (int) now; // the int difference, modulo 2^32
        if (d < -skew || d > skew) return false;
        byte[] k = new byte[Exchange.CID + Exchange.NONCE];
        System.arraycopy(q.cid(), 0, k, 0, Exchange.CID);
        System.arraycopy(q.nonce(), 0, k, Exchange.CID, Exchange.NONCE);
        return nonces.add(now + d, k, now - skew);
    }

    private Gone gone(ByteBuffer cid) {
        Gone g = gone.get(cid);
        if (g != null && System.nanoTime() - g.until > 0) { gone.remove(cid, g); return null; }
        return g;
    }

    /** Remembers a Carrier ID as gone, for late requests. */
    private void remember(ByteBuffer cid, Exchange.Key key) {
        gone.putIfAbsent(cid, new Gone(key, System.nanoTime() + cfg.goneMemory().toNanos()));
        if (gone.size() > 1024) gone.values().removeIf(x -> System.nanoTime() - x.until > 0);
    }

    /** The carriers with state, for tests. */
    int live() { return carriers.size(); }

    void close() {
        closed = true;
        for (Carrier c : carriers.values()) c.loop.execute(() -> c.end(new ClosedChannelException()));
    }

    /**
     * The answer to one authenticated request, written on its own channel. An HTTP/1.1
     * connection reads no further request until it is written, so that answers keep the order
     * of their requests.
     */
    private final class Answer {
        final ChannelHandlerContext ctx;
        final HttpRequest req;
        final Exchange.Key key;
        final Exchange.Request q;
        /** The request's Sequence as sent: a copy, as an UP piece's body may be freed before the answer. */
        private final ByteBuffer seqRaw;
        private ByteBuf hold;
        private final boolean h1;

        Answer(ChannelHandlerContext ctx, HttpRequest req, Exchange.Key key, Exchange.Request q, ByteBuf hold) {
            this.ctx = ctx; this.req = req; this.key = key; this.q = q; this.hold = hold;
            ByteBuffer s = ByteBuffer.allocate(q.seqRaw().remaining());
            seqRaw = s.put(q.seqRaw().duplicate()).flip();
            h1 = !(ctx.channel() instanceof io.netty.handler.codec.http2.Http2StreamChannel);
            if (h1) ctx.channel().config().setAutoRead(false);
        }

        /** Takes the request's body (the UP piece's bytes) out of the answer. */
        ByteBuf takeBody() {
            ByteBuf b = hold;
            hold = null;
            return b;
        }

        /** Writes the response with flags and data (a piece, not consumed). */
        void send(int flags, ByteBuf data) {
            ByteBuffer d = data == null ? ByteBuffer.allocate(0) : data.nioBuffer();
            ByteBuffer hdr = ByteBuffer.allocate(Exchange.RESP_HDR);
            hdr.put(key.responseMac(q.cid(), q.kind(), seqRaw, q.nonce(), flags, d)).put((byte) flags).flip();
            ByteBuf body = data == null ? Unpooled.wrappedBuffer(hdr) : Unpooled.wrappedBuffer(Unpooled.wrappedBuffer(hdr), data.retainedDuplicate());
            if (cfg.base64()) {
                ByteBuffer bin = ByteBuffer.allocate(body.readableBytes());
                body.getBytes(body.readerIndex(), bin);
                body.release();
                body = Unpooled.wrappedBuffer(Base64.getEncoder().encode(bin.flip()));
            }
            FullHttpResponse resp = new DefaultFullHttpResponse(req.protocolVersion(), HttpResponseStatus.OK, body);
            resp.headers().set(HttpHeaderNames.CONTENT_TYPE, Requests.OCTETS).set(Requests.CACHE, Requests.NO_STORE);
            HttpUtil.setContentLength(resp, body.readableBytes());
            boolean keep = HttpUtil.isKeepAlive(req);
            HttpUtil.setKeepAlive(resp, keep);
            var f = ctx.writeAndFlush(resp);
            if (h1) {
                if (keep) f.addListener(x -> ctx.channel().config().setAutoRead(true));
                else f.addListener(io.netty.channel.ChannelFutureListener.CLOSE);
            }
            if (hold != null) { hold.release(); hold = null; }
        }
    }

    /** An assigned downlink piece. */
    private record Piece(ByteBuf data, boolean fin) {}

    /** A held DOWN request. */
    private static final class Held {
        final Answer a;
        final long n;
        ScheduledFuture<?> timer;
        boolean expired;

        Held(Answer a, long n) { this.a = a; this.n = n; }
    }

    /** One exchange carrier on the server; every field is its loop's. */
    final class Carrier {
        final EventLoop loop;
        private final ByteBufAllocator alloc;
        final byte[] cid;
        final Exchange.Key key;
        private boolean ended;
        private Throwable err;
        private long last = System.nanoTime();
        private ScheduledFuture<?> idleTimer, coalesceTimer;

        // Uplink.
        private long upNext, upMax, upFin = -1;
        private final Map<Long, ByteBuf> upOoo = new HashMap<>();
        private final ArrayDeque<ByteBuf> upQ = new ArrayDeque<>();
        /** Bytes stored (upOoo, upQ), and handed to zmux and not yet consumed. */
        private long upLen, delivered;
        /** What the out-of-order entries of upOoo count besides their bytes: PIECE_COST each. */
        private long upOooCost;
        private boolean rclosed;
        private PipeReader.Sink sink;

        // Downlink.
        private long ack, asgEnd, fill, finLow = -1;
        private final Map<Long, Piece> pieces = new HashMap<>();
        private ByteBuf cur;
        private long curAt;
        private boolean wclosed;
        private final List<Held> held = new ArrayList<>();
        /** A write waiting for the window: its completion. */
        private PipeWriter.Completion blocked;
        private OutboundBatch blockedBatch;
        private int blockedAt;

        Carrier(EventLoop loop, ByteBufAllocator alloc, byte[] cid, Exchange.Key key) {
            this.loop = loop; this.alloc = alloc; this.cid = cid; this.key = key;
        }

        void idle() {
            loop.execute(() -> idleTimer = loop.schedule(this::onIdle, cfg.idleTimeout().toNanos(), TimeUnit.NANOSECONDS));
        }

        private void onIdle() {
            if (ended) return;
            long rest = cfg.idleTimeout().toNanos() - (System.nanoTime() - last);
            if (held.isEmpty() && rest <= 0) {
                // Both FINs on their way: released as complete.
                boolean complete = upFin >= 0 && upNext > upFin && finLow >= 0;
                end(complete ? null : new IOException("exchange carrier idle"));
                return;
            }
            idleTimer = loop.schedule(this::onIdle, Math.max(rest, 1_000_000), TimeUnit.NANOSECONDS);
        }

        /** Ends the carrier (null: completed) and releases what it holds but uplink bytes zmux may still read. */
        void end(Throwable e) {
            if (ended) return;
            ended = true;
            err = e;
            if (idleTimer != null) idleTimer.cancel(false);
            if (coalesceTimer != null) coalesceTimer.cancel(false);
            for (Piece p : pieces.values()) if (p.data != null) p.data.release();
            pieces.clear();
            if (cur != null) { cur.release(); cur = null; }
            for (Held h : held) { if (h.timer != null) h.timer.cancel(false); h.a.send(Exchange.FLAG_GONE, null); }
            held.clear();
            if (e != null) dropUplink();
            ByteBuffer id = ByteBuffer.wrap(cid);
            carriers.remove(id, this);
            remember(id, key);
            if (blocked != null) { blocked.failed(new ClosedChannelException()); blocked = null; blockedBatch = null; }
            deliver();
        }

        private void dropUplink() {
            for (ByteBuf b : upQ) b.release();
            upQ.clear();
            for (ByteBuf b : upOoo.values()) if (b != null) b.release();
            upOoo.clear();
            upLen = 0;
            upOooCost = 0;
        }

        private boolean complete() {
            if (upFin >= 0 && upNext > upFin && finLow >= 0 && ack > finLow) { end(null); return true; }
            return false;
        }

        /** Serves an authenticated request. */
        void serve(Answer a) {
            Exchange.Request q = a.q;
            last = System.nanoTime();
            if (ended) { a.send(Exchange.FLAG_GONE, null); return; }
            if (q.ack() > ack) {
                if (q.ack() > asgEnd) {
                    end(new IOException("exchange carrier protocol violation"));
                    a.send(Exchange.FLAG_GONE, null);
                    return;
                }
                for (Iterator<Map.Entry<Long, Piece>> it = pieces.entrySet().iterator(); it.hasNext(); ) {
                    var e = it.next();
                    if (e.getKey() < q.ack()) { if (e.getValue().data != null) e.getValue().data.release(); it.remove(); }
                }
                ack = q.ack();
                unblock();
            }
            complete();
            if (ended) {
                // Completed by this acknowledgement: the request is a repetition.
                a.send(q.kind() == Exchange.KIND_DOWN ? Exchange.FLAG_GONE : 0, null);
                return;
            }
            switch (q.kind()) {
                case Exchange.KIND_UP -> {
                    int flags = up(a);
                    complete();
                    a.send(ended && err != null ? Exchange.FLAG_GONE : flags, null);
                }
                case Exchange.KIND_CLOSE -> {
                    end(new IOException("exchange carrier closed by the peer"));
                    a.send(0, null);
                }
                default -> down(a);
            }
        }

        /** Stores an UP piece; returns the response flags. */
        private int up(Answer a) {
            Exchange.Request q = a.q;
            long n = q.seq();
            boolean fin = q.fin();
            if (fin && (upFin >= 0 && upFin != n || upMax > n + 1) || !fin && upFin >= 0 && n > upFin) {
                end(new IOException("exchange carrier protocol violation"));
                return Exchange.FLAG_GONE;
            }
            if (n < upNext || upOoo.containsKey(n)) return 0;
            ByteBuf piece = null;
            int len = q.data().remaining();
            boolean ooo = n != upNext;
            int stored = rclosed ? 0 : len;
            if (ooo) {
                // Out of order: leave room for the piece zmux waits for, and count each held piece
                // with a fixed overhead so that empty pieces cannot be held without bound.
                long room = cfg.upStoreLimit() - cfg.upLimit();
                if (upLen + delivered + upOooCost + stored + PIECE_COST > room) return Exchange.FLAG_BUSY;
            } else if (stored > 0 && upLen + delivered + upOooCost + stored > cfg.upStoreLimit()) {
                return Exchange.FLAG_BUSY;
            }
            if (stored > 0) {
                ByteBuf body = a.takeBody();
                // The data is the end of the request body: a slice of it, zero copy.
                piece = body.slice(body.readerIndex() + body.readableBytes() - len, len);
                upLen += len;
            }
            if (fin) upFin = n;
            upMax = Math.max(upMax, n + 1);
            if (ooo) { upOoo.put(n, piece); upOooCost += PIECE_COST; return 0; }
            if (piece != null) upQ.add(piece);
            upNext++;
            for (ByteBuf x; upOoo.containsKey(upNext); upNext++) {
                x = upOoo.remove(upNext);
                upOooCost -= PIECE_COST;
                if (x != null) upQ.add(x);
            }
            deliver();
            return 0;
        }

        /** Hands the contiguous uplink to zmux, within its receive memory. */
        private void deliver() {
            if (sink == null) return;
            while (!upQ.isEmpty() && (delivered == 0 || delivered + upQ.getFirst().readableBytes() <= cfg.upStoreLimit())) {
                ByteBuf b = upQ.removeFirst();
                int n = b.readableBytes();
                upLen -= n;
                delivered += n;
                sink.onChunk(Chunk.of(b.nioBuffer(), b::release), () -> loop.execute(() -> {
                    delivered -= n;
                    deliver();
                }));
                if (sink == null) return;
            }
            if (!upQ.isEmpty()) return;
            PipeReader.Sink s = sink;
            if (ended && err != null) { sink = null; s.onError(err); }
            else if (upFin >= 0 && upNext > upFin) { sink = null; s.onEnd(); }
        }

        /** Answers a DOWN request, now or once its piece is assigned. */
        private void down(Answer a) {
            Held h = new Held(a, a.q.seq());
            if (answer(h)) return;
            held.add(h);
            h.timer = loop.schedule(() -> { h.expired = true; if (held.remove(h) && !answer(h)) held.add(h); },
                    cfg.holdTime().toNanos(), TimeUnit.NANOSECONDS);
            coalesce();
            answerHeld();
        }

        /** Answers h when its piece can be given now; false: it waits. */
        private boolean answer(Held h) {
            long n = h.n;
            Piece p = pieces.get(n);
            int flags = -1;
            if (ended) flags = Exchange.FLAG_GONE;
            else if (n < ack) flags = Exchange.FLAG_BUSY;
            else if (p == null && n >= ack + cfg.downWindow()) flags = Exchange.FLAG_BUSY;
            else if (p == null && wclosed && (cur == null || !cur.isReadable())) p = assign(n, null, true);
            else if (p == null && h.expired) {
                // The hold time ran out: the open piece, or an empty one.
                p = n == fill && cur != null && cur.isReadable() ? assignCur() : assign(n, null, false);
            }
            if (flags < 0 && p == null) return false;
            if (h.timer != null) h.timer.cancel(false);
            last = System.nanoTime();
            if (p == null) h.a.send(flags, null);
            else h.a.send(p.fin ? Exchange.FLAG_FIN : 0, p.data);
            return true;
        }

        private void answerHeld() {
            held.removeIf(this::answer);
        }

        private Piece assign(long n, ByteBuf data, boolean fin) {
            Piece p = new Piece(data, fin);
            pieces.put(n, p);
            asgEnd = Math.max(asgEnd, n + 1);
            if (fin && (finLow < 0 || n < finLow)) finLow = n;
            while (pieces.containsKey(fill)) fill++;
            return p;
        }

        private Piece assignCur() {
            ByteBuf b = cur;
            cur = null;
            return assign(fill, b, wclosed);
        }

        /** Assigns piece fill when full, or when a request can take it and its coalescing delay ran out; else arms the timer. */
        private void coalesce() {
            if (cur == null || !cur.isReadable() || ended) return;
            if (cur.readableBytes() >= cfg.downPieceSize() || wclosed) { assignCur(); answerHeld(); return; }
            if (held.isEmpty()) return;
            long due = curAt + cfg.coalesce().toNanos() - System.nanoTime();
            if (due <= 0) { assignCur(); answerHeld(); return; }
            if (coalesceTimer == null || coalesceTimer.isDone()) {
                coalesceTimer = loop.schedule(this::coalesce, due, TimeUnit.NANOSECONDS);
            }
        }

        /** Takes zmux's bytes into the open piece while the window allows. */
        private void take() {
            OutboundBatch b = blockedBatch;
            if (b == null) return;
            while (blockedAt < b.segmentCount()) {
                ByteBuffer seg = b.segment(blockedAt);
                int rem = seg.remaining() - segOffset;
                if (rem <= 0) { blockedAt++; segOffset = 0; continue; }
                if (fill >= ack + cfg.downWindow()) return; // the window is full: the write waits
                if (cur == null) { cur = alloc.buffer(Math.min(cfg.downPieceSize(), 64 << 10)); curAt = System.nanoTime(); }
                int n = Math.min(rem, cfg.downPieceSize() - cur.readableBytes());
                cur.writeBytes(seg.slice(seg.position() + segOffset, n));
                segOffset += n;
                if (cur.readableBytes() >= cfg.downPieceSize()) { assignCur(); answerHeld(); }
            }
            PipeWriter.Completion c = blocked;
            blocked = null;
            blockedBatch = null;
            coalesce();
            c.completed();
        }

        private int segOffset;

        private void unblock() { if (blocked != null) take(); }

        final class Reader implements PipeReader {
            @Override public void start(Sink s) { loop.execute(() -> { if (!rclosed) { sink = s; deliver(); } }); }

            @Override public void close() {
                loop.execute(() -> {
                    rclosed = true;
                    sink = null;
                    dropUplink();
                });
            }
        }

        final class Writer implements PipeWriter {
            @Override public void write(OutboundBatch batch, Completion done) {
                loop.execute(() -> {
                    if (ended || wclosed) { done.failed(new ClosedChannelException()); return; }
                    blocked = done;
                    blockedBatch = batch;
                    blockedAt = 0;
                    segOffset = 0;
                    take();
                });
            }

            @Override public void close() {
                loop.execute(() -> {
                    if (wclosed || ended) return;
                    wclosed = true;
                    if (cur != null && cur.isReadable()) assignCur();
                    answerHeld(); // held requests take an empty FIN piece
                    complete();
                });
            }

            @Override public void abort(Throwable cause) {
                loop.execute(() -> end(cause == null ? new ClosedChannelException() : cause));
            }
        }
    }
}
