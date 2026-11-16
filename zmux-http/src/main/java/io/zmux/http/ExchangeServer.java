package io.zmux.http;

import io.zmux.internal.spi.LentInput;
import io.zmux.internal.spi.Loops;
import io.zmux.ReplayVerdict;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import java.util.concurrent.ScheduledFuture;
import io.zmux.CarrierKind;
import io.zmux.Chunk;
import io.zmux.JoinHint;
import io.zmux.carrier.Framing;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import io.zmux.ReplayFilter;
import javax.crypto.Mac;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
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
    /** What a refused call fails besides its own completion or sink: nothing; a carrier goes with its loop. */
    private static final Runnable NOTHING = () -> {};
    private final HttpEndpoint ep;
    final ExchangeConfig cfg;
    private final ReplayFilter nonces;
    private final ConcurrentHashMap<ByteBuffer, Carrier> carriers = new ConcurrentHashMap<>();
    /** Ended carriers remembered to answer GONE: key and expiry (System.nanoTime). */
    private final GoneTable<Gone> gone;
    private volatile boolean closed;
    /** The clock of the time window, in seconds since 1970 (tests set it). */
    volatile java.util.function.LongSupplier seconds = () -> System.currentTimeMillis() / 1000;

    private record Gone(Exchange.Key key) {}

    ExchangeServer(HttpEndpoint ep, ExchangeConfig cfg) {
        this.ep = ep;
        this.cfg = cfg;
        gone = new GoneTable<>(cfg.goneMemory().toNanos(), System::nanoTime);
        nonces = new ReplayFilter(cfg.maxClockSkew().plusSeconds(1), cfg.nonceCacheSize(), null, new java.time.Clock() {
            @Override public java.time.ZoneId getZone() { return java.time.ZoneOffset.UTC; }
            @Override public java.time.Clock withZone(java.time.ZoneId z) { return this; }
            @Override public java.time.Instant instant() { return java.time.Instant.ofEpochSecond(seconds.getAsLong()); }
        });
    }

    /** The encoded body limit of a request. */
    private int limit() {
        long n = (long) Exchange.REQ_OVERHEAD + cfg.upLimit();
        return (int) Math.min(Integer.MAX_VALUE, cfg.isBase64() ? Exchange.base64Length((int) n) : n);
    }

    /** The encoded length of a body with Data of n bytes. */
    private long encoded(long n) {
        long b = Exchange.REQ_OVERHEAD + n;
        return cfg.isBase64() ? (b + 2) / 3 * 4 : b;
    }

    /** The encoded length of the authentication header: 66 bytes, 88 characters in base64 (66 is a multiple of 3). */
    private int headerLength() { return cfg.isBase64() ? Exchange.base64Length(Exchange.HDR) : Exchange.HDR; }

    /**
     * Starts reading a request that {@link HttpClass#classify} found an exchange request; null
     * when the request is not collected: with no slot of its source or of the listener it is
     * answered 503 with no body, its body unread and never given to the host; with a
     * Content-Length below a body of the header and the Data MAC or above the limit it goes to
     * the host as it is. A request to a closed listener holds no slot: it is still
     * authenticated, to be answered GONE.
     */
    // reserve returns null when the source or the listener has no slot left; it lives in another module, where the analyzer takes its result for a constant.
    @SuppressWarnings("ConstantValue")
    Collect collect(Router router, HttpRequest req) {
        var ch = router.context().channel();
        var slot = ep.listener.adapter().reserve(Requests.peer(ch), Requests.source(ch, req, ep.trusted));
        if (slot == null && !ep.listener.isClosed()) {
            router.refuse(req, HttpResponseStatus.SERVICE_UNAVAILABLE);
            return null;
        }
        long cl = HttpUtil.getContentLength(req, -1L);
        if (cl < encoded(0) || cl > limit()) {
            if (slot != null) slot.release();
            router.toHost(req, List.of(), false);
            return null;
        }
        return new Collect(router, req, (int) cl, slot);
    }

    /**
     * One exchange request being read, on its channel's event loop. It holds a slot of the
     * listener from its arrival: the authentication header is read and verified within the
     * listener's first-record timeout, and only then is the rest of the body read, within the
     * handshake timeout. The buffer grows with the bytes received, never with a declared length.
     */
    final class Collect {
        private final Router router;
        private final HttpRequest req;
        private final int length;
        /**
         * The listener's slots of the request: given back once its header is authenticated, unless it creates a carrier;
         * null for a request that arrived at a closed listener.
         */
        private io.zmux.adapter.Slot slot;
        /** The body read so far, encoded, in write mode; a heap buffer that grows with what arrives. */
        private ByteBuffer body;
        private ScheduledFuture<?> timer;
        /** Set once the header is authenticated. */
        private Exchange.Header h;
        private Exchange.Key key;
        /** A MAC keyed with key: the header's, then the data's. */
        private Mac mac;
        private Carrier car;
        private Gone g;
        private boolean done;

        Collect(Router router, HttpRequest req, int length, io.zmux.adapter.Slot slot) {
            this.router = router;
            this.req = req;
            this.length = length;
            this.slot = slot;
            body = ByteBuffer.allocate(headerLength());
            timer = schedule(this::firstRecordTimedOut, ep.listener.adapter().firstRecordTimeout().toNanos());
        }

        /** Runs task on the channel's event loop after nanos. */
        private ScheduledFuture<?> schedule(Runnable task, long nanos) {
            // The executor is the channel's event loop, owned and closed by Netty.
            //noinspection resource
            return Loops.schedule(router.context().executor(), task, nanos, TimeUnit.NANOSECONDS, NOTHING); // refused: the channel goes with its loop
        }

        /** Takes body bytes (not consumed); last: the body ended. */
        void add(ByteBuf b, boolean last) {
            if (done) return;
            int n = b.readableBytes();
            if (n > length - body.position()) {
                // Longer than its Content-Length: the host's before the authentication, else refused.
                if (h == null) {
                    end();
                    releaseSlot();
                    router.toHost(req, List.of(Unpooled.wrappedBuffer(body.flip()), b.retainedSlice()), last);
                } else answerError(HttpResponseStatus.BAD_REQUEST, true);
                return;
            }
            body = Exchange.append(body, b, b.readerIndex(), n, length);
            if (h == null && body.position() >= headerLength() && !authenticate()) { fallback(last); return; }
            if (body.position() == length) { complete(); return; }
            if (last) {
                // The body ended early.
                if (h == null) fallback(true); else answerError(HttpResponseStatus.BAD_REQUEST, false);
            }
        }

        private void firstRecordTimedOut() {
            if (!done && h == null) fallback(false);
        }

        private void handshakeTimedOut() {
            if (!done) answerError(HttpResponseStatus.BAD_REQUEST, true);
        }

        /** Gives the slots back, unless the carrier took them. */
        private void releaseSlot() {
            if (slot != null) { slot.release(); slot = null; }
        }

        private void end() {
            done = true;
            if (timer != null) { timer.cancel(false); timer = null; }
            router.endCollect(this);
        }

        /** The channel ended or the router gave the request up: what it holds goes. */
        void abandon() {
            if (done) return;
            done = true;
            if (timer != null) { timer.cancel(false); timer = null; }
            releaseSlot();
        }

        /** Not authenticated: the host's, with every byte read replayed; ended: the body ended. */
        private void fallback(boolean ended) {
            end();
            releaseSlot();
            router.toHost(req, List.of(Unpooled.wrappedBuffer(body.flip())), ended);
        }

        /** Authenticated, the rest failed: nothing changes; close: the body was not read to its end. */
        private void answerError(HttpResponseStatus status, boolean close) {
            end();
            refuse(status, close);
        }

        /** Answers with status, without a body, and gives the slots back. */
        private void refuse(HttpResponseStatus status, boolean close) {
            releaseSlot();
            FullHttpResponse resp = new DefaultFullHttpResponse(req.protocolVersion(), status);
            HttpUtil.setContentLength(resp, 0);
            boolean keep = !close && HttpUtil.isKeepAlive(req);
            HttpUtil.setKeepAlive(resp, keep);
            var f = router.context().writeAndFlush(resp);
            if (!keep) f.addListener(io.netty.channel.ChannelFutureListener.CLOSE);
        }

        /** Authenticates the header (Section 7.1 steps 3 to 6); false: it is not. */
        private boolean authenticate() {
            // A copy: the header outlives the body buffer, which moves as it grows.
            ByteBuffer raw;
            if (cfg.isBase64()) {
                raw = Exchange.decodeBase64(body.slice(0, headerLength()));
                if (raw == null) return false;
            } else {
                byte[] copy = new byte[Exchange.HDR];
                body.get(0, copy);
                raw = ByteBuffer.wrap(copy);
            }
            Exchange.Header hd = Exchange.parseHeader(raw);
            ByteBuffer cid = ByteBuffer.wrap(hd.cid());
            Carrier c = carriers.get(cid);
            Gone gn = c == null ? gone(cid) : null;
            Exchange.Key k = c != null ? c.key : gn != null ? gn.key : Exchange.key(cfg.pskRef(), hd.cid());
            Mac m = k.newMac();
            if (!Exchange.verifyHeader(m, hd)) return false;
            if (!fresh(hd)) return false;
            if (hd.dataLength() > hd.dataLimit(cfg.upLimit()) || encoded(hd.dataLength()) != length) return false;
            h = hd; key = k; mac = m; car = c; g = gn;
            Loops.cancel(timer);
            timer = null;
            // A request that cannot create a carrier gives its slots back now.
            if (c != null || gn != null || hd.kind() == Exchange.KIND_CLOSE || !hd.valid((int) hd.dataLength())) releaseSlot();
            ep.listener.config().handshakeTimeout().ifPresent(d ->
                    timer = schedule(this::handshakeTimedOut, d.toNanos()));
            return true;
        }

        /** The whole body is read: verifies the Data MAC, then serves the request. */
        private void complete() {
            end();
            int dl = (int) h.dataLength();
            ByteBuffer data;
            ByteBuffer dataMac;
            if (cfg.isBase64()) {
                ByteBuffer rest = Exchange.decodeBase64(body.slice(headerLength(), length - headerLength()));
                if (rest == null || rest.remaining() != dl + Exchange.MAC) { failData(); return; }
                data = rest.slice(0, dl);
                dataMac = rest.slice(dl, Exchange.MAC);
            } else {
                data = body.slice(Exchange.HDR, dl);
                dataMac = body.slice(Exchange.HDR + dl, Exchange.MAC);
            }
            if (!Exchange.verifyData(mac, h, data.duplicate(), dataMac)) { failData(); return; }
            serve(this, data);
        }

        /** The Data or its MAC did not verify: nothing changes. */
        private void failData() { refuse(HttpResponseStatus.BAD_REQUEST, false); }
    }

    /** Serves a request whose Data MAC verified; data is its Data. */
    private void serve(Collect c, ByteBuffer data) {
        Exchange.Header q = c.h;
        ByteBuffer cid = ByteBuffer.wrap(q.cid());
        ChannelHandlerContext ctx = c.router.context();
        Answer a = new Answer(ctx, c.req, c.key, q, data);
        Carrier car = c.car;
        // The carrier may have ended or been created since the header was authenticated.
        if (car == null && c.g == null) car = carriers.get(cid);
        if (!q.valid(data.remaining())) {
            Carrier bad = car;
            if (bad != null) {
                Loops.run(bad.loop, () -> bad.end(new IOException("exchange carrier protocol violation")), NOTHING); // refused: gone with its loop
            }
            c.releaseSlot();
            a.send(Exchange.FLAG_GONE, null);
            return;
        }
        if (car != null) { c.releaseSlot(); serveOn(car, a); return; }
        if (c.g != null || gone(cid) != null) { c.releaseSlot(); a.send(Exchange.FLAG_GONE, null); return; }
        if (q.kind() == Exchange.KIND_CLOSE) {
            // Creates none, and remembers the carrier as gone.
            remember(cid, c.key);
            a.send(0, null);
            return;
        }
        // Arrived at a closed listener (or the server is closed): GONE. The listener closed since the request arrived: BUSY.
        if (closed || c.slot == null) { c.releaseSlot(); a.send(Exchange.FLAG_GONE, null); return; }
        if (ep.listener.isClosed()) { c.releaseSlot(); a.send(Exchange.FLAG_BUSY, null); return; }
        Carrier nc = new Carrier(ctx.channel().eventLoop(), q.cid(), c.key);
        Carrier old = carriers.putIfAbsent(cid, nc);
        if (old != null) { c.releaseSlot(); serveOn(old, a); return; } // created concurrently
        if (closed || gone(cid) != null) {
            // Ended (or the server closed) between the lookup and the creation.
            carriers.remove(cid, nc);
            c.releaseSlot();
            a.send(Exchange.FLAG_GONE, null);
            return;
        }
        nc.watchLoop();
        // The request's slots go with the carrier, until it enters a session.
        var slot = c.slot;
        c.slot = null;
        PipeIo io = new PipeIo(nc.new Reader(), nc.new Writer(), Framing.BYTE_STREAM, "http exchange", nc)
                .withReceiveMemory(cfg.upStoreLimit()).withCarrier(CarrierKind.EXCHANGE);
        // Ended before a session took it while the listener closed: the listener closed before the carrier was established.
        slot.handle(io, JoinHint.inBand(slot.peer()), null).taken().thenAccept(t -> {
            if (!t && ep.listener.isClosed()) {
                Loops.run(nc.loop, nc::refused, NOTHING); // refused: gone with its loop
            }
        });
        nc.idle();
        serveOn(nc, a);
    }

    /** Hands a to k on its loop; a carrier whose loop shut down is gone, and a gets GONE here. */
    private static void serveOn(Carrier k, Answer a) {
        Loops.run(k.loop, () -> k.serve(a), () -> a.send(Exchange.FLAG_GONE, null));
    }

    /** Section 7.1 steps 4 and 5: Time within the window, and the (Carrier ID, Nonce) pair recorded once. */
    @SuppressWarnings("IntLongMath") // the time difference is taken in 32 bits on purpose: modulo 2^32
    private boolean fresh(Exchange.Header q) {
        long now = seconds.getAsLong();
        long skew = cfg.maxClockSkew().toSeconds();
        long d = q.time() - (int) now; // the int difference, modulo 2^32
        if (d < -skew || d > skew) return false;
        byte[] k = new byte[Exchange.CID + Exchange.NONCE];
        System.arraycopy(q.cid(), 0, k, 0, Exchange.CID);
        System.arraycopy(q.nonce(), 0, k, Exchange.CID, Exchange.NONCE);
        return nonces.check(k, (now + d) * 1000) == ReplayVerdict.FRESH;
    }

    private Gone gone(ByteBuffer cid) {
        return gone.get(cid);
    }

    /** Remembers a Carrier ID as gone, for late requests. */
    private void remember(ByteBuffer cid, Exchange.Key key) {
        gone.remember(cid, new Gone(key));
    }

    /** The carriers with state, for tests. */
    int live() { return carriers.size(); }

    /** The carriers remembered as gone, for tests. */
    int remembered() { return gone.size(); }

    void close() {
        closed = true;
        for (Carrier c : carriers.values()) {
            // A loop that refuses it is shutting down: the carrier goes with it, and its requests' channels with their loops.
            Loops.run(c.loop, () -> c.end(new ClosedChannelException()), () -> carriers.remove(ByteBuffer.wrap(c.cid), c));
        }
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
        final Exchange.Header q;
        /** The request's Data length. */
        final int dataLength;
        /** The request's Data, until taken. */
        private ByteBuffer hold;
        private final boolean h1;

        Answer(ChannelHandlerContext ctx, HttpRequest req, Exchange.Key key, Exchange.Header q, ByteBuffer data) {
            this.ctx = ctx; this.req = req; this.key = key; this.q = q; this.hold = data;
            dataLength = data.remaining();
            h1 = !(ctx.channel() instanceof io.netty.handler.codec.http2.Http2StreamChannel);
            if (h1) {
                ctx.channel().config().setAutoRead(false);
                // Requests pipelined behind this one, already decoded, wait for its answer too.
                PipelineGate gate = PipelineGate.of(ctx);
                if (gate != null) gate.hold();
            }
        }

        /** Takes the request's Data (the UP piece's bytes) out of the answer. */
        ByteBuffer takeData() {
            ByteBuffer b = hold;
            hold = null;
            return b;
        }

        /**
         * Writes the response with flags and data (a piece, which may be sent again), on any
         * thread. A connection whose loop has shut down fails the write, which drops the response.
         */
        void send(int flags, ByteBuffer data) {
            hold = null;
            ByteBuffer d = data == null ? ByteBuffer.allocate(0) : data.duplicate();
            ByteBuffer hdr = ByteBuffer.allocate(Exchange.RESP_HDR);
            hdr.put(key.responseMac(q.cid(), q.kind(), q.seq(), q.nonce(), flags, d.duplicate())).put((byte) flags).flip();
            ByteBuf body;
            if (cfg.isBase64()) {
                ByteBuffer bin = ByteBuffer.allocate(hdr.remaining() + d.remaining()).put(hdr).put(d).flip();
                body = Unpooled.wrappedBuffer(Base64.getEncoder().encode(bin));
            } else {
                body = d.hasRemaining() ? Unpooled.wrappedBuffer(hdr, d) : Unpooled.wrappedBuffer(hdr);
            }
            FullHttpResponse resp = new DefaultFullHttpResponse(req.protocolVersion(), HttpResponseStatus.OK, body);
            resp.headers().set(HttpHeaderNames.CONTENT_TYPE, Requests.OCTETS).set(Requests.CACHE, Requests.NO_STORE);
            HttpUtil.setContentLength(resp, body.readableBytes());
            boolean keep = HttpUtil.isKeepAlive(req);
            HttpUtil.setKeepAlive(resp, keep);
            var f = ctx.writeAndFlush(resp);
            if (h1) {
                if (keep) f.addListener(x -> {
                    PipelineGate gate = PipelineGate.of(ctx);
                    if (gate != null) gate.release();
                    ctx.channel().config().setAutoRead(true);
                });
                else f.addListener(io.netty.channel.ChannelFutureListener.CLOSE);
            }
        }
    }

    /** An assigned downlink piece. */
    private record Piece(ByteBuffer data, boolean fin) {}

    /** A held DOWN request. */
    private static final class Held {
        final Answer a;
        final long n;
        ScheduledFuture<?> timer;
        boolean expired;

        Held(Answer a, long n) { this.a = a; this.n = n; }
    }

    /** One exchange carrier on the server; every field is its loop's, and its pieces are heap buffers. */
    final class Carrier {
        final EventLoop loop;
        final byte[] cid;
        final Exchange.Key key;
        private boolean ended;
        /** Ended by the listener's close before a session took it: requests are answered BUSY and nothing is remembered. */
        private boolean refused;
        private Throwable err;
        private long last = System.nanoTime();
        private ScheduledFuture<?> idleTimer, coalesceTimer;

        // Uplink.
        private long upNext, upMax, upFin = -1;
        private final Map<Long, ByteBuffer> upOoo = new HashMap<>();
        /** The contiguous uplink, lent to zmux's reader within the uplink store limit. */
        private final LentInput up;
        /** The bytes of upOoo. */
        private long oooLen;
        /** What the out-of-order entries of upOoo count besides their bytes: PIECE_COST each. */
        private long upOooCost;
        private boolean rclosed;
        /** Fails the carrier when its loop terminates: no call of zmux or timer of the loop ends it any more. */
        private final io.netty.util.concurrent.GenericFutureListener<io.netty.util.concurrent.Future<Object>> onTerminated =
                f -> end(new ClosedChannelException());

        // Downlink.
        private long ack, asgEnd, fill, finLow = -1;
        private final Map<Long, Piece> pieces = new HashMap<>();
        /** The open downlink piece, in write mode; null: none. */
        private ByteBuffer cur;
        private long curAt;
        private boolean wclosed;
        private final List<Held> held = new ArrayList<>();
        /** A write waiting for the window: its completion. */
        private PipeWriter.Completion blocked;
        private OutboundBatch blockedBatch;
        private int blockedAt;

        Carrier(EventLoop loop, byte[] cid, Exchange.Key key) {
            this.loop = loop; this.cid = cid; this.key = key;
            up = new LentInput(cfg.upStoreLimit(), loop, loop::inEventLoop, more -> {}); // the client repeats what is refused BUSY
        }

        /**
         * Registered once the carrier won its creation: one that lost it is dropped unregistered, as the loop
         * would otherwise keep it until termination and then end, and remember, a carrier that never existed.
         */
        void watchLoop() {
            loop.terminationFuture().addListener(onTerminated);
        }

        void idle() {
            Loops.run(loop, () -> idleTimer = Loops.schedule(loop, this::onIdle, cfg.idleTimeout().toNanos(), TimeUnit.NANOSECONDS, NOTHING), NOTHING);
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
            idleTimer = Loops.schedule(loop, this::onIdle, Math.max(rest, 1_000_000), TimeUnit.NANOSECONDS, NOTHING);
        }

        /** The listener closed before a session took the carrier: it ends as if never created. */
        void refused() {
            if (ended) return;
            refused = true;
            end(new ClosedChannelException());
        }

        /** The answer to a request reaching the carrier once it has ended. */
        private int endedFlags() { return refused ? Exchange.FLAG_BUSY : Exchange.FLAG_GONE; }

        /** Ends the carrier (null: completed) and drops what it holds but uplink bytes zmux may still read. */
        void end(Throwable e) {
            if (ended) return;
            ended = true;
            err = e;
            if (idleTimer != null) idleTimer.cancel(false);
            if (coalesceTimer != null) coalesceTimer.cancel(false);
            pieces.clear();
            cur = null;
            for (Held h : held) { if (h.timer != null) h.timer.cancel(false); h.a.send(endedFlags(), null); }
            held.clear();
            loop.terminationFuture().removeListener(onTerminated);
            if (e != null) { dropUplink(); up.fail(e); }
            ByteBuffer id = ByteBuffer.wrap(cid);
            carriers.remove(id, this);
            if (!refused) remember(id, key);
            if (blocked != null) { blocked.failed(new ClosedChannelException()); blocked = null; blockedBatch = null; }
        }

        private void dropUplink() {
            upOoo.clear();
            oooLen = 0;
            upOooCost = 0;
        }

        private boolean complete() {
            if (upFin >= 0 && upNext > upFin && finLow >= 0 && ack > finLow) { end(null); return true; }
            return false;
        }

        /** Serves an authenticated request. */
        private void serve(Answer a) {
            Exchange.Header q = a.q;
            last = System.nanoTime();
            if (ended) { a.send(endedFlags(), null); return; }
            if (q.ack() > ack) {
                if (q.ack() > asgEnd) {
                    end(new IOException("exchange carrier protocol violation"));
                    a.send(Exchange.FLAG_GONE, null);
                    return;
                }
                long acked = q.ack();
                pieces.keySet().removeIf(k -> k < acked);
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
            Exchange.Header q = a.q;
            long n = q.seq();
            boolean fin = q.fin();
            if ((fin && ((upFin >= 0 && upFin != n) || upMax > n + 1)) || (!fin && upFin >= 0 && n > upFin)) {
                end(new IOException("exchange carrier protocol violation"));
                return Exchange.FLAG_GONE;
            }
            if (n < upNext || upOoo.containsKey(n)) return 0;
            ByteBuffer piece = null;
            int len = a.dataLength;
            boolean ooo = n != upNext;
            int stored = rclosed ? 0 : len;
            if (ooo) {
                // Out of order: leave room for the piece zmux waits for, and count each held piece
                // with a fixed overhead so that empty pieces cannot be held without bound.
                long room = (long) cfg.upStoreLimit() - cfg.upLimit();
                if (up.bytes() + oooLen + upOooCost + stored + PIECE_COST > room) return Exchange.FLAG_BUSY;
            } else if (stored > 0 && up.bytes() + oooLen + upOooCost + stored > cfg.upStoreLimit()) {
                return Exchange.FLAG_BUSY;
            }
            if (stored > 0) {
                // The request's Data, zero copy.
                piece = a.takeData();
            }
            if (fin) upFin = n;
            upMax = Math.max(upMax, n + 1);
            if (ooo) {
                upOoo.put(n, piece);
                upOooCost += PIECE_COST;
                if (piece != null) oooLen += len;
                return 0;
            }
            if (piece != null) up.offer(Chunk.of(piece), len);
            upNext++;
            for (ByteBuffer x; upOoo.containsKey(upNext); upNext++) {
                x = upOoo.remove(upNext);
                upOooCost -= PIECE_COST;
                if (x != null) { oooLen -= x.remaining(); up.offer(Chunk.of(x), x.remaining()); }
            }
            if (upFin >= 0 && upNext > upFin) up.end();
            return 0;
        }

        /** Answers a DOWN request, now or once its piece is assigned. */
        private void down(Answer a) {
            Held h = new Held(a, a.q.seq());
            if (answer(h)) return;
            held.add(h);
            h.timer = Loops.schedule(loop, () -> { h.expired = true; if (held.remove(h) && !answer(h)) held.add(h); },
                    cfg.holdTime().toNanos(), TimeUnit.NANOSECONDS, NOTHING);
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
            else if (p == null && wclosed && (cur == null || cur.position() == 0)) p = assign(n, null, true);
            else if (p == null && h.expired) {
                // The hold time ran out: the open piece, or an empty one.
                p = n == fill && cur != null && cur.position() > 0 ? assignCur() : assign(n, null, false);
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

        private Piece assign(long n, ByteBuffer data, boolean fin) {
            Piece p = new Piece(data, fin);
            pieces.put(n, p);
            asgEnd = Math.max(asgEnd, n + 1);
            if (fin && (finLow < 0 || n < finLow)) finLow = n;
            while (pieces.containsKey(fill)) fill++;
            return p;
        }

        private Piece assignCur() {
            ByteBuffer b = cur.flip();
            cur = null;
            return assign(fill, b, wclosed);
        }

        /** Assigns piece fill when full, or when a request can take it and its coalescing delay ran out; else arms the timer. */
        private void coalesce() {
            if (cur == null || cur.position() == 0 || ended) return;
            if (cur.position() >= cfg.downPieceSize() || wclosed) { assignCur(); answerHeld(); return; }
            if (held.isEmpty()) return;
            long due = curAt + cfg.coalesce().toNanos() - System.nanoTime();
            if (due <= 0) { assignCur(); answerHeld(); return; }
            if (coalesceTimer == null || coalesceTimer.isDone()) {
                coalesceTimer = Loops.schedule(loop, this::coalesce, due, TimeUnit.NANOSECONDS, NOTHING);
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
                if (cur == null) { cur = ByteBuffer.allocate(Math.min(cfg.downPieceSize(), 64 << 10)); curAt = System.nanoTime(); }
                int n = Math.min(rem, cfg.downPieceSize() - cur.position());
                cur = Exchange.append(cur, seg.slice(seg.position() + segOffset, n), cfg.downPieceSize());
                segOffset += n;
                if (cur.position() >= cfg.downPieceSize()) { assignCur(); answerHeld(); }
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
            @Override public void start(Sink s) {
                Loops.run(loop, () -> { if (!rclosed) up.start(s); }, () -> s.onError(new ClosedChannelException()));
            }

            @Override public void close() {
                Loops.run(loop, () -> {
                    rclosed = true;
                    up.close();
                    dropUplink();
                }, NOTHING);
            }
        }

        final class Writer implements PipeWriter {
            @Override public void write(OutboundBatch batch, Completion done) {
                Loops.run(loop, () -> {
                    if (ended || wclosed) { done.failed(new ClosedChannelException()); return; }
                    blocked = done;
                    blockedBatch = batch;
                    blockedAt = 0;
                    segOffset = 0;
                    take();
                }, () -> done.failed(new ClosedChannelException()));
            }

            @Override public void close() {
                Loops.run(loop, () -> {
                    if (wclosed || ended) return;
                    wclosed = true;
                    if (cur != null && cur.position() > 0) assignCur();
                    answerHeld(); // held requests take an empty FIN piece
                    complete();
                }, NOTHING);
            }

            @Override public void abort(Throwable cause) {
                Loops.run(loop, () -> end(cause == null ? new ClosedChannelException() : cause), NOTHING);
            }
        }
    }
}
