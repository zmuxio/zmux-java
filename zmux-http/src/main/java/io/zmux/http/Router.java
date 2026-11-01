package io.zmux.http;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.util.ReferenceCountUtil;
import io.zmux.CarrierKind;
import io.zmux.Admission;
import io.zmux.JoinHint;
import io.zmux.RequestHead;
import io.zmux.ZmuxListener;
import io.zmux.carrier.Framing;
import io.zmux.carrier.PipeIo;
import java.nio.channels.ClosedChannelException;
import java.util.List;

/**
 * The per-channel handler of an {@link HttpEndpoint}, after the HTTP codec of an HTTP/1.1
 * connection or of one HTTP/2 stream: it classifies each request by its head and serves it with
 * the adapter of its class, or passes it on, every byte of it, to the handler after it (the
 * host's). State is the channel's event loop's.
 */
final class Router extends ChannelInboundHandlerAdapter {
    private enum Mode { IDLE, HOST, DISCARD, CARRIER, COLLECT, FRAMES }

    private final HttpEndpoint ep;
    private ChannelHandlerContext ctx;
    private Mode mode = Mode.IDLE;
    /** The carrier of the request in CARRIER and FRAMES modes. */
    private BodyCarrier carrier;
    /** The messages of a WebSocket carrier (FRAMES mode, and DISCARD before it). */
    private WebSocketFrames frames;
    /** The exchange request being collected in COLLECT mode. */
    private ExchangeServer.Collect collect;
    /** The request's body ended (CARRIER mode). */
    private boolean requestEnded;
    /** The verdict on the carrier of the request; null while pending. */
    private Admission.Verdict verdict;

    Router(HttpEndpoint ep) { this.ep = ep; }

    @Override public void handlerAdded(ChannelHandlerContext ctx) { this.ctx = ctx; }

    @Override public void handlerRemoved(ChannelHandlerContext ctx) {
        if (collect != null) { collect.abandon(); collect = null; }
    }

    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpRequest r && (mode == Mode.IDLE || mode == Mode.DISCARD)) {
            if (msg instanceof io.netty.handler.codec.http.FullHttpRequest f) {
                // A request whose head ended it: its head and its (last) content apart.
                var head = new io.netty.handler.codec.http.DefaultHttpRequest(f.protocolVersion(), f.method(), f.uri(), f.headers());
                begin(head, true);
                content(new DefaultLastHttpContent(f.content(), f.trailingHeaders()));
                return;
            }
            begin(r, false);
            return;
        }
        if (mode == Mode.FRAMES) {
            frames.read(msg);
            return;
        }
        if (msg instanceof HttpContent c) {
            content(c);
            return;
        }
        if (mode == Mode.HOST || mode == Mode.IDLE) ctx.fireChannelRead(msg);
        else ReferenceCountUtil.release(msg);
    }

    private void begin(HttpRequest r, boolean full) {
        HttpClass cls = HttpClass.classify(Requests.head(ctx.channel(), r, full));
        boolean served = switch (cls) {
            case DOWNLINK, UPLINK, BIDIRECTIONAL -> ep.streaming;
            case EXCHANGE -> ep.exchange != null;
            case WEBSOCKET -> ep.webSocket != null && Requests.version(ctx.channel(), r) == 1;
            default -> false;
        };
        if (!served) {
            host(r);
            return;
        }
        if (Requests.earlyData(r)) {
            // A request forwarded from early data takes no carrier: the intermediary repeats it. The upgrade
            // failed: a setup failure of the listener.
            ep.listener.countSetupFailure();
            mode = Mode.DISCARD;
            FullHttpResponse tooEarly = new DefaultFullHttpResponse(r.protocolVersion(), HttpResponseStatus.valueOf(425));
            HttpUtil.setContentLength(tooEarly, 0);
            ctx.writeAndFlush(tooEarly);
            return;
        }
        switch (cls) {
            case EXCHANGE -> {
                collect = ep.exchange.collect(this, r);
                mode = collect == null ? Mode.HOST : Mode.COLLECT;
                if (collect == null) ctx.fireChannelRead(r);
            }
            case WEBSOCKET -> {
                frames = ep.webSocket.open(this, ctx, r);
                carrier = frames == null ? null : frames.carrier;
                mode = frames == null ? Mode.HOST : Mode.DISCARD;
                if (frames == null) ctx.fireChannelRead(r);
            }
            default -> stream(r, cls);
        }
    }

    /** Passes the request on to the host's handler. */
    private void host(HttpRequest r) {
        mode = Mode.HOST;
        ctx.fireChannelRead(r);
    }

    private void content(HttpContent c) {
        boolean last = c instanceof LastHttpContent;
        switch (mode) {
            case HOST -> {
                ctx.fireChannelRead(c);
                if (last) mode = Mode.IDLE;
                return;
            }
            case CARRIER -> {
                carrier.in(c.content().retain());
                if (last) { requestEnded = true; carrier.inEnd(); }
            }
            case COLLECT -> {
                boolean fits = collect.add(c.content());
                if (!fits || last) {
                    var col = collect;
                    collect = null;
                    mode = Mode.DISCARD;
                    col.complete(last, fits ? null : c.content().retain());
                }
            }
            default -> { }
        }
        c.release();
        if (last && mode == Mode.DISCARD) mode = Mode.IDLE;
    }

    // ---- streaming carriers ----

    private void stream(HttpRequest r, HttpClass cls) {
        var ch = ctx.channel();
        boolean h1 = !(ch instanceof Http2StreamChannel);
        boolean down = cls != HttpClass.UPLINK, up = cls != HttpClass.DOWNLINK;
        ZmuxListener l = ep.listener;
        Admission adm = l.admission();
        BodyCarrier[] self = new BodyCarrier[1];
        BodyCarrier.Out out = new BodyCarrier.Out() {
            @Override public ChannelFuture head() {
                HttpResponse resp = new DefaultHttpResponse(r.protocolVersion(), HttpResponseStatus.OK);
                if (h1) HttpUtil.setTransferEncodingChunked(resp, true);
                // An uplink's response carries nothing; it ends with the carrier.
                if (down) {
                    resp.headers().set(HttpHeaderNames.CONTENT_TYPE, Requests.OCTETS).set(Requests.CACHE, Requests.NO_STORE)
                            .set(Requests.ACCEL, "no");
                }
                // An HTTP/1.1 connection is not reused after a carrier: the rest of a request
                // body is not read, so the client's next request could meet a closing connection.
                if (h1) resp.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
                return ctx.writeAndFlush(resp);
            }
            @Override public Object content(ByteBuf data) { return new DefaultHttpContent(data); }
            @Override public Object end() { return new DefaultLastHttpContent(); }
        };
        // The verdict and whether a session took the carrier: once both halves are closed, a
        // carrier the listener refused with nothing written goes to the host.
        var refused = new java.util.concurrent.CompletableFuture<Boolean>();
        BodyCarrier c = new BodyCarrier(ch, Framing.BYTE_STREAM, up, down, out, () -> refused.whenComplete((no, e) ->
                ChannelLoops.executor(ch).execute(() -> end(self[0], r, down, Boolean.TRUE.equals(no)))), cause -> { ch.close(); return null; });
        self[0] = c;
        carrier = c;
        mode = Mode.CARRIER;
        requestEnded = false;
        verdict = null;
        if (up) c.keepForReplay(); // until a session takes the carrier or the admission decides
        c.holdEmptyEnd();
        boolean headOnly = !up;
        JoinHint hint = JoinHint.of(RequestHead.of(Requests.fields(r.headers()), headOnly), cls == HttpClass.UPLINK, Requests.peer(ch))
                .withSource(Requests.source(ch, r, ep.trusted));
        PipeIo io = Requests.pipe(new PipeIo(c.reader(), c.writer(), Framing.BYTE_STREAM, "http " + cls, Requests.connection(ch)), ch)
                .withCarrier(cls == HttpClass.BIDIRECTIONAL ? CarrierKind.STREAMING : CarrierKind.SPLIT);
        ZmuxListener.Handled a = l.handle(io, hint, (given, info) -> ChannelLoops.executor(ch).execute(() -> fallback(c, r, given)));
        a.verdict().thenCombine(a.taken(), (v, taken) -> v == Admission.Verdict.ACCEPT && !taken)
                .whenComplete((no, e) -> refused.complete(Boolean.TRUE.equals(no)));
        a.verdict().whenComplete((v, e) -> ChannelLoops.executor(ch).execute(() -> {
            if (c.detached() || v == Admission.Verdict.FALLBACK) return; // the fallback replays what was kept
            verdict = v == null ? Admission.Verdict.CLOSE : v;
            if (v != Admission.Verdict.ACCEPT) c.keepNoMore();
            else if (adm != null) { c.keepNoMore(); c.start(); } // admitted: the header goes out now
        }));
        a.taken().whenComplete((taken, e) -> ChannelLoops.executor(ch).execute(() -> {
            if (c.detached() || !Boolean.TRUE.equals(taken)) return;
            c.keepNoMore();
            if (!down) c.start(); // an uplink's header goes out once a session took it
        }));
    }

    /**
     * Both halves of a streaming carrier are closed: a carrier the listener refused, with
     * nothing written, goes to the host's handler with every body byte replayed; otherwise the
     * request ends (without a response when nothing was answered).
     */
    private void end(BodyCarrier c, HttpRequest r, boolean down, boolean refused) {
        if (c.detached()) return;
        var ch = ctx.channel();
        if (refused && !c.started()) {
            List<ByteBuf> replay = c.detach();
            if (carrier == c) carrier = null;
            toHost(r, replay, requestEnded);
            return;
        }
        if (verdict != Admission.Verdict.ACCEPT) {
            // Stalled, closed or not decided: the request ends without a response.
            ch.close();
            return;
        }
        if (!down || c.endedUnstarted()) {
            // An uplink's empty response, or a downlink zmux ended without writing.
            c.endOut(() -> c.afterWrites(ch::close));
            return;
        }
        finish(c);
    }

    /** The end of a carrier with a downlink, once zmux closed both halves. */
    private void finish(BodyCarrier c) {
        var ch = ctx.channel();
        if (!c.started()) {
            // Nothing was answered: the request ends without a response.
            ch.close();
            return;
        }
        c.afterWrites(ch::close);
    }

    /** The admission's fallback: the request goes to the host's handler with every body byte replayed. */
    private void fallback(BodyCarrier c, HttpRequest r, PipeIo given) {
        List<ByteBuf> replay = c.detach();
        closeQuietly(given);
        if (carrier == c) carrier = null;
        toHost(r, replay, requestEnded);
    }

    static void closeQuietly(PipeIo io) {
        try { if (io.reader() != null) io.reader().close(); } catch (RuntimeException ignored) { /* Gone. */ }
        try { if (io.writer() != null) io.writer().close(); } catch (RuntimeException ignored) { /* Gone. */ }
    }

    /** The exchange or WebSocket adapter gives the request to the host after all, with what it read. */
    void toHost(HttpRequest r, List<ByteBuf> read, boolean ended) {
        mode = ended ? Mode.IDLE : Mode.HOST;
        ctx.fireChannelRead(r);
        for (ByteBuf b : read) ctx.fireChannelRead(new DefaultHttpContent(b));
        if (ended) ctx.fireChannelRead(new DefaultLastHttpContent());
    }

    /** The WebSocket handshake went out: frames follow. */
    void upgraded() { mode = Mode.FRAMES; }

    ChannelHandlerContext context() { return ctx; }

    @Override public void channelInactive(ChannelHandlerContext ctx) {
        if (carrier != null) carrier.lost(new ClosedChannelException());
        if (frames != null) frames.closed();
        if (collect != null) { collect.abandon(); collect = null; }
        ctx.fireChannelInactive();
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (carrier != null || collect != null) {
            if (carrier != null) carrier.lost(cause);
            ctx.close();
            return;
        }
        ctx.fireExceptionCaught(cause);
    }
}
