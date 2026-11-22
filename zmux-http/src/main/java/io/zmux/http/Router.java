package io.zmux.http;

import io.zmux.internal.spi.AdapterPorts;
import io.zmux.internal.spi.Loops;
import io.zmux.AdmissionVerdict;
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
import io.zmux.http.internal.HttpTransport;
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
final class Router extends ChannelInboundHandlerAdapter implements HttpTransport.Route {
    private static final Runnable NOTHING = () -> {};
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
    private io.zmux.adapter.Handled h3Setup;
    /** The request's body ended (CARRIER mode). */
    private boolean requestEnded;
    private io.netty.handler.codec.http.HttpHeaders trailers;
    /** Keeps a replayed request under its original setup until the host receives the last content. */
    private record HostBody(BodyCarrier source, java.util.concurrent.CompletableFuture<Void> read) {}
    private HostBody hostBody;

    /** The verdict on the carrier of the request; null while pending. */
    private AdmissionVerdict verdict;

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
        trailers = null;
        HttpClass cls = HttpClass.classify(Requests.head(ctx.channel(), r, full), ep.pairField);
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
            ep.listener.adapter().countSetupFailure();
            refuse(r, HttpResponseStatus.valueOf(425));
            return;
        }
        switch (cls) {
            case EXCHANGE -> {
                collect = ep.exchange.collect(this, r);
                if (collect != null) mode = Mode.COLLECT;
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
                var body = hostBody;
                if (last) { mode = Mode.IDLE; hostBody = null; }
                ctx.fireChannelRead(c);
                if (last && body != null) body.read.complete(null);
                return;
            }
            case CARRIER -> {
                carrier.in(c.content().retain());
                if (c instanceof LastHttpContent tail) {
                    if (!tail.trailingHeaders().isEmpty()) trailers = tail.trailingHeaders().copy();
                    requestEnded = true;
                    carrier.inEnd();
                }
            }
            case COLLECT -> collect.add(c.content(), last);
            default -> { }
        }
        c.release();
        if (last && mode == Mode.DISCARD) mode = Mode.IDLE;
    }

    // ---- streaming carriers ----

    private void stream(HttpRequest r, HttpClass cls) {
        var ch = ctx.channel();
        boolean h1 = !Requests.multiplexed(ch);
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
        // A loop that refuses a task below is shutting down: it closes the channel, whose loss
        // releases what the carrier keeps; a refused fallback closes the pipe it was given.
        BodyCarrier c = new BodyCarrier(ch, Framing.BYTE_STREAM, up, down, out, () -> refused.whenComplete((no, e) ->
                Loops.run(ch.eventLoop(), () -> end(self[0], r, down, Boolean.TRUE.equals(no)), NOTHING)), cause -> { HttpTransport.abort(ch); return null; });
        self[0] = c;
        carrier = c;
        mode = Mode.CARRIER;
        requestEnded = false;
        verdict = null;
        if (up) c.keepForReplay(); // until a session takes the carrier or the admission decides
        c.holdEmptyEnd();
        boolean headOnly = !up;
        JoinHint hint = JoinHint.of(RequestHead.of(Requests.fields(r.headers())), ep.pairField, cls == HttpClass.UPLINK, Requests.peer(ch))
                .withSource(Requests.source(ch, r, ep.trusted));
        PipeIo io = Requests.pipe(new PipeIo(c.reader(), c.writer(), Framing.BYTE_STREAM, "http " + cls, Requests.connection(ch)), ch)
                .withCarrier(cls == HttpClass.BIDIRECTIONAL
                        ? (Requests.h3(ch) ? CarrierKind.HTTP3 : CarrierKind.STREAMING)
                        : CarrierKind.SPLIT);
        // A request the admission gives back is a request: the host's handler answers it, with the body bytes read.
        io.zmux.adapter.Handled a = l.adapter().handleRequest(io, hint, headOnly, requestFallback(handoff -> toHostLater(c, r, handoff)), c.opening());
        withdrawOnClose(ch, a);
        trackH3Setup(a);
        AdapterPorts.setup(a).onFinished(() -> Loops.run(ch.eventLoop(), () -> {
            if (hostBody != null && hostBody.source == c) ch.close();
        }, NOTHING));
        // The verdict and the taken flag come from the listener adapter in another module; the analyzer takes the result for a constant.
        //noinspection ConstantValue
        a.verdict().thenCombine(a.taken(), (v, taken) -> v == AdmissionVerdict.ACCEPT && !taken)
                .whenComplete((no, e) -> refused.complete(Boolean.TRUE.equals(no)));
        a.verdict().whenComplete((v, e) -> Loops.run(ch.eventLoop(), () -> {
            // A request given back to the host's handler replays what was kept.
            if (c.detached() || (v == AdmissionVerdict.FALLBACK && !a.takenOver())) return;
            verdict = v == null ? AdmissionVerdict.CLOSE : v;
            if (v != AdmissionVerdict.ACCEPT) c.keepNoMore();
            // Taken over by the listener's fallback: the response head goes out, and the request and response bodies
            // are the host's full-duplex byte stream until it closes both halves (detached or not); nothing starts here.
            if (v == AdmissionVerdict.ACCEPT && adm != null) { c.keepNoMore(); c.start(); } // admitted: the header goes out now
        }, NOTHING));
        a.taken().whenComplete((taken, e) -> Loops.run(ch.eventLoop(), () -> {
            if (c.detached() || !Boolean.TRUE.equals(taken)) return;
            c.keepNoMore();
            if (!down) c.start(); // an uplink's header goes out once a session took it
        }, NOTHING));
    }

    /**
     * Both halves of a streaming carrier are closed: a carrier the listener refused, with
     * nothing written, goes to the host's handler with every body byte replayed; otherwise the
     * request ends (without a response when nothing was answered). A carrier the listener's fallback took over ends
     * here too, once its host closed both halves.
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
        if (verdict != AdmissionVerdict.ACCEPT && verdict != AdmissionVerdict.TAKEOVER && verdict != AdmissionVerdict.FALLBACK) {
            // Stalled, closed or not decided: the request ends without a response.
            HttpTransport.abort(ch);
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
            HttpTransport.abort(ch);
            return;
        }
        c.afterWrites(ch::close);
    }

    /** The admission's fallback: the request goes to the host's handler with every body byte replayed. */
    private void fallback(BodyCarrier c, HttpRequest r) {
        List<ByteBuf> replay = c.detach();
        if (carrier == c) carrier = null;
        toHost(r, replay, requestEnded);
    }

    /**
     * The admission gave c's request back: on the channel's loop it goes to the host's handler,
     * with the buffered prefix and any trailers. The stage lasts through the last body event so
     * listener stop also interrupts a host waiting for the rest of the request.
     */
    Admission.Fallback requestFallback(
            java.util.function.Function<java.util.function.BooleanSupplier, java.util.concurrent.CompletionStage<Void>> dispatch) {
        return HttpTransport.requestFallback(ctx, dispatch);
    }

    private java.util.concurrent.CompletionStage<Void> toHostLater(BodyCarrier c, HttpRequest r, java.util.function.BooleanSupplier handoff) {
        var given = new java.util.concurrent.CompletableFuture<Void>();
        Loops.run(ctx.channel().eventLoop(), () -> {
            if (!ctx.channel().isActive() || !handoff.getAsBoolean()) { given.completeExceptionally(new ClosedChannelException()); return; }
            boolean complete = requestEnded;
            if (!complete) hostBody = new HostBody(c, given);
            fallback(c, r);
            if (complete) given.complete(null);
        }, () -> given.completeExceptionally(new ClosedChannelException()));
        return given;
    }

    /** The exchange or WebSocket adapter gives the request to the host after all, with what it read. */
    void toHost(HttpRequest r, List<ByteBuf> read, boolean ended) {
        h3Setup = null; // Host replay has begun. The host owns this request now.
        mode = ended ? Mode.IDLE : Mode.HOST;
        ctx.fireChannelRead(r);
        for (ByteBuf b : read) ctx.fireChannelRead(new DefaultHttpContent(b));
        if (ended) {
            var last = new DefaultLastHttpContent();
            if (trailers != null) { last.trailingHeaders().set(trailers); trailers = null; }
            ctx.fireChannelRead(last);
        }
    }

    /** Answers r with status and no body, without reading the body, which is discarded. */
    void refuse(HttpRequest r, HttpResponseStatus status) {
        mode = Mode.DISCARD;
        FullHttpResponse resp = new DefaultFullHttpResponse(r.protocolVersion(), status);
        HttpUtil.setContentLength(resp, 0);
        ctx.writeAndFlush(resp);
    }

    /** The exchange request c is done with its body: what is left of it is discarded, unless c gave it to the host. */
    void endCollect(ExchangeServer.Collect c) {
        if (collect != c) return;
        collect = null;
        if (mode == Mode.COLLECT) mode = Mode.DISCARD;
    }

    /** The WebSocket handshake went out: frames follow. */
    void upgraded() { mode = Mode.FRAMES; }

    ChannelHandlerContext context() { return ctx; }

    void trackH3Setup(io.zmux.adapter.Handled handled) {
        if (Requests.h3(ctx.channel())) {
            var setup = AdapterPorts.setup(handled);
            h3Setup = handled;
            setup.onFinished(() -> Loops.run(ctx.channel().eventLoop(), () -> {
                if (h3Setup == handled) h3Setup = null;
            }, NOTHING));
        }
    }

    @Override public void afterSetup(Runnable done) {
        if (h3Setup == null) done.run();
        else AdapterPorts.setup(h3Setup).onFinished(() -> Loops.run(ctx.channel().eventLoop(), done, NOTHING));
    }

    @Override public void stop() {
        var pending = h3Setup;
        if (pending != null && !pending.detached()) AdapterPorts.setup(pending).interrupt();
        if (collect != null) {
            collect.abandon();
            collect = null;
            HttpTransport.abort(ctx.channel());
        }
    }

    static void withdrawOnClose(io.netty.channel.Channel ch, io.zmux.adapter.Handled a) {
        var setup = AdapterPorts.setup(a);
        io.netty.channel.ChannelFutureListener withdrawn = ignored -> setup.interrupt();
        ch.closeFuture().addListener(withdrawn);
        setup.onFinished(() -> ch.closeFuture().removeListener(withdrawn));
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) {
        if (hostBody != null) {
            var body = hostBody;
            hostBody = null;
            body.read.completeExceptionally(new ClosedChannelException());
        }
        if (carrier != null) carrier.lost(new ClosedChannelException());
        if (frames != null) frames.closed();
        if (collect != null) { collect.abandon(); collect = null; }
        ctx.fireChannelInactive();
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (h3Setup != null) AdapterPorts.setup(h3Setup).interrupt();
        if (carrier != null || collect != null) {
            if (carrier != null) carrier.lost(cause);
            ctx.close();
            return;
        }
        if (cause instanceof java.io.IOException) { // a reset or a broken connection: nothing for the host to act on beyond channelInactive
            ctx.close();
            return;
        }
        ctx.fireExceptionCaught(cause);
    }
}
