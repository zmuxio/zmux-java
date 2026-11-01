package io.zmux.http;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.websocketx.WebSocketDecoderConfig;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import io.zmux.CarrierKind;
import io.zmux.Admission;
import io.zmux.JoinHint;
import io.zmux.RequestHead;
import io.zmux.ZmuxConfig;
import io.zmux.ZmuxListener;
import io.zmux.carrier.Framing;
import io.zmux.carrier.PipeIo;
import java.util.ArrayList;
import java.util.List;

/**
 * The server of WebSocket carriers on HTTP/1.1: a valid upgrade request
 * that offers an accepted subprotocol passes the listener's admission on its head alone (a
 * prefix token in its {@code Zmux-Prefix-Auth} field), and only an accepted one is answered with
 * 101; any other request, and one the admission gives back, goes to the host's handler. The
 * subprotocol is chosen in the server's order of preference, each identifier also accepted as
 * its draft-01 alias (SPEC.md Section 6.2), and is the carrier's application protocol.
 */
final class WebSocketServer {
    private final HttpEndpoint ep;
    /** The identifiers accepted, by preference; empty: the listener's. */
    private final List<String> protocols;

    WebSocketServer(HttpEndpoint ep, List<String> protocols) {
        this.ep = ep;
        this.protocols = List.copyOf(protocols);
    }

    /** The subprotocol to select among offered: the first accepted identifier offered as it is or as its alias; null: none. */
    static String select(List<String> accepted, List<String> offered) {
        for (String p : accepted) {
            for (String form : ZmuxConfig.withAliases(List.of(p))) if (offered.contains(form)) return form;
        }
        return null;
    }

    /** The subprotocols a request offers, in its order. */
    static List<String> offered(HttpRequest r) {
        List<String> out = new ArrayList<>();
        for (String v : r.headers().getAll(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL)) {
            for (String t : v.split(",")) if (!t.isBlank()) out.add(t.strip());
        }
        return out;
    }

    /**
     * Starts the carrier of an upgrade request: null when the request is not a valid upgrade
     * or offers no accepted subprotocol (it is then the host's). Nothing is written until the
     * admission accepts the carrier.
     */
    WebSocketFrames open(Router router, ChannelHandlerContext ctx, HttpRequest r) {
        ZmuxListener l = ep.listener;
        String protocol = select(protocols.isEmpty() ? l.config().alpn() : protocols, offered(r));
        if (protocol == null) return null;
        // A request for one of the listener's subprotocols that is no valid upgrade is a failed upgrade.
        if (!HttpClass.hasToken(r.headers().getAll(HttpHeaderNames.CONNECTION), "upgrade")
                || !"13".equals(r.headers().get(HttpHeaderNames.SEC_WEBSOCKET_VERSION))
                || r.headers().get(HttpHeaderNames.SEC_WEBSOCKET_KEY, "").isBlank()) {
            l.countSetupFailure();
            return null;
        }
        Channel ch = ctx.channel();
        WebSocketServerHandshaker hs = new WebSocketServerHandshakerFactory("ws://" + r.headers().get(HttpHeaderNames.HOST, "") + r.uri(),
                protocol, WebSocketDecoderConfig.newBuilder().maxFramePayloadLength(WebSocketFrames.READ_LIMIT).allowExtensions(false).build())
                .newHandshaker(r);
        if (hs == null) { l.countSetupFailure(); return null; }
        var full = new DefaultFullHttpRequest(r.protocolVersion(), r.method(), r.uri(), Unpooled.EMPTY_BUFFER, r.headers(),
                io.netty.handler.codec.http.EmptyHttpHeaders.INSTANCE);
        Admission.Verdict[] verdict = new Admission.Verdict[1];
        BodyCarrier[] self = new BodyCarrier[1];
        BodyCarrier.Out out = new BodyCarrier.Out() {
            @Override public ChannelFuture head() {
                router.upgraded();
                return hs.handshake(ch, full);
            }
            @Override public Object content(io.netty.buffer.ByteBuf data) { return WebSocketFrames.OUT.content(data); }
            @Override public Object end() { return WebSocketFrames.OUT.end(); }
        };
        BodyCarrier c = new BodyCarrier(ch, Framing.MESSAGES, true, true, out, () -> {
            if (self[0].detached()) return;
            if (verdict[0] != Admission.Verdict.ACCEPT || !self[0].started()) ch.close();
            else self[0].afterWrites(ch::close);
        }, cause -> { ch.close(); return null; });
        self[0] = c;
        JoinHint hint = JoinHint.of(RequestHead.of(Requests.fields(r.headers()), true), false, Requests.peer(ch))
                .withSource(Requests.source(ch, r, ep.trusted));
        PipeIo io = Requests.pipe(new PipeIo(c.reader(), c.writer(), Framing.MESSAGES, "websocket", Requests.connection(ch)), ch)
                .withProtocol(protocol).withCarrier(CarrierKind.WEBSOCKET);
        ZmuxListener.Handled a = l.handle(io, hint, (given, info) -> ChannelLoops.executor(ch).execute(() -> {
            c.detach();
            Router.closeQuietly(given);
            router.toHost(r, List.of(), true);
        }));
        a.verdict().whenComplete((v, e) -> ChannelLoops.executor(ch).execute(() -> {
            if (c.detached() || v == Admission.Verdict.FALLBACK) return;
            verdict[0] = v == null ? Admission.Verdict.CLOSE : v;
            if (v == Admission.Verdict.ACCEPT) c.start(); // the 101 goes out now, and frames may follow
        }));
        return new WebSocketFrames(c);
    }
}
