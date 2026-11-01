package io.zmux.http;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakeException;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.util.ReferenceCountUtil;
import io.zmux.CarrierKind;
import io.zmux.Admission;
import io.zmux.Credential;
import io.zmux.HttpStatusException;
import io.zmux.RequestHead;
import io.zmux.ZmuxConfig;
import io.zmux.carrier.Framing;
import io.zmux.carrier.PipeIo;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Opens WebSocket carriers on an HTTP/1.1 connection of an {@link HttpCarrierClient}. */
final class WebSocketClient {
    private WebSocketClient() {}

    static PipeIo open(HttpCarrierClient http, URI url, Map<String, List<String>> headers, List<String> protocols, Credential credential)
            throws IOException {
        String scheme = url.getScheme() == null ? "" : url.getScheme().toLowerCase(java.util.Locale.ROOT);
        if (!List.of("ws", "wss", "http", "https").contains(scheme) || url.getHost() == null) {
            throw new IllegalArgumentException("not a ws or wss URL: " + url);
        }
        boolean secure = Connections.secure(url);
        URI ws = URI.create((secure ? "wss" : "ws") + url.toString().substring(scheme.length()));
        // The client offers its identifiers, then their aliases (SPEC.md Section 6.2).
        List<String> offer = ZmuxConfig.withAliases(protocols.isEmpty() ? List.of(ZmuxConfig.DEFAULT_ALPN) : protocols);
        Channel ch = http.await(http.conns.open(url, false, true), "connect");
        try {
            HttpHeaders h = new DefaultHttpHeaders();
            headers.forEach((k, vs) -> { for (String v : vs) h.add(k, v); });
            if (credential != null) {
                var e = Requests.tls(ch);
                var info = new Admission.CarrierInfo(Connections.address(url), Connections.address(url), "", e == null ? "" : url.getHost(),
                        e == null ? null : Requests.exporter(e), RequestHead.of(Map.of(), true));
                Credential.HeaderField f = credential.header(info);
                h.add(f.name(), f.value());
            }
            WebSocketClientHandshaker hs = WebSocketClientHandshakerFactory.newHandshaker(ws, WebSocketVersion.V13,
                    String.join(",", offer), false, h, WebSocketFrames.READ_LIMIT, true, false, 10_000, false, false);
            BodyCarrier[] self = new BodyCarrier[1];
            // Both halves closed: the connection closes once the close frame went out.
            BodyCarrier c = new BodyCarrier(ch, Framing.MESSAGES, true, true, WebSocketFrames.OUT, () -> self[0].afterWrites(ch::close),
                    cause -> { ch.close(); return null; });
            self[0] = c;
            WebSocketFrames frames = new WebSocketFrames(c);
            CompletableFuture<String> done = new CompletableFuture<>();
            ch.pipeline().addLast("ws-aggregator", new HttpObjectAggregator(64 << 10));
            ch.pipeline().addLast(new Handler(hs, frames, done));
            hs.handshake(ch).addListener(f -> { if (!f.isSuccess()) done.completeExceptionally(f.cause()); });
            String protocol = http.await(done, "websocket handshake");
            return new PipeIo(c.reader(), c.writer(), Framing.MESSAGES, "websocket", Requests.connection(ch))
                    .withReceiveMemory(BodyCarrier.RECEIVE_MEMORY).withProtocol(protocol)
                    .withCarrier(CarrierKind.WEBSOCKET);
        } catch (IOException | RuntimeException e) {
            ch.close();
            throw e;
        }
    }

    /** The handshake's response, then the carrier's messages. */
    private static final class Handler extends ChannelInboundHandlerAdapter {
        private final WebSocketClientHandshaker hs;
        private final WebSocketFrames frames;
        private final CompletableFuture<String> done;

        Handler(WebSocketClientHandshaker hs, WebSocketFrames frames, CompletableFuture<String> done) {
            this.hs = hs; this.frames = frames; this.done = done;
        }

        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof WebSocketFrame) {
                frames.read(msg);
                return;
            }
            try {
                if (msg instanceof FullHttpResponse r && !hs.isHandshakeComplete()) {
                    try {
                        hs.finishHandshake(ctx.channel(), r);
                        if (ctx.pipeline().get("ws-aggregator") != null) ctx.pipeline().remove("ws-aggregator");
                        done.complete(hs.actualSubprotocol() == null ? "" : hs.actualSubprotocol());
                    } catch (WebSocketClientHandshakeException e) {
                        var resp = e.response();
                        int s = resp == null ? 0 : resp.status().code();
                        done.completeExceptionally(s != 0 && s != 101
                                ? new HttpStatusException(s, resp.status().reasonPhrase()) : new IOException(e.getMessage(), e));
                        ctx.close();
                    }
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            done.completeExceptionally(new ClosedChannelException());
            frames.carrier.lost(new ClosedChannelException());
            frames.closed();
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            done.completeExceptionally(cause);
            frames.carrier.lost(cause);
            ctx.close();
        }
    }
}
