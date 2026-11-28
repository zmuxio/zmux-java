package io.zmux.http3;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpHeaderValidationUtil;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.Http3ErrorCode;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.zmux.Admission;
import io.zmux.CarrierCredential;
import io.zmux.HttpStatusException;
import io.zmux.RequestHead;
import io.zmux.carrier.Framing;
import io.zmux.http.internal.HttpTransport;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Client-side Extended CONNECT. The pool owns the connection and this attempt owns one stream. */
final class WebTransportConnect extends SimpleChannelInboundHandler<Http3HeadersFrame> {
    private final WebTransportSession session;
    private final CompletableFuture<WebTransportSession> result;
    private WebTransportConnect(WebTransportSession session, CompletableFuture<WebTransportSession> result) {
        this.session = session;
        this.result = result;
    }

    static Http3HeadersFrame request(URI target, Map<String, List<String>> fields) {
        if (H3Authority.of(Objects.requireNonNull(target, "target")) == null) {
            throw new IllegalArgumentException("expected an absolute HTTPS WebTransport target");
        }
        var head = new DefaultHttp3HeadersFrame();
        String path = target.getRawPath().isEmpty() ? "/" : target.getRawPath();
        if (target.getRawQuery() != null) path += "?" + target.getRawQuery();
        head.headers().method("CONNECT").scheme("https").authority(target.getRawAuthority())
                .path(path).protocol(WebTransportConnection.PROTOCOL);
        fields.forEach((name, values) -> values.forEach(value -> add(head.headers(), name, value)));
        return head;
    }

    private static void add(Http3Headers headers, String name, String value) {
        var field = name.toLowerCase(Locale.ROOT);
        if (name.isEmpty() || HttpHeaderValidationUtil.validateToken(name) >= 0
                || HttpHeaderValidationUtil.validateValidHeaderValue(value) >= 0
                || switch (field) {
                    case "host", "connection", "transfer-encoding", "upgrade", "keep-alive", "proxy-connection",
                            "content-length", "early-data", "wt-available-protocols", "wt-protocol" -> true;
                    default -> false;
                }) throw new IllegalArgumentException("invalid WebTransport CONNECT field " + name);
        headers.add(field, value);
    }

    static void start(QuicStreamChannel stream, URI target, Http3HeadersFrame head, CarrierCredential credential,
                      CompletableFuture<WebTransportSession> result) {
        try {
            if (result.isDone()) { HttpTransport.abort(stream); return; }
            var connection = WebTransportConnection.get(stream.parent());
            if (connection == null) throw new UnsupportedOperationException("HTTP/3 connection cannot dispatch WebTransport");
            var session = connection.reserve(stream, false, pipe -> {
                // Clients have no listener for peer-initiated bidirectional bootstrap requests.
                pipe.reader().close();
                pipe.writer().abort(new IOException("unsolicited bidirectional pipe"));
            });
            if (credential != null) {
                var carrier = Http3Streams.requestStream(stream);
                var info = new Admission.CarrierInfo(carrier.peer(), carrier.peer(), "h3", target.getHost(),
                        carrier.exporter(), RequestHead.of(Map.of()))
                        .withOuterSecurity(carrier.outerSecurity())
                        .at(Admission.Stage.HEAD, Framing.BYTE_STREAM, true);
                var field = credential.header(info);
                add(head.headers(), field.name(), field.value());
            }
            var response = new WebTransportConnect(session, result);
            Http3Streams.raw(stream, response);
            session.closed().whenComplete((closed, error) -> {
                if (error != null) result.completeExceptionally(error);
            });
            stream.pipeline().context(response).writeAndFlush(head).addListener(done -> {
                if (!done.isSuccess()) session.rejected(done.cause(), Http3ErrorCode.H3_REQUEST_CANCELLED.code());
            });
        } catch (IOException | RuntimeException failed) {
            result.completeExceptionally(failed);
            HttpTransport.abort(stream);
        }
    }

    static int status(Http3Headers headers) throws IOException {
        var raw = headers.status();
        if (raw == null || raw.length() != 3) throw new IOException("invalid CONNECT status");
        int value = 0;
        for (int i = 0; i < 3; i++) {
            int digit = raw.charAt(i) - '0';
            if (digit < 0 || digit > 9) throw new IOException("invalid CONNECT status");
            value = 10 * value + digit;
        }
        if (value < 100 || value > 599) throw new IOException("invalid CONNECT status");
        if (value == 101 || value >= 300) throw new HttpStatusException(value, null);
        return value;
    }

    @Override protected void channelRead0(ChannelHandlerContext ctx, Http3HeadersFrame head) throws IOException {
        if (status(head.headers()) < 200) return;
        ctx.pipeline().remove(this);
        session.committed();
        if (!result.complete(session)) session.close();
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        result.completeExceptionally(cause);
        session.rejected(cause, cause instanceof HttpStatusException ? Http3ErrorCode.H3_REQUEST_CANCELLED.code()
                : Http3ErrorCode.H3_MESSAGE_ERROR.code());
    }
}
