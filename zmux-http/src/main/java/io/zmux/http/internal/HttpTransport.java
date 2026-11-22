package io.zmux.http.internal;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.zmux.Admission;
import io.zmux.carrier.PipeIo;
import io.zmux.http.HttpEndpoint;
import io.zmux.http.HttpCarrierClient;
import io.zmux.internal.spi.AdapterPorts;
import io.zmux.internal.spi.CarrierProvider;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Consumer;
import java.util.function.BiFunction;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLEngine;

/** The HTTP adapter boundary for request streams provided by a sibling transport module. */
public final class HttpTransport {
    private static final AttributeKey<Stream> STREAM = AttributeKey.valueOf(HttpTransport.class, "stream");
    private static final AttributeKey<String> PROTOCOL = AttributeKey.valueOf(HttpTransport.class, "protocol");

    private HttpTransport() {}

    /** Supplies connections and opens individual request streams for the shared pool. */
    public interface Client {
        CompletableFuture<Channel> connect(EventLoopGroup group, URI uri, Duration timeout, Consumer<Channel> track);
        boolean reusable(Channel connection);
        CompletableFuture<Channel> stream(Channel connection, Duration timeout);
    }

    /** An opening that can be attempted on the next connection before any request has been written. */
    public static final class RetryOpen extends IOException {
        public RetryOpen(Throwable cause) { super("HTTP connection cannot accept another request", cause); }
    }

    /** Transport metadata and cancellation for one request stream. */
    public interface Stream extends CarrierProvider {
        Channel connection();
        InetSocketAddress peer();
        SSLEngine tls();
        PipeIo.Exporter exporter();
        void abort();
    }

    /** The request router lifecycle observed by a hosted transport. */
    public interface Route extends ChannelHandler {
        void stop();
        void afterSetup(Runnable done);
    }

    public static void attach(Channel ch, Stream stream) { ch.attr(STREAM).set(stream); }
    public static Stream stream(Channel ch) { return ch.hasAttr(STREAM) ? ch.attr(STREAM).get() : null; }

    /** Abandons the request on ch: a stream reset when a transport stream is attached, else a close. */
    public static void abort(Channel ch) {
        Stream stream = stream(ch);
        if (stream != null) stream.abort();
        else ch.close();
    }
    public static void protocol(Channel ch, String value) { ch.attr(PROTOCOL).set(value); }
    public static String protocol(Channel ch) {
        String value = ch.attr(PROTOCOL).get();
        return value == null ? "" : value;
    }

    private static final AtomicReference<BiFunction<HttpCarrierClient.Builder, Client, HttpCarrierClient.Builder>>
            CLIENT_BUILDER = new AtomicReference<>();

    // HttpCarrierClient registers its package-private builder method at initialization.
    public static void installClientBuilder(BiFunction<HttpCarrierClient.Builder, Client, HttpCarrierClient.Builder> factory) {
        if (!CLIENT_BUILDER.compareAndSet(null, factory)) throw new IllegalStateException("client builder already installed");
    }

    public static HttpCarrierClient.Builder clientBuilder(Client transport) {
        var builder = HttpCarrierClient.builder();
        return CLIENT_BUILDER.get().apply(builder, transport);
    }

    public static Route route(HttpEndpoint endpoint, ChannelPipeline pipeline) {
        Route route = (Route) endpoint.handler();
        pipeline.addLast(route);
        return route;
    }

    public static ChannelHandler notFound() { return new NotFound(); }

    /** The request target is the first downstream inbound handler, or absent if there is none. */
    public static Admission.Fallback requestFallback(ChannelHandlerContext router,
            Function<BooleanSupplier, CompletionStage<Void>> dispatch) {
        boolean downstream = false;
        for (var entry : router.pipeline()) {
            if (downstream && entry.getValue() instanceof ChannelInboundHandler handler) {
                return new AdapterPorts.RequestFallback(handler instanceof NotFound,
                        (request, handoff) -> dispatch.apply(handoff));
            }
            if (entry.getKey().equals(router.name())) downstream = true;
        }
        return null;
    }

    /** The host's handler when none is given: 404 to every request. */
    @ChannelHandler.Sharable
    private static final class NotFound extends ChannelInboundHandlerAdapter {
        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpRequest r) {
                var resp = new DefaultFullHttpResponse(r.protocolVersion(), HttpResponseStatus.NOT_FOUND);
                HttpUtil.setContentLength(resp, 0);
                if (!HttpUtil.isKeepAlive(r)) resp.headers().set(HttpHeaderNames.CONNECTION, "close");
                ctx.writeAndFlush(resp);
            }
            if (msg instanceof HttpContent || msg instanceof HttpRequest) ReferenceCountUtil.release(msg); // LastHttpContent is an HttpContent
            else ctx.fireChannelRead(msg);
        }
    }

}
