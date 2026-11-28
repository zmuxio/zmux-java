package io.zmux.http3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http3.DefaultHttp3GoAwayFrame;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3GoAwayFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.zmux.ListenerConfig;
import io.zmux.Zmux;
import io.zmux.http.HttpCarrierClient;
import io.zmux.http.HttpEndpoint;
import io.zmux.http.internal.HttpTransport;
import io.zmux.quic.QuicOptions;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class Http3StreamCreditTest {
    @Test void anotherRequestWaitsForStreamCreditOnTheSameUdpSocket() throws Exception {
        try (var fixture = new Fixture(1)) {
            Channel first = fixture.open().get(5, TimeUnit.SECONDS);
            var firstReply = fixture.request(first, "first");
            var firstServerStream = fixture.arrived();
            assertEquals(0L, first.eventLoop().submit(() ->
                    ((QuicChannel) first.parent()).peerAllowedStreams(QuicStreamType.BIDIRECTIONAL)).get(5, TimeUnit.SECONDS));

            var opening = fixture.open();
            // The submitted barrier runs after enqueueing the second request on this connection's loop.
            first.eventLoop().submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertFalse(opening.isDone());
            assertEquals(1, HttpTransport.openConnections(fixture.client));

            respond(firstServerStream, "one");
            assertEquals("one", firstReply.get(5, TimeUnit.SECONDS));
            Channel second = opening.get(5, TimeUnit.SECONDS);
            assertSame(first.parent(), second.parent());
            assertEquals(1, HttpTransport.openConnections(fixture.client));
            var secondReply = fixture.request(second, "second");
            respond(fixture.arrived(), "two");
            assertEquals("two", secondReply.get(5, TimeUnit.SECONDS));
        }
    }

    @Test void goAwayRetiresOnlyAfterTheLastRequestAndNewRequestsUseAnotherSocket() throws Exception {
        try (var fixture = new Fixture(2)) {
            Channel first = fixture.open().get(5, TimeUnit.SECONDS);
            var firstReply = fixture.request(first, "first");
            var firstServerStream = fixture.arrived();
            var old = (QuicChannel) first.parent();
            var oldSocket = old.parent();

            Channel last = fixture.open().get(5, TimeUnit.SECONDS);
            assertSame(old, last.parent());
            var lastReply = fixture.request(last, "last");
            var lastServerStream = fixture.arrived();
            var control = Http3.getLocalControlStream(lastServerStream.parent());
            assertNotNull(control);
            var received = observeGoAway(old);
            long goAwayId = lastServerStream.streamId() + 4;
            control.writeAndFlush(new DefaultHttp3GoAwayFrame(goAwayId)).get(5, TimeUnit.SECONDS);
            assertEquals(goAwayId, received.get(5, TimeUnit.SECONDS));

            Channel newStream = fixture.open().get(5, TimeUnit.SECONDS);
            assertNotSame(old, newStream.parent());
            assertNotSame(oldSocket, newStream.parent().parent());
            assertTrue(old.isActive());
            assertEquals(2, HttpTransport.openConnections(fixture.client));
            var newReply = fixture.request(newStream, "new");
            respond(fixture.arrived(), "new");
            assertEquals("new", newReply.get(5, TimeUnit.SECONDS));

            respond(firstServerStream, "original");
            assertEquals("original", firstReply.get(5, TimeUnit.SECONDS));
            first.closeFuture().get(5, TimeUnit.SECONDS);
            old.eventLoop().submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertTrue(old.isActive(), "a second request still uses the draining connection");
            assertEquals(2, HttpTransport.openConnections(fixture.client));

            respond(lastServerStream, "last");
            assertEquals("last", lastReply.get(5, TimeUnit.SECONDS));
            old.closeFuture().get(5, TimeUnit.SECONDS);
            oldSocket.closeFuture().get(5, TimeUnit.SECONDS);
            oldSocket.eventLoop().submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertEquals(1, HttpTransport.openConnections(fixture.client));
            assertTrue(newStream.parent().isActive());
        }
    }

    @Test void anExpiredCreditWaitMakesTheConnectionDraining() throws Exception {
        try (var fixture = new Fixture(1)) {
            Channel first = fixture.open().get(5, TimeUnit.SECONDS);
            var firstReply = fixture.request(first, "first");
            var firstServerStream = fixture.arrived();
            Channel old = first.parent();

            // This wait's deadline is already due, so the loop settles it without wall-clock timing.
            var opening = fixture.transport.stream(old, Duration.ZERO);
            var failure = assertThrows(ExecutionException.class, () -> opening.get(5, TimeUnit.SECONDS));
            var retry = assertInstanceOf(HttpTransport.RetryOpen.class, failure.getCause());
            assertInstanceOf(TimeoutException.class, retry.getCause());
            assertFalse(fixture.transport.reusable(old));
            assertTrue(old.isActive());

            Channel next = fixture.open().get(5, TimeUnit.SECONDS);
            assertNotSame(old, next.parent());
            var nextReply = fixture.request(next, "new");
            respond(fixture.arrived(), "new");
            assertEquals("new", nextReply.get(5, TimeUnit.SECONDS));

            respond(firstServerStream, "original");
            assertEquals("original", firstReply.get(5, TimeUnit.SECONDS));
            old.closeFuture().get(5, TimeUnit.SECONDS);
        }
    }

    private static CompletableFuture<Long> observeGoAway(QuicChannel connection) throws Exception {
        var received = new CompletableFuture<Long>();
        connection.eventLoop().submit(() -> connection.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
                if (event instanceof Http3GoAwayFrame goAway) {
                    received.complete(goAway.id());
                    ctx.pipeline().remove(this);
                }
                ctx.fireUserEventTriggered(event);
            }
        })).get(5, TimeUnit.SECONDS);
        return received;
    }

    private static void respond(QuicStreamChannel stream, String value) throws Exception {
        var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                Unpooled.copiedBuffer(value, StandardCharsets.US_ASCII));
        HttpUtil.setContentLength(response, response.content().readableBytes());
        stream.writeAndFlush(response).get(5, TimeUnit.SECONDS);
    }

    private static final class Fixture implements AutoCloseable {
        private final LinkedBlockingQueue<QuicStreamChannel> requests = new LinkedBlockingQueue<>();
        private final io.zmux.ZmuxListener listener;
        private final HttpEndpoint endpoint;
        private final Http3Server server;
        final Http3ClientTransport transport;
        final HttpCarrierClient client;
        private final URI uri;

        Fixture(int streamLimit) throws Exception {
            var options = QuicOptions.defaults().toBuilder().bidirectionalStreams(streamLimit).build();
            listener = Zmux.listener(ListenerConfig.builder().build());
            endpoint = HttpEndpoint.builder(listener).build();
            server = Http3Server.bind(new InetSocketAddress("127.0.0.1", 0),
                    QuicSslContextBuilder.forServer(H3TestSupport.kmf(), "changeit"), options, endpoint,
                    () -> new SimpleChannelInboundHandler<HttpObject>() {
                        @Override protected void channelRead0(ChannelHandlerContext ctx, HttpObject message) {
                            if (message instanceof HttpRequest) requests.add((QuicStreamChannel) ctx.channel());
                        }
                    });
            transport = new Http3ClientTransport(QuicSslContextBuilder.forClient().trustManager(H3TestSupport.tmf()),
                    QuicOptions.defaults());
            client = HttpTransport.clientBuilder(transport).timeout(Duration.ofSeconds(20)).build();
            uri = URI.create("https://localhost:" + server.address().getPort() + "/");
        }

        CompletableFuture<Channel> open() throws ReflectiveOperationException {
            return HttpTransport.openRequest(client, uri);
        }

        QuicStreamChannel arrived() throws InterruptedException {
            var stream = requests.poll(5, TimeUnit.SECONDS);
            assertNotNull(stream);
            return stream;
        }

        CompletableFuture<String> request(Channel stream, String path) throws Exception {
            var result = new CompletableFuture<String>();
            stream.eventLoop().submit(() -> stream.pipeline().addLast(new SimpleChannelInboundHandler<HttpObject>() {
                private final StringBuilder body = new StringBuilder();

                @Override protected void channelRead0(ChannelHandlerContext ctx, HttpObject message) {
                    if (message instanceof HttpResponse response && !response.status().equals(HttpResponseStatus.OK)) {
                        result.completeExceptionally(new IllegalStateException("HTTP status " + response.status()));
                    }
                    if (message instanceof HttpContent content) body.append(content.content().toString(StandardCharsets.US_ASCII));
                    if (message instanceof LastHttpContent) result.complete(body.toString());
                }

                @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                    result.completeExceptionally(cause);
                    ctx.close();
                }
            })).get(5, TimeUnit.SECONDS);
            var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/" + path, Unpooled.EMPTY_BUFFER);
            request.headers().set(HttpHeaderNames.HOST, uri.getRawAuthority());
            HttpUtil.setContentLength(request, 0);
            stream.writeAndFlush(request).get(5, TimeUnit.SECONDS);
            return result;
        }

        @Override public void close() {
            try (listener; endpoint; server; client) {
                // Close the client first, then the endpoint owners.
            }
        }
    }
}
