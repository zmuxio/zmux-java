package io.zmux.http;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.*;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import io.zmux.Admission;
import io.zmux.JoinHint;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class FallbackBodyTest {
    private static final class Request extends ChannelInboundHandlerAdapter {
        final CompletableFuture<Void> receivedLast = new CompletableFuture<>();
        final CompletableFuture<HttpRequest> head = new CompletableFuture<>();
        final CompletableFuture<HttpHeaders> trailers = new CompletableFuture<>();
        final CompletableFuture<Void> disconnected = new CompletableFuture<>();
        final AtomicInteger responses = new AtomicInteger();
        final StringBuilder body = new StringBuilder();

        @Override public void handlerAdded(ChannelHandlerContext ctx) {
            var pipeline = ctx.pipeline();
            pipeline.addBefore(pipeline.context(Router.class).name(), "observe-request", new ChannelDuplexHandler() {
                @Override public void channelRead(ChannelHandlerContext c, Object msg) {
                    boolean last = msg instanceof LastHttpContent;
                    c.fireChannelRead(msg);
                    if (last) receivedLast.complete(null);
                }
                @Override public void write(ChannelHandlerContext c, Object msg, ChannelPromise p) {
                    if (msg instanceof HttpResponse) responses.incrementAndGet();
                    c.write(msg, p);
                }
            });
        }

        @Override public void channelRead(ChannelHandlerContext c, Object msg) {
            try {
                if (msg instanceof HttpRequest r) head.complete(r);
                if (msg instanceof HttpContent part) body.append(part.content().toString(US_ASCII));
                if (msg instanceof LastHttpContent end) trailers.complete(end.trailingHeaders().copy());
            } finally { ReferenceCountUtil.release(msg); }
        }
        @Override public void channelInactive(ChannelHandlerContext c) { disconnected.complete(null); }
    }

    private static Fixture fixture(Request request, CompletableFuture<Admission.Decision> decision) throws Exception {
        return new Fixture(b -> b, l -> l.admission(Admission.builder(1, (i,p,d) -> decision).build()),
                ep -> ep.streaming(), false, () -> request);
    }

    // Decide only once Netty has delivered the complete body and trailers to the router.
    @Test void bodyFallbackReplaysUnmodifiedHeadersAndTrailersBeforeAnyResponse() throws Exception {
        // HS-13.
        for (boolean chunked : new boolean[]{false, true}) for (boolean expect : new boolean[]{false, true})
            for (boolean declare : chunked ? new boolean[]{false, true} : new boolean[]{false}) {
                var request = new Request();
                var decision = new CompletableFuture<Admission.Decision>();
                try (var f = fixture(request, decision); var socket = new Socket()) {
                    socket.connect(f.server.localAddress());
                    String header = "POST /z HTTP/1.1\r\nHost: " + f.base.getHost() + "\r\n" + JoinHint.PAIR_HEADER + ": "
                            + java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[16])
                            + "\r\nX-Original: first\r\nX-Original: second\r\n"
                            + (expect ? "Expect: 100-continue\r\n" : "")
                            + (declare ? "Trailer: X-End\r\n" : "")
                            + (chunked ? "Transfer-Encoding: chunked" : "Content-Length: 6") + "\r\n\r\n";
                    String body = chunked ? "3\r\nabc\r\n3\r\ndef\r\n0\r\nX-End: value\r\n\r\n" : "abcdef";
                    socket.getOutputStream().write((header + body).getBytes(US_ASCII));
                    request.receivedLast.get(5, TimeUnit.SECONDS);
                    assertFalse(request.head.isDone());
                    decision.complete(Admission.Decision.FALLBACK);
                    var original = request.head.get(5, TimeUnit.SECONDS);
                    var tail = request.trailers.get(5, TimeUnit.SECONDS);
                    assertEquals("/z", original.uri());
                    assertEquals(java.util.List.of("first", "second"), original.headers().getAll("X-Original"));
                    assertEquals(expect, HttpUtil.is100ContinueExpected(original));
                    assertEquals(declare, original.headers().contains("Trailer"));
                    assertEquals(chunked, HttpUtil.isTransferEncodingChunked(original));
                    if (!chunked) assertEquals(6, HttpUtil.getContentLength(original));
                    assertEquals("abcdef", request.body.toString());
                    assertEquals(chunked ? "value" : null, tail.get("X-End"));
                    assertEquals(0, request.responses.get());
                }
            }
    }

    @Test void defaultNotFoundCountsAsStalledAndAHostNotFoundCountsAsFallback() throws Exception {
        var admission = Admission.builder(0, Admission.Decider.immediate((i, p) -> Admission.Decision.FALLBACK.withVariant("request"))).build();
        for (boolean custom : new boolean[]{false, true}) {
            var received = new CompletableFuture<Void>();
            java.util.function.Supplier<io.netty.channel.ChannelHandler> host = custom
                    ? () -> new io.netty.channel.ChannelInitializer<io.netty.channel.Channel>() {
                        @Override protected void initChannel(io.netty.channel.Channel channel) {
                            channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    if (msg instanceof HttpRequest) received.complete(null);
                                    ctx.fireChannelRead(msg);
                                }
                            });
                            channel.pipeline().addLast(io.zmux.http.internal.HttpTransport.notFound());
                        }
                    } : null;
            try (var f = new Fixture(b -> b, l -> l.admission(admission), ep -> ep.streaming(), false, host);
                 var socket = new Socket()) {
                socket.connect(f.server.localAddress());
                socket.setSoTimeout(5_000);
                socket.getOutputStream().write(("POST /z HTTP/1.1\r\nHost: " + f.base.getHost()
                        + "\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n0\r\n\r\n").getBytes(US_ASCII));
                var reader = new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream(), US_ASCII));
                assertTrue(reader.readLine().startsWith("HTTP/1.1 404 "));
                assertEquals(custom, received.isDone());
                long fallback = custom ? 1 : 0;
                long stalled = custom ? 0 : 1;
                assertEquals(new io.zmux.AdmissionStats(0, fallback, 0, stalled, 0, 0, 0, 0, 0), f.listener.stats().admission());
                assertEquals(java.util.List.of(new Admission.VariantStat("request", false, 0, fallback, 0, stalled, 0)),
                        f.listener.variantStats());
            }
        }
    }

    // A Netty host waiting for more HttpContent observes channelInactive on listener stop.
    @SuppressWarnings("try")
    @Test void listenerStopInterruptsAnIncompleteBodyAlreadyGivenToTheHost() throws Exception {
        // HS-14.
        var request = new Request();
        try (var f = fixture(request, CompletableFuture.completedFuture(Admission.Decision.FALLBACK)); var socket = new Socket()) {
            socket.connect(f.server.localAddress());
            socket.getOutputStream().write(("POST /z HTTP/1.1\r\nHost: " + f.base.getHost()
                    + "\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n").getBytes(US_ASCII));
            request.head.get(5, TimeUnit.SECONDS);
            assertFalse(request.trailers.isDone());
            f.listener.close();
            request.disconnected.get(5, TimeUnit.SECONDS);
            assertFalse(request.trailers.isDone());
            assertEquals(0, request.responses.get());
        }
    }
}
