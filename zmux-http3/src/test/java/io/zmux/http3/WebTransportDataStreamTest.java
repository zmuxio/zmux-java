package io.zmux.http3;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamResetException;
import io.zmux.Admission;
import io.zmux.AdmissionVerdict;
import io.zmux.HttpStatusException;
import io.zmux.ListenerConfig;
import io.zmux.MemoryPrefixStore;
import io.zmux.PrefixAuth;
import io.zmux.PrefixStore;
import io.zmux.Role;
import io.zmux.Zmux;
import io.zmux.ZmuxConfig;
import io.zmux.carrier.CarrierChannel;
import io.zmux.carrier.PipeIo;
import io.zmux.http.HttpEndpoint;
import io.zmux.http.internal.HttpTransport;
import io.zmux.quic.QuicOptions;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class WebTransportDataStreamTest {
    private static final Duration LIMIT = Duration.ofSeconds(5);
    private static final QuicOptions OPTIONS = QuicOptions.builder().bidirectionalStreams(12).lanes(8).build();

    /** Models the missing native operations. TLS, H3, QPACK, streams and datagrams run over loopback UDP. */
    private static class Transport implements WebTransportProvider {
        private volatile QuicChannel server;

        @Override public QuicOptions receiveLimits(QuicChannel channel) {
            server = channel;
            return OPTIONS;
        }
        @Override public Duration reorderTimeout(QuicChannel channel) { return LIMIT; }
        @Override public long receiveCapacity(QuicChannel channel) { return OPTIONS.connectionWindow(); }
        @Override public ChannelFuture writePreface(QuicStreamChannel stream, ByteBuf header) { return stream.writeAndFlush(header); }
        @Override public ChannelFuture stop(QuicStreamChannel stream, long wireCode) { return stream.newSucceededFuture(); }
        @Override public ChannelFuture reset(QuicStreamChannel stream, long wireCode) { return stream.close(); }
        @Override public void observeTermination(QuicStreamChannel stream, Consumer<WebTransportStreamException> consumer) { }
    }

    @Test void ownedConnectDataStreamCarriesBootstrapAndApplicationBytesInBothDirections() throws Exception {
        var config = ZmuxConfig.builder().enableDatagrams(true).build();
        var ticketOptions = new PrefixAuth.CarrierOptions(true, false);
        var ticketId = PrefixStore.Id.of(new byte[16]);
        var generation = new PrefixStore.Generation(new byte[32], 0, -1);
        var slot = Duration.ofSeconds(30);
        var clock = Clock.fixed(Instant.ofEpochSecond(900), ZoneOffset.UTC);
        var random = SecureRandom.getInstance("SHA1PRNG");
        random.setSeed(new byte[]{3, 2, 1});
        var ticket = new PrefixAuth(ticketOptions, slot, ticketId, new byte[32], List.of(generation), clock, random);
        var store = new MemoryPrefixStore(new PrefixStore.Window(slot, 1), List.of(generation));
        store.addTicket(new PrefixStore.Ticket(ticketId, new byte[32], 0, 0, -1, false));
        var verifier = new PrefixAuth.Verifier(ticketOptions, store, 1, clock);
        var decisions = new AtomicInteger();
        var admission = Admission.builder(PrefixAuth.TOKEN_LENGTH, (info, prefix, deadline) -> {
            assertEquals(Admission.Stage.HEAD, info.stage());
            assertEquals(Admission.OuterSecurity.QUIC, info.outerSecurity());
            decisions.incrementAndGet();
            return verifier.decide(info, prefix, deadline);
        }).build();
        try (var listener = Zmux.listener(ListenerConfig.builder().session(config).admission(admission).build());
             var endpoint = HttpEndpoint.builder(listener).webTransport().build();
             var server = Http3Server.bind(new InetSocketAddress("127.0.0.1", 0),
                     QuicSslContextBuilder.forServer(H3TestSupport.kmf(), "changeit"), OPTIONS, endpoint, null);
             var client = Http3Client.builder(
                     QuicSslContextBuilder.forClient().trustManager(H3TestSupport.tmf()), OPTIONS).timeout(LIMIT).build()) {
            URI target = URI.create("https://localhost:" + server.address().getPort() + "/wt");
            // A rejected CONNECT consumes ID 0. The next session uses quarter ID 1 and pipe ID 8.
            var refusal = assertThrows(ExecutionException.class,
                    () -> Http3Client.openWebTransportAsync(client, target, Map.of(), null, LIMIT).get(5, SECONDS));
            assertInstanceOf(QuicStreamResetException.class, refusal.getCause());
            assertEquals(1, decisions.get());
            try (var wt = Http3Client.openWebTransportAsync(client, target, Map.of(), ticket, LIMIT).get(5, SECONDS)) {
                assertEquals(4, wt.id());
                assertEquals(2, decisions.get());
                var pipe = wt.openPipeAsync().get(5, SECONDS);
                assertEquals(8, pipe.capabilities().datagramChannel().channelStreamId());
                try (var sender = Zmux.open(pipe, Role.CLIENT, config);
                     var receiver = listener.accept(LIMIT)) {
                    assertNotNull(receiver);
                    H3TestSupport.exchange(sender, receiver, 4097);
                    H3TestSupport.exchange(receiver, sender, 8193);
                    byte[] marker = {0x4a, 0x47, 0x32};
                    sender.datagrams().send(ByteBuffer.wrap(marker));
                    assertArrayEquals(marker, receiver.datagrams().receive());
                    assertEquals(1, receiver.stats().datagramsReceivedChannel());
                    assertTrue(wt.draining().cancel(false));
                    wt.drainAsync().get(5, SECONDS);
                    wt.draining().get(5, SECONDS);
                    H3TestSupport.exchange(sender, receiver, 37);
                }
                wt.closeAsync(42, "结束").get(5, SECONDS);
                assertEquals(new WebTransportSession.Close(42, "结束", false), wt.closed().get(5, SECONDS));
            }
        }
    }
    @Test void connectFallbackWritesOneFinalStatusAndPreservesTheConnection() throws Exception {
        var transport = new Transport();
        var fallback = new AtomicInteger();
        var admission = Admission.builder(0, (info, prefix, deadline) ->
                CompletableFuture.completedFuture(Admission.Decision.FALLBACK)).build();
        try (var listener = Zmux.listener(ListenerConfig.builder().admission(admission).build());
             var endpoint = HttpEndpoint.builder(listener).webTransport().build();
             var server = Http3Server.bind(new InetSocketAddress("127.0.0.1", 0),
                     QuicSslContextBuilder.forServer(H3TestSupport.kmf(), "changeit"), OPTIONS, endpoint,
                     () -> new SimpleChannelInboundHandler<HttpObject>() {
                         @Override protected void channelRead0(ChannelHandlerContext ctx,
                                 HttpObject message) {
                             if (message instanceof HttpRequest) {
                                 fallback.incrementAndGet();
                                 var reply = new DefaultFullHttpResponse(
                                         HttpVersion.HTTP_1_1,
                                         HttpResponseStatus.TEMPORARY_REDIRECT);
                                 reply.headers().set("location", "/different");
                                 ctx.writeAndFlush(reply);
                             }
                         }
                     }, transport);
             var client = HttpTransport.clientBuilder(new Http3ClientTransport(
                     QuicSslContextBuilder.forClient().trustManager(H3TestSupport.tmf()), OPTIONS, transport)).timeout(LIMIT).build()) {
            URI target = URI.create("https://localhost:" + server.address().getPort() + "/wt");
            for (var test : List.of(Map.entry(403, Map.of("origin", List.of("https://example.invalid"))),
                    Map.entry(307, Map.<String, List<String>>of()))) {
                var failure = assertThrows(ExecutionException.class,
                        () -> Http3Client.openWebTransportAsync(client, target, test.getValue(), null, LIMIT).get(5, SECONDS));
                assertEquals(test.getKey().intValue(), assertInstanceOf(HttpStatusException.class, failure.getCause()).status());
            }
            assertEquals(1, fallback.get());
            assertEquals(1, HttpTransport.openConnections(client));
        }
    }

    @Test void cancellationWhileFinalHeadersAreHeldNeverHandsOffAPipe() throws Exception {
        var blocked = new CompletableFuture<Runnable>();
        var retired = new CompletableFuture<Void>();
        var fallback = new AtomicInteger();
        var transport = new Transport() {
            @Override public long receiveCapacity(QuicChannel channel) {
                // Install at the wire boundary before Netty registers the server's first request.
                channel.pipeline().addFirst(new ChannelInboundHandlerAdapter() {
                    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        if (msg instanceof QuicStreamChannel request && request.streamId() == 0) {
                            request.closeFuture().addListener(done -> retired.complete(null));
                            request.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
                                @Override public void write(ChannelHandlerContext out, Object encoded,
                                                           ChannelPromise done) {
                                    if (encoded instanceof ByteBuf bytes && blocked.complete(() ->
                                            out.executor().execute(() -> {
                                                bytes.release();
                                                done.tryFailure(new IOException("injected HEADERS write failure"));
                                            }))) return;
                                    out.write(encoded, done);
                                }
                            });
                        }
                        ctx.fireChannelRead(msg);
                    }
                });
                return super.receiveCapacity(channel);
            }
        };
        try (var listener = Zmux.listener(ListenerConfig.of(ZmuxConfig.defaults()));
             var endpoint = HttpEndpoint.builder(listener).webTransport().build();
             var server = Http3Server.bind(new InetSocketAddress("127.0.0.1", 0),
                     QuicSslContextBuilder.forServer(H3TestSupport.kmf(), "changeit"), OPTIONS, endpoint,
                     () -> new SimpleChannelInboundHandler<HttpObject>() {
                         @Override protected void channelRead0(ChannelHandlerContext ctx, HttpObject message) {
                             fallback.incrementAndGet();
                         }
                     }, transport);
             var client = HttpTransport.clientBuilder(new Http3ClientTransport(
                     QuicSslContextBuilder.forClient().trustManager(H3TestSupport.tmf()), OPTIONS, transport)).timeout(LIMIT).build()) {
            URI target = URI.create("https://localhost:" + server.address().getPort() + "/wt");
            var opening = Http3Client.openWebTransportAsync(client, target, Map.of(), null, LIMIT);
            Runnable failWrite = blocked.get(5, SECONDS);
            try {
                assertFalse(opening.isDone());
                assertEquals(0, listener.stats().sessions());
                assertTrue(opening.cancel(false));
            } finally {
                failWrite.run();
            }
            retired.get(5, SECONDS);
            assertTrue(opening.isCancelled());
            assertEquals(0, listener.stats().sessions());
            assertEquals(0, fallback.get());
        }
    }

    @Test void cancelAfterPrefaceSubmissionUsesFullWidthResetAndDropsLateCompletion() throws Exception {
        var submitted = new CompletableFuture<ChannelPromise>();
        var stopped = new CompletableFuture<Long>();
        var reset = new CompletableFuture<Long>();
        var transport = new Transport() {
            @Override public ChannelFuture writePreface(QuicStreamChannel stream, ByteBuf header) {
                if (stream.streamId() != 8) return super.writePreface(stream, header);
                var gate = stream.newPromise();
                super.writePreface(stream, header).addListener(done -> {
                    if (done.isSuccess()) submitted.complete(gate);
                    else submitted.completeExceptionally(done.cause());
                });
                return gate;
            }
            @Override public ChannelFuture stop(QuicStreamChannel stream, long code) {
                if (stream.streamId() == 8 && stream.isLocalCreated()) stopped.complete(code);
                return super.stop(stream, code);
            }
            @Override public ChannelFuture reset(QuicStreamChannel stream, long code) {
                if (stream.streamId() == 8 && stream.isLocalCreated()) reset.complete(code);
                return super.reset(stream, code);
            }
        };
        try (var listener = Zmux.listener(ListenerConfig.of(ZmuxConfig.defaults()));
             var endpoint = HttpEndpoint.builder(listener).webTransport().build();
             var server = Http3Server.bind(new InetSocketAddress("127.0.0.1", 0),
                     QuicSslContextBuilder.forServer(H3TestSupport.kmf(), "changeit"), OPTIONS, endpoint, null, transport);
             var client = HttpTransport.clientBuilder(new Http3ClientTransport(
                     QuicSslContextBuilder.forClient().trustManager(H3TestSupport.tmf()), OPTIONS, transport)).timeout(LIMIT).build()) {
            URI target = URI.create("https://localhost:" + server.address().getPort() + "/wt");
            try (var wt = Http3Client.openWebTransportAsync(client, target, Map.of(), null, LIMIT).get(5, SECONDS);
                 var sender = Zmux.open(wt.openPipeAsync().get(5, SECONDS), Role.CLIENT, ZmuxConfig.defaults());
                 var receiver = listener.accept(LIMIT)) {
                assertNotNull(receiver);
                var opening = wt.openPipeAsync();
                var gate = submitted.get(5, SECONDS);
                assertTrue(opening.cancel(false));
                long code = WebTransportStreamException.wireCode(PipeIo.CARRIER_FAILED);
                assertEquals(code, stopped.get(5, SECONDS));
                assertEquals(code, reset.get(5, SECONDS));
                gate.channel().eventLoop().submit(() -> gate.trySuccess()).sync();
                assertTrue(opening.isCancelled());
                assertFalse(wt.closed().isDone());
                H3TestSupport.exchange(sender, receiver, 311);

                wt.closeAsync(0, "").get(5, SECONDS);
            }
        }
    }

    @Test void onlyOneSessionPerConnectionAndDrainKeepsPipesAlive() throws Exception {
        var transport = new Transport();
        try (var listener = Zmux.listener(ListenerConfig.of(ZmuxConfig.defaults()));
             var endpoint = HttpEndpoint.builder(listener).webTransport().build();
             var server = Http3Server.bind(new InetSocketAddress("127.0.0.1", 0),
                     QuicSslContextBuilder.forServer(H3TestSupport.kmf(), "changeit"), OPTIONS, endpoint, null, transport);
             var client = HttpTransport.clientBuilder(new Http3ClientTransport(
                     QuicSslContextBuilder.forClient().trustManager(H3TestSupport.tmf()), OPTIONS, transport)).timeout(LIMIT).build()) {
            URI target = URI.create("https://localhost:" + server.address().getPort() + "/wt");
            try (var first = Http3Client.openWebTransportAsync(client, target, Map.of(), null, LIMIT).get(5, SECONDS);
                 var sender = Zmux.open(first.openPipeAsync().get(5, SECONDS), Role.CLIENT, ZmuxConfig.defaults());
                 var receiver = listener.accept(LIMIT)) {
                assertNotNull(receiver);
                var failure = assertThrows(ExecutionException.class,
                        () -> Http3Client.openWebTransportAsync(client, target, Map.of(), null, LIMIT).get(5, SECONDS));
                assertEquals("single WebTransport session per connection", failure.getCause().getMessage());
                first.drainAsync().get(5, SECONDS);
                first.draining().get(5, SECONDS);
                var remote = transport.server.eventLoop().submit(() ->
                        WebTransportConnection.get(transport.server).session(first.id())).get(5, SECONDS);
                remote.draining().get(5, SECONDS);
                assertFalse(remote.closed().isDone());
                H3TestSupport.exchange(sender, receiver, 37);
                first.closeAsync(41, "first").get(5, SECONDS);
                assertEquals(new WebTransportSession.Close(41, "first", true), remote.closed().get(5, SECONDS));
            }
            var failure = assertThrows(ExecutionException.class,
                    () -> Http3Client.openWebTransportAsync(client, target, Map.of(), null, LIMIT).get(5, SECONDS));
            assertEquals("single WebTransport session per connection", failure.getCause().getMessage());
        }
    }

    @Test void detachedWebTransportTakeoverReplaysPrefixAndKeepsOriginalDatagrams() throws Exception {
        var original = new CompletableFuture<CarrierChannel>();
        var detached = new CompletableFuture<PipeIo>();
        var admission = Admission.builder(4, Admission.Decider.immediate((info, prefix) ->
                info.stage() == Admission.Stage.HEAD ? new Admission.Decision(AdmissionVerdict.DEFER, 0)
                        : Admission.Decision.TAKEOVER)).build();
        var firstPipe = WebTransportRequest.class.getDeclaredField("first");
        firstPipe.setAccessible(true);
        var config = ListenerConfig.builder().admission(admission).fallback(takeover -> {
            assertTrue(takeover.detach());
            detached.complete(takeover.pipe());
            return null;
        }).build();
        try (var listener = Zmux.listener(config);
             var endpoint = HttpEndpoint.builder(listener).webTransport().build();
             var server = Http3Server.bind(new InetSocketAddress("127.0.0.1", 0),
                     QuicSslContextBuilder.forServer(H3TestSupport.kmf(), "changeit"), OPTIONS, endpoint,
                     () -> new ChannelInboundHandlerAdapter() {
                         @Override public void handlerAdded(ChannelHandlerContext ctx) throws IllegalAccessException {
                             var request = ctx.pipeline().get(WebTransportRequest.class);
                             original.complete((WebTransportFirstPipe) firstPipe.get(request));
                         }
                     });
             var client = Http3Client.builder(QuicSslContextBuilder.forClient().trustManager(H3TestSupport.tmf()),
                     OPTIONS).build()) {
            var target = URI.create("https://localhost:" + server.address().getPort() + "/wt");
            try (var session = Http3Client.openWebTransportAsync(client, target, Map.of(), null, LIMIT).get(5, SECONDS)) {
                var outgoing = session.openPipeAsync().get(5, SECONDS);
                H3TestSupport.write(outgoing.writer(), "TUN1payload");
                var incoming = detached.get(5, SECONDS);
                try {
                    var replay = H3TestSupport.reading(incoming.reader());
                    assertEquals("TUN1payload", H3TestSupport.take(replay, 11));
                    var nativeChannel = original.get(5, SECONDS);
                    var received = new CompletableFuture<Byte>();
                    var remove = nativeChannel.receive((bytes, from, to) -> {
                        assertEquals(from + 1, to);
                        received.complete(bytes.get(from));
                    });
                    try {
                        // A response on the stream tells the client its peer's datagram route is installed.
                        var response = H3TestSupport.reading(outgoing.reader());
                        H3TestSupport.write(incoming.writer(), "ready");
                        assertEquals("ready", H3TestSupport.take(response, 5));
                        var send = outgoing.capabilities().datagramChannel();
                        assertEquals(4, send.channelStreamId());
                        var sent = new CompletableFuture<CarrierChannel.Outcome>();
                        send.send(new byte[]{4, 42}, Long.MAX_VALUE, sent::complete);
                        assertEquals(CarrierChannel.Outcome.ACCEPTED, sent.get(5, SECONDS));
                        assertEquals((byte) 42, received.get(5, SECONDS));
                    } finally {
                        remove.run();
                    }
                } finally {
                    incoming.reader().close();
                    incoming.writer().close();
                    outgoing.reader().close();
                    outgoing.writer().close();
                }
            }
        }
    }

}
