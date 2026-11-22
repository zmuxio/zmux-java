package io.zmux.http3;

import static io.zmux.http3.H3TestSupport.reading;
import static io.zmux.http3.H3TestSupport.take;
import static io.zmux.http3.H3TestSupport.write;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http3.Http3FrameToHttpObjectCodec;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.util.ReferenceCountUtil;
import io.zmux.Admission;
import io.zmux.CarrierKind;
import io.zmux.CarrierSample;
import io.zmux.CarrierSource;
import io.zmux.EventLoops;
import io.zmux.JoinHint;
import io.zmux.ListenerConfig;
import io.zmux.PipeAuth;
import io.zmux.PipeOptions;
import io.zmux.QuicCounters;
import io.zmux.RecordMode;
import io.zmux.Role;
import io.zmux.Takeover;
import io.zmux.Zmux;
import io.zmux.ZmuxConfig;
import io.zmux.ZmuxListener;
import io.zmux.ZmuxSession;
import io.zmux.adapter.Handled;
import io.zmux.carrier.PipeIo;
import io.zmux.http.ExchangeConfig;
import io.zmux.http.HttpCarrierClient;
import io.zmux.http.HttpEndpoint;
import io.zmux.http.internal.HttpTransport;
import io.zmux.internal.spi.AdapterTestSupport;
import io.zmux.internal.spi.AdapterPorts;
import io.zmux.quic.QuicOptions;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 40, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class Http3CarrierTest {
    static final QuicOptions QUIC = QuicOptions.defaults();
    static final InetSocketAddress LOOPBACK = new InetSocketAddress("127.0.0.1", 0);
    static final Duration WAIT = Duration.ofSeconds(5);
    private static final Duration PEEK_TIMEOUT = Duration.ofMinutes(1);

    private static CarrierSample carrierSample(ZmuxSession session) throws Exception {
        long deadline = System.nanoTime() + WAIT.toNanos();
        do {
            var sample = session.pipeStats().getFirst().carrierSample();
            if (sample.isPresent()) return sample.orElseThrow();
            Thread.sleep(5);
        } while (System.nanoTime() - deadline < 0);
        throw new AssertionError("HTTP/3 carrier did not publish a sample");
    }

    @Test void streamingAndExchangeReachTheSameListener() throws Exception {
        var exchange = ExchangeConfig.builder(new byte[32]).holdTime(Duration.ofMillis(100)).build();
        try (var fixture = new QuicFixture(b -> b, b -> b.streaming().exchange(exchange), null)) {
            assertEquals(CarrierKind.HTTP3, fixture.client.streamingCandidate(fixture.uri, Map.of()).kind());
            try (var client = Zmux.open(fixture.stream(), Role.CLIENT, fixture.config);
                 var accepted = fixture.listener.accept(WAIT)) {
                assertNotNull(accepted);
                H3TestSupport.exchange(client, accepted, 100_000);
                H3TestSupport.exchange(accepted, client, 100_001);
                for (var session : List.of(client, accepted)) {
                    var sample = carrierSample(session);
                    assertEquals(CarrierSource.NETTY_QUIC, sample.source());
                    var counters = assertInstanceOf(QuicCounters.class, sample.counts().orElseThrow());
                    assertTrue(counters.packetsSent() > 0);
                }
            }
            try (var client = Zmux.open(fixture.client.exchangePipe(fixture.uri.toString(), exchange), Role.CLIENT, fixture.config);
                 var accepted = fixture.listener.accept(WAIT)) {
                assertNotNull(accepted);
                H3TestSupport.exchange(client, accepted, 32_001);
                H3TestSupport.exchange(accepted, client, 32_002);
            }
        }
    }

    @Test void exporterAuthenticatesABoundJoinOverTheSharedConnection() throws Exception {
        try (var fixture = new QuicFixture(b -> b.pipeAuth(PipeAuth.exporter().withEndToEnd())
                .allowRecordMode((carrier, mode, peer) -> true).maxPipes(4), b -> b, HttpEndpoint.Builder::streaming, null)) {
            var io = fixture.stream();
            assertTrue(io.isCarrierSecure());
            assertNotNull(io.capabilities().exporter());
            try (var client = Zmux.open(io, Role.CLIENT, fixture.config);
                 var accepted = fixture.listener.accept(WAIT)) {
                assertNotNull(accepted);
                var joined = fixture.stream();
                var member = client.addPipe(joined, PipeOptions.builder().recordMode(RecordMode.BOUND).build());
                assertEquals(RecordMode.BOUND, member.stats().orElseThrow().recordMode());
                H3TestSupport.exchange(client, accepted, 32_003);
            }
        }
    }

    @Test void splitPairsInBothOrdersAndAnOrphanCloseLeavesConnectionUsable() throws Exception {
        var arrivals = new LinkedBlockingQueue<Arrival>();
        try (var fixture = new QuicFixture(b -> b, HttpEndpoint.Builder::streaming, observeSplit(arrivals));
             var secondConnection = QuicFixture.newClient()) {
            io.zmux.CarrierId uploadId = null;
            for (boolean uploadFirst : new boolean[]{false, true}) {
                var token = uploadFirst ? "AAAAAAAAAAAAAAAAAAAAAQ" : "AAAAAAAAAAAAAAAAAAAAAA";
                var headers = Map.of(JoinHint.PAIR_HEADER, List.of(token));
                PipeIo up;
                PipeIo down;
                if (uploadFirst) {
                    up = fixture.client.pipe(fixture.uri, headers, PipeOptions.Direction.SEND_ONLY, null);
                    var upload = next(arrivals);
                    assertEquals(HttpMethod.POST, upload.method());
                    down = secondConnection.pipe(fixture.uri, headers, PipeOptions.Direction.RECEIVE_ONLY, null);
                    var download = next(arrivals);
                    assertEquals(HttpMethod.GET, download.method());
                    assertFalse(upload.channel().parent().equals(download.channel().parent()));
                } else {
                    down = fixture.client.pipe(fixture.uri, headers, PipeOptions.Direction.RECEIVE_ONLY, null);
                    var download = next(arrivals);
                    assertEquals(HttpMethod.GET, download.method());
                    up = fixture.client.pipe(fixture.uri, headers, PipeOptions.Direction.SEND_ONLY, null);
                    var upload = next(arrivals);
                    assertEquals(HttpMethod.POST, upload.method());
                    assertEquals(download.channel().parent(), upload.channel().parent());
                }
                try (var client = Zmux.open(PipeIo.split(up, down), Role.CLIENT, fixture.config);
                     var accepted = fixture.listener.accept(WAIT)) {
                    assertNotNull(accepted);
                    H3TestSupport.exchange(client, accepted, 8193);
                    var sample = carrierSample(client);
                    assertEquals(CarrierSource.NETTY_QUIC, sample.source());
                    if (uploadId == null) uploadId = sample.carrierId();
                    else assertEquals(uploadId, sample.carrierId(), "receiving connection does not change the sender identity");
                    var replay = fixture.client.pipe(fixture.uri, headers, PipeOptions.Direction.RECEIVE_ONLY, null);
                    assertEquals(HttpMethod.GET, next(arrivals).method());
                    var rejected = reading(replay.reader()).poll(5, TimeUnit.SECONDS);
                    assertNotNull(rejected);
                    assertTrue(rejected.startsWith("error"), rejected);
                    replay.reader().close();
                    H3TestSupport.exchange(accepted, client, 8194);
                }
            }
            var orphan = fixture.client.pipe(fixture.uri, Map.of(JoinHint.PAIR_HEADER, List.of("AAAAAAAAAAAAAAAAAAAAAg")),
                    PipeOptions.Direction.SEND_ONLY, null);
            var unmatched = next(arrivals);
            assertEquals(HttpMethod.POST, unmatched.method());
            unmatched.channel().close().get(5, TimeUnit.SECONDS);
            orphan.writer().close();
            try (var client = Zmux.open(fixture.client.splitPipe(fixture.uri, Map.of(), null), Role.CLIENT, fixture.config);
                 var accepted = fixture.listener.accept(WAIT)) {
                assertNotNull(accepted);
                H3TestSupport.exchange(client, accepted, 8195);
            }
        }
    }

    @Test void splitJoinAttachesBothDirectionsAsOneMember() throws Exception {
        try (var fixture = new QuicFixture(b -> b.maxPipes(4).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()),
                b -> b, HttpEndpoint.Builder::streaming, null);
             var client = Zmux.open(fixture.stream(), Role.CLIENT, fixture.config);
             var accepted = fixture.listener.accept(WAIT)) {
            assertNotNull(accepted);
            var joined = fixture.client.splitJoin(client, fixture.uri, Map.of(), null, PipeOptions.defaults());
            var stats = joined.stats().orElseThrow();
            assertNotNull(stats.inbound());
            assertNotNull(stats.outbound());
            H3TestSupport.exchange(client, accepted, 8196);
        }
    }

    @Test void authenticatedExchangeCanBeTakenOver() throws Exception {
        var taken = new LinkedBlockingQueue<Takeover>();
        var exchange = ExchangeConfig.builder(new byte[32]).holdTime(Duration.ofMillis(100)).build();
        try (var fixture = new QuicFixture(admitTakeover(taken, () -> {}), b -> b.exchange(exchange), null)) {
            var client = fixture.client.exchangePipe(fixture.uri.toString(), exchange);
            var response = reading(client.reader());
            write(client.writer(), "exchange up");
            var takeover = taken.poll(5, TimeUnit.SECONDS);
            assertNotNull(takeover);
            var request = reading(takeover.pipe().reader());
            assertEquals("exchange up", take(request, 11));
            write(takeover.pipe().writer(), "exchange down");
            assertEquals("exchange down", take(response, 13));
            client.writer().close();
            client.reader().close();
            takeover.pipe().writer().close();
            takeover.pipe().reader().close();
        }
    }

    @Test void takeoverWaitsForNativeHeadAndPreservesBothHalves() throws Exception {
        var held = new CompletableFuture<Head>();
        var taken = new LinkedBlockingQueue<Takeover>();
        // Record the promise state at callback entry, before the host can touch either half.
        var committedWhenInvoked = new AtomicBoolean();
        try (var fixture = new QuicFixture(admitTakeover(taken, () -> {
            var intercepted = held.getNow(null);
            committedWhenInvoked.set(intercepted != null && intercepted.promise().isSuccess());
        }), HttpEndpoint.Builder::streaming, holdHead(held))) {
            var client = fixture.stream();
            var response = reading(client.reader());
            write(client.writer(), "TUN1hello");
            try (var head = held.get(5, TimeUnit.SECONDS)) {
                assertFalse(head.promise().isDone());
                head.forward();
                Takeover takeover = taken.poll(5, TimeUnit.SECONDS);
                assertNotNull(takeover);
                assertTrue(committedWhenInvoked.get(), "the native write had completed when the callback ran");
                var request = reading(takeover.pipe().reader());
                assertEquals("TUN1hello", take(request, 9));
                write(takeover.pipe().writer(), "echo");
                assertEquals("echo", take(response, 4));
                takeover.pipe().writer().close();
                assertEquals("end", response.poll(5, TimeUnit.SECONDS));
                write(client.writer(), "still up");
                assertEquals("still up", take(request, 8));
                client.writer().close();
                assertEquals("end", request.poll(5, TimeUnit.SECONDS));
                takeover.pipe().reader().close();
                client.reader().close();
            }
        }
    }

    @Test void failedNativeHeadRetiresTakeoverWithoutInvokingIt() throws Exception {
        unopenedTakeover((listener, head) -> { });
    }

    @Test void expiredTakeoverRetainsSlotUntilNativeHeadSettles() throws Exception {
        unopenedTakeover((listener, head) -> {
            AdapterTestSupport.advanceTimers(head.handled(), PEEK_TIMEOUT).get(5, TimeUnit.SECONDS);
            awaitSetupResult(head);
            assertThrows(SocketTimeoutException.class, head.handled()::awaitSetup);
        });
    }

    @Test void withdrawnTakeoverRetainsSlotUntilNativeHeadSettles() throws Exception {
        var held = new CompletableFuture<Head>();
        var taken = new LinkedBlockingQueue<Takeover>();
        var netty = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try (var loops = EventLoops.create(1)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var limits = ListenerConfig.builder().session(config).maxPending(1);
            try (var listener = Zmux.listener(admitTakeover(taken, () -> {}).apply(limits).build());
                 var endpoint = HttpEndpoint.builder(listener).streaming().build();
                 var server = Http3HostedTest.bind(netty, endpoint, holdHead(held));
                 var client = QuicFixture.newClient()) {
                var pipe = client.pipe(server.uri().resolve("/carrier"), Map.of(), PipeOptions.Direction.BIDIRECTIONAL, null);
                checkUnopenedTakeover(listener, pipe, held, taken, (activeListener, head) -> {
                    assertTrue(server.observed().get(5, TimeUnit.SECONDS).cancel(false));
                    awaitSetupResult(head);
                });
            }
        } finally {
            netty.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).awaitUninterruptibly(5, TimeUnit.SECONDS);
        }
    }

    @Test void listenerCloseRetainsSlotUntilNativeHeadSettles() throws Exception {
        unopenedTakeover((listener, head) -> {
            listener.close();
            awaitSetupResult(head);
        });
    }

    @FunctionalInterface
    private interface Ending {
        void end(ZmuxListener listener, Head head) throws Exception;
    }

    private static void unopenedTakeover(Ending ending) throws Exception {
        var held = new CompletableFuture<Head>();
        var taken = new LinkedBlockingQueue<Takeover>();
        try (var fixture = new QuicFixture(b -> admitTakeover(taken, () -> {}).apply(b).maxPending(1),
                HttpEndpoint.Builder::streaming, holdHead(held))) {
            checkUnopenedTakeover(fixture.listener, fixture.stream(), held, taken, ending);
        }
    }

    private static void checkUnopenedTakeover(ZmuxListener listener, PipeIo client, CompletableFuture<Head> held,
                                                LinkedBlockingQueue<Takeover> taken, Ending ending) throws Exception {
        reading(client.reader());
        write(client.writer(), "TUN1");
        var head = held.get(5, TimeUnit.SECONDS);
        var retired = head.handled().closed();
        try (head) {
            ending.end(listener, head);
            assertFalse(head.promise().isDone());
            assertFalse(retired.isDone());
            assertTrue(AdapterTestSupport.holdsAdmissionSlot(head.handled()));
            assertFalse(canReserve(listener));
            assertTrue(taken.isEmpty());
        }
        retired.get(5, TimeUnit.SECONDS);
        assertFalse(AdapterTestSupport.holdsAdmissionSlot(head.handled()));
        if (!listener.isClosed()) assertTrue(canReserve(listener));
        assertTrue(taken.isEmpty());
    }

    private static void awaitSetupResult(Head head) throws Exception {
        // A final setup result can arrive before the held write relinquishes its attachment share.
        var finished = new CompletableFuture<Void>();
        AdapterPorts.setup(head.handled()).onFinished(() -> finished.complete(null));
        finished.get(5, TimeUnit.SECONDS);
    }

    private static boolean canReserve(ZmuxListener listener) {
        var slot = listener.adapter().reserve(LOOPBACK, LOOPBACK);
        if (slot == null) return false;
        slot.release();
        return true;
    }

    private static UnaryOperator<ListenerConfig.Builder> admitTakeover(LinkedBlockingQueue<Takeover> taken, Runnable entered) {
        return b -> b.admission(Admission.builder(4, Admission.Decider.immediate((info, prefix) ->
                Admission.Decision.takeover("tunnel"))).peekTimeout(PEEK_TIMEOUT).build())
                .fallback(t -> {
                    entered.run();
                    assertTrue(t.detach());
                    taken.add(t);
                    return null;
                });
    }

    private record Arrival(HttpMethod method, Channel channel) { }

    private static Arrival next(LinkedBlockingQueue<Arrival> arrivals) throws InterruptedException {
        var arrival = arrivals.poll(5, TimeUnit.SECONDS);
        assertNotNull(arrival);
        return arrival;
    }

    private static Supplier<ChannelHandler> observeSplit(LinkedBlockingQueue<Arrival> arrivals) {
        return () -> new ChannelInitializer<Channel>() {
            @Override protected void initChannel(Channel channel) {
                var codec = channel.pipeline().context(Http3FrameToHttpObjectCodec.class).name();
                channel.pipeline().addAfter(codec, null, new ChannelInboundHandlerAdapter() {
                    @Override public void channelRead(ChannelHandlerContext context, Object message) {
                        if (message instanceof HttpRequest request && request.headers().contains(JoinHint.PAIR_HEADER)) {
                            arrivals.add(new Arrival(request.method(), context.channel()));
                        }
                        context.fireChannelRead(message);
                    }
                });
                channel.pipeline().addLast(HttpTransport.notFound());
            }
        };
    }

    private static Supplier<ChannelHandler> holdHead(CompletableFuture<Head> held) {
        return () -> new ChannelInitializer<Channel>() {
            @Override protected void initChannel(Channel channel) {
                channel.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
                    @Override public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                        if (msg instanceof ByteBuf bytes && !held.isDone()) {
                            var handled = H3TestSupport.currentSetup(ctx.channel());
                            if (handled != null && held.complete(new Head(ctx, bytes, promise, handled))) return;
                        }
                        ctx.write(msg, promise);
                    }
                });
                channel.pipeline().addLast(HttpTransport.notFound());
            }
        };
    }

    private record Head(ChannelHandlerContext context, AtomicReference<ByteBuf> encoded,
                        ChannelPromise promise, Handled handled) implements AutoCloseable {
        Head(ChannelHandlerContext context, ByteBuf bytes, ChannelPromise promise, Handled handled) {
            this(context, new AtomicReference<>(bytes), promise, handled);
        }

        void forward() throws Exception {
            context.executor().submit(() -> {
                ByteBuf bytes = encoded.getAndSet(null);
                if (bytes != null) context.writeAndFlush(bytes, promise);
            }).get(5, TimeUnit.SECONDS);
        }

        @Override public void close() {
            ByteBuf bytes = encoded.getAndSet(null);
            if (bytes != null) {
                ReferenceCountUtil.release(bytes);
                promise.tryFailure(new ClosedChannelException());
            }
        }
    }

    static final class QuicFixture implements AutoCloseable {
        final EventLoops loops = EventLoops.create(2);
        final ZmuxConfig config;
        final ZmuxListener listener;
        final HttpEndpoint endpoint;
        final Http3Server server;
        final HttpCarrierClient client;
        final URI uri;

        QuicFixture(UnaryOperator<ListenerConfig.Builder> listenerConfig, UnaryOperator<HttpEndpoint.Builder> endpointConfig,
                    Supplier<ChannelHandler> host) throws Exception {
            this(b -> b, listenerConfig, endpointConfig, host);
        }

        QuicFixture(UnaryOperator<ZmuxConfig.Builder> sessionConfig, UnaryOperator<ListenerConfig.Builder> listenerConfig,
                    UnaryOperator<HttpEndpoint.Builder> endpointConfig, Supplier<ChannelHandler> host) throws Exception {
            config = sessionConfig.apply(ZmuxConfig.builder().eventLoops(loops)).build();
            listener = Zmux.listener(listenerConfig.apply(ListenerConfig.builder().session(config)).build());
            endpoint = endpointConfig.apply(HttpEndpoint.builder(listener)).build();
            server = Http3Server.bind(LOOPBACK, QuicSslContextBuilder.forServer(H3TestSupport.kmf(), "changeit"),
                    QUIC, endpoint, host);
            client = newClient();
            uri = URI.create("https://localhost:" + server.address().getPort() + "/carrier");
        }

        static HttpCarrierClient newClient() throws Exception {
            return Http3Client.builder(QuicSslContextBuilder.forClient().trustManager(H3TestSupport.tmf()), QUIC)
                    .timeout(WAIT).build();
        }

        PipeIo stream() throws IOException {
            return client.pipe(uri, Map.of(), PipeOptions.Direction.BIDIRECTIONAL, null);
        }

        @Override public void close() {
            try (loops; listener; endpoint; server; client) {
                // Close in reverse ownership order.
            }
        }
    }
}
