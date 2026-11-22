package io.zmux.quic;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.DefaultHttp3DataFrame;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3ClientConnectionHandler;
import io.netty.handler.codec.http3.Http3DataFrame;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.http3.Http3RequestStreamFrame;
import io.netty.handler.codec.http3.Http3ServerConnectionHandler;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.http3.Http3SettingsFrame;
import io.netty.handler.codec.quic.DefaultQuicStreamFrame;
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicClientCodecBuilder;
import io.netty.handler.codec.quic.QuicServerCodecBuilder;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamResetException;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.concurrent.Future;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/** Loopback-only wire peer. Transport fixtures deliberately live in the adapter's test sources. */
public final class H3WirePeer {
    private static final long TIMEOUT_SECONDS = 8;
    static final int RESET_CODE = 23;
    static final int BODY_LIMIT = 65_536;
    private static final String QUIC_REGISTRATION = "quic-registration";
    private static final String PAIR_HEADER = "zmux-pipe-pair";
    private static final String PAIR_TOKEN = "d2lyZS1wZWVyLXBhaXItMQ";
    private static final QuicOptions OPTIONS = QuicOptions.builder().bidirectionalStreams(16).lanes(12)
            .idleTimeout(Duration.ofSeconds(10)).build();

    private H3WirePeer() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5 || !(args[0].equals("server") || args[0].equals("client"))
                || !(args[1].equals("h3") || args[1].equals("wt"))) {
            throw new IllegalArgumentException("server|client h3|wt PORT CERT KEY");
        }
        int port = Integer.parseInt(args[2]);
        boolean wt = args[1].equals("wt");
        if (args[0].equals("server")) {
            try (Endpoint server = Endpoint.server(port, Path.of(args[3]).toFile(), Path.of(args[4]).toFile(), wt)) {
                System.out.println("READY " + server.port());
                // The matrix also bounds and terminates this process after each scenario.
                server.udp.closeFuture().await(30, TimeUnit.SECONDS);
            }
        } else {
            try (Endpoint client = Endpoint.client(port, Path.of(args[3]).toFile(), wt)) {
                client.exercise(wt);
            }
            System.out.println("PASS " + args[1]);
        }
    }

    static <T> T await(Future<T> future) throws Exception {
        if (!future.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new IOException("Netty operation timed out");
        }
        if (!future.isSuccess()) throw new IOException("Netty operation failed", future.cause());
        return future.getNow();
    }

    static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    static ByteBuf bytes(String text) {
        return Unpooled.copiedBuffer(text, StandardCharsets.UTF_8);
    }

    static String utf8(ByteArrayOutputStream body) {
        return body.toString(StandardCharsets.UTF_8);
    }

    static void append(ByteArrayOutputStream dest, ByteBuf source) throws IOException {
        int count = source.readableBytes();
        if (count > BODY_LIMIT - dest.size()) throw new IOException("wire peer body limit exceeded");
        source.getBytes(source.readerIndex(), dest, count);
    }

    static void same(String expected, String actual) throws IOException {
        if (!expected.equals(actual)) throw new IOException("expected " + expected + ", received " + actual);
    }

    /**
     * Sends a zero-length FIN from a fresh loop task. quiche 0.30.0 (Netty 4.2.21) drops such a
     * FIN when an ACK for the stream's last data is processed between queueing and sending it
     * (quiche #2525, fixed in 0.30.1); inside a read batch Netty sends only at the batch end.
     */
    static ChannelFuture fin(QuicStreamChannel stream) {
        return fin(stream, stream.newPromise());
    }

    private static ChannelFuture fin(QuicStreamChannel stream, ChannelPromise promise) {
        stream.eventLoop().execute(() -> stream.shutdownOutput(promise));
        return promise;
    }

    static ChannelFuture finishAfter(ChannelFuture future) {
        var stream = (QuicStreamChannel) future.channel();
        var fin = stream.newPromise();
        future.addListener(done -> {
            if (done.isSuccess()) fin(stream, fin);
            else {
                fin.tryFailure(done.cause());
                stream.close();
            }
        });
        return fin;
    }

    static DefaultHttp3HeadersFrame status(String status) {
        var frame = new DefaultHttp3HeadersFrame();
        frame.headers().status(status);
        return frame;
    }

    static final class Endpoint implements AutoCloseable {
        private final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        private Channel udp;
        private QuicChannel connection;
        private Connection state;

        static Endpoint server(int port, File cert, File key, boolean wt) throws Exception {
            var endpoint = new Endpoint();
            try {
                var tls = QuicSslContextBuilder.forServer(key, null, cert).applicationProtocols("h3").earlyData(false);
                var codec = OPTIONS.apply(new QuicServerCodecBuilder()).sslContext(tls.build())
                        .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                        .handler(new ChannelInitializer<QuicChannel>() {
                            @Override protected void initChannel(QuicChannel channel) {
                                new Connection(true, wt, channel).install();
                            }
                        })
                        .streamOption(ChannelOption.AUTO_READ, false)
                        .build();
                endpoint.bind(codec, port);
                return endpoint;
            } catch (Exception | Error failure) {
                endpoint.close();
                throw failure;
            }
        }

        static Endpoint client(int port, File cert, boolean wt) throws Exception {
            var endpoint = new Endpoint();
            try {
                var tls = QuicSslContextBuilder.forClient().trustManager(cert).endpointIdentificationAlgorithm("HTTPS")
                        .applicationProtocols("h3").earlyData(false).build();
                var destination = new InetSocketAddress("127.0.0.1", port);
                var codec = OPTIONS.apply(new QuicClientCodecBuilder())
                        .sslEngineProvider(channel -> tls.newEngine(channel.alloc(), "localhost", port)).build();
                endpoint.bind(codec, 0);
                endpoint.connection = await(QuicChannel.newBootstrap(endpoint.udp)
                        .handler(new ChannelInitializer<QuicChannel>() {
                            @Override protected void initChannel(QuicChannel channel) {
                                endpoint.state = new Connection(false, wt, channel);
                                endpoint.state.install();
                            }
                        })
                        .streamOption(ChannelOption.AUTO_READ, false)
                        .remoteAddress(destination).connect());
                return endpoint;
            } catch (Exception | Error failure) {
                endpoint.close();
                throw failure;
            }
        }

        private void bind(ChannelHandler codec, int port) throws Exception {
            ChannelFuture binding = new Bootstrap().group(group).channel(NioDatagramChannel.class)
                    .handler(codec).bind(new InetSocketAddress("127.0.0.1", port));
            udp = binding.channel();
            await(binding);
        }

        int port() {
            return ((InetSocketAddress) udp.localAddress()).getPort();
        }

        void exercise(boolean wt) throws Exception {
            await(state.settings);
            if (wt) {
                state.wt.runClient(state);
                return;
            }
            var streaming = new Response();
            var stream = request("POST", "/stream", streaming, null, null);
            await(stream.writeAndFlush(new DefaultHttp3DataFrame(bytes("stream-"))));
            await(streaming.head);
            // Require a response DATA frame while the upload is still open.
            await(streaming.firstData);
            await(stream.writeAndFlush(new DefaultHttp3DataFrame(bytes("payload"))));
            await(stream.shutdownOutput());
            streaming.expect("stream-payload");

            var exchange = new Response();
            String payload = "exchange-payload";
            stream = request("POST", "/exchange", exchange, payload.length(), null);
            await(stream.writeAndFlush(new DefaultHttp3DataFrame(bytes(payload))));
            await(stream.shutdownOutput());
            exchange.expect(payload);

            split(false);
            split(true);
            health();
        }

        private void split(boolean uploadFirst) throws Exception {
            String token = uploadFirst ? "d2lyZS1wZWVyLXBhaXItMg" : PAIR_TOKEN;
            var down = new Response();
            var up = new Response();
            QuicStreamChannel download;
            QuicStreamChannel upload;
            if (uploadFirst) {
                upload = request("POST", "/split", up, null, token);
                download = request("GET", "/split", down, null, token);
            } else {
                download = request("GET", "/split", down, null, token);
                upload = request("POST", "/split", up, null, token);
            }
            await(download.shutdownOutput());
            await(upload.writeAndFlush(new DefaultHttp3DataFrame(bytes("split-payload"))));
            await(upload.shutdownOutput());
            down.expect("split-payload");
            up.expect("");
        }

        void exerciseReset() throws Exception {
            var reset = new Response();
            var stream = request("POST", "/reset", reset, null, null);
            same("200", await(reset.head));
            await(stream.writeAndFlush(new DefaultHttp3DataFrame(bytes("reset"))));
            try {
                await(reset.finished);
                throw new IOException("reset arrived as FIN");
            } catch (ExecutionException failed) {
                if (!(failed.getCause() instanceof QuicStreamResetException cause)
                        || cause.applicationProtocolCode() != RESET_CODE) throw failed;
            }
            health();
        }

        Connection state() {
            return state;
        }

        void health() throws Exception {
            var health = new Response();
            await(request("GET", "/health", health, null, null).shutdownOutput());
            health.expect("ok");
        }

        private QuicStreamChannel request(String method, String path, Response response, Integer length, String pair)
                throws Exception {
            var frame = state.requestHeaders(method, path);
            if (length != null) frame.headers().setInt("content-length", length);
            if (pair != null) frame.headers().set(PAIR_HEADER, pair);
            return state.request(frame, response);
        }

        @Override public void close() {
            try {
                if (connection != null) connection.close().awaitUninterruptibly(2, TimeUnit.SECONDS);
                if (udp != null) udp.close().awaitUninterruptibly(2, TimeUnit.SECONDS);
            } finally {
                group.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly(3, TimeUnit.SECONDS);
            }
        }
    }

    static final class Connection {
        final QuicChannel channel;
        final boolean server;
        final CompletableFuture<Http3Settings> settings = new CompletableFuture<>();
        final H3WireWebTransport wt;
        private final Map<String, SplitPair> pairs = new HashMap<>();

        Connection(boolean server, boolean wt, QuicChannel channel) {
            this.server = server;
            this.channel = channel;
            this.wt = wt ? new H3WireWebTransport(this) : null;
        }

        void install() {
            var control = new SimpleChannelInboundHandler<Http3SettingsFrame>() {
                @Override protected void channelRead0(ChannelHandlerContext ctx, Http3SettingsFrame frame) {
                    if (wt != null) System.err.println("SETTINGS " + frame.settings());
                    settings.complete(frame.settings());
                }

                @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                    settings.completeExceptionally(cause);
                    ctx.fireExceptionCaught(cause);
                }
            };
            channel.closeFuture().addListener(done ->
                    settings.completeExceptionally(new IOException("connection closed before SETTINGS")));
            var local = wt == null ? null : wt.settings();
            // The first connection handler decides when reads may start. QUIC can mark a child
            // readable before emitting the child-channel event to the parent pipeline.
            channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                @Override public void channelRead(ChannelHandlerContext ctx, Object message) {
                    if (!(message instanceof QuicStreamChannel stream)) {
                        ctx.fireChannelRead(message);
                        return;
                    }
                    if (server && wt != null && stream.type() == QuicStreamType.BIDIRECTIONAL) {
                        wt.routeBidirectional(ctx, stream);
                        // The parent tail applies QUIC stream options and registers the child.
                        ctx.pipeline().context(QUIC_REGISTRATION).fireChannelRead(stream);
                    } else {
                        ctx.fireChannelRead(stream);
                    }
                    stream.config().setAutoRead(true);
                }
            });
            if (server) {
                channel.pipeline().addLast(new Http3ServerConnectionHandler(new ChannelInitializer<QuicStreamChannel>() {
                    @Override protected void initChannel(QuicStreamChannel stream) {
                        stream.pipeline().addLast(new Request(Connection.this));
                    }
                }, control, wt == null ? null : wt::unknownStream, local, true, H3WireWebTransport::keepSetting));
                if (wt != null) {
                    channel.pipeline().addLast(QUIC_REGISTRATION, new ChannelInboundHandlerAdapter() {
                        @Override public void channelRead(ChannelHandlerContext ctx, Object message) {
                            // The prefix probe replays an already registered request channel to H3.
                            if (!(message instanceof QuicStreamChannel stream) || !stream.isRegistered()) {
                                ctx.fireChannelRead(message);
                            }
                        }
                    });
                }
            } else {
                channel.pipeline().addLast(new Http3ClientConnectionHandler(control, null,
                        wt == null ? null : wt::unknownStream, local, true, H3WireWebTransport::keepSetting) {
                    @Override protected void initBidirectionalStream(ChannelHandlerContext ctx, QuicStreamChannel stream) {
                        if (wt == null) super.initBidirectionalStream(ctx, stream);
                        else wt.routeBidirectional(null, stream);
                    }
                });
            }
        }

        DefaultHttp3HeadersFrame requestHeaders(String method, String path) {
            var frame = new DefaultHttp3HeadersFrame();
            int port = ((InetSocketAddress) channel.remoteSocketAddress()).getPort();
            frame.headers().method(method).scheme("https").authority("localhost:" + port).path(path);
            return frame;
        }

        QuicStreamChannel request(DefaultHttp3HeadersFrame headers, ChannelHandler inbound) throws Exception {
            var stream = await(Http3.newRequestStream(channel, inbound));
            await(stream.writeAndFlush(headers));
            return stream;
        }

        private void pair(String token, Request request) throws IOException {
            SplitPair pair = pairs.get(token);
            if (pair == null) {
                if (pairs.size() == 16) throw new IOException("too many pending split requests");
                pair = new SplitPair();
                pairs.put(token, pair);
            }
            if (request.upload ? pair.up != null : pair.down != null) throw new IOException("duplicate split half");
            if (request.upload) pair.up = request;
            else pair.down = request;
            request.pair = pair;
            pair.pump();
        }

        private final class SplitPair {
            private Request down;
            private Request up;
            private boolean done;

            void pump() {
                if (done || down == null || up == null) return;
                byte[] data = up.body.toByteArray();
                up.body.reset();
                if (data.length != 0) down.stream.writeAndFlush(new DefaultHttp3DataFrame(Unpooled.wrappedBuffer(data)));
                if (up.ended) {
                    done = true;
                    fin(down.stream);
                    headWithFin(up.stream);
                    pairs.remove(up.token);
                }
            }
        }
    }

    private static void headWithFin(QuicStreamChannel stream) {
        // Put the header-only reply and FIN into one native write, with the original HEADERS
        // promise. The HTTP/3 encoder emits this HEADERS frame in one ByteBuf.
        stream.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
            @Override public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
                ctx.pipeline().remove(this);
                ctx.write(message instanceof ByteBuf encoded ? new DefaultQuicStreamFrame(encoded, true) : message,
                        promise);
            }
        });
        stream.writeAndFlush(status("200")).addListener(done -> {
            if (!done.isSuccess()) stream.close();
        });
    }

    private static final class Request extends SimpleChannelInboundHandler<Http3RequestStreamFrame> {
        final Connection connection;
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        QuicStreamChannel stream;
        Connection.SplitPair pair;
        String path;
        String token;
        boolean upload;
        boolean ended;
        boolean terminal;

        Request(Connection connection) {
            this.connection = connection;
        }

        @Override protected void channelRead0(ChannelHandlerContext ctx, Http3RequestStreamFrame frame) throws IOException {
            if (path == null && frame instanceof Http3HeadersFrame head) {
                stream = (QuicStreamChannel) ctx.channel();
                Http3Headers headers = head.headers();
                path = headers.path() == null ? "" : headers.path().toString();
                switch (path) {
                    case "/health" -> {
                        if (connection.wt == null) respondHealth();
                        else connection.wt.serverClosed.thenRun(this::respondHealth);
                    }
                    case "/stream", "/reset" -> stream.writeAndFlush(status("200"));
                    case "/exchange" -> { }
                    case "/split" -> {
                        token = headers.get(PAIR_HEADER, "").toString();
                        if (token.isEmpty()) throw new IOException("missing split pair header");
                        upload = "POST".contentEquals(headers.method());
                        if (!upload) stream.writeAndFlush(status("200"));
                        connection.pair(token, this);
                    }
                    case "/wt" -> {
                        if (connection.wt != null && connection.wt.accept(ctx, headers)) return;
                        terminal = true;
                        finishAfter(stream.writeAndFlush(status("400")));
                    }
                    default -> {
                        terminal = true;
                        finishAfter(stream.writeAndFlush(status("404")));
                    }
                }
            } else if (frame instanceof Http3DataFrame data && !terminal && path != null) {
                if (path.equals("/stream")) stream.writeAndFlush(data.retain());
                // Ordinary QUIC reset probe; Netty cannot emit a draft-16 WT application code.
                if (path.equals("/reset")) {
                    terminal = true;
                    stream.shutdownOutput(RESET_CODE);
                }
                if (path.equals("/exchange") || upload) {
                    append(body, data.content());
                    if (pair != null) pair.pump();
                }
            }
        }

        private void respondHealth() {
            stream.write(status("200"));
            finishAfter(stream.writeAndFlush(new DefaultHttp3DataFrame(bytes("ok"))));
        }

        @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
            if (event == ChannelInputShutdownEvent.INSTANCE && path != null && !terminal) {
                ended = true;
                switch (path) {
                    case "/stream" -> fin(stream);
                    case "/exchange" -> {
                        stream.write(status("200"));
                        finishAfter(stream.writeAndFlush(new DefaultHttp3DataFrame(Unpooled.wrappedBuffer(body.toByteArray()))));
                    }
                    case "/split" -> pair.pump();
                    default -> { }
                }
            }
            ctx.fireUserEventTriggered(event);
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable failure) {
            System.err.println("request stream failed: " + failure);
            ctx.close();
        }
    }

    static class Response extends SimpleChannelInboundHandler<Http3RequestStreamFrame> {
        final CompletableFuture<String> head = new CompletableFuture<>();
        final CompletableFuture<Void> firstData = new CompletableFuture<>();
        final CompletableFuture<String> finished = new CompletableFuture<>();
        private final ByteArrayOutputStream body = new ByteArrayOutputStream();

        @Override protected void channelRead0(ChannelHandlerContext ctx, Http3RequestStreamFrame frame) throws IOException {
            if (frame instanceof Http3HeadersFrame headers) head.complete(headers.headers().status().toString());
            if (frame instanceof Http3DataFrame data) {
                append(body, data.content());
                if (data.content().isReadable()) firstData.complete(null);
            }
        }

        @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
            if (event == ChannelInputShutdownEvent.INSTANCE) finished.complete(utf8(body));
            ctx.fireUserEventTriggered(event);
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            fail(new IOException("response stream closed without FIN"));
            ctx.fireChannelInactive();
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable failure) {
            fail(failure);
            ctx.close();
        }

        void expect(String expected) throws Exception {
            same("200", await(head));
            same(expected, await(finished));
        }

        private void fail(Throwable failure) {
            head.completeExceptionally(failure);
            firstData.completeExceptionally(failure);
            finished.completeExceptionally(failure);
        }
    }
}
