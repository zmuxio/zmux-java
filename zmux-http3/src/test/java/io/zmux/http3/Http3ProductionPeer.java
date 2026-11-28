package io.zmux.http3;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
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
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.zmux.JoinHint;
import io.zmux.ListenerConfig;
import io.zmux.PipeOptions;
import io.zmux.Role;
import io.zmux.Zmux;
import io.zmux.ZmuxConfig;
import io.zmux.http.HttpCarrierClient;
import io.zmux.http.HttpEndpoint;
import io.zmux.http.internal.HttpTransport;
import io.zmux.quic.QuicOptions;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Reuses step 1's cross-language H3 wire commands with the production Java request transport. */
public final class Http3ProductionPeer {
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final int LIMIT = 1024;
    private static final QuicOptions QUIC = QuicOptions.builder().build();

    private Http3ProductionPeer() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 5 || !(args[1].equals("h3") || args[1].equals("wt"))) {
            throw new IllegalArgumentException("usage: java-peer server|client h3|wt PORT CERT KEY");
        }
        int port = Integer.parseInt(args[2]);
        File cert = Path.of(args[3]).toFile();
        if (args[1].equals("wt")) {
            webTransport(args[0], port, cert, Path.of(args[4]).toFile());
            return;
        }
        switch (args[0]) {
            case "server" -> serve(port, cert, Path.of(args[4]).toFile());
            case "client" -> runClient(port, cert);
            default -> throw new IllegalArgumentException("expected server or client");
        }
    }

    private static void webTransport(String role, int port, File cert, File key) throws Exception {
        var config = ZmuxConfig.builder().enableDatagrams(true).build();
        byte[] marker = "owned-wt".getBytes(StandardCharsets.US_ASCII);
        byte[] armed = "cancel-ready".getBytes(StandardCharsets.US_ASCII);
        if ("server".equals(role)) {
            var connect = new CompletableFuture<QuicStreamChannel>();
            var peerClose = new CompletableFuture<WebTransportSession.Close>();
            try (var listener = Zmux.listener(ListenerConfig.of(config));
                 var endpoint = HttpEndpoint.builder(listener).webTransport().build();
                 var server = Http3Server.bind(new InetSocketAddress("127.0.0.1", port),
                         QuicSslContextBuilder.forServer(key, null, cert), QUIC, endpoint, () -> observeConnect(connect, peerClose))) {
                System.out.println("READY " + server.address().getPort());
                try (var session = listener.accept(TIMEOUT); var probe = listener.accept(TIMEOUT)) {
                    if (probe == null) {
                        throw new IOException("cancellation probe did not bootstrap");
                    }
                    var request = connect.get(5, TimeUnit.SECONDS);
                    var wt = request.eventLoop().submit(() -> WebTransportConnection.get(request.parent())
                            .session(request.streamId())).get(5, TimeUnit.SECONDS);
                    var removed = probe.pipes().getFirst().closed();
                    try (var ready = probe.acceptStream(TIMEOUT)) {
                        if (ready == null || !Arrays.equals(armed, ready.readAll(armed.length))) {
                            throw new IOException("cancellation probe did not send its marker");
                        }
                        ready.write(ByteBuffer.wrap(armed));
                        ready.finish();
                    }
                    removed.get(5, TimeUnit.SECONDS);
                    if (session == null || !Arrays.equals(marker, session.datagrams().receive(TIMEOUT))
                            || session.stats().datagramsReceivedChannel() != 1) {
                        throw new IOException("first WebTransport datagram exchange failed");
                    }
                    session.datagrams().send(ByteBuffer.wrap(marker));
                    var close = wt.closed().get(5, TimeUnit.SECONDS);
                    if (!close.equals(new WebTransportSession.Close(42, "done", true))) {
                        throw new IOException("unexpected peer close " + close);
                    }
                    peerClose.complete(close);
                }
                System.out.println("PASS wt server bootstrap=1 datagram=1 cancel=peer close=42 reason=done");
                // CONNECT ended, but the H3 connection still belongs to its host.
                // Keep serving so a client sharing that connection can check /wt-closed.
                new CountDownLatch(1).await();
            }
        } else if ("client".equals(role)) {
            URI target = URI.create("https://localhost:" + port + "/wt");
            try (var client = Http3Client.builder(QuicSslContextBuilder.forClient().trustManager(cert), QUIC).build();
                 var wt = Http3Client.openWebTransportAsync(client, target, Map.of(), null, TIMEOUT).get(5, TimeUnit.SECONDS);
                 var session = Zmux.open(wt.openPipeAsync().get(5, TimeUnit.SECONDS), Role.CLIENT, config)) {
                var pipe = wt.openPipeAsync().get(5, TimeUnit.SECONDS);
                try (var probe = Zmux.open(pipe, Role.CLIENT, config)) {
                    try (var ready = probe.openStream()) {
                        ready.write(ByteBuffer.wrap(armed));
                        ready.finish();
                        if (!Arrays.equals(armed, ready.readAll(armed.length))) {
                            throw new IOException("peer did not acknowledge the cancellation probe");
                        }
                    }
                    pipe.writer().abort(new IOException("interop cancellation"));
                    pipe.reader().close();
                    session.datagrams().send(ByteBuffer.wrap(marker));
                    if (!Arrays.equals(marker, session.datagrams().receive(TIMEOUT))
                            || session.stats().datagramsReceivedChannel() != 1) throw new IOException("WT datagram mismatch");
                }
                wt.drainAsync().get(5, TimeUnit.SECONDS);
                wt.closeAsync(42, "done").get(5, TimeUnit.SECONDS);
                if (!"1".equals(System.getenv("ZMUX_WT_DEDICATED_SERVER"))) {
                    var quic = (QuicChannel) io.zmux.internal.spi.CarrierBudget.physical(wt.namespace());
                    if (!"closed".equals(Http3HostedTest.health(quic, target))) {
                        throw new IOException("peer did not retire CONNECT on the live H3 connection");
                    }
                }
            }
            System.out.println("PASS wt client bootstrap=1 datagram=1 cancel=1 close=42");
        } else throw new IllegalArgumentException("expected server or client");
    }

    private static ChannelInitializer<QuicStreamChannel> observeConnect(
            CompletableFuture<QuicStreamChannel> connect, CompletableFuture<WebTransportSession.Close> peerClose) {
        return new ChannelInitializer<>() {
            @Override protected void initChannel(QuicStreamChannel stream) {
                connect.complete(stream);
                stream.pipeline().addLast(new SimpleChannelInboundHandler<HttpObject>() {
                    @Override protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
                        if (msg instanceof HttpRequest req && "/wt-closed".equals(req.uri())) {
                            peerClose.thenAccept(closed -> {
                                var body = Unpooled.copiedBuffer("closed", StandardCharsets.US_ASCII);
                                var reply = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, body);
                                HttpUtil.setContentLength(reply, body.readableBytes());
                                ctx.writeAndFlush(reply);
                            });
                        }
                    }
                });
            }
        };
    }

    private static void serve(int port, File cert, File key) throws Exception {
        var pairs = new ConcurrentHashMap<String, CompletableFuture<byte[]>>();
        try (var listener = Zmux.listener(ListenerConfig.of(ZmuxConfig.builder().build()));
             var endpoint = HttpEndpoint.builder(listener).build();
             var server = Http3Server.bind(new InetSocketAddress("127.0.0.1", port),
                     QuicSslContextBuilder.forServer(key, null, cert), QUIC, endpoint, () -> new WireEcho(pairs))) {
            System.out.println("READY " + server.address().getPort());
            // The matrix kills this server after the client completes or the cell deadline fires.
            new CountDownLatch(1).await();
        }
    }

    private static void runClient(int port, File cert) throws Exception {
        URI base = URI.create("https://localhost:" + port);
        try (var client = Http3Client.builder(QuicSslContextBuilder.forClient().trustManager(cert), QUIC)
                .timeout(TIMEOUT).build()) {
            var stream = client.pipe(base.resolve("/stream"), Map.of(), PipeOptions.Direction.BIDIRECTIONAL, null);
            var received = H3TestSupport.reading(stream.reader());
            H3TestSupport.write(stream.writer(), "one");
            same("one", H3TestSupport.take(received, 3));
            H3TestSupport.write(stream.writer(), "two");
            stream.writer().close();
            same("two", H3TestSupport.take(received, 3));
            same("end", received.poll(5, TimeUnit.SECONDS));
            stream.reader().close();

            var split = client.splitPipe(base.resolve("/split"), Map.<String, List<String>>of(), null);
            var down = H3TestSupport.reading(split.reader());
            H3TestSupport.write(split.writer(), "split");
            split.writer().close();
            same("split", H3TestSupport.take(down, 5));
            same("end", down.poll(5, TimeUnit.SECONDS));
            split.reader().close();

            fixedRequest(client, base.resolve("/exchange"), HttpMethod.POST, "exchange", "exchange");
            fixedRequest(client, base.resolve("/health"), HttpMethod.GET, "", "ok");
        }
        System.out.println("PASS h3");
    }

    private static void fixedRequest(HttpCarrierClient client, URI uri, HttpMethod method, String payload, String expected)
            throws Exception {
        var channel = HttpTransport.openRequest(client, uri).get(5, TimeUnit.SECONDS);
        var done = new CompletableFuture<String>();
        var response = new ByteArrayOutputStream();
        channel.eventLoop().submit(() -> channel.pipeline().addLast(new SimpleChannelInboundHandler<HttpObject>() {
            @Override protected void channelRead0(ChannelHandlerContext ctx, HttpObject message) throws IOException {
                if (message instanceof HttpResponse head && head.status().code() != 200) {
                    throw new IOException("unexpected response " + head.status());
                }
                if (message instanceof HttpContent content) append(response, content);
                if (message instanceof LastHttpContent) done.complete(response.toString(StandardCharsets.US_ASCII));
            }

            @Override public void channelInactive(ChannelHandlerContext ctx) {
                done.completeExceptionally(new IOException("response stream closed before FIN"));
                ctx.fireChannelInactive();
            }

            @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                done.completeExceptionally(cause);
                ctx.close();
            }
        })).get(5, TimeUnit.SECONDS);
        var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri.getRawPath(),
                Unpooled.copiedBuffer(payload, StandardCharsets.US_ASCII));
        request.headers().set(HttpHeaderNames.HOST, uri.getRawAuthority());
        if (method.equals(HttpMethod.POST)) HttpUtil.setContentLength(request, payload.length());
        try {
            channel.writeAndFlush(request).get(5, TimeUnit.SECONDS);
            same(expected, done.get(5, TimeUnit.SECONDS));
        } finally {
            channel.close();
        }
    }

    private static void same(String expected, String received) throws IOException {
        if (!expected.equals(received)) throw new IOException("expected " + expected + ", received " + received);
    }

    private static void append(ByteArrayOutputStream body, HttpContent content) throws IOException {
        var bytes = content.content();
        if (bytes.readableBytes() > LIMIT - body.size()) throw new IOException("wire fixture body exceeds limit");
        bytes.getBytes(bytes.readerIndex(), body, bytes.readableBytes());
    }

    private static final class WireEcho extends SimpleChannelInboundHandler<HttpObject> {
        private final Map<String, CompletableFuture<byte[]>> pairs;
        private final ByteArrayOutputStream body = new ByteArrayOutputStream();
        private String path;
        private String token;
        private boolean upload;

        WireEcho(Map<String, CompletableFuture<byte[]>> pairs) {
            this.pairs = pairs;
        }

        @Override protected void channelRead0(ChannelHandlerContext ctx, HttpObject message) throws IOException {
            if (message instanceof HttpRequest request) readHead(ctx, request);
            if (message instanceof HttpContent content && upload) {
                if ("/stream".equals(path) && content.content().isReadable()) {
                    ctx.writeAndFlush(new DefaultHttpContent(content.content().retain()));
                } else if (!"/stream".equals(path)) {
                    append(body, content);
                }
            }
            if (message instanceof LastHttpContent && upload) readEnd(ctx);
        }

        private void readHead(ChannelHandlerContext ctx, HttpRequest request) throws IOException {
            if (HttpTransport.stream(ctx.channel()) == null) throw new IOException("HTTP/3 required");
            path = request.uri();
            upload = request.method().equals(HttpMethod.POST);
            switch (path) {
                case "/health" -> reply(ctx, new byte[]{'o', 'k'});
                case "/stream" -> {
                    if (!upload || HttpUtil.getContentLength(request, -1) >= 0) {
                        throw new IOException("expected streaming POST");
                    }
                    ctx.writeAndFlush(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
                }
                case "/exchange" -> {
                    if (!upload || HttpUtil.getContentLength(request, -1) < 0) {
                        throw new IOException("expected fixed-length POST");
                    }
                }
                case "/split" -> {
                    token = request.headers().get(JoinHint.PAIR_HEADER);
                    if (token == null) throw new IOException("expected split token");
                    if (!upload) {
                        var bodyFuture = pairs.computeIfAbsent(token, ignored -> new CompletableFuture<>());
                        bodyFuture.thenAccept(bytes -> ctx.executor().execute(() -> {
                            pairs.remove(token, bodyFuture);
                            reply(ctx, bytes);
                        }));
                    }
                }
                default -> throw new IOException("unexpected target " + path);
            }
        }

        private void readEnd(ChannelHandlerContext ctx) {
            switch (path) {
                case "/stream" -> ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
                case "/exchange" -> reply(ctx, body.toByteArray());
                case "/split" -> {
                    pairs.computeIfAbsent(token, ignored -> new CompletableFuture<>()).complete(body.toByteArray());
                    reply(ctx, new byte[0]);
                }
                default -> { }
            }
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("wire request: " + cause);
            ctx.close();
        }

        private static void reply(ChannelHandlerContext ctx, byte[] bytes) {
            var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                    Unpooled.wrappedBuffer(bytes));
            HttpUtil.setContentLength(response, bytes.length);
            ctx.writeAndFlush(response);
        }
    }
}
