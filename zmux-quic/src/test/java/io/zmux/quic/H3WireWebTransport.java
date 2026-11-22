package io.zmux.quic;

import static io.zmux.quic.H3WirePeer.append;
import static io.zmux.quic.H3WirePeer.await;
import static io.zmux.quic.H3WirePeer.bytes;
import static io.zmux.quic.H3WirePeer.fin;
import static io.zmux.quic.H3WirePeer.finishAfter;
import static io.zmux.quic.H3WirePeer.same;
import static io.zmux.quic.H3WirePeer.status;
import static io.zmux.quic.H3WirePeer.utf8;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http3.DefaultHttp3DataFrame;
import io.netty.handler.codec.http3.DefaultHttp3SettingsFrame;
import io.netty.handler.codec.http3.Http3DataFrame;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.http3.Http3RequestStreamFrame;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.handler.codec.quic.QuicStreamResetException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Draft-16 framing probe with one CONNECT and fixed test traffic. */
final class H3WireWebTransport {
    private static final long ENABLED = 0x2c7cf000L;
    private static final long MAX_SESSIONS = 0x14e9cd29L;
    private static final long INITIAL_MAX_DATA = 0x2b61L;
    private static final long INITIAL_MAX_STREAMS_UNI = 0x2b64L;
    private static final long INITIAL_MAX_STREAMS_BIDI = 0x2b65L;
    private static final long BIDI = 0x41;
    private static final long UNI = 0x54;
    private static final long CLOSE = 0x2843;
    private static final long SESSION_GONE = 0x170d7b68;
    private static final String PROTOCOL = "webtransport-h3";
    private static final String CLIENT_BIDI = "client-bidi";
    private static final String CLIENT_UNI = "client-uni";
    private static final String SERVER_BIDI = "server-bidi";
    private static final String SERVER_UNI = "server-uni";
    private static final String CLOSE_REASON = "done";
    private static final int CLOSE_CODE = 42;

    final CompletableFuture<Void> serverClosed = new CompletableFuture<>();
    private final H3WirePeer.Connection connection;
    private Session session;

    H3WireWebTransport(H3WirePeer.Connection connection) {
        this.connection = connection;
    }

    // Preserve unknown settings in the control frame so the probe can diagnose peer profiles.
    static boolean keepSetting(long id, long value) {
        return id >= 0 && id < (1L << 62) && value >= 0 && value < (1L << 62);
    }

    DefaultHttp3SettingsFrame settings() {
        var fields = new Http3Settings(H3WireWebTransport::keepSetting);
        if (connection.server) fields.enableConnectProtocol(true);
        fields.enableH3Datagram(true);
        fields.put(ENABLED, 1L);
        if (connection.server) fields.put(MAX_SESSIONS, 1L);
        fields.put(INITIAL_MAX_DATA, (long) H3WirePeer.BODY_LIMIT);
        fields.put(INITIAL_MAX_STREAMS_BIDI, 8L);
        fields.put(INITIAL_MAX_STREAMS_UNI, 8L);
        return new DefaultHttp3SettingsFrame(fields);
    }

    boolean accept(ChannelHandlerContext ctx, Http3Headers headers) {
        CharSequence protocol = headers.protocol();
        System.err.println("CONNECT " + protocol);
        if (!"CONNECT".contentEquals(headers.method()) || protocol == null || !PROTOCOL.contentEquals(protocol)
                || session != null) return false;
        var stream = (QuicStreamChannel) ctx.channel();
        session = new Session(stream);
        ctx.pipeline().replace(ctx.name(), null, new Control(session));
        var current = session;
        // A CONNECT can be observed before its peer control stream.
        connection.settings.whenComplete((peer, cause) -> {
            if (cause != null || !enabled(peer, false)) {
                System.err.println("WT peer SETTINGS incompatible: " + peer);
                finishAfter(stream.writeAndFlush(status("400")));
                return;
            }
            stream.writeAndFlush(status("200")).addListener(done -> {
                if (done.isSuccess()) current.accepted = true;
                else stream.close();
            });
        });
        return true;
    }

    private static boolean enabled(Http3Settings peer, boolean server) {
        return peer.getOrDefault(ENABLED, 0) == 1 && Boolean.TRUE.equals(peer.h3DatagramEnabled())
                && (!server || Boolean.TRUE.equals(peer.connectProtocolEnabled()));
    }

    void runClient(H3WirePeer.Connection state) throws Exception {
        var peer = await(state.settings);
        if (!enabled(peer, true)) throw new IOException("WT peer SETTINGS incompatible: " + peer);
        var headers = state.requestHeaders("CONNECT", "/wt");
        headers.headers().protocol(PROTOCOL);
        var control = new Control(null);
        QuicStreamChannel connect = state.request(headers, control);
        same("200", await(control.status));
        Session current = await(connection.channel.eventLoop().submit(() -> session));
        var echo = new Raw(false, false, null);
        var bidi = await(open(current, QuicStreamType.BIDIRECTIONAL, echo, CLIENT_BIDI));
        await(bidi.shutdownOutput());
        same(CLIENT_BIDI, await(echo.finished));
        var uni = await(open(current, QuicStreamType.UNIDIRECTIONAL, null, CLIENT_UNI));
        await(uni.shutdownOutput());
        same(SERVER_UNI, await(current.incomingUni));
        same(SERVER_BIDI, await(current.incomingBidi));

        var capsule = Unpooled.buffer(9);
        writeVarint(capsule, CLOSE);
        writeVarint(capsule, 4L + CLOSE_REASON.length());
        capsule.writeInt(CLOSE_CODE).writeCharSequence(CLOSE_REASON, StandardCharsets.UTF_8);
        await(finishAfter(connect.writeAndFlush(new DefaultHttp3DataFrame(capsule))));

        var health = new H3WirePeer.Response();
        var request = state.request(state.requestHeaders("GET", "/health"), health);
        await(request.shutdownOutput());
        health.expect("ok");
    }

    ChannelHandler unknownStream(long type) {
        if (type == UNI) return new Prefix(null, false);
        return new SimpleChannelInboundHandler<ByteBuf>() {
            @Override protected void channelRead0(ChannelHandlerContext ctx, ByteBuf ignored) { }
        };
    }

    void routeBidirectional(ChannelHandlerContext parent, QuicStreamChannel stream) {
        stream.pipeline().addLast(new Prefix(parent, true));
    }

    private final class Prefix extends ByteToMessageDecoder {
        private final ChannelHandlerContext parent;
        private final boolean bidi;

        Prefix(ChannelHandlerContext parent, boolean bidi) {
            this.parent = parent;
            this.bidi = bidi;
        }

        @Override protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws IOException {
            int start = in.readerIndex();
            if (bidi) {
                long type = readVarint(in);
                if (type < 0) return;
                if (type != BIDI) {
                    if (parent == null) throw new IOException("server bidi stream lacks WT preface");
                    in.readerIndex(start);
                    // The stock server adds the request codec after this probe. Removing the
                    // probe replays the original, untouched bytes into that codec once.
                    parent.fireChannelRead(ctx.channel());
                    ctx.pipeline().remove(this);
                    return;
                }
            }
            long id = readVarint(in);
            if (id < 0) {
                in.readerIndex(start);
                return;
            }
            Session current = session;
            if (current == null || current.connect.streamId() != id || !current.accepted) {
                throw new IOException("WT stream for unavailable CONNECT " + id);
            }
            ctx.pipeline().addLast(new Raw(bidi, bidi, current));
            ctx.pipeline().remove(this);
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable failure) {
            System.err.println("WT prefix failed: " + failure);
            ctx.close();
        }
    }

    private io.netty.util.concurrent.Future<QuicStreamChannel> open(Session current, QuicStreamType type,
                                                                    Raw inbound, String payload) {
        ChannelHandler handler = inbound == null ? new SimpleChannelInboundHandler<ByteBuf>() {
            @Override protected void channelRead0(ChannelHandlerContext ctx, ByteBuf bytes) {
                ctx.close();
            }
        } : inbound;
        var opened = connection.channel.createStream(type, handler);
        opened.addListener(done -> {
            if (!done.isSuccess()) {
                current.incomingBidi.completeExceptionally(done.cause());
                current.incomingUni.completeExceptionally(done.cause());
                return;
            }
            QuicStreamChannel stream = opened.getNow();
            ByteBuf prefix = stream.alloc().buffer();
            writeVarint(prefix, type == QuicStreamType.BIDIRECTIONAL ? BIDI : UNI);
            writeVarint(prefix, current.connect.streamId());
            stream.write(prefix);
            ChannelFuture write = stream.writeAndFlush(bytes(payload));
            if (connection.server) finishAfter(write);
        });
        return opened;
    }

    private void replyToClientUni(Session current, String received) throws IOException {
        same(CLIENT_UNI, received);
        open(current, QuicStreamType.UNIDIRECTIONAL, null, SERVER_UNI);
        var echo = new Raw(false, false, current);
        echo.finished.whenComplete((value, cause) -> {
            if (cause != null || !SERVER_BIDI.equals(value)) {
                System.err.println("WT server bidi echo failed: " + (cause == null ? value : cause));
                current.serverEcho.completeExceptionally(new IOException("server bidi echo invalid", cause));
                current.connect.close();
            } else {
                current.serverEcho.complete(null);
            }
        });
        open(current, QuicStreamType.BIDIRECTIONAL, echo, SERVER_BIDI);
    }

    private final class Raw extends SimpleChannelInboundHandler<ByteBuf> {
        final CompletableFuture<String> finished = new CompletableFuture<>();
        private final boolean bidi;
        private final boolean echo;
        private final Session current;
        private final ByteArrayOutputStream body = new ByteArrayOutputStream();

        Raw(boolean bidi, boolean echo, Session current) {
            this.bidi = bidi;
            this.echo = echo;
            this.current = current;
        }

        @Override protected void channelRead0(ChannelHandlerContext ctx, ByteBuf data) throws IOException {
            append(body, data);
            if (echo) ctx.writeAndFlush(data.retain());
        }

        @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws IOException {
            if (event == ChannelInputShutdownEvent.INSTANCE) {
                String value = utf8(body);
                if (echo) {
                    var stream = (QuicStreamChannel) ctx.channel();
                    if (connection.server) same(CLIENT_BIDI, value);
                    // For the client validate its echoed write and FIN before asking the peer to close.
                    fin(stream).addListener(done -> {
                        if (!connection.server && done.isSuccess()) current.incomingBidi.complete(value);
                    });
                } else if (current != null && !bidi && connection.server && !finished.isDone()) {
                    // Only peer-created uni streams use this branch. For an outbound server
                    // bidi with inbound echo, finished is consumed by replyToClientUni.
                    if (((QuicStreamChannel) ctx.channel()).type() == QuicStreamType.UNIDIRECTIONAL) {
                        replyToClientUni(current, value);
                    }
                } else if (!bidi && current != null && !connection.server) {
                    current.incomingUni.complete(value);
                }
                finished.complete(value);
            }
            ctx.fireUserEventTriggered(event);
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable failure) {
            finished.completeExceptionally(failure);
            ctx.close();
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            finished.completeExceptionally(new IOException("WT stream closed without FIN"));
            ctx.fireChannelInactive();
        }
    }

    private static final class Session {
        final QuicStreamChannel connect;
        final CompletableFuture<Void> serverEcho = new CompletableFuture<>();
        final CompletableFuture<String> incomingUni = new CompletableFuture<>();
        final CompletableFuture<String> incomingBidi = new CompletableFuture<>();
        boolean accepted;

        Session(QuicStreamChannel connect) {
            this.connect = connect;
        }
    }

    private final class Control extends SimpleChannelInboundHandler<Http3RequestStreamFrame> {
        final CompletableFuture<String> status = new CompletableFuture<>();
        private Session current;
        private ByteBuf capsules;

        Control(Session current) {
            this.current = current;
        }

        @Override protected void channelRead0(ChannelHandlerContext ctx, Http3RequestStreamFrame frame) throws IOException {
            if (frame instanceof Http3HeadersFrame response && !connection.server) {
                String code = response.headers().status().toString();
                if ("200".equals(code)) {
                    current = new Session((QuicStreamChannel) ctx.channel());
                    current.accepted = true;
                    session = current;
                }
                status.complete(code);
            } else if (frame instanceof Http3DataFrame data) {
                if (capsules == null) capsules = ctx.alloc().buffer(64, H3WirePeer.BODY_LIMIT);
                capsules.writeBytes(data.content(), data.content().readerIndex(), data.content().readableBytes());
                readCapsules();
            }
        }

        private void readCapsules() throws IOException {
            while (capsules.isReadable()) {
                int start = capsules.readerIndex();
                long type = readVarint(capsules);
                long length = type < 0 ? -1 : readVarint(capsules);
                if (length > H3WirePeer.BODY_LIMIT) throw new IOException("capsule exceeds probe limit");
                if (length < 0 || capsules.readableBytes() < length) {
                    capsules.readerIndex(start);
                    break;
                }
                if (type == CLOSE) {
                    if (length < 4) throw new IOException("truncated WT close capsule");
                    int code = capsules.readInt();
                    String reason = capsules.readCharSequence((int) length - 4, StandardCharsets.UTF_8).toString();
                    System.err.println("WT CLOSE code=" + code + " reason=" + reason);
                    if (connection.server && (code != CLOSE_CODE || !CLOSE_REASON.equals(reason))) {
                        throw new IOException("unexpected WT close capsule");
                    }
                    current.accepted = false;
                    if (connection.server) {
                        current.serverEcho.whenComplete((done, failure) -> {
                            if (failure == null) {
                                System.out.println("PASS wt server close=42 reason=done");
                                fin(current.connect);
                                serverClosed.complete(null);
                            } else {
                                current.connect.close();
                            }
                        });
                    } else {
                        fin(current.connect);
                    }
                } else {
                    capsules.skipBytes((int) length);
                    System.err.println("WT CAPSULE type=0x" + Long.toHexString(type) + " length=" + length);
                }
            }
            capsules.discardReadBytes();
        }

        @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
            if (event == ChannelInputShutdownEvent.INSTANCE && current != null) {
                boolean wasOpen = current.accepted;
                current.accepted = false;
                if (wasOpen && !current.connect.isOutputShutdown()) fin(current.connect);
            }
            ctx.fireUserEventTriggered(event);
        }

        @Override public void handlerRemoved(ChannelHandlerContext ctx) {
            if (capsules != null) {
                capsules.release();
                capsules = null;
            }
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            if (session == current) session = null;
            status.completeExceptionally(new IOException("CONNECT stream closed before response"));
            ctx.fireChannelInactive();
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (cause instanceof QuicStreamResetException reset && reset.applicationProtocolCode() == SESSION_GONE) {
                System.err.println("WT CONNECT RESET code=0x" + Long.toHexString(SESSION_GONE));
            } else {
                System.err.println("WT CONNECT failed: " + cause);
            }
            status.completeExceptionally(cause);
            ctx.close();
        }
    }

    static long readVarint(ByteBuf in) {
        if (!in.isReadable()) return -1;
        int first = in.getUnsignedByte(in.readerIndex());
        int length = 1 << (first >>> 6);
        if (in.readableBytes() < length) return -1;
        long value = in.readUnsignedByte() & 0x3f;
        for (int i = 1; i < length; i++) value = value << 8 | in.readUnsignedByte();
        return value;
    }

    static void writeVarint(ByteBuf out, long value) {
        if (value < 64) out.writeByte((int) value);
        else if (value < 16_384) out.writeShort((int) value | 0x4000);
        else if (value < (1L << 30)) out.writeInt((int) value | 0x80000000);
        else out.writeLong(value | 0xc000000000000000L);
    }
}
