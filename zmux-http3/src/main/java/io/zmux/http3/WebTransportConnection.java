package io.zmux.http3;

import static io.zmux.quic.internal.NettyLoops.getBorrowedEventLoop;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3ConnectionHandler;
import io.netty.handler.codec.http3.Http3ErrorCode;
import io.netty.handler.codec.http3.Http3GoAwayFrame;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamLimitChangedEvent;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.AttributeKey;
import io.zmux.carrier.PipeIo;
import io.zmux.internal.spi.CarrierBudget;
import io.zmux.internal.spi.Loops;
import io.zmux.quic.QuicOptions;
import io.zmux.quic.internal.QuicDatagrams;
import io.zmux.quic.internal.QuicHandshake;
import io.zmux.quic.internal.QuicVarInts;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/** Dispatch and reservations for one H3 connection, all confined to its event loop. */
final class WebTransportConnection {
    static final String PROTOCOL = "webtransport-h3";
    static final String LEGACY_PROTOCOL = "webtransport";

    static boolean accepts(CharSequence protocol) {
        return protocol != null && (PROTOCOL.contentEquals(protocol) || LEGACY_PROTOCOL.contentEquals(protocol));
    }
    static final long UNI_STREAM = 0x54;
    static final long BIDI_STREAM = 0x41;
    static final int SESSION_GONE = 0x170d7b68;
    private static final int BUFFERED_STREAM_REJECTED = 0x3994bd84;
    private static final AttributeKey<WebTransportConnection> STATE =
            AttributeKey.valueOf(WebTransportConnection.class, "state");
    private static final ChannelInitializer<QuicStreamChannel> OPEN = new ChannelInitializer<>() {
        @Override protected void initChannel(QuicStreamChannel stream) { stream.config().setAutoRead(false); }
    };

    private final QuicChannel channel;
    private final long receiveMemory;
    private final QuicDatagrams datagrams;
    private final Map<Long, WebTransportSession> sessions = new HashMap<>();
    // Adjacent retired client-bidi IDs coalesce. Each gap is an outstanding QUIC stream
    // (including implicitly opened lower IDs) and is bounded by MAX_STREAMS.
    private final TreeMap<Long, Long> retired = new TreeMap<>();
    private final Map<Long, QuicStreamChannel> ordinary = new HashMap<>();
    private final Registration registration = new Registration();
    private final StreamOpener bidi = new StreamOpener(QuicStreamType.BIDIRECTIONAL);
    private final StreamOpener uni = new StreamOpener(QuicStreamType.UNIDIRECTIONAL);

    final CarrierBudget budget;
    final WebTransportProvider provider;
    final WebTransportReorder reorder;
    QuicChannel physical() { return channel; }
    long receiveCapacity() { return receiveMemory; }
    private final boolean server;
    private WebTransportSettings peer;
    private boolean draining, sessionEstablished;
    final CompletableFuture<Void> settingsReady = new CompletableFuture<>();

    WebTransportConnection(QuicChannel channel, QuicOptions options, boolean server, WebTransportProvider provider,
                           CarrierBudget budget, Duration setupTimeout) {
        this.channel = channel;
        this.provider = provider;
        this.server = server;
        receiveMemory = provider.receiveCapacity(channel);
        if (receiveMemory <= 0) throw new UnsupportedOperationException("unattested WebTransport receive capacity");
        this.budget = budget;
        reorder = new WebTransportReorder(getBorrowedEventLoop(channel), System::nanoTime, setupTimeout.toNanos(),
                options.bidirectionalStreams() + options.lanes());
        var release = budget.acquire(channel, receiveMemory, 0);
        if (release == null) throw new IllegalStateException("WebTransport receive budget exhausted");
        channel.closeFuture().addListener(done -> release.run());
        datagrams = QuicDatagrams.observe(channel);
        channel.attr(STATE).set(this);
        channel.closeFuture().addListener(done -> settingsReady.completeExceptionally(new ClosedChannelException()));
    }

    static WebTransportConnection get(QuicChannel channel) { return channel.attr(STATE).get(); }

    void settings(Http3Settings settings) {
        var control = Http3.getLocalControlStream(channel);
        if (control != null) {
            control.pipeline().addLast(new ChannelOutboundHandlerAdapter() {
                @Override public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
                    if (message instanceof Http3GoAwayFrame goAway) goAway(goAway.id(), server);
                    ctx.write(message, promise);
                }
            });
        }
        peer = WebTransportSettings.read(settings);
        settingsReady.complete(null);
    }

    boolean draining() { return draining; }

    void goAway(long cutoff) { goAway(cutoff, !server); }

    private void goAway(long cutoff, boolean requestCutoff) {
        draining = true;
        for (var session : List.copyOf(sessions.values())) {
            if (!session.ready() && requestCutoff && session.id() >= cutoff) {
                session.rejected(new IOException("HTTP/3 GOAWAY rejected CONNECT"), Http3ErrorCode.H3_REQUEST_REJECTED.code());
            } else session.notifyDraining();
        }
    }

    void install(Http3ConnectionHandler http3) {
        requireLoop();
        var pipeline = channel.pipeline();
        if (pipeline.get(Http3ConnectionHandler.class) != null) {
            throw new IllegalStateException("connection already has an HTTP/3 handler");
        }
        pipeline.addLast(new IncomingStreams(), http3, registration);
    }

    WebTransportSession reserve(QuicStreamChannel connect, boolean legacyRequest, Consumer<PipeIo> incoming) throws IOException {
        requireLoop();
        long id = connect.streamId();
        if (connect.parent() != channel || id < 0 || id > QuicVarInts.MAX || (id & 3) != 0) {
            throw new IllegalArgumentException("expected a client-initiated CONNECT stream on this connection");
        }
        if (!connect.isActive()) throw new IllegalStateException("CONNECT stream closed");
        if (sessions.containsKey(id)) throw new IllegalStateException("CONNECT stream already has a session");
        if (draining) throw new IOException("HTTP/3 connection is draining");
        if (peer == null || !peer.permits(!server, datagrams.capacity() > 0, legacyRequest)) {
            throw new UnsupportedOperationException("WebTransport not negotiated on this HTTP/3 connection");
        }
        if (sessionEstablished || !sessions.isEmpty()) throw new IOException("single WebTransport session per connection");
        var session = new WebTransportSession(this, connect, incoming, receiveMemory);
        sessions.put(id, session);
        session.install();
        return session;
    }

    WebTransportSession session(long id) { return sessions.get(id); }

    void committed(WebTransportSession session) {
        sessionEstablished = true;
        reorder.resolve(session.id(), session);
    }

    QuicDatagrams datagrams() { return datagrams; }

    void retire(WebTransportSession session) {
        sessions.remove(session.id(), session);
        retireRequest(session.id());
        bidi.withdraw(session);
        uni.withdraw(session);
    }

    CompletableFuture<QuicStreamChannel> open(WebTransportSession session, QuicStreamType type) {
        return (type == QuicStreamType.BIDIRECTIONAL ? bidi : uni).enqueue(session);
    }

    void ordinary(QuicStreamChannel stream) {
        long id = stream.streamId();
        ordinary.put(id, stream);
        retireRequest(id);
        stream.closeFuture().addListener(done -> ordinary.remove(id));
    }

    private void unexpectedDatagram(long id) {
        var request = ordinary.get(id);
        if (request != null && !request.isInputShutdown()) abort(request, Http3ErrorCode.H3_DATAGRAM_ERROR.code());
    }

    private boolean gone(long id) {
        var range = retired.floorEntry(id);
        return range != null && id <= range.getValue();
    }

    private void retireRequest(long id) {
        if ((id & 3) != 0) return;
        var left = retired.floorEntry(id);
        if (left != null && left.getValue() >= id) return;
        long first = left != null && left.getValue() == id - 4 ? left.getKey() : id;
        var right = retired.remove(id + 4);
        retired.put(first, right == null ? id : right);
        reorder.resolve(id, null);
    }

    void observeRequest(QuicStreamChannel stream) {
        long id = stream.streamId();
        if ((id & 3) == 0) stream.closeFuture().addListener(done -> retireRequest(id));
    }

    static long datagramSessionId(ByteBuf packet) {
        long quarter = QuicVarInts.read(packet);
        return quarter < 0 || quarter > QuicVarInts.MAX / 4 ? -1 : quarter * 4;
    }

    private void datagram(ByteBuf packet) {
        try {
            long id = datagramSessionId(packet);
            if (id < 0) {
                channel.close(true, Http3ErrorCode.H3_DATAGRAM_ERROR.code(), Unpooled.EMPTY_BUFFER);
                return;
            }
            if (ordinary.containsKey(id)) {
                unexpectedDatagram(id);
                return;
            }
            // The connection owns the registered session; this lookup only borrows it.
            //noinspection resource
            var session = session(id);
            if (session != null && session.ready()) {
                session.datagram(packet.nioBuffer());
            } else if (session != null || !gone(id)) {
                reorder.hold(id, packet.readableBytes(), budget, channel, () -> {
                    byte[] copy = new byte[packet.readableBytes()];
                    packet.readBytes(copy);
                    return new WebTransportReorder.Input() {
                        @Override public void deliver(WebTransportSession owner) { owner.datagram(ByteBuffer.wrap(copy)); }
                        @Override public void discard(boolean retired) { if (retired) unexpectedDatagram(id); }
                    };
                });
            }
        } finally { packet.release(); }
    }

    void requireLoop() {
        if (!getBorrowedEventLoop(channel).inEventLoop()) throw new IllegalStateException("call on the HTTP/3 connection loop");
    }

    private final class IncomingStreams extends ChannelInboundHandlerAdapter {
        @Override public void channelRead(ChannelHandlerContext ctx, Object message) {
            if (message instanceof ByteBuf packet) {
                datagram(packet);
                return;
            }
            if (!(message instanceof QuicStreamChannel stream)) {
                ctx.fireChannelRead(message);
                return;
            }
            observeRequest(stream);
            QuicHandshake.observe(stream);
            stream.pipeline().addLast(new Prefix(ctx));
            // Register the child once via the QUIC tail, before classifying its first bytes.
            ctx.pipeline().context(registration).fireChannelRead(stream);
            stream.config().setAutoRead(true);
        }

        @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
            if (event instanceof QuicStreamLimitChangedEvent) {
                bidi.creditChanged();
                uni.creditChanged();
            }
            ctx.fireUserEventTriggered(event);
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            settingsReady.completeExceptionally(cause);
            for (var session : List.copyOf(sessions.values())) session.failed(cause);
            ctx.fireExceptionCaught(cause);
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            reorder.close();
            bidi.withdraw(null);
            uni.withdraw(null);
            for (var session : List.copyOf(sessions.values())) session.failed(new ClosedChannelException());
            ctx.fireChannelInactive();
        }
    }

    private record Opening(WebTransportSession session, CompletableFuture<QuicStreamChannel> result) {
        boolean belongsTo(WebTransportSession owner) { return session == owner; }
    }

    /** A QUIC stream-credit queue, shared across all CONNECT streams in a direction. */
    private final class StreamOpener {
        private final QuicStreamType type;
        private final QuicStreamCredit credit;
        private final ArrayDeque<Opening> waiting = new ArrayDeque<>();
        private boolean allocating;

        StreamOpener(QuicStreamType type) {
            this.type = type;
            credit = new QuicStreamCredit(type);
        }

        CompletableFuture<QuicStreamChannel> enqueue(WebTransportSession session) {
            var result = new CompletableFuture<QuicStreamChannel>();
            var pending = new Opening(session, result);
            waiting.addLast(pending);
            result.whenComplete((stream, failure) -> {
                if (result.isCancelled()) Loops.run(getBorrowedEventLoop(channel), () -> {
                    waiting.remove(pending);
                    pump();
                }, () -> {});
            });
            pump();
            return result;
        }

        void creditChanged() {
            credit.changed();
            pump();
        }

        void withdraw(WebTransportSession session) {
            if (waiting.isEmpty()) return;
            var withdrawn = new ArrayList<Opening>();
            waiting.removeIf(pending -> {
                if (session != null && !pending.belongsTo(session)) return false;
                withdrawn.add(pending);
                return true;
            });
            for (var pending : withdrawn) pending.result().completeExceptionally(new ClosedChannelException());
            if (session != null) pump();
        }

        private void pump() {
            if (allocating || !channel.isActive() || !credit.available(channel)) return;
            var pending = waiting.peekFirst();
            if (pending == null) return;
            allocating = true;
            try {
                var opening = channel.createStream(type, OPEN);
                opening.addListener(done -> allocated(pending, opening.getNow(), done.cause()));
            } catch (RuntimeException failure) {
                allocated(pending, null, failure);
            }
        }

        private void allocated(Opening pending, QuicStreamChannel stream, Throwable failure) {
            allocating = false;
            if (stream != null) {
                retireRequest(stream.streamId());
                if (!waiting.remove(pending) || !pending.result().complete(stream)) stream.close();
            } else if (!credit.waitForCredit(failure) && waiting.remove(pending)) {
                pending.result().completeExceptionally(failure);
            }
            if (!waiting.isEmpty() && !credit.blocked()) {
                Loops.run(getBorrowedEventLoop(channel), this::pump, () -> {});
            }
        }
    }

    private static final class Registration extends ChannelInboundHandlerAdapter {
        @Override public void channelRead(ChannelHandlerContext ctx, Object message) {
            // When a normal request is classified, replay it to HTTP/3 without registering it again.
            if (!(message instanceof QuicStreamChannel stream) || !stream.isRegistered()) ctx.fireChannelRead(message);
        }
    }

    static final long MORE = -1, HTTP = -2;

    static long readPreface(ByteBuf input, QuicStreamType streamType) {
        int start = input.readerIndex();
        long type = QuicVarInts.read(input);
        if (type < 0) return MORE;
        long marker = streamType == QuicStreamType.BIDIRECTIONAL ? BIDI_STREAM : UNI_STREAM;
        if (type != marker) {
            input.readerIndex(start);
            return HTTP;
        }
        long id = QuicVarInts.read(input);
        if (id < 0) input.readerIndex(start);
        return id;
    }

    private final class Prefix extends ByteToMessageDecoder {
        private final ChannelHandlerContext parent;

        private boolean held, reset;

        Prefix(ChannelHandlerContext parent) {
            this.parent = parent;
            setCumulator(COMPOSITE_CUMULATOR);
        }

        @Override protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            if (held) return;
            int start = in.readerIndex();
            var stream = (QuicStreamChannel) ctx.channel();
            long id = readPreface(in, stream.type());
            if (id == MORE) return;
            if (id == HTTP) {
                if (stream.type() == QuicStreamType.BIDIRECTIONAL) {
                    ctx.pipeline().addAfter(ctx.name(), null, new WebTransportHttpFrames(0));
                } else {
                    long type = QuicVarInts.read(in);
                    in.readerIndex(start);
                    // Control streams begin with their type; push streams also carry the push ID.
                    if (type == 0 || type == 1) ctx.pipeline().addAfter(ctx.name(), null,
                            new WebTransportHttpFrames(type == 0 ? 1 : 2));
                }
                parent.fireChannelRead(stream);
                ctx.pipeline().remove(this);
                return;
            }
            retireRequest(stream.streamId());
            if ((id & 3) != 0) {
                in.skipBytes(in.readableBytes());
                channel.close(true, Http3ErrorCode.H3_ID_ERROR.code(), Unpooled.EMPTY_BUFFER);
                return;
            }
            // The connection owns the registered session; this lookup only borrows it.
            //noinspection resource
            var session = session(id);
            stream.config().setAutoRead(false);
            if (session != null && session.ready()) {
                attach(ctx, stream, session);
                return;
            }
            boolean retired = session == null && gone(id);
            if (!retired) {
                held = true;
                // The descriptor owns the paused QUIC stream. Already decrypted bytes remain in
                // the provider's reserved receive window until handler removal replays them.
                Runnable cancel = reorder.hold(id, 0, budget, channel, () -> new WebTransportReorder.Input() {
                    @Override public void deliver(WebTransportSession owner) {
                        if (stream.isActive()) attach(ctx, stream, owner);
                    }
                    @Override public void discard(boolean gone) {
                        if (stream.isActive()) abort(stream, gone ? SESSION_GONE : BUFFERED_STREAM_REJECTED);
                    }
                });
                if (cancel != null) {
                    stream.closeFuture().addListener(done -> cancel.run());
                    return;
                }
            }
            in.skipBytes(in.readableBytes());
            abort(stream, retired ? SESSION_GONE : BUFFERED_STREAM_REJECTED);
        }

        private void attach(ChannelHandlerContext ctx, QuicStreamChannel stream, WebTransportSession session) {
            stream.config().setAutoRead(false);
            var incoming = session.take(stream);
            ctx.pipeline().remove(this);
            session.incoming(incoming);
        }

        @Override protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            if (reset) {
                in.skipBytes(in.readableBytes());
                return;
            }
            super.decodeLast(ctx, in, out);
            if (in.isReadable()) throw new IOException("incomplete WebTransport stream preface");
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable failure) {
            if (failure instanceof io.netty.handler.codec.quic.QuicStreamResetException) {
                reset = true;
                ctx.close();
            } else abort((QuicStreamChannel) ctx.channel(), BUFFERED_STREAM_REJECTED);
        }
    }

    void endDataStream(QuicStreamChannel stream, long wireCode) {
        if (!stream.isActive()) return;
        boolean bidiStream = stream.type() == QuicStreamType.BIDIRECTIONAL;
        if ((bidiStream || !stream.isLocalCreated()) && !stream.isInputShutdown()) provider.stop(stream, wireCode);
        if ((bidiStream || stream.isLocalCreated()) && !stream.isOutputShutdown()) {
            provider.reset(stream, wireCode).addListener(ChannelFutureListener.CLOSE);
        } else stream.close();
    }

    static void abort(QuicStreamChannel stream, int code) {
        var reset = stream.type() == QuicStreamType.BIDIRECTIONAL ? stream.shutdown(code)
                : stream.isLocalCreated() ? stream.shutdownOutput(code) : stream.shutdownInput(code);
        reset.addListener(ChannelFutureListener.CLOSE);
    }
}
