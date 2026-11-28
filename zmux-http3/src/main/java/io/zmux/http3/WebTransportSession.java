package io.zmux.http3;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.handler.codec.http3.Http3DataFrame;
import io.netty.handler.codec.http3.Http3ErrorCode;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.zmux.CarrierKind;
import io.zmux.Zmux;
import io.zmux.ZmuxFuture;
import io.zmux.carrier.PipeIo;
import io.zmux.internal.spi.CarrierBudget;
import io.zmux.internal.spi.Loops;
import io.zmux.internal.spi.Spi;
import io.zmux.quic.QuicStreamPipe;
import io.zmux.quic.internal.QuicVarInts;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/** Owns one Extended CONNECT and its WebTransport data streams. */
public final class WebTransportSession implements AutoCloseable {
    /** Unsigned 32-bit session close code, UTF-8 reason and initiating endpoint. */
    public record Close(long code, String reason, boolean remote) { }

    record Stream(QuicStreamChannel channel, PipeIo io) {}

    private final WebTransportConnection connection;
    private final CarrierBudget.Namespace namespace;

    Object namespace() { return namespace; }
    private final QuicStreamChannel connect;
    private final Consumer<PipeIo> bidirectional;
    private final CompletableFuture<Void> draining = new CompletableFuture<>();
    private final CompletableFuture<Void> drainSent = new CompletableFuture<>();
    private boolean sendingDrain;
    private boolean finishing;
    private ChannelHandlerContext control;
    private boolean waitForFirst;
    private boolean firstDelivered;
    private boolean admitted;
    private final long receiveMemory;
    private final Set<QuicStreamChannel> streams = new HashSet<>();
    // A CONNECT can overtake a peer's streams. Their reads stop after the preface,
    // so both their count and their buffered bytes are bounded by the connection's QUIC credit.
    private final ArrayDeque<Stream> pending = new ArrayDeque<>();
    private final CompletableFuture<Close> closed = new CompletableFuture<>();
    private final CompletableFuture<Void> outputFinished = new CompletableFuture<>();
    private boolean upgraded;
    private boolean ended;
    private WebTransportBinding binding;


    WebTransportSession(WebTransportConnection connection, QuicStreamChannel connect,
                        Consumer<PipeIo> bidirectional, long receiveMemory) {
        this.connection = connection;
        namespace = connection::physical;
        this.connect = connect;
        this.bidirectional = bidirectional;
        this.receiveMemory = receiveMemory;
    }

    void install() {
        connect.config().setAllowHalfClosure(true);

        connect.closeFuture().addListener(done -> failed(new ClosedChannelException()));
    }

    void committed() {
        connection.requireLoop();
        if (ended()) throw new IllegalStateException("WebTransport session has ended");
        if (upgraded) throw new IllegalStateException("CONNECT already committed");
        binding = new WebTransportBinding(this, connection.datagrams());
        upgraded = true;
        var handler = new Control();
        Http3Streams.raw(connect, handler);
        control = connect.pipeline().context(handler);
        connection.committed(this);

        var early = List.copyOf(pending);
        pending.clear();
        for (var stream : early) incoming(stream);
    }

    void datagram(ByteBuffer payload) {
        if (ready()) binding.datagram(payload);
    }

    long id() { return connect.streamId(); }

    /** Observes the first definitive session termination or transport failure. */
    public ZmuxFuture<Close> closed() { return Spi.future(closed, null); }

    /** Observes local or remote DRAIN, or GOAWAY on the enclosing connection. */
    public ZmuxFuture<Void> draining() { return Spi.future(draining, null); }

    void notifyDraining() { draining.complete(null); }

    /** Sends one session DRAIN. Existing pipes may continue to transfer data. */
    public ZmuxFuture<Void> drainAsync() {
        dispatch(() -> {
            if (sendingDrain) return;
            if (!ready()) {
                drainSent.completeExceptionally(new ClosedChannelException());
                return;
            }
            sendingDrain = true;
            control.writeAndFlush(WebTransportCapsules.drain(connect.alloc())).addListener(done -> {
                if (done.isSuccess()) notifyDraining();
                else rejected(done.cause(), Http3ErrorCode.H3_INTERNAL_ERROR.code());
                complete(drainSent, done.cause());
            });
        }, () -> drainSent.completeExceptionally(new ClosedChannelException()));
        return Spi.future(drainSent, null);
    }

    /** Opens a bidirectional zmux pipe with a session-scoped lane opener and datagram channel. */
    public ZmuxFuture<PipeIo> openPipeAsync() {
        var open = openStream(QuicStreamType.BIDIRECTIONAL);
        var result = new CompletableFuture<PipeIo>();
        open.whenComplete((stream, cause) -> {
            if (cause != null) result.completeExceptionally(cause);
            else dispatch(() -> {
                if (ready() && !result.isDone()) {
                    if (!result.complete(binding.pipe(stream))) cancelStream(stream.channel);
                } else {
                    cancelStream(stream.channel);
                    result.completeExceptionally(new ClosedChannelException());
                }
            }, () -> result.completeExceptionally(new ClosedChannelException()));
        });
        return Spi.future(result, () -> { result.cancel(false); open.cancel(false); });
    }

    CompletableFuture<Stream> openStream(QuicStreamType type) {
        Objects.requireNonNull(type, "type");
        var result = new CompletableFuture<Stream>();
        dispatch(() -> open(type, result), () -> result.completeExceptionally(new ClosedChannelException()));
        return result;
    }

    private void open(QuicStreamType type, CompletableFuture<Stream> result) {
        if (result.isDone()) return;
        if (!ready()) {
            result.completeExceptionally(new ClosedChannelException());
            return;
        }
        var opened = connection.open(this, type);
        result.whenComplete((stream, failure) -> opened.cancel(false));
        opened.whenComplete((stream, failure) -> {
            if (failure != null) {
                result.completeExceptionally(failure);
                return;
            }
            if (ended() || result.isDone()) {
                stream.close();
                result.completeExceptionally(new ClosedChannelException());
                return;
            }
            track(stream);
            result.whenComplete((value, rejected) -> {
                if (rejected != null) dispatch(() -> cancelStream(stream), () -> {});
            });
            try {
                var preface = stream.alloc().buffer(16);
                QuicVarInts.write(preface, type == QuicStreamType.BIDIRECTIONAL
                        ? WebTransportConnection.BIDI_STREAM : WebTransportConnection.UNI_STREAM);
                QuicVarInts.write(preface, id());
                var write = connection.provider.writePreface(stream, preface);
                var outgoing = take(stream);
                write.addListener(written -> {
                    if (!written.isSuccess()) result.completeExceptionally(written.cause());
                    else if (ended()) result.completeExceptionally(new ClosedChannelException());
                    else if (!result.complete(outgoing)) cancelStream(stream);
                });
            } catch (RuntimeException badStream) {
                result.completeExceptionally(badStream);
            }
        });
    }

    /** Sends CLOSE with the supplied code and reason, then finishes CONNECT. */
    public ZmuxFuture<Void> closeAsync(long code, String reason) {
        Objects.requireNonNull(reason, "reason");
        var result = new CompletableFuture<Void>();
        dispatch(() -> close(code, reason, result), () -> result.completeExceptionally(new ClosedChannelException()));
        return Spi.future(result, null);
    }

    /** Begins a normal session close. */
    @Override public void close() { closeAsync(0, ""); }

    private void close(long code, String reason, CompletableFuture<Void> result) {
        if (ended()) {
            outputFinished.whenComplete((ignored, failure) -> complete(result, failure));
            return;
        }
        try {
            var capsule = WebTransportCapsules.close(connect.alloc(), code, reason);
            outputFinished.whenComplete((ignored, failure) -> complete(result, failure));
            end(new Close(code, reason, false), null);
            if (!upgraded) {
                capsule.release();
                connect.close().addListener(done -> complete(outputFinished, done.cause()));
                return;
            }
            control.writeAndFlush(capsule).addListener(done -> {
                if (done.isSuccess()) finishOutput();
                else {
                    complete(outputFinished, done.cause());
                    connect.close();
                }
            });
        } catch (IllegalArgumentException badClose) {
            result.completeExceptionally(badClose);
        }
    }

    void peerClosed(long code, String reason) {
        if (ended()) return;
        end(new Close(code, reason, true), null);
        finishOutput();
    }

    private void finishOutput() {
        if (finishing) return;
        finishing = true;
        QuicStreamPipe.finish(connect).addListener(done -> {
            complete(outputFinished, done.cause());
            if (!done.isSuccess()) connect.close();
        });
    }

    private static void complete(CompletableFuture<Void> result, Throwable cause) {
        if (cause == null) result.complete(null);
        else result.completeExceptionally(cause);
    }

    void failed(Throwable cause) {
        if (ended()) return;
        end(null, cause);
        outputFinished.completeExceptionally(cause);
    }

    private void end(Close value, Throwable cause) {
        if (ended()) return;
        ended = true;
        connection.retire(this);
        if (binding != null) binding.stop();
        draining.completeExceptionally(cause == null ? new ClosedChannelException() : cause);
        for (var stream : List.copyOf(streams)) cancelStream(stream);
        pending.clear();
        if (cause == null) closed.complete(value);
        else closed.completeExceptionally(cause);
    }

    Stream take(QuicStreamChannel stream) {
        var data = new WebTransportData(stream, connection.provider);
        stream.pipeline().addLast(data);

        var io = QuicStreamPipe.take(stream).withCarrier(CarrierKind.WEBTRANSPORT)
                .withCarrierConn(namespace).withProtocol("").withReceiveMemory(receiveMemory);
        track(stream);
        return new Stream(stream, io);
    }

    void cancelStream(QuicStreamChannel stream) {
        if (streams.remove(stream)) {
            long code = ended() ? WebTransportConnection.SESSION_GONE : WebTransportStreamException.wireCode(PipeIo.CARRIER_FAILED);
            connection.endDataStream(stream, code);
        }
    }

    private void track(QuicStreamChannel stream) {
        if (streams.add(stream)) stream.closeFuture().addListener(done -> streams.remove(stream));
    }

    void waitForFirst() { waitForFirst = true; }

    void admitted() {
        admitted = true;
        for (var stream : List.copyOf(pending)) {
            pending.remove(stream);
            incoming(stream);
        }
    }

    void incoming(Stream stream) {
        if (!stream.channel.isActive()) return;
        if (ended()) {
            stream.channel.close();
            return;
        }
        boolean bidi = stream.channel.type() == QuicStreamType.BIDIRECTIONAL;
        if (!upgraded || (waitForFirst && !admitted && (!bidi || firstDelivered))) {
            pending.add(stream);
            stream.channel.closeFuture().addListener(done -> pending.remove(stream));
            return;
        }
        if (stream.channel.type() == QuicStreamType.UNIDIRECTIONAL) Zmux.handleLane(stream.io);
        else {
            firstDelivered = true;
            bidirectional.accept(binding.pipe(stream));
        }
    }

    void rejected(Throwable cause, int error) {
        failed(cause);
        WebTransportConnection.abort(connect, error);
    }

    private final class Control extends SimpleChannelInboundHandler<Http3DataFrame> implements WebTransportCapsules.Receiver {
        private final WebTransportCapsules capsules = new WebTransportCapsules(this);

        @Override protected void channelRead0(ChannelHandlerContext ctx, Http3DataFrame frame) {
            try { capsules.read(frame.content()); }
            catch (IOException malformed) { rejected(malformed, Http3ErrorCode.H3_MESSAGE_ERROR.code()); }
        }

        @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
            if (event == ChannelInputShutdownEvent.INSTANCE) {
                try { capsules.end(); }
                catch (IOException truncated) { rejected(truncated, Http3ErrorCode.H3_MESSAGE_ERROR.code()); }
            }
            ctx.fireUserEventTriggered(event);
        }

        @Override public void peerClosed(long code, String reason) { WebTransportSession.this.peerClosed(code, reason); }
        @Override public void peerDraining() { notifyDraining(); }
        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            failed(cause);
            ctx.close();
        }
    }

    boolean ended() { return ended; }
    boolean ready() { return upgraded && !ended() && connect.isActive(); }

    void dispatch(Runnable action, Runnable rejected) {
        var loop = connect.eventLoop();
        Loops.dispatch(loop, loop.inEventLoop(), action, rejected);
    }
}
