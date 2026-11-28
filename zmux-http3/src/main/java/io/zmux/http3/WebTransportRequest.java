package io.zmux.http3;

import static io.zmux.quic.internal.NettyLoops.getBorrowedEventLoop;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http3.DefaultHttp3DataFrame;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.Http3DataFrame;
import io.netty.handler.codec.http3.Http3ErrorCode;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.ReferenceCountUtil;
import io.zmux.Admission;
import io.zmux.AdmissionVerdict;
import io.zmux.JoinHint;
import io.zmux.adapter.Handled;
import io.zmux.adapter.Upgrade;
import io.zmux.carrier.PipeIo;
import io.zmux.http.internal.HttpTransport;
import io.zmux.internal.spi.CarrierBudget;
import io.zmux.internal.spi.Loops;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

/** Owns one server CONNECT until HTTP fallback or first pipe handoff. */
final class WebTransportRequest extends ChannelInboundHandlerAdapter implements Upgrade {
    private record Body(byte[] bytes, Runnable release) { }

    private final WebTransportConnection connection;
    private final HttpTransport.Route route;
    private final QuicStreamChannel stream;
    private final WebTransportFirstPipe first;
    private final CompletableFuture<Void> writeReleased = new CompletableFuture<>();
    private final ArrayDeque<Body> early = new ArrayDeque<>();
    private ChannelHandlerContext ctx;
    private Http3HeadersFrame head;
    private WebTransportSession session;
    private Handled handled;
    private ScheduledFuture<?> deadline;
    private CompletableFuture<Void> awaitingSettings;
    private boolean started, replayed, finished, inputEnded;

    WebTransportRequest(WebTransportConnection connection, HttpTransport.Route route, QuicStreamChannel stream) {
        this.connection = connection;
        this.route = route;
        this.stream = stream;
        first = new WebTransportFirstPipe(() -> dispatch(() -> {
            if (!replayed) reject(new ClosedChannelException(), Http3ErrorCode.H3_REQUEST_CANCELLED.code());
        }));
    }

    @Override public void handlerAdded(ChannelHandlerContext context) {
        ctx = context;
        stream.closeFuture().addListener(done -> cleanup(new ClosedChannelException()));
    }

    @Override public void channelRead(ChannelHandlerContext context, Object message) {
        if (message instanceof Http3HeadersFrame headers && head == null) {
            var fields = headers.headers();
            if (!"CONNECT".contentEquals(String.valueOf(fields.method())) || !WebTransportConnection.accepts(fields.protocol())) {
                connection.ordinary(stream);
                context.pipeline().remove(this);
                context.fireChannelRead(message);
                return;
            }
            head = headers;
            int refusal = Http3Streams.requestStream(stream).outerSecurity() != Admission.OuterSecurity.QUIC ? 425 : refusal(fields);
            if (refusal != 0) {
                if (refusal < 0) reject(new IOException("malformed WebTransport CONNECT"), Http3ErrorCode.H3_MESSAGE_ERROR.code());
                else refuse(refusal);
                return;
            }
            stream.config().setAutoRead(false);
            // Netty owns the context executor; this request only owns the scheduled deadline.
            //noinspection resource
            deadline = context.executor().schedule(() -> reject(new TimeoutException("WebTransport setup timeout"),
                    Http3ErrorCode.H3_REQUEST_CANCELLED.code()), route.setupTimeout().toNanos(), TimeUnit.NANOSECONDS);
            awaitingSettings = connection.settingsReady.whenComplete((v, error) -> {
                if (finished) return;
                if (error != null) reject(error, Http3ErrorCode.H3_REQUEST_CANCELLED.code());
                else admit();
            });
        } else if (message instanceof Http3DataFrame data) {
            try {
                if (!finished && data.content().isReadable()) buffer(data);
            } finally { data.release(); }
        } else {
            ReferenceCountUtil.release(message);
            reject(new IOException("unexpected frame on WebTransport CONNECT"), Http3ErrorCode.H3_MESSAGE_ERROR.code());
        }
    }

    static int refusal(Http3Headers fields) {
        var path = fields.path();
        if (!"https".contentEquals(String.valueOf(fields.scheme())) || path == null
                || !path.toString().startsWith("/") || fields.authority() == null) return -1;
        H3Authority authority;
        try { authority = H3Authority.origin(URI.create("https://" + fields.authority())); }
        catch (IllegalArgumentException invalid) { return -1; }
        for (var early : fields.getAll("early-data")) if (early.toString().strip().equals("1")) return 425;
        var origins = fields.getAll("origin");
        if (origins.isEmpty()) return 0;
        if (origins.size() != 1) return 403;
        try {
            var origin = URI.create(origins.getFirst().toString());
            return origin.getRawPath().isEmpty() && H3Authority.origin(origin).equals(authority) ? 0 : 403;
        } catch (IllegalArgumentException | NullPointerException invalid) { return 403; }
    }

    void stopBeforeAdmission() {
        if (finished || handled != null) return;
        if (head == null) ctx.pipeline().remove(this);
        else reject(new ClosedChannelException(), Http3ErrorCode.H3_REQUEST_CANCELLED.code());
    }

    private void admit() {
        try {
            var fields = head.headers();
            var protocol = fields.protocol();
            var path = fields.path();
            assert protocol != null && path != null : "CONNECT pseudo-headers were validated before admission";
            session = connection.reserve(stream, WebTransportConnection.LEGACY_PROTOCOL.contentEquals(protocol), this::pipe);
            session.waitForFirst();
            var request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.CONNECT, path.toString());
            request.headers().set("host", fields.authority());
            for (var field : fields) {
                if (field.getKey().charAt(0) != ':') request.headers().add(field.getKey(), field.getValue());
            }
            var io = first.pipe(session.namespace()).withReceiveMemory(connection.receiveCapacity());
            handled = route.admitUpgrade(request, io, this, this::fallback);
            handled.verdict().whenComplete((verdict, cause) -> dispatch(() -> {
                if (finished && !session.ready()) return;
                if (verdict == AdmissionVerdict.ACCEPT) {
                    first.first.thenRun(() -> dispatch(session::admitted));
                } else if (verdict == AdmissionVerdict.CLOSE || cause != null) {
                    reject(cause == null ? new ClosedChannelException() : cause, Http3ErrorCode.H3_REQUEST_CANCELLED.code());
                }
            }));
            handled.closed().whenComplete((v, error) -> dispatch(() -> {
                if (!session.ready() || handled.detached() || replayed) return;
                if (handled.verdict().getNow(null) != AdmissionVerdict.ACCEPT) session.close();
            }));
        } catch (UnsupportedOperationException malformed) {
            reject(malformed, Http3ErrorCode.H3_MESSAGE_ERROR.code());
        } catch (IOException | RuntimeException failure) {
            reject(failure, Http3ErrorCode.H3_REQUEST_REJECTED.code());
        }
    }

    private void pipe(PipeIo io) {
        if (first.first.complete(io)) {
            Loops.cancel(deadline);
        } else if (session.ready() && !first.first.isCompletedExceptionally()) {
            handled.sibling(io, JoinHint.inBand(Http3Streams.requestStream(stream).peer()));
        } else {
            io.reader().close();
            io.writer().abort(new ClosedChannelException());
        }
    }

    @Override public CompletionStage<Void> released() { return writeReleased.minimalCompletionStage(); }

    @Override public CompletionStage<?> open() {
        var result = new CompletableFuture<Void>();
        dispatch(() -> {
            if (finished || result.isCancelled()) {
                writeReleased.complete(null);
                result.completeExceptionally(new ClosedChannelException());
                return;
            }
            started = true;
            ctx.writeAndFlush(status(200)).addListener(done -> {
                writeReleased.complete(null);
                if (!done.isSuccess() || finished || result.isCancelled()) {
                    Throwable cause = done.isSuccess() ? new ClosedChannelException() : done.cause();
                    reject(cause, Http3ErrorCode.H3_REQUEST_CANCELLED.code());
                    result.completeExceptionally(cause);
                    return;
                }
                session.committed();
                finished = true;
                ctx.pipeline().remove(this);
                replayBody();
                stream.config().setAutoRead(true);
                first.first.whenComplete((io, error) -> {
                    if (error == null) result.complete(null);
                    else result.completeExceptionally(error);
                });
            });
        });
        result.whenComplete((v, error) -> {
            if (result.isCancelled()) dispatch(() -> reject(new ClosedChannelException(), Http3ErrorCode.H3_REQUEST_CANCELLED.code()));
        });
        return result;
    }

    private CompletionStage<Void> fallback(BooleanSupplier handoff) {
        var done = new CompletableFuture<Void>();
        dispatch(() -> {
            if (finished || started || !handoff.getAsBoolean()) {
                done.completeExceptionally(new ClosedChannelException());
                return;
            }
            replayed = true;
            cleanup(new ClosedChannelException());
            session.failed(new ClosedChannelException());
            ctx.pipeline().remove(this);
            connection.ordinary(stream);
            ctx.fireChannelRead(head);
            replayBody();
            stream.config().setAutoRead(true);
            done.complete(null);
        });
        return done;
    }

    private void buffer(Http3DataFrame frame) {
        int n = frame.content().readableBytes();
        var release = connection.budget.acquire(stream.parent(), 0, n + CarrierBudget.DELIVERY_BYTES);
        if (release == null) {
            reject(new IOException("WebTransport setup receive budget exhausted"), Http3ErrorCode.H3_EXCESSIVE_LOAD.code());
            return;
        }
        try {
            var copy = new byte[n];
            frame.content().readBytes(copy);
            early.addLast(new Body(copy, release));
        } catch (RuntimeException | Error failed) {
            release.run();
            throw failed;
        }
    }

    private void replayBody() {
        for (Body body; (body = early.pollFirst()) != null; ) {
            try { ctx.fireChannelRead(new DefaultHttp3DataFrame(Unpooled.wrappedBuffer(body.bytes()))); }
            finally { body.release().run(); }
        }
        if (inputEnded) ctx.fireUserEventTriggered(ChannelInputShutdownEvent.INSTANCE);
    }

    @Override public void userEventTriggered(ChannelHandlerContext context, Object event) {
        if (event == ChannelInputShutdownEvent.INSTANCE && head != null && !finished) inputEnded = true;
        else context.fireUserEventTriggered(event);
    }

    private void dispatch(Runnable action) {
        Loops.dispatch(getBorrowedEventLoop(stream), getBorrowedEventLoop(stream).inEventLoop(), action, () -> {
            first.first.completeExceptionally(new ClosedChannelException());
            writeReleased.complete(null);
        });
    }

    private void refuse(int status) {
        cleanup(new IOException("WebTransport request refused"));
        ctx.writeAndFlush(status(status)).addListener(QuicStreamChannel.SHUTDOWN_OUTPUT);
    }

    private static Http3HeadersFrame status(int status) {
        var head = new DefaultHttp3HeadersFrame();
        head.headers().status(Integer.toString(status));
        return head;
    }

    private void reject(Throwable cause, int code) {
        if (replayed || !stream.isOpen()) return;
        cleanup(cause);
        if (session != null) session.rejected(cause, code);
        else WebTransportConnection.abort(stream, code);
    }

    private void cleanup(Throwable cause) {
        finished = true;
        if (awaitingSettings != null) awaitingSettings.cancel(false);
        Loops.cancel(deadline);
        if (!started) writeReleased.complete(null);
        first.first.completeExceptionally(cause);
        if (!replayed) {
            for (Body body; (body = early.pollFirst()) != null; ) body.release().run();
        }
    }
}
