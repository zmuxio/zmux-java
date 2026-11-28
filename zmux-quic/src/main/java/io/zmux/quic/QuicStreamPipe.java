package io.zmux.quic;

import static io.zmux.quic.internal.NettyLoops.getBorrowedEventLoop;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.socket.ChannelOutputShutdownException;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.zmux.CarrierKind;
import io.zmux.Admission.OuterSecurity;
import io.zmux.carrier.PipeIo;
import io.zmux.internal.spi.Loops;
import io.zmux.quic.internal.QuicHandshake;
import java.nio.channels.ClosedChannelException;

/** Adapts a host-routed QUIC byte stream, such as the data stream of a WebTransport session. */
public final class QuicStreamPipe {
    private QuicStreamPipe() { }

    /**
     * Records handshake evidence at stream arrival. A host that buffers streams before
     * handing them to {@link #take} calls this on the stream's event loop before buffering.
     */
    public static void observe(QuicStreamChannel stream) {
        if (!getBorrowedEventLoop(stream).inEventLoop()) throw new IllegalStateException("observe the stream on its event loop");
        QuicHandshake.observe(stream);
    }

    /**
     * Takes the stream's data halves on its event loop. Install after any incoming preface
     * decoder so its remaining bytes go to the pipe. The pipe owns only the stream; its
     * framing, namespace and further streams are up to the caller.
     */
    public static PipeIo take(QuicStreamChannel stream) {
        if (!getBorrowedEventLoop(stream).inEventLoop()) throw new IllegalStateException("take the stream on its event loop");
        if (stream.pipeline().get(StreamCarrier.class) != null) throw new IllegalStateException("stream already taken");
        var parent = stream.parent();
        var tls = parent.sslEngine();
        if (tls == null) throw new IllegalStateException("expected a QUIC stream with a TLS engine");
        boolean both = stream.type() == QuicStreamType.BIDIRECTIONAL;
        var security = QuicHandshake.observe(stream);
        var carrier = new StreamCarrier(stream, QuicServer.hostOf(getBorrowedEventLoop(stream)),
                both || !stream.isLocalCreated(), both || stream.isLocalCreated(), security);
        stream.pipeline().addLast(carrier);
        var pipe = carrier.halves().withCarrier(CarrierKind.QUIC).withCarrierConn(parent)
                .withExporter((label, context, length) -> QuicExporter.export(tls, label, context, length))
                .withProtocol(tls.getApplicationProtocol() == null ? "" : tls.getApplicationProtocol())
                .withTerminatedTls(security == OuterSecurity.QUIC).withOuterSecurity(security);
        return security == OuterSecurity.QUIC ? pipe.withCarrierSecure() : pipe;
    }

    /**
     * Sends a graceful FIN after the current I/O batch. On quiche 0.30.0 an ACK processed
     * between queueing a bare FIN and sending it can discard it (quiche #2525).
     */
    public static ChannelFuture finish(QuicStreamChannel stream) {
        var result = stream.newPromise();
        finish(stream, done -> {
            if (done.isSuccess()) result.trySuccess();
            else result.tryFailure(done.cause());
        }, () -> result.tryFailure(new ClosedChannelException()));
        return result;
    }

    static void finish(QuicStreamChannel stream, ChannelFutureListener finished, Runnable rejected) {
        var afterIteration = NettyHost.afterIteration(getBorrowedEventLoop(stream));
        Loops.run(afterIteration, () -> finishWhenDrained(stream, finished, rejected), rejected);
    }

    private static void finishWhenDrained(QuicStreamChannel stream, ChannelFutureListener finished, Runnable rejected) {
        if (!stream.isActive() || stream.isOutputShutdown()) {
            stream.newSucceededFuture().addListener(finished);
            return;
        }

        // shutdownOutput bypasses Netty's stream write queue. An empty data write
        // goes through that queue and needs no credit when it reaches the head.
        var drained = stream.newPromise();
        stream.unsafe().write(Unpooled.EMPTY_BUFFER, drained);
        if (drained.isSuccess()) {
            finishDrained(stream).addListener(finished);
        } else {
            drained.addListener(done -> {
                if (done.isSuccess()) {
                    // Leave Netty's writable callback before queueing the FIN.
                    finish(stream, finished, rejected);
                } else {
                    finished.operationComplete(drained);
                }
            });
        }
    }

    private static ChannelFuture finishDrained(QuicStreamChannel stream) {
        var fin = stream.shutdownOutput();
        if (fin.isDone()) return fin;

        // With no preceding data left, quiche accepts an empty FIN even at zero
        // capacity. A pending promise here means Netty queued QUICHE_ERR_DONE
        // from a collected stream, which can never become writable again.
        var stopped = stream.newPromise();
        stream.close().addListener(ignored ->
                stopped.tryFailure(new ChannelOutputShutdownException("QUIC stream already terminated")));
        return stopped;
    }
}
