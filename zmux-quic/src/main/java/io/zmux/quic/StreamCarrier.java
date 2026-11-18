package io.zmux.quic;

import io.zmux.internal.spi.Loops;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.ChannelInputShutdownReadComplete;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.zmux.CarrierKind;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import io.zmux.internal.loop.CarrierChannel;
import io.zmux.internal.loop.NativePipe;
import io.zmux.internal.loop.Transport;
import io.zmux.internal.loop.TransportInfo;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * One QUIC stream. Its pipe input ({@link #io}) is taken over by a session on the
 * connection's loop as a {@link QuicTransport} that parses and writes the stream directly;
 * used through its halves instead (wrapped by an application, or by a session on another
 * host), it lends each read to the reader's sink and writes each batch as it comes.
 * Everything but the halves' entry points runs on the stream's loop.
 */
final class StreamCarrier extends ChannelInboundHandlerAdapter {
    private static final int FRESH = 0, HALVES = 1, CLAIMED = 2;

    private final QuicStreamChannel stream;
    private final NettyHost host;
    private final boolean receive;
    private final boolean send;
    /** Who uses the stream: nobody yet, its pipe input's halves, or a transport. */
    private final AtomicInteger use = new AtomicInteger(FRESH);
    /** The transport reading the stream (native use), or null. */
    private QuicTransport transport;
    /** The input lent to the halves' reader (lent use), or null; one read at a time, as the reader consumes it. */
    private io.zmux.internal.spi.LentInput lent;
    /** Input no owner has taken yet: what arrives before an owner starts and what a moving transport left. */
    private final ArrayDeque<ByteBuf> early = new ArrayDeque<>();
    private boolean end;
    private Throwable inputFailure;
    private boolean inputClosed;
    private boolean outputClosed;
    private boolean inputDone, outputDone;
    private final AtomicBoolean halvesStarted = new AtomicBoolean();
    private PipeReader reader;
    private PipeWriter writer;
    /** The connection of the stream, known once its pipe input is made. */
    private volatile ConnectionCarrier connection;
    private PipeChannel channel;

    StreamCarrier(QuicStreamChannel stream, NettyHost host, boolean receive, boolean send) {
        this.stream = stream; this.host = host; this.receive = receive; this.send = send;
        stream.config().setAutoRead(false).setReadFrames(true)
                .setRecvByteBufAllocator(new io.netty.channel.AdaptiveRecvByteBufAllocator(4 << 10, 64 << 10, 256 << 10));
        if (stream.type() == io.netty.handler.codec.quic.QuicStreamType.BIDIRECTIONAL) stream.config().setAllowHalfClosure(true);
        // Netty completes the close future on the loop as the stream closes, before any later
        // task; channelInactive is such a task, which a terminating loop may never run.
        stream.closeFuture().addListener(f -> gone());
    }

    boolean unidirectional() { return stream.type() == io.netty.handler.codec.quic.QuicStreamType.UNIDIRECTIONAL; }
    void dispose() { dispatch(stream::close); }
    QuicStreamChannel stream() { return stream; }
    boolean receives() { return receive; }
    boolean sends() { return send; }

    /** The stream's pipe input; a session on the connection's loop takes it over ({@link NativePipe}). */
    PipeIo io(ConnectionCarrier connection) {
        this.connection = connection;
        if (reader == null && writer == null) {
            reader = !receive ? null : new NativeReader();
            writer = !send ? null : new NativeWriter();
            // A stream a session took is one of the connection's pipes until it closes.
            taken.stage().thenAccept(t -> { if (t) dispatch(() -> connection.pipeStarted(stream)); });
        }
        return PipeIo.of(reader, writer).withCarrierConn(connection.channel())
                .withReceiveMemory(connection.options().connectionWindow())
                .withProtocol(connection.applicationProtocol())
                .withTerminatedTls(true).withExporter(connection::export).withCarrierSecure() // A QUIC stream is end to end.
                .withLaneOpener(send ? connection::openLane : null).withCarrier(CarrierKind.QUIC);
    }

    /**
     * The datagram channel of the pipe this bidirectional stream is: the connection's QUIC datagrams, named by the stream's ID; null for a lane.
     */
    synchronized io.zmux.internal.loop.CarrierChannel channel() {
        if (channel == null && connection != null && !unidirectional()) channel = new PipeChannel(connection, stream.streamId());
        return channel;
    }

    /** The channel of one pipe on the connection's shared QUIC datagrams. */
    private record PipeChannel(ConnectionCarrier connection, long id) implements io.zmux.internal.loop.CarrierChannel {
        @Override public long channelStreamId() { return id; }
        @Override public int maxChannelDatagram() { return connection.maxDatagram(); }
        @Override public void send(byte[] datagram, Runnable refused) { connection.sendDatagram(datagram, refused); }
        @Override public Runnable receive(CarrierChannel.Sink sink) { return connection.receiveDatagrams(id, sink); }
    }

    /** The session on this stream ended with e; the connection keeps its code for its own close. On the loop. */
    void sessionEnded(io.zmux.internal.core.SessionError e) {
        ConnectionCarrier c = connection;
        if (c != null) c.sessionEnded(e);
    }

    /** This stream's connection's exporter, read now; null while it is unavailable. */
    byte[] export(String label, byte[] context, int length) { return connection == null ? null : connection.export(label, context, length); }

    boolean owns(Object half) { return half != null && (half == reader || half == writer); }

    private Transport claim(PipeIo io) {
        if (io.reader() != reader || io.writer() != writer) return null;
        if (!use.compareAndSet(FRESH, CLAIMED)) return null;
        return new QuicTransport(host, this, TransportInfo.of(io), io.capabilities().laneOpener());
    }

    private final class NativeReader implements PipeReader, NativePipe {
        @Override public Transport claim(PipeIo io) { return StreamCarrier.this.claim(io); }
        @Override public void start(PipeReader.Sink target) {
            if (!use.compareAndSet(FRESH, HALVES) && use.get() != HALVES) throw new IllegalStateException("carrier taken over");
            startHalves(target);
        }
        @Override public void close() { closeHalvesInput(); }
    }

    private final class NativeWriter implements PipeWriter, NativePipe {
        @Override public Transport claim(PipeIo io) { return StreamCarrier.this.claim(io); }
        @Override public void write(OutboundBatch batch, Completion done) {
            if (use.get() == CLAIMED) { done.failed(new IllegalStateException("carrier taken over")); return; }
            use.compareAndSet(FRESH, HALVES);
            writeHalves(batch, done);
        }
        @Override public java.util.OptionalLong retransmitted() { return StreamCarrier.this.retransmitted(); }
        @Override public void close() { closeOutput(false); }
        @Override public void abort(Throwable cause) { closeOutput(true); }
    }

    /** The lost bytes of the stream's connection, as last collected; empty before the first collection. */
    java.util.OptionalLong retransmitted() {
        ConnectionCarrier c = connection;
        long n = c == null ? -1 : c.lostBytes();
        return n < 0 ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(n);
    }

    /**
     * The stream as a pipe input whose halves serve a session on another host, after a
     * transport took it over and moved (its unread input is in {@link #early}).
     */
    PipeIo adapter(TransportInfo info, PipeIo.LaneOpener lanes) {
        PipeReader r = !receive ? null : new PipeReader() {
            @Override public void start(PipeReader.Sink target) { startHalves(target); }
            @Override public void close() { closeHalvesInput(); }
        };
        PipeWriter w = !send ? null : new PipeWriter() {
            @Override public void write(OutboundBatch batch, Completion done) { writeHalves(batch, done); }
            @Override public java.util.OptionalLong retransmitted() { return StreamCarrier.this.retransmitted(); }
            @Override public void close() { closeOutput(false); }
            @Override public void abort(Throwable cause) { closeOutput(true); }
        };
        PipeIo io = PipeIo.of(r, w).withTerminatedTls(true).withCarrierConn(info.connection()).withReceiveMemory(info.receiveMemory())
                .withProtocol(info.applicationProtocol()).withFateGroup(info.fateGroup())
                .withCarrier(CarrierKind.QUIC);
        io = io.withExporter(connection::export).withCarrierSecure();
        return lanes == null ? io : io.withLaneOpener(lanes);
    }

    // ---- lent use: the halves ----

    private void startHalves(PipeReader.Sink target) {
        if (!halvesStarted.compareAndSet(false, true)) throw new IllegalStateException("carrier reader already started");
        dispatch(() -> {
            //noinspection resource: borrowed: the channel owns its event loop
            lent = new io.zmux.internal.spi.LentInput(1, stream.eventLoop(), stream.eventLoop()::inEventLoop,
                    more -> { if (more && !inputClosed && !end) stream.read(); });
            lent.start(target);
            for (ByteBuf b; (b = early.poll()) != null; ) lend(b);
            // The input's demand reads on: once at its start, and after each delivery while within budget.
            if (end) { if (inputFailure == null) lent.end(); else lent.fail(inputFailure); }
        }, () -> target.onError(new java.nio.channels.ClosedChannelException()));
    }

    private void closeHalvesInput() {
        dispatch(this::closeInput);
    }

    private void writeHalves(OutboundBatch batch, PipeWriter.Completion done) {
        dispatch(() -> write(batch, cause -> { if (cause == null) done.completed(); else done.failed(cause); }),
                () -> done.failed(new java.nio.channels.ClosedChannelException()));
    }

    /**
     * Lends one read to the halves' reader as chunks, part by part (nioBuffer() of a composite
     * would merge it into a copy): each part holds a reference of its own.
     */
    private void lend(ByteBuf data) {
        int n = data.readableBytes();
        if (n == 0) { data.release(); return; }
        if (data.nioBufferCount() == 1) { lent.offer(io.zmux.Chunk.of(data.nioBuffer(), data::release), n); return; }
        for (ByteBuffer part : data.nioBuffers()) {
            data.retain();
            lent.offer(io.zmux.Chunk.of(part, data::release), part.remaining());
        }
        data.release();
    }

    // ---- native use: the transport ----

    /** The transport starts reading: what is waiting goes to it first, then the end if it came. */
    void attach(QuicTransport t) {
        transport = t;
        t.adopt(early);
        if (end) t.ended(inputFailure == null);
    }

    /** A transport that moved on leaves its unread input for the next owner, oldest first. */
    void leave(QuicTransport t, ByteBuf unread) {
        if (transport == t) transport = null;
        if (unread != null && unread.isReadable() && !gone) early.add(unread);
        else if (unread != null) unread.release();
    }

    /**
     * Asks for the next read at the end of the loop's iteration: what the iteration's
     * datagrams brought for this stream then arrives as one read, not one per packet, so
     * that more records are parsed in place.
     */
    void read() {
        if (readQueued || inputClosed || end) return;
        readQueued = true;
        Loops.run(NettyHost.afterIteration(stream.eventLoop()), readTask, () -> readQueued = false);
    }

    private boolean readQueued;
    private final Runnable readTask = this::readNow;

    private void readNow() {
        readQueued = false;
        if (!inputClosed && !end) stream.read();
    }

    /** Writes batch; done gets the outcome (null on success) on the loop. */
    void write(OutboundBatch batch, java.util.function.Consumer<Throwable> done) {
        if (outputClosed || !stream.isActive()) { done.accept(new IOException("QUIC stream closed")); return; }
        ByteBuf data = Unpooled.wrappedBuffer(batch.segments(new ByteBuffer[batch.segmentCount()]));
        stream.writeAndFlush(data).addListener(result -> done.accept(result.isSuccess() ? null : result.cause()));
    }

    void closeInput() {
        if (inputClosed) return;
        inputClosed = true;
        for (ByteBuf b; (b = early.poll()) != null; ) b.release();
        if (lent != null) lent.close();
        stream.shutdownInput(PipeIo.CARRIER_FAILED).addListener(done -> { inputDone = true; closeIfReleased(); });
    }

    void closeOutput(boolean abort) {
        dispatch(() -> {
            if (outputClosed) return;
            outputClosed = true;
            var shut = abort ? stream.shutdownOutput(PipeIo.CARRIER_FAILED) : stream.shutdownOutput();
            shut.addListener(done -> { outputDone = true; closeIfReleased(); });
        });
    }

    /** Closes the stream if a graceful close left it open (a peer that never finishes its side). */
    void abortOpen() {
        dispatch(() -> { if (stream.isOpen()) stream.close(); });
    }

    /** Whether a session took the stream (Transport#whenTaken), across the transports that move it. */
    final io.zmux.internal.loop.TakenSignal taken = new io.zmux.internal.loop.TakenSignal();

    java.util.concurrent.CompletionStage<Void> whenClosed() {
        var f = new java.util.concurrent.CompletableFuture<Void>();
        stream.closeFuture().addListener(done -> f.complete(null));
        return f;
    }

    /** STOP/RESET need not produce peer FIN; once both owned halves finish, release the local channel explicitly. */
    private void closeIfReleased() {
        if ((!receive || (inputClosed && inputDone)) && (!send || (outputClosed && outputDone))) stream.close();
    }

    private void dispatch(Runnable action) { dispatch(action, () -> {}); }
    private void dispatch(Runnable action, Runnable rejected) {
        //noinspection resource: borrowed: the channel owns its event loop
        Loops.dispatch(stream.eventLoop(), stream.eventLoop().inEventLoop(), action, rejected);
    }

    // ---- the channel ----

    @Override public void channelRead(ChannelHandlerContext context, Object message) {
        boolean fin = message instanceof io.netty.handler.codec.quic.QuicStreamFrame frame && frame.hasFin();
        ByteBuf data;
        if (message instanceof io.netty.handler.codec.quic.QuicStreamFrame frame) data = frame.content();
        else if (message instanceof ByteBuf bytes) data = bytes;
        else { io.netty.util.ReferenceCountUtil.release(message); return; }
        if (inputClosed || end) data.release();
        else if (transport != null) transport.received(data);
        else if (lent != null) lend(data);
        else if (data.isReadable()) early.add(data);
        else data.release();
        if (fin) ended(null);
    }

    @Override public void channelReadComplete(ChannelHandlerContext context) {
        if (transport != null) {
            transport.readMore();
            host.endRoundNow();
        }
        context.fireChannelReadComplete();
    }

    private void ended(Throwable failure) {
        if (end) return;
        end = true;
        inputFailure = failure;
        if (transport != null) transport.ended(failure == null);
        else if (lent != null) { if (failure == null) lent.end(); else lent.fail(failure); }
    }

    @Override public void userEventTriggered(ChannelHandlerContext context, Object event) {
        if (event == ChannelInputShutdownReadComplete.INSTANCE) ended(null);
        context.fireUserEventTriggered(event);
    }

    @Override public void exceptionCaught(ChannelHandlerContext context, Throwable error) {
        ended(error);
        context.close();
    }

    @Override public void channelInactive(ChannelHandlerContext context) {
        gone();
        context.fireChannelInactive();
    }

    private boolean gone;

    /** The stream closed: its input ends, and what no owner took is released. Once, on the loop. */
    private void gone() {
        if (gone) return;
        gone = true;
        // A loop that is shutting down runs no more of zmux's calls: what the reader has not taken goes.
        //noinspection resource: borrowed: the channel owns its event loop
        if (lent != null && stream.eventLoop().isShuttingDown()) lent.fail(new java.nio.channels.ClosedChannelException());
        if (receive && !inputClosed) ended(new IOException("QUIC stream ended without FIN"));
        for (ByteBuf b; (b = early.poll()) != null; ) b.release();
        if (transport != null) transport.inactive();
    }
}
