package io.zmux.quic;

import static io.zmux.quic.internal.NettyLoops.getBorrowedEventLoop;

import io.zmux.internal.spi.Loops;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.ChannelInputShutdownReadComplete;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.zmux.CarrierKind;
import io.zmux.Admission.OuterSecurity;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import io.zmux.carrier.CarrierChannel;
import io.zmux.internal.loop.NativePipe;
import io.zmux.internal.loop.Transport;
import io.zmux.internal.loop.TransportInfo;
import io.zmux.internal.spi.CarrierProvider;
import io.zmux.quic.internal.QuicCarrierStats;
import io.zmux.quic.internal.QuicHandshake;
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
    private final OuterSecurity outerSecurity;
    /** Who uses the stream: nobody yet, its pipe input's halves, or a transport. */
    private final AtomicInteger use = new AtomicInteger(FRESH);
    /** The transport reading the stream (native use), or null. */
    private QuicTransport transport;
    /** The input lent to the halves' reader (lent use), or null; one read at a time, as the reader consumes it. */
    private io.zmux.internal.spi.LentInput lent;
    private static final io.netty.util.AttributeKey<io.zmux.internal.spi.ProviderBackings> EXTERNAL_BACKINGS =
            io.netty.util.AttributeKey.valueOf(StreamCarrier.class, "externalBackings");
    private final io.zmux.internal.spi.ProviderBackings backings;
    /** Input no owner has taken yet: what arrives before an owner starts and what a moving transport left. */
    private final ArrayDeque<ByteBuf> early = new ArrayDeque<>();
    private boolean end;
    private Throwable inputFailure;
    private boolean reliableReset;
    private boolean inputClosed;
    private boolean outputClosed;
    private final java.util.concurrent.CompletableFuture<Void> inputReleased = new java.util.concurrent.CompletableFuture<>();
    private final java.util.concurrent.CompletableFuture<Void> outputReleased = new java.util.concurrent.CompletableFuture<>();
    private boolean inputDone, outputDone;
    private final AtomicBoolean halvesStarted = new AtomicBoolean();
    private PipeReader reader;
    private PipeWriter writer;
    /** The connection of the stream, known once its pipe input is made. */
    private volatile ConnectionCarrier connection;
    private PipeChannel channel;
    private final java.util.concurrent.atomic.AtomicBoolean attached = new java.util.concurrent.atomic.AtomicBoolean();

    StreamCarrier(QuicStreamChannel stream, NettyHost host, boolean receive, boolean send) {
        this(stream, host, receive, send, QuicHandshake.security(stream.parent()));
    }

    StreamCarrier(QuicStreamChannel stream, NettyHost host, boolean receive, boolean send, OuterSecurity outerSecurity) {
        this.stream = stream; this.host = host; this.receive = receive; this.send = send;
        this.outerSecurity = outerSecurity;
        if (host != null) backings = new io.zmux.internal.spi.ProviderBackings(host::retainedInput);
        else {
            // External streams run on the connection loop and deliver to the receiving pipe's inbox.
            var shared = stream.parent().attr(EXTERNAL_BACKINGS);
            if (shared.get() == null) shared.set(new io.zmux.internal.spi.ProviderBackings());
            backings = shared.get();
        }
        stream.config().setAutoRead(false).setReadFrames(true)
                .setRecvByteBufAllocator(new io.netty.channel.AdaptiveRecvByteBufAllocator(4 << 10, 64 << 10, 256 << 10));
        if (stream.type() == io.netty.handler.codec.quic.QuicStreamType.BIDIRECTIONAL) stream.config().setAllowHalfClosure(true);
        // Netty completes the close future on the loop as the stream closes, before any later
        // task; channelInactive is such a task, which a terminating loop may never run.
        //noinspection resource: borrowed: the stream owns its event loop
        var termination = getBorrowedEventLoop(stream).terminationFuture();
        io.netty.util.concurrent.GenericFutureListener<io.netty.util.concurrent.Future<Object>> terminated = f -> gone();
        termination.addListener(terminated);
        stream.closeFuture().addListener(f -> {
            gone();
            termination.removeListener(terminated);
        });
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
            taken.stage().thenAccept(t -> { if (t) markAttached(); });
        }
        var pipe = PipeIo.of(reader, writer).withCarrierConn(connection.channel())
                .withReceiveMemory(connection.options().connectionWindow())
                .withProtocol(connection.applicationProtocol())
                .withTerminatedTls(outerSecurity == OuterSecurity.QUIC).withOuterSecurity(outerSecurity).withExporter(connection::export)
                .withLaneOpener(send && receive ? connection::openLane : null).withCarrier(CarrierKind.QUIC);
        return outerSecurity == OuterSecurity.QUIC ? pipe.withCarrierSecure() : pipe;
    }

    boolean attached() { return attached.get(); }

    /** Publish connection membership on the native owner loop before notifying external callers. */
    void markAttached() {
        var parent = connection;
        if (parent != null && attached.compareAndSet(false, true)) dispatch(() -> parent.pipeStarted(stream));
    }

    /**
     * The datagram channel of the pipe this bidirectional stream is: the connection's QUIC datagrams, named by the stream's ID; null for a lane.
     */
    synchronized io.zmux.carrier.CarrierChannel channel() {
        if (channel == null && connection != null && !unidirectional()) channel = new PipeChannel(connection, stream);
        return channel;
    }

    /** The channel of one pipe on the connection's shared QUIC datagrams. */
    private record PipeChannel(ConnectionCarrier connection, QuicStreamChannel stream) implements io.zmux.carrier.CarrierChannel {
        @Override public long channelStreamId() { return stream.streamId(); }
        @Override public int maxChannelDatagram() { return stream.isActive() ? connection.datagramRoom() : 0; }
        @Override public int channelDatagramLimit(int room) {
            return room > 0 ? room : stream.isActive() ? connection.maxDatagram() : 0;
        }
        @Override public void send(byte[] datagram, long expiry, java.util.function.Consumer<Outcome> completed) {
            if (stream.isActive()) connection.sendDatagram(datagram, expiry, completed);
            else completed.accept(Outcome.REJECTED);
        }
        @Override public Runnable watch(Runnable changed) { return connection.watchDatagrams(changed); }
        @Override public Runnable receive(CarrierChannel.Sink sink) { return connection.receiveDatagrams(stream.streamId(), sink); }
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

    private interface Half extends io.zmux.internal.spi.AdapterPorts.ReleasingHalf, CarrierProvider {
        QuicChannel connection();

        @Override default Object carrierIdentity() { return connection(); }
        @Override default CarrierProvider.Query carrierQuery(Object identity) {
            return QuicCarrierStats.query(connection(), identity);
        }
    }

    private interface ReaderHalf extends PipeReader, Half {}
    private interface WriterHalf extends PipeWriter, Half {}

    private final class NativeReader implements ReaderHalf, NativePipe {
        @Override public QuicChannel connection() { return connectionChannel(); }
        @Override public java.util.concurrent.CompletionStage<Void> released() { return inputReleased.minimalCompletionStage(); }
        @Override public Transport claim(PipeIo io) { return StreamCarrier.this.claim(io); }
        @Override public void start(PipeReader.Sink target) {
            if (!use.compareAndSet(FRESH, HALVES) && use.get() != HALVES) throw new IllegalStateException("carrier taken over");
            startHalves(target);
        }
        @Override public void close() { closeHalvesInput(); }
    }

    private final class NativeWriter implements WriterHalf, NativePipe {
        @Override public QuicChannel connection() { return connectionChannel(); }
        @Override public java.util.concurrent.CompletionStage<Void> released() { return outputReleased.minimalCompletionStage(); }
        @Override public Transport claim(PipeIo io) { return StreamCarrier.this.claim(io); }
        @Override public void write(OutboundBatch batch, Completion done) {
            if (use.get() == CLAIMED) { done.failed(new IllegalStateException("carrier taken over")); return; }
            use.compareAndSet(FRESH, HALVES);
            writeHalves(batch, done);
        }
        @Override public void close() { closeOutput(false); }
        @Override public void abort(Throwable cause) { closeOutput(true); }
    }

    QuicChannel connectionChannel() { return stream.parent(); }

    /**
     * The stream as a pipe input whose halves serve a session on another host, after a
     * transport took it over and moved (its unread input is in {@link #early}).
     */
    PipeIo adapter(TransportInfo info, PipeIo.LaneOpener lanes) {
        PipeIo io = halves().withTerminatedTls(outerSecurity == OuterSecurity.QUIC).withOuterSecurity(outerSecurity).withCarrierConn(info.connection()).withReceiveMemory(info.receiveMemory())
                .withProtocol(info.applicationProtocol()).withFateGroup(info.fateGroup())
                .withCarrier(CarrierKind.QUIC);
        io = io.withExporter(this::export);
        if (outerSecurity == OuterSecurity.QUIC) io = io.withCarrierSecure();
        return lanes == null ? io : io.withLaneOpener(lanes);
    }

    /** The stream through the external carrier contract, including after a host parsed a preface. */
    PipeIo halves() {
        PipeReader r = !receive ? null : new ReaderHalf() {
            @Override public QuicChannel connection() { return connectionChannel(); }
            @Override public java.util.concurrent.CompletionStage<Void> released() { return inputReleased.minimalCompletionStage(); }
            @Override public void start(PipeReader.Sink target) { startHalves(target); }
            @Override public void close() { closeHalvesInput(); }
        };
        PipeWriter w = !send ? null : new WriterHalf() {
            @Override public QuicChannel connection() { return connectionChannel(); }
            @Override public java.util.concurrent.CompletionStage<Void> released() { return outputReleased.minimalCompletionStage(); }
            @Override public void write(OutboundBatch batch, Completion done) { writeHalves(batch, done); }
            @Override public void close() { closeOutput(false); }
            @Override public void abort(Throwable cause) { closeOutput(true); }
        };
        return PipeIo.of(r, w);
    }

    // ---- lent use: the halves ----

    private void startHalves(PipeReader.Sink target) {
        if (!halvesStarted.compareAndSet(false, true)) throw new IllegalStateException("carrier reader already started");
        dispatch(() -> {
            //noinspection resource: borrowed: the channel owns its event loop
            lent = new io.zmux.internal.spi.LentInput(1, getBorrowedEventLoop(stream), getBorrowedEventLoop(stream)::inEventLoop,
                    more -> { if (more && !inputClosed && !end) stream.read(); });
            lent.start(target);
            for (ByteBuf b; (b = early.poll()) != null; ) lend(b);
            // The input's demand reads on: once at its start, and after each delivery while within budget.
            if (end) { if (inputFailure == null || reliableReset) lent.endAfterInput(inputFailure); else lent.fail(inputFailure); }
        }, () -> target.onError(new java.nio.channels.ClosedChannelException()));
    }

    private void closeHalvesInput() {
        dispatch(this::closeInput, () -> released.thenRun(() -> inputReleased.complete(null)));
    }

    private void writeHalves(OutboundBatch batch, PipeWriter.Completion done) {
        dispatch(() -> write(batch, cause -> {
            if (cause == null) done.completed();
            else if (cause instanceof io.zmux.quic.internal.QuicStreamTermination.PartialWrite partial) {
                done.failed(partial.getCause(), partial.confirmed());
            } else done.failed(cause);
        }),
                () -> done.failed(new java.nio.channels.ClosedChannelException()));
    }

    /** Lends one read with its physical backing to the halves' reader. */
    private void lend(ByteBuf data) {
        int n = data.readableBytes();
        if (n == 0) { data.release(); return; }
        io.zmux.Chunk chunk;
        try { chunk = ownedChunk(data); }
        finally { data.release(); }
        lent.offer(chunk, n);
    }

    io.zmux.Chunk ownedChunk(ByteBuf data) {
        // Keep the Netty dependency in this adapter, independent of the HTTP module.
        ByteBuf root = data;
        for (ByteBuf parent; (parent = root.unwrap()) != null;) root = parent;
        int capacity = root.writerIndex() + root.maxFastWritableBytes();
        if (data.nioBufferCount() == 1 && !(root instanceof io.netty.buffer.CompositeByteBuf) && capacity < root.maxCapacity()
                && (root.alloc() instanceof io.netty.buffer.PooledByteBufAllocator
                    || root.alloc() instanceof io.netty.buffer.AdaptiveByteBufAllocator)) {
            // The pooled allocators report their allocation size here unless clipped by maxCapacity.
            return backings.chunk(root, capacity, data.nioBuffer(), data::retain, data::release);
        }
        ByteBuffer copy = ByteBuffer.allocate(data.readableBytes());
        data.getBytes(data.readerIndex(), copy);
        return io.zmux.Chunk.of(copy.flip());
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
        Loops.run(NettyHost.afterIteration(getBorrowedEventLoop(stream)), readTask, () -> readQueued = false);
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
        io.zmux.quic.internal.QuicStreamTermination.stop(stream, PipeIo.CARRIER_FAILED).addListener(done -> { inputDone = true; finishHalf(inputReleased); });
    }

    void closeOutput(boolean abort) {
        dispatch(() -> {
            if (outputClosed) return;
            outputClosed = true;
            if (abort) {
                io.zmux.quic.internal.QuicStreamTermination.reset(stream, PipeIo.CARRIER_FAILED).addListener(done -> { outputDone = true; finishHalf(outputReleased); });
                return;
            }
            QuicStreamPipe.finish(stream, done -> { outputDone = true; finishHalf(outputReleased); },
                    () -> released.thenRun(() -> outputReleased.complete(null)));
        }, () -> released.thenRun(() -> outputReleased.complete(null)));
    }

    /** Closes the stream if a graceful close left it open (a peer that never finishes its side). */
    void abortOpen() {
        dispatch(() -> { if (stream.isOpen()) stream.close(); });
    }

    /** Whether a session took the stream (Transport#whenTaken), across the transports that move it. */
    final io.zmux.internal.loop.TakenSignal taken = new io.zmux.internal.loop.TakenSignal();

    private final java.util.concurrent.CompletableFuture<Void> released = new java.util.concurrent.CompletableFuture<>();

    java.util.concurrent.CompletionStage<Void> whenClosed() { return released.minimalCompletionStage(); }

    /** STOP/RESET need not produce peer FIN; once both owned halves finish, release the local channel explicitly. */
    private void finishHalf(java.util.concurrent.CompletableFuture<Void> half) {
        if ((!receive || inputDone) && (!send || outputDone)) {
            released.thenRun(() -> half.complete(null));
            stream.close();
        } else {
            half.complete(null);
        }
    }

    private void dispatch(Runnable action) { dispatch(action, () -> {}); }
    private void dispatch(Runnable action, Runnable rejected) {
        //noinspection resource: borrowed: the channel owns its event loop
        Loops.dispatch(getBorrowedEventLoop(stream), getBorrowedEventLoop(stream).inEventLoop(), action, rejected);
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
        if (event instanceof io.zmux.quic.internal.QuicStreamTermination.InputReset(var cause)) {
            end = true;
            inputFailure = cause;
            reliableReset = true;
            if (lent != null) lent.endAfterInput(inputFailure);
            return;
        }
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
        if (lent != null && getBorrowedEventLoop(stream).isShuttingDown()) lent.fail(new java.nio.channels.ClosedChannelException());
        if (receive && !inputClosed) ended(new IOException("QUIC stream ended without FIN"));
        if (!reliableReset || inputClosed || getBorrowedEventLoop(stream).isShuttingDown()) {
            for (ByteBuf b; (b = early.poll()) != null; ) b.release();
        }
        if (transport != null) transport.inactive();
        released.complete(null);
    }
}
