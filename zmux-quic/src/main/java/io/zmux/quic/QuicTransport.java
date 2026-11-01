package io.zmux.quic;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.internal.loop.AbstractTransport;
import io.zmux.internal.loop.SessionHost;
import io.zmux.internal.loop.Transport;
import io.zmux.internal.loop.TransportInfo;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;

/**
 * A QUIC stream as a session's transport on the connection's own loop: reads are parsed
 * where Netty put them (a session keeps references to the read buffers instead of copying
 * stream data; only a record cut at a read's end is copied), and a batch is written as a
 * wrapped buffer of its segments. A session on another host gets the stream through its
 * pipe-input halves instead ({@link #moveTo}).
 */
final class QuicTransport extends AbstractTransport {
    private final StreamCarrier carrier;
    private final TransportInfo info;
    /** The properties as the pipe input declared them, without the channel. */
    private final TransportInfo declared;
    /** The pipe's datagram channel; null for a lane. */
    private final io.zmux.internal.loop.CarrierChannel channel;
    private final PipeIo.LaneOpener lanes;
    /** Reads that arrived while input was paused, oldest first. */
    private final ArrayDeque<ByteBuf> held = new ArrayDeque<>();
    private boolean paused;
    private boolean inEnded;
    /** The input ended while reads were held: reported once they are delivered. */
    private int endPending;
    private boolean shutAfterWrite;
    private boolean closed;
    /** Inside {@link #drain}: a move hands over what is left once the delivery returns. */
    private boolean delivering;

    QuicTransport(NettyHost host, StreamCarrier carrier, TransportInfo info, PipeIo.LaneOpener lanes) {
        super(host);
        this.carrier = carrier;
        this.channel = carrier.channel();
        this.declared = info;
        this.info = channel == null ? info : info.withChannel(channel);
        this.lanes = lanes;
    }

    @Override public TransportInfo info() { return info; }
    @Override public io.zmux.internal.loop.CarrierChannel channel() { return channel; }
    @Override public boolean closed() { return closed; }
    @Override public boolean ownsHalf(Object half) { return carrier.owns(half); }
    @Override public java.util.concurrent.CompletionStage<Void> whenClosed() { return carrier.whenClosed(); }
    @Override public java.util.concurrent.CompletionStage<Boolean> whenTaken() { return carrier.taken.stage(); }
    @Override public void taken() { carrier.taken.decide(true); }
    @Override public java.util.concurrent.CompletionStage<PipeIo> openLane() { return lanes == null ? null : lanes.open(); }

    @Override
    public int directions() {
        return ((carrier.receives() ? IN : 0) | (carrier.sends() ? OUT : 0)) & mask();
    }

    @Override public long retransmitted() {
        var n = carrier.retransmitted();
        return n.isPresent() ? n.getAsLong() : -1;
    }

    @Override public boolean writable() { return super.writable() && carrier.sends() && !closed; }

    @Override
    protected void begin() {
        openReader();
        carrier.attach(this);
        drain();
    }

    @Override protected boolean accepting() { return started() && !closed && !inEnded && !paused; }

    @Override
    protected void inputFailed(IOException cause) {
        endInput(false);
        close(cause);
    }

    // ---- input, on the loop ----

    /** Takes the reads waiting for an owner, oldest first. */
    void adopt(ArrayDeque<ByteBuf> early) {
        held.addAll(early);
        early.clear();
    }

    /** One read; this transport owns it from here. */
    void received(ByteBuf data) {
        if (closed || inEnded || !carrier.receives()) { data.release(); return; }
        held.add(data);
        if (!delivering) drain();
    }

    /** Delivers the held reads while input is accepted; then reports a pending end or asks for more. */
    private void drain() {
        delivering = true;
        try {
            while (accepting()) {
                ByteBuf b = held.poll();
                if (b == null) break;
                deliver(b);
            }
        } finally {
            delivering = false;
        }
        if (closed || paused || inEnded || !held.isEmpty()) return;
        if (endPending != 0) endInput(endPending == 1);
        else carrier.read();
    }

    /** Parses data in place; what a pause leaves is held, what a move leaves goes to the next owner. */
    private void deliver(ByteBuf data) {
        int start = data.readerIndex(), taken = 0;
        ByteBuffer[] parts = data.nioBufferCount() == 1 ? new ByteBuffer[] {data.nioBuffer()} : data.nioBuffers();
        for (ByteBuffer part : parts) {
            int n = part.remaining();
            data.retain(); // The part's own reference; the session may keep it past this call.
            input(part, data::release);
            taken += n - part.remaining();
            if (part.hasRemaining()) break;
        }
        if (taken == data.readableBytes()) { data.release(); return; }
        data.readerIndex(start + taken);
        if (!closed) { held.addFirst(data); return; }
        carrier.leave(this, data);
        for (ByteBuf b; (b = held.poll()) != null; ) carrier.leave(this, b);
    }

    void readMore() {
        if (!paused && !inEnded && !closed && held.isEmpty()) carrier.read();
    }

    void ended(boolean graceful) {
        if (closed || inEnded) return;
        if (paused || delivering || !held.isEmpty()) {
            endPending = graceful ? 1 : 2;
            return;
        }
        endInput(graceful);
    }

    private void endInput(boolean graceful) {
        if (inEnded) return;
        inEnded = true;
        for (ByteBuf b; (b = held.poll()) != null; ) b.release();
        inputEnded(graceful);
    }

    /** The stream's channel went inactive (Netty fails a write in flight with it). */
    void inactive() {
        if (!inEnded && !closed && carrier.receives()) ended(false);
    }

    @Override public void pauseInput() { paused = true; }

    @Override
    public void resumeInput() {
        if (!paused) return;
        paused = false;
        if (!started() || closed || inEnded) return;
        if (!redeliverBuffered() || paused) return;
        drain();
    }

    @Override
    public void redeliver() {
        if (started() && !closed && !inEnded) redeliverBuffered();
    }

    // ---- output ----

    @Override
    protected void send(OutboundBatch b) {
        if (closed) { writeDone(new IOException("carrier closed")); return; }
        // The batch stays referenced until Netty reports the write: only then is it released.
        carrier.write(b, cause -> {
            if (closed) { dropWrite(); return; }
            writeDone(cause);
            if (shutAfterWrite) shutdownOutput();
        });
    }

    @Override
    public void shutdownOutput() {
        if (outputShut || closed || !carrier.sends()) return;
        if (writing()) { shutAfterWrite = true; return; }
        outputShut = true;
        carrier.closeOutput(false);
    }

    @Override
    public void shutdownInput() {
        if (inEnded && closed || !carrier.receives()) return;
        inEnded = true;
        for (ByteBuf b; (b = held.poll()) != null; ) b.release();
        takeUnread();
        carrier.closeInput();
    }

    @Override
    public void close(Throwable cause) {
        if (closed) return;
        closed = true;
        carrier.taken.decide(false); // Not taken before: the listener refused it.
        if (carrier.receives()) {
            inEnded = true;
            for (ByteBuf b; (b = held.poll()) != null; ) b.release();
            takeUnread();
            carrier.closeInput();
        }
        if (carrier.sends()) carrier.closeOutput(cause != null);
        // A write in flight is released when Netty reports it.
    }

    // ---- moving ----

    /**
     * Leaves this transport's owner: a not yet started transport (from any thread) or one
     * bound to a gate (on its loop). On this loop the stream goes on as a new transport;
     * on another host as a pipe input whose halves an external transport there uses. The
     * unread input goes first to whichever takes the stream.
     */
    @Override
    public Transport moveTo(SessionHost to) {
        if (closed || writing()) { close(null); return null; }
        closed = true;
        if (started()) {
            byte[] unread = takeUnread();
            carrier.leave(this, unread == null ? null : Unpooled.wrappedBuffer(unread));
            if (!delivering) for (ByteBuf b; (b = held.poll()) != null; ) carrier.leave(this, b);
        }
        if (to == loop()) return new QuicTransport((NettyHost) loop(), carrier, declared, lanes);
        Transport next = Transport.of(to, carrier.adapter(declared, lanes));
        carrier.taken.follow(next);
        return next;
    }
}
