package io.zmux.internal.loop;

import io.zmux.carrier.OutboundBatch;
import io.zmux.internal.core.CarrierInfo;
import io.zmux.internal.core.Framing;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/** A TCP (or Unix-domain) transport on the loop's selector: non-blocking, writability-driven writes. */
final class SocketTransport extends NioTransport {
    private static final TransportInfo INFO = new TransportInfo(
            new CarrierInfo(Framing.BYTES, false, false, false, false).withKind(io.zmux.CarrierKind.RAW), "", null, 0, 0);

    /** The write in flight: segments [outFrom, outEnd) of out still have bytes; none when outEnd is 0. */
    private ByteBuffer[] out;
    private int outFrom, outEnd;
    private boolean shutAfterWrite;
    private boolean bulkRead;
    private final java.util.concurrent.CompletableFuture<Void> released = new java.util.concurrent.CompletableFuture<>();
    @Override public java.util.concurrent.CompletionStage<Void> whenClosed() { return released.minimalCompletionStage(); }


    SocketTransport(EventLoop loop, SocketChannel ch) {
        super(loop, ch);
    }

    @Override
    public boolean resumesAfterPeekTimeout() { return true; }

    @Override public Object carrierIdentity() { return ch; }

    @Override
    public TransportInfo info() {
        return INFO;
    }

    @Override
    protected boolean accepting() {
        return !inEnded && !closed && !paused;
    }

    @Override
    protected void begin() {
        openReader();
        try {
            if (key != null) {
                key.interestOps(inEnded || paused ? 0 : SelectionKey.OP_READ);
            } else {
                ch.configureBlocking(false);
                key = nio.register(ch, !inEnded && !paused, this);
            }
        } catch (IOException | java.nio.channels.CancelledKeyException e) {
            failed(e);
            return;
        }
        redeliverBuffered();
    }

    boolean canDetach() { return !closed && !writing(); }

    /**
     * On the loop, nothing written: gives the channel up, off the selector and in blocking mode, without closing it;
     * the transport is closed from now on. Null when it cannot be (closed, writing, the mode refused), the channel then closed.
     */
    java.nio.channels.SocketChannel detachPhysical(CarrierSetup.Bound.Permit permit) {
        java.util.Objects.requireNonNull(permit);
        closed = true;
        if (reader != null) reader.release();
        try {
            if (key != null) nio.deregister(key);
            ch.configureBlocking(true);
            return ch;
        } catch (IOException | RuntimeException e) {
            try { ch.close(); } catch (IOException ignored) { /* Gone. */ }
            return null;
        } finally { released.complete(null); }
    }

    @Override
    Transport movePhysical(CarrierSetup.Bound.Permit permit, SessionHost host) {
        java.util.Objects.requireNonNull(permit);
        if (closed) return null;
        if (!(host instanceof EventLoop to)) return bridge(host);
        closed = true;
        SocketTransport n = new SocketTransport(to, ch);
        n.key = handOver(key, nio, to, n);
        moveInputTo(n);
        n.bulkRead = bulkRead;
        released.complete(null); // This view returned its selector and input to the new owner.
        return n;
    }

    @Override
    public void ready(SelectionKey k) {
        if (k.isValid() && k.isWritable()) flushWrite();
        if (k.isValid() && k.isReadable()) readSome();
    }

    @Override
    public void failed(Throwable t) {
        if (closed) return;
        // A failure of the endpoint, not of the carrier: a session closes with INTERNAL_ERROR.
        if (internalFailure(t)) return;
        discardInputOnFailure();
        if (writing()) { clearOut(); writeDone(confirmed, t); }
        // An unexpected failure leaves the channel in an unknown state: cancel its key so the
        // selector stops reporting it, and close it; later writes fail through write().
        CarrierSetup.connectionLost(this, t);
    }

    /** At most one read per readiness, into at most one pool buffer: a busy socket cannot starve the loop. */
    private void readSome() {
        if (inEnded || closed || paused) return;
        ByteBuffer b = inputRoom(!bulkRead && inbound().segmentedInput(), 1);
        int n;
        try {
            n = ch.read(b);
        } catch (IOException e) {
            endInput(false);
            return;
        }
        if (n < 0) {
            endInput(true);
            return;
        }
        if (n == 0) {
            if (!reader.hasPartial()) {
                reader.release();
                bulkRead = false;
            }
            return;
        }
        bulkRead = n >= BufferPool.IDLE_SIZE;
        reader.lim += n;
        redeliver(reader);
    }

    @Override
    public void redeliver() {
        if (!inEnded && !closed) redeliver(reader);
    }

    @Override
    protected void send(OutboundBatch b) {
        confirmed = 0;
        if (key == null || closed) {
            writeDone(new IOException("carrier closed"));
            return;
        }
        out = b.segments(out);
        outFrom = 0;
        outEnd = b.segmentCount();
        if (outEnd == 0) {
            writeDone(null);
            return;
        }
        flushWrite();
    }

    private int confirmed;

    private void clearOut() {
        if (out != null) java.util.Arrays.fill(out, 0, outEnd, null);
        outFrom = outEnd = 0;
    }

    private void flushWrite() {
        if (outEnd == 0 || closed) return;
        try {
            confirmed += (int) ch.write(out, outFrom, outEnd - outFrom);
        } catch (IOException e) {
            clearOut();
            if (key != null && key.isValid()) key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            writeDone(confirmed, e);
            return;
        }
        while (outFrom < outEnd && !out[outFrom].hasRemaining()) outFrom++;
        if (outFrom < outEnd) {
            writeProgress(confirmed);
            if ((key.interestOps() & SelectionKey.OP_WRITE) == 0) {
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                writableChanged(false);
            }
            return;
        }
        clearOut();
        if ((key.interestOps() & SelectionKey.OP_WRITE) != 0) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            writableChanged(true);
        }
        writeDone(null);
        if (shutAfterWrite) shutdownOutput();
    }

    @Override
    public void shutdownOutput() {
        if (outputShut || closed) return;
        if (writing()) {
            shutAfterWrite = true;
            return;
        }
        outputShut = true;
        try {
            ch.shutdownOutput();
        } catch (IOException ignored) {
            // The peer is gone; the read side reports it.
        }
        outputShutDone();
    }

    @Override
    void closePhysical(CarrierSetup.Bound.Permit permit, Throwable cause) {
        java.util.Objects.requireNonNull(permit);
        if (closed) return;
        closed = true;
        if (key != null) key.cancel();
        if (cause != null) {
            // A socket that already ended can refuse the option; it must still close.
            try { ch.setOption(java.net.StandardSocketOptions.SO_LINGER, 0); } catch (IOException ended) { /* Nothing left to abort. */ }
        }
        try {
            ch.close();
        } catch (IOException ignored) {
            // Closing anyway.
        }
        if (reader != null) reader.release();
        clearOut();
        writeDone(confirmed, cause != null ? cause : new IOException("carrier closed"));
        released.complete(null);
        setup().transportClosed(this);
    }
}
