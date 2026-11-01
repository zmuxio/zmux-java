package io.zmux.internal.loop;

import io.zmux.carrier.OutboundBatch;
import io.zmux.internal.core.CarrierInfo;
import io.zmux.internal.core.Framing;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/** A TCP (or Unix-domain) transport on the loop's selector: non-blocking, writability-driven writes. */
final class SocketTransport extends AbstractTransport implements EventLoop.IoHandler {
    private static final TransportInfo INFO = new TransportInfo(
            new CarrierInfo(Framing.BYTES, false, false, false, false), "", null, 0, 0, 32);

    private final SocketChannel ch;
    private SelectionKey key;
    /** The write in flight: segments [outFrom, outEnd) of out still have bytes; none when outEnd is 0. */
    private ByteBuffer[] out;
    private int outFrom, outEnd;
    private boolean inEnded;
    private boolean paused;
    /** The input ended while paused with input still buffered: 1 gracefully, 2 not; reported on resume. */
    private int endPending;
    private boolean shutAfterWrite;
    private boolean closed;

    /** The NIO loop whose selector serves the channel. */
    private final EventLoop nio;

    SocketTransport(EventLoop loop, SocketChannel ch) {
        super(loop);
        nio = loop;
        this.ch = ch;
    }

    @Override
    public TransportInfo info() {
        return INFO;
    }

    @Override
    public boolean closed() {
        return closed;
    }

    @Override
    protected boolean accepting() {
        return !inEnded && !closed && !paused;
    }

    @Override
    protected void inputFailed(IOException cause) {
        endInput(false);
    }

    @Override
    protected void begin() {
        reader = new ChannelReader(loop.pool);
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
        replay(reader);
    }

    @Override
    public Transport moveTo(SessionHost host) {
        if (closed) return null;
        if (!(host instanceof EventLoop to)) return bridge(host);
        closed = true;
        SocketTransport n = new SocketTransport(to, ch);
        n.key = handOver(key, nio, to, n);
        n.prefix(unread(reader));
        if (reader != null) reader.release();
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
        if (!inEnded) {
            inEnded = true;
            inputEnded(false, false);
        }
        if (writing()) { clearOut(); writeDone(t); }
        // An unexpected failure leaves the channel in an unknown state: cancel its key so the
        // selector stops reporting it, and close it; later writes fail through write().
        close(t);
    }

    /** At most one read per readiness, into at most one pool buffer: a busy socket cannot starve the loop. */
    private void readSome() {
        if (inEnded || closed || paused) return;
        ByteBuffer b = reader.room();
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
            if (!reader.hasPartial()) reader.release();
            return;
        }
        reader.lim += n;
        int c = deliver(reader.buf, reader.buf.data(), reader.pos, reader.lim);
        if (c == Inbound.GONE) return;
        reader.consumed(c);
    }

    @Override
    public void pauseInput() {
        paused = true;
        if (key != null && key.isValid()) key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
    }

    @Override
    public void resumeInput() {
        if (!paused) return;
        paused = false;
        if (inEnded || closed || reader == null) return;
        if (!redeliver(reader) || paused || inEnded) return;
        if (endPending != 0) {
            endInput(endPending == 1);
            return;
        }
        if (key != null && key.isValid()) key.interestOps(key.interestOps() | SelectionKey.OP_READ);
    }

    @Override
    public void redeliver() {
        if (!inEnded && !closed) redeliver(reader);
    }

    private void endInput(boolean graceful) {
        if (paused && reader.hasPartial()) {
            endPending = graceful ? 1 : 2;
            if (key != null && key.isValid()) key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
            return;
        }
        inEnded = true;
        boolean partial = reader.hasPartial();
        reader.release();
        if (key != null && key.isValid()) key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        inputEnded(graceful, partial);
    }

    @Override
    protected void send(OutboundBatch b) {
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

    private void clearOut() {
        if (out != null) java.util.Arrays.fill(out, 0, outEnd, null);
        outFrom = outEnd = 0;
    }

    private void flushWrite() {
        if (outEnd == 0 || closed) return;
        try {
            ch.write(out, outFrom, outEnd - outFrom);
        } catch (IOException e) {
            clearOut();
            if (key != null && key.isValid()) key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            writeDone(e);
            return;
        }
        while (outFrom < outEnd && !out[outFrom].hasRemaining()) outFrom++;
        if (outFrom < outEnd) {
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
    }

    @Override public void shutdownInput() {
        if (inEnded || closed) return;
        inEnded = true;
        if (reader != null) reader.release();
        if (key != null && key.isValid()) key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        try { ch.shutdownInput(); } catch (IOException ignored) { /* The input is already gone. */ }
    }

    @Override
    public void close(Throwable cause) {
        if (closed) return;
        closed = true;
        if (key != null) key.cancel();
        try {
            if (cause != null) ch.setOption(java.net.StandardSocketOptions.SO_LINGER, 0);
            ch.close();
        } catch (IOException ignored) {
            // Closing anyway.
        }
        if (reader != null) reader.release();
        clearOut();
        dropWrite();
    }
}
