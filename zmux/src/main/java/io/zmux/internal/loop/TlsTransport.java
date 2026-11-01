package io.zmux.internal.loop;

import io.zmux.carrier.OutboundBatch;
import io.zmux.internal.core.CarrierInfo;
import io.zmux.internal.core.Framing;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

/**
 * A TLS carrier: an {@link SSLEngine} over a non-blocking socket on the loop. The handshake
 * runs on the loop (delegated tasks on a pool, resumed on the loop); records are wrapped
 * from the batch's segments without flattening (IMPLEMENTATION.md Section 4).
 */
final class TlsTransport extends AbstractTransport implements EventLoop.IoHandler {
    /** Runs the engine's delegated tasks; the common pool is borrowed, never closed. */
    private static final java.util.concurrent.Executor DELEGATED_TASKS = ForkJoinPool.commonPool();

    private static final CarrierInfo INFO = new CarrierInfo(Framing.BYTES, true, false, false, false);
    private static final ByteBuffer[] NONE = new ByteBuffer[0];

    private final SocketChannel ch;
    private final SSLEngine engine;
    /** The NIO loop whose selector serves the channel. */
    private final EventLoop nio;
    private SelectionKey key;
    private ByteBuffer netIn;
    private final ByteBuffer netOut;
    /** The write in flight: segments [srcFrom, srcEnd) of srcs still have bytes. */
    private ByteBuffer[] srcs = NONE;
    private int srcFrom, srcEnd;
    private BiConsumer<TlsTransport, Throwable> onHandshake;
    private boolean handshaking = true;
    private TransportInfo info;
    private boolean inEnded;
    private boolean paused;
    /** The input ended while paused with input still buffered: 1 gracefully, 2 not; reported on resume. */
    private int endPending;
    private boolean shutAfterWrite;
    private boolean closeNotifyPending;
    private boolean closed;

    private TlsTransport(EventLoop loop, SocketChannel ch, SSLEngine engine) {
        super(loop);
        nio = loop;
        this.ch = ch;
        this.engine = engine;
        int packet = engine.getSession().getPacketBufferSize();
        // Direct: socket reads land without the JDK copying through a temporary direct buffer.
        this.netIn = ByteBuffer.allocateDirect(packet * 2);
        this.netOut = ByteBuffer.allocateDirect(packet * 4);
        this.netOut.flip();
        this.reader = new ChannelReader(loop.pool);
    }

    /** Runs the handshake of engine over ch on loop; done receives the carrier or the failure, on the loop. */
    static Runnable handshake(EventLoop loop, SocketChannel ch, SSLEngine engine, long timeout,
                          BiConsumer<TlsTransport, Throwable> done) {
        TlsTransport c = new TlsTransport(loop, ch, engine);
        c.onHandshake = done;
        if (!loop.tryExecute(() -> {
            try {
                loop.onShutdown(c.handshakeShutdown);
                loop.schedule(c.handshakeTimer, io.zmux.internal.core.Time.add(Clock.now(), timeout));
                ch.configureBlocking(false);
                c.key = loop.register(ch, true, c);
                engine.beginHandshake();
                c.step();
            } catch (IOException | RuntimeException e) {
                c.handshakeFailed(e);
            }
        })) {
            c.close(null); done.accept(null, new IOException("event loop closed"));
        }
        return () -> loop.tryExecute(() -> {
            if (c.handshaking && !c.closed) c.handshakeFailed(new java.io.InterruptedIOException("TLS handshake cancelled"));
        });
    }

    private final Runnable handshakeShutdown = () -> {
        if (handshaking && !closed) handshakeFailed(new IOException("event loop closed"));
    };

    @Override
    public TransportInfo info() {
        if (info == null) info = new TransportInfo(INFO, applicationProtocol(), null, 0, 0, 32);
        return info;
    }

    @Override
    public boolean closed() {
        return closed;
    }

    /** The negotiated application protocol, or "" when none. */
    String applicationProtocol() {
        String p = engine.getApplicationProtocol();
        return p == null ? "" : p;
    }

    SSLEngine engine() {
        return engine;
    }

    /**
     * The server name (SNI) of the handshake: on a server, the host name the client requested;
     * on a client, the one it sent. "" when none.
     */
    String serverName() {
        java.util.List<javax.net.ssl.SNIServerName> names;
        if (engine.getUseClientMode()) names = engine.getSSLParameters().getServerNames();
        else if (engine.getSession() instanceof ExtendedSSLSession session) names = session.getRequestedServerNames();
        else names = null;
        if (names != null) for (var name : names) if (name instanceof javax.net.ssl.SNIHostName host) return host.getAsciiName();
        return "";
    }

    @Override
    public byte[] export(String label, byte[] context, int length) {
        try {
            return ((ExtendedSSLSession) engine.getSession()).exportKeyingMaterialData(label, context, length);
        } catch (SSLException | UnsupportedOperationException | ClassCastException e) {
            return null;
        }
    }

    @Override
    protected boolean accepting() {
        return started() && !inEnded && !closed && !paused;
    }

    @Override
    protected void inputFailed(IOException cause) {
        endInput(false);
    }

    @Override
    protected void begin() {
        try {
            if (key != null) key.interestOps(inEnded || paused ? 0 : SelectionKey.OP_READ);
            else key = nio.register(ch, !inEnded && !paused, this);
        } catch (IOException | java.nio.channels.CancelledKeyException e) {
            failed(e);
            return;
        }
        if (!replay(reader)) return;
        processInput();
    }

    @Override
    public Transport moveTo(SessionHost host) {
        if (closed || handshaking) {
            close(null);
            return null;
        }
        if (!(host instanceof EventLoop to)) return bridge(host);
        closed = true;
        TlsTransport n = new TlsTransport(to, ch, engine);
        n.key = handOver(key, nio, to, n);
        n.handshaking = false;
        n.prefix(unread(reader));
        reader.release();
        netIn.flip();
        if (n.netIn.capacity() < netIn.remaining()) n.netIn = ByteBuffer.allocateDirect(netIn.remaining());
        n.netIn.put(netIn);
        n.netOut.compact();
        n.netOut.put(netOut);
        n.netOut.flip();
        return n;
    }

    // ---- handshake ----

    private void step() throws IOException {
        while (handshaking) {
            SSLEngineResult.HandshakeStatus hs = engine.getHandshakeStatus();
            switch (hs) {
                case NEED_WRAP -> {
                    if (!flushNet()) return;
                    wrapInto(NONE, 0, 0);
                }
                case NEED_UNWRAP, NEED_UNWRAP_AGAIN -> {
                    if (!flushNet()) return;
                    if (!unwrapOnce()) return;
                }
                case NEED_TASK -> {
                    Runnable task = engine.getDelegatedTask();
                    DELEGATED_TASKS.execute(() -> {
                        RuntimeException failure = null;
                        try {
                            for (Runnable t = task; t != null; t = engine.getDelegatedTask()) t.run();
                        } catch (RuntimeException e) { failure = e; }
                        RuntimeException taskFailure = failure;
                        loop.tryExecute(() -> {
                            // The handshake may have timed out, been cancelled or failed meanwhile.
                            if (closed || !handshaking) return;
                            if (taskFailure != null) { handshakeFailed(taskFailure); return; }
                            try {
                                step();
                            } catch (IOException | RuntimeException e) {
                                handshakeFailed(e);
                            }
                        });
                    });
                    return;
                }
                default -> {
                    if (!flushNet()) return;
                    handshaking = false;
                    loop.removeShutdownHook(handshakeShutdown);
                    loop.schedule(handshakeTimer, io.zmux.internal.core.Time.NEVER);
                    BiConsumer<TlsTransport, Throwable> d = onHandshake;
                    onHandshake = null;
                    d.accept(this, null);
                    return;
                }
            }
        }
    }

    private final TimerHeap.Timer handshakeTimer = new TimerHeap.Timer() {
        @Override
        void fire(long now) {
            if (handshaking) handshakeFailed(new java.net.SocketTimeoutException("TLS handshake timed out"));
        }
    };

    private void handshakeFailed(Throwable e) {
        handshaking = false;
        loop.removeShutdownHook(handshakeShutdown);
        loop.schedule(handshakeTimer, io.zmux.internal.core.Time.NEVER);
        BiConsumer<TlsTransport, Throwable> d = onHandshake;
        onHandshake = null;
        close(e);
        if (d != null) d.accept(null, e);
    }

    /** One unwrap for the handshake; false when more network bytes are needed. */
    private boolean unwrapOnce() throws IOException {
        netIn.flip();
        ByteBuffer dst = reader.room();
        SSLEngineResult r = engine.unwrap(netIn, dst);
        netIn.compact();
        reader.lim = dst.position();
        switch (r.getStatus()) {
            case BUFFER_UNDERFLOW -> {
                if (!netIn.hasRemaining()) netIn = grow(netIn);
                int n = ch.read(netIn);
                if (n < 0) throw new IOException("connection closed during the TLS handshake");
                return n > 0;
            }
            case CLOSED -> throw new IOException("TLS closed during the handshake");
            default -> {
                return true;
            }
        }
    }

    private static ByteBuffer grow(ByteBuffer b) {
        ByteBuffer n = ByteBuffer.allocateDirect(b.capacity() * 2);
        b.flip();
        n.put(b);
        return n;
    }

    // ---- I/O ----

    @Override
    public void ready(SelectionKey k) {
        try {
            if (handshaking) {
                if (k.isValid() && k.isWritable() && flushNet() && (k.interestOps() & SelectionKey.OP_WRITE) != 0) {
                    k.interestOps(k.interestOps() & ~SelectionKey.OP_WRITE);
                }
                if (k.isValid() && k.isReadable()) {
                    if (ch.read(netIn) < 0) throw new IOException("connection closed during the TLS handshake");
                }
                step();
                return;
            }
            if (k.isValid() && k.isWritable()) continueWrite();
            if (k.isValid() && k.isReadable()) readSome();
        } catch (IOException | RuntimeException e) {
            if (handshaking) handshakeFailed(e);
            else failed(e);
        }
    }

    @Override
    public void failed(Throwable t) {
        if (handshaking) {
            handshakeFailed(t);
            return;
        }
        if (closed || !started()) return;
        // A failure of the endpoint, not of the carrier: a session closes with INTERNAL_ERROR.
        if (internalFailure(t)) return;
        if (!inEnded) {
            inEnded = true;
            reader.release();
            inputEnded(false, false);
        }
        if (writing()) {
            clearSrcs();
            writeDone(t);
        }
    }

    private void readSome() throws IOException {
        for (int i = 0; i < 4 && !inEnded && !closed && !paused; i++) {
            if (!netIn.hasRemaining()) netIn = grow(netIn);
            int n = ch.read(netIn);
            if (n < 0) {
                processInput();
                if (!closed) endInput(false);
                return;
            }
            if (n == 0) break;
            processInput();
        }
        if (!inEnded && !reader.hasPartial()) reader.release();
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
        if (!started() || inEnded || closed) return;
        if (!feed() || paused || inEnded) return;
        if (endPending != 0) {
            endInput(endPending == 1);
            return;
        }
        processInput();
        if (!closed && !paused && !inEnded && key != null && key.isValid()) key.interestOps(key.interestOps() | SelectionKey.OP_READ);
    }

    @Override
    public void redeliver() {
        if (started() && !inEnded && !closed) feed();
    }

    /** Unwraps what netIn holds and feeds complete records to the core. */
    private void processInput() {
        if (!started() || inEnded || closed) return;
        try {
            while (!paused) {
                netIn.flip();
                if (!netIn.hasRemaining()) {
                    netIn.compact();
                    break;
                }
                ByteBuffer dst = reader.room();
                if (dst.remaining() < engine.getSession().getApplicationBufferSize()) {
                    netIn.compact();
                    if (!feed()) return;
                    dst = reader.room();
                    netIn.flip();
                }
                SSLEngineResult r = engine.unwrap(netIn, dst);
                netIn.compact();
                reader.lim = dst.position();
                if (r.getStatus() == SSLEngineResult.Status.CLOSED) {
                    if (!feed()) return;
                    endInput(true);
                    return;
                }
                if (r.getStatus() != SSLEngineResult.Status.OK || r.bytesConsumed() == 0) break;
            }
        } catch (SSLException e) {
            endInput(false);
            return;
        }
        feed();
    }

    /** Delivers the plaintext read so far; false when the carrier left its owner. */
    private boolean feed() {
        if (reader.buf == null || reader.pos == reader.lim || inEnded) return !closed;
        int c = deliver(reader.buf, reader.buf.data(), reader.pos, reader.lim);
        if (c == Inbound.GONE) return false;
        reader.consumed(c);
        return true;
    }

    private void endInput(boolean graceful) {
        if (paused && reader.hasPartial()) {
            endPending = graceful ? 1 : 2;
            if (key != null && key.isValid()) key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
            return;
        }
        if (inEnded) return;
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
        srcs = b.segments(srcs == NONE ? null : srcs);
        srcFrom = 0;
        srcEnd = b.segmentCount();
        continueWrite();
    }

    private void continueWrite() {
        try {
            while (true) {
                if (!flushNet()) return;
                while (srcFrom < srcEnd && !srcs[srcFrom].hasRemaining()) srcFrom++;
                if (srcFrom == srcEnd && !closeNotifyPending) break;
                if (closeNotifyPending && srcFrom == srcEnd) {
                    wrapInto(NONE, 0, 0);
                    if (engine.isOutboundDone()) {
                        closeNotifyPending = false;
                        if (!flushNet()) return;
                        shutSocket();
                    }
                    continue;
                }
                wrapInto(srcs, srcFrom, srcEnd - srcFrom);
            }
        } catch (IOException e) {
            clearSrcs();
            if (writing()) writeDone(e);
            return;
        }
        if (writing()) {
            clearSrcs();
            writeDone(null);
            if (shutAfterWrite) shutdownOutput();
        }
    }

    private void clearSrcs() {
        java.util.Arrays.fill(srcs, 0, srcEnd, null);
        srcFrom = srcEnd = 0;
    }

    /** Wraps in[off, off + len) into netOut (which flushNet emptied). */
    private void wrapInto(ByteBuffer[] in, int off, int len) throws SSLException {
        netOut.compact();
        SSLEngineResult r;
        try {
            r = engine.wrap(in, off, len, netOut);
        } finally {
            netOut.flip();
        }
        // A closed engine takes nothing more: fail the write instead of wrapping again.
        if (r.bytesConsumed() == 0 && r.bytesProduced() == 0 && len > 0) {
            throw new SSLException("TLS output closed (" + r.getStatus() + ")");
        }
    }

    /** Writes netOut; false (and OP_WRITE) when the socket did not take all of it. */
    private boolean flushNet() throws IOException {
        if (!netOut.hasRemaining()) return true;
        ch.write(netOut);
        if (netOut.hasRemaining()) {
            if ((key.interestOps() & SelectionKey.OP_WRITE) == 0) {
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                writableChanged(false);
            }
            return false;
        }
        if ((key.interestOps() & SelectionKey.OP_WRITE) != 0) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            writableChanged(true);
        }
        return true;
    }

    @Override
    public void shutdownOutput() {
        if (outputShut || closed) return;
        if (writing()) {
            shutAfterWrite = true;
            return;
        }
        outputShut = true;
        engine.closeOutbound();
        closeNotifyPending = true;
        continueWrite();
    }

    private void shutSocket() {
        try {
            ch.shutdownOutput();
        } catch (IOException ignored) {
            // The read side reports it.
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
            ch.close();
        } catch (IOException ignored) {
            // Closing anyway.
        }
        reader.release();
        clearSrcs();
        dropWrite();
    }
}
