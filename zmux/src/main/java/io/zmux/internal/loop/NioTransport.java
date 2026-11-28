package io.zmux.internal.loop;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/** Read ownership for the selector's plaintext and TLS transports. */
abstract class NioTransport extends AbstractTransport implements EventLoop.IoHandler {
    final EventLoop nio;
    final SocketChannel ch;
    SelectionKey key;
    boolean closed;
    boolean paused;
    boolean inEnded;
    /** An end held until the buffered input has been delivered. */
    private Boolean pendingEnd;

    NioTransport(EventLoop loop, SocketChannel socket) {
        super(loop);
        nio = loop;
        ch = socket;
    }

    @Override public final boolean isClosed() { return closed; }

    @Override public final void pauseInput() {
        paused = true;
        readInterest(false);
    }

    @Override public final void resumeInput() {
        if (!paused) return;
        paused = false;
        if (!started() || inEnded || closed || !resumeBufferedInput() || paused || inEnded || closed) return;
        if (pendingEnd != null) endInput(pendingEnd);
        else readInterest(true);
    }

    /** Processes input already read before arming the socket again. */
    boolean resumeBufferedInput() { return redeliver(reader); }

    boolean hasBufferedInput() { return reader != null && reader.hasPartial(); }

    @Override protected final void inputFailed(IOException cause) { endInput(false); }

    final void endInput(boolean graceful) {
        if (inEnded || closed) return;
        if (paused && hasBufferedInput()) {
            pendingEnd = graceful;
            readInterest(false);
            return;
        }
        boolean partial = dropPartialInput();
        stopInput();
        inputEnded(graceful, partial);
    }

    final void discardInputOnFailure() {
        if (stopInput()) inputEnded(false, false);
    }

    @Override public final void shutdownInput() {
        if (!stopInput()) return;
        try { ch.shutdownInput(); } catch (IOException ignored) { /* The input is already gone. */ }
    }

    private boolean stopInput() {
        if (inEnded || closed) return false;
        inEnded = true;
        pendingEnd = null;
        if (reader != null) reader.release();
        readInterest(false);
        return true;
    }

    private void readInterest(boolean enabled) {
        if (key == null || !key.isValid()) return;
        int ops = key.interestOps();
        key.interestOps(enabled ? ops | SelectionKey.OP_READ : ops & ~SelectionKey.OP_READ);
    }
}
