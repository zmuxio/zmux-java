package io.zmux.seal;

import io.zmux.Chunk;
import io.zmux.carrier.Framing;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Blocking reads and writes of a handshake on a pipe input, on a virtual thread, and the pipe input that
 * continues behind it. A {@link io.zmux.carrier.PipeChannel} cannot hand its reader on: what the handshake
 * did not read must go on, in order and with its message boundaries, to the session, without a thread per
 * carrier on the data path. An unread delivery keeps its chunk and its consumed callback, which is the
 * carrier's backpressure; a handshake that fails closes them ({@link #fail}).
 */
final class HandshakeIo implements PipeReader.Sink {
    private record Item(Chunk chunk, Runnable consumed) {}

    private final PipeIo io;
    private final ArrayDeque<Object> queue = new ArrayDeque<>();
    private final boolean messages;
    /** What was read and not consumed: bytes of a stream, or one whole message. */
    byte[] buf = new byte[0];
    /** The session's sink once the handshake handed its input on; deliveries then go there without the lock. */
    private volatile PipeReader.Sink next;
    /** The handshake failed: what arrives is closed at once. */
    private boolean failed;

    HandshakeIo(PipeIo io) {
        this.io = io;
        messages = io.framing() == Framing.MESSAGES;
        io.reader().start(this);
    }

    boolean messages() { return messages; }

    /** Reads once more: replaces a message, appends to a stream. */
    void fill() throws IOException {
        Object o;
        synchronized (this) {
            try {
                while ((o = queue.poll()) == null) wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException();
            }
        }
        switch (o) {
            case Item(Chunk chunk, Runnable consumed) -> {
                ByteBuffer data = chunk.buffer();
                int have = messages ? 0 : buf.length;
                byte[] b = java.util.Arrays.copyOf(buf, have + data.remaining());
                if (messages) b = new byte[data.remaining()];
                data.get(b, have, b.length - have);
                buf = b;
                chunk.close();
                consumed.run();
            }
            case Throwable t -> {
                queue.addFirst(t);
                throw t instanceof IOException x ? x : new IOException(t);
            }
            default -> {
                queue.addFirst(o);
                throw new java.io.EOFException("carrier ended during the handshake");
            }
        }
    }

    void consume(int n) { buf = java.util.Arrays.copyOfRange(buf, n, buf.length); }

    /** One write on the carrier: one message on a message carrier. */
    void write(byte[] unit) throws IOException {
        CompletableFuture<Void> done = new CompletableFuture<>();
        io.writer().write(OutboundBatch.of(ByteBuffer.wrap(unit)), new PipeWriter.Completion() {
            @Override public void completed() { done.complete(null); }
            @Override public void failed(Throwable cause) { done.completeExceptionally(cause == null ? new IOException("write failed") : cause); }
        });
        try {
            done.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        } catch (ExecutionException e) {
            throw e.getCause() instanceof IOException x ? x : new IOException(e.getCause());
        }
    }

    /** The pipe input behind the handshake: first (or null), the unconsumed bytes, then the carrier's input. */
    PipeIo rest(byte[] first) {
        byte[] held = buf;
        if (first != null) {
            held = java.util.Arrays.copyOf(first, first.length + buf.length);
            System.arraycopy(buf, 0, held, first.length, buf.length);
        }
        buf = new byte[0];
        byte[] prefix = held;
        PipeReader reader = new PipeReader() {
            @Override public void start(Sink sink) { hand(prefix, sink); }
            @Override public void close() { fail(); io.reader().close(); }
        };
        return io.withHalves(reader, io.writer());
    }

    /**
     * Under the lock, so that nothing of the carrier's passes what was held; a sink here only
     * takes its deliveries. Later deliveries go to it without the lock (they do not overlap).
     */
    private synchronized void hand(byte[] prefix, PipeReader.Sink sink) {
        if (prefix.length > 0) sink.onChunk(Chunk.of(ByteBuffer.wrap(prefix)), () -> {});
        for (Object o; (o = queue.poll()) != null; ) forward(o, sink);
        next = sink;
    }

    /** The handshake failed or its input was given up: what is held and what still arrives is closed. */
    synchronized void fail() {
        failed = true;
        for (Object o; (o = queue.poll()) != null; ) {
            if (o instanceof Item(Chunk chunk, Runnable consumed)) { chunk.close(); consumed.run(); }
        }
    }

    private static void forward(Object o, PipeReader.Sink sink) {
        switch (o) {
            case Item(Chunk chunk, Runnable consumed) -> sink.onChunk(chunk, consumed);
            case Throwable t -> sink.onError(t);
            default -> sink.onEnd();
        }
    }

    private void offer(Object o) {
        PipeReader.Sink to = next;
        if (to == null) {
            synchronized (this) {
                to = next;
                if (to == null) {
                    if (failed && o instanceof Item(Chunk chunk, Runnable consumed)) { chunk.close(); consumed.run(); return; }
                    queue.add(o);
                    notifyAll();
                    return;
                }
            }
        }
        forward(o, to);
    }

    @Override public void onChunk(Chunk chunk, Runnable consumed) { offer(new Item(chunk, consumed)); }
    @Override public void onEnd() { offer(true); }
    @Override public void onError(Throwable cause) { offer(cause == null ? new IOException("carrier failed") : cause); }
}
