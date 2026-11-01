package io.zmux.carrier;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A pipe input as a blocking {@link ByteChannel}, for stream-based code (an SSLEngine loop, a
 * framing layer) that runs over a carrier: in an {@link io.zmux.InboundTransform}, or in a
 * dialer that wraps its carrier. Every {@link #write} is exactly one write on the pipe input,
 * never merged with another: on a {@link Framing#MESSAGES} carrier one message; {@link #read}
 * returns the bytes of the input in order (of the messages in order). A layer over the channel
 * must keep that, one write here for each write it takes. Calls block (use virtual threads);
 * reads and writes may run concurrently, one of each at a time.
 *
 * <p>{@link #pipe} is the reverse: a pipe input over a pair of blocking channels, the layer's
 * plaintext side, with which a transform completes.
 */
public final class PipeChannel implements ByteChannel {
    private final PipeIo io;
    private final AtomicBoolean started = new AtomicBoolean(), closed = new AtomicBoolean(), outputShut = new AtomicBoolean();
    /** Delivered and not yet read: buffers with their releases; an end or failure last. */
    private final ArrayDeque<Object> queue = new ArrayDeque<>();
    private ByteBuffer current;
    private Runnable currentRelease;
    private boolean ended;
    private Throwable failure;

    private PipeChannel(PipeIo io) { this.io = io; }

    /** The channel of io, which it takes over: closing the channel closes both halves. */
    public static PipeChannel of(PipeIo io) { return new PipeChannel(Objects.requireNonNull(io, "io")); }

    /** The framing of the pipe input. */
    public Framing framing() { return io.framing(); }

    private record Item(ByteBuffer data, Runnable release) {}

    private void start() {
        if (io.reader() == null || !started.compareAndSet(false, true)) return;
        io.reader().start(new PipeReader.Sink() {
            @Override public void onData(ByteBuffer data, Runnable release) { offer(new Item(data, release)); }
            @Override public void onEnd() { offer(Boolean.TRUE); }
            @Override public void onError(Throwable cause) { offer(cause == null ? new IOException("carrier failed") : cause); }
        });
    }

    private synchronized void offer(Object o) {
        if (closed.get() && o instanceof Item i) { i.release().run(); return; }
        queue.add(o);
        notifyAll();
    }

    /** Reads what arrived, blocking until a byte, the end (-1) or a failure. */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (closed.get()) throw new ClosedChannelException();
        if (io.reader() == null) return -1;
        start();
        synchronized (this) {
            while (current == null) {
                if (ended) return -1;
                if (failure != null) throw failure instanceof IOException e ? e : new IOException(failure);
                Object o = queue.poll();
                if (o == null) {
                    if (closed.get()) throw new ClosedChannelException();
                    try { wait(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new InterruptedIOException(); }
                    continue;
                }
                if (o instanceof Item i) {
                    if (!i.data().hasRemaining()) { i.release().run(); continue; }
                    current = i.data(); currentRelease = i.release();
                } else if (o instanceof Throwable t) failure = t;
                else ended = true;
            }
            if (!dst.hasRemaining()) return 0;
            int n = Math.min(dst.remaining(), current.remaining());
            ByteBuffer part = current.slice(current.position(), n);
            dst.put(part);
            current.position(current.position() + n);
            if (!current.hasRemaining()) { current = null; Runnable r = currentRelease; currentRelease = null; r.run(); }
            return n;
        }
    }

    /** Writes src's remaining bytes as one write on the pipe input (one message), and waits until it took them. */
    @Override
    public int write(ByteBuffer src) throws IOException {
        if (closed.get() || outputShut.get()) throw new ClosedChannelException();
        if (io.writer() == null) throw new IOException("pipe without a sending half");
        int n = src.remaining();
        ByteBuffer copy = ByteBuffer.allocate(n).put(src).flip();
        CompletableFuture<Void> done = new CompletableFuture<>();
        io.writer().write(OutboundBatch.of(copy), new PipeWriter.Completion() {
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
        return n;
    }

    /** Ends the sending half gracefully, once. */
    public void shutdownOutput() {
        if (io.writer() != null && outputShut.compareAndSet(false, true)) io.writer().close();
    }

    @Override public boolean isOpen() { return !closed.get(); }

    /** Closes both halves, once; a blocked read fails. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        try {
            if (io.reader() != null) io.reader().close();
        } finally {
            shutdownOutput();
            synchronized (this) {
                if (currentRelease != null) { currentRelease.run(); currentRelease = null; current = null; }
                for (Object o; (o = queue.poll()) != null; ) if (o instanceof Item i) i.release().run();
                notifyAll();
            }
        }
    }

    /**
     * A pipe input of framing over blocking channels: its reader reads in (null: no receiving
     * half) on a virtual thread, each read one delivery (one message with {@link
     * Framing#MESSAGES}, so in must keep message boundaries, as a {@link PipeChannel} does);
     * its writer makes one write call on out (null: no sending half) for each write it takes,
     * the batch gathered (one call per message with {@link Framing#MESSAGES}), on a virtual
     * thread. Closing a half closes its channel.
     *
     * @param readSize the largest read
     */
    public static PipeIo pipe(ReadableByteChannel in, WritableByteChannel out, Framing framing, int readSize) {
        Objects.requireNonNull(framing, "framing");
        if (readSize < 1) throw new IllegalArgumentException("readSize below 1");
        PipeReader r = in == null ? null : new PipeReader() {
            private final AtomicBoolean once = new AtomicBoolean();
            @Override public void start(Sink sink) {
                if (!once.compareAndSet(false, true)) throw new IllegalStateException("reader already started");
                Thread.ofVirtual().name("zmux-pipe-channel-read").start(() -> {
                    try {
                        while (true) {
                            ByteBuffer b = ByteBuffer.allocate(readSize);
                            int n = in.read(b);
                            if (n < 0) { sink.onEnd(); return; }
                            if (n == 0) continue;
                            var released = new java.util.concurrent.CountDownLatch(1);
                            sink.onData(b.flip(), released::countDown);
                            released.await(); // the release is the backpressure
                        }
                    } catch (Throwable e) {
                        if (in.isOpen()) sink.onError(e); else sink.onEnd();
                    }
                });
            }
            @Override public void close() { closeQuietly(in); }
        };
        PipeWriter w = out == null ? null : new PipeWriter() {
            @Override public void write(OutboundBatch batch, Completion done) {
                Thread.ofVirtual().name("zmux-pipe-channel-write").start(() -> {
                    try {
                        if (framing == Framing.MESSAGES) {
                            for (int m = 0, from = 0; m < batch.messageCount(); m++) {
                                int to = batch.messageEnd(m);
                                writeFully(out, gather(batch, from, to));
                                from = to;
                            }
                        } else if (batch.bytes() > 0) {
                            if (out instanceof GatheringByteChannel g) {
                                ByteBuffer[] v = batch.segments(null);
                                long left = batch.bytes();
                                while (left > 0) left -= g.write(v, 0, batch.segmentCount());
                            } else writeFully(out, gather(batch, 0, batch.segmentCount()));
                        }
                        done.completed();
                    } catch (Throwable e) {
                        done.failed(e);
                    }
                });
            }
            @Override public void close() { closeQuietly(out); }
        };
        return new PipeIo(r, w, framing, null, null);
    }

    private static ByteBuffer gather(OutboundBatch batch, int from, int to) {
        int n = 0;
        for (int i = from; i < to; i++) n += batch.segment(i).remaining();
        ByteBuffer b = ByteBuffer.allocate(n);
        for (int i = from; i < to; i++) b.put(batch.segment(i));
        return b.flip();
    }

    private static void writeFully(WritableByteChannel out, ByteBuffer b) throws IOException {
        while (b.hasRemaining()) out.write(b);
    }

    private static void closeQuietly(java.nio.channels.Channel c) {
        try { c.close(); } catch (IOException ignored) { /* Closing anyway. */ }
    }
}
