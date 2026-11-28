package io.zmux.carrier;

import io.zmux.Chunk;
import io.zmux.internal.loop.ChunkAccess;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
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
            @Override public void onChunk(Chunk chunk, Runnable consumed) {
                var owned = ChunkAccess.take(chunk);
                offer(new Item(owned.data(), () -> {
                    try {
                        owned.buf().unref();
                    } finally {
                        consumed.run();
                    }
                }));
            }
            @Override public void onEnd() { offer(true); }
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
                switch (queue.poll()) {
                    case null -> {
                        if (closed.get()) throw new ClosedChannelException();
                        try { wait(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new InterruptedIOException(); }
                    }
                    case Item(ByteBuffer data, Runnable release) when !data.hasRemaining() -> release.run();
                    case Item(ByteBuffer data, Runnable release) -> { current = data; currentRelease = release; }
                    case Throwable t -> failure = t;
                    default -> ended = true;
                }
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
     * thread. Closing a half closes its channel. When in and out are one channel, the channel
     * closes with the second half: a socket channel shuts each direction down as its half closes
     * (the sending half's close sends FIN); any other channel cannot close one direction, so the
     * sending half's close leaves it open for the receiving half (a peer's last write and its
     * close code still arrive). An abort of the sending half closes the channel.
     *
     * <p>A selectable half (a socket channel) is served by a platform thread of its own instead:
     * on Windows, JDK 25 and later lose the wake-up of a virtual thread waiting to write on a
     * socket while another virtual thread waits to read from it, so a write that fills the send
     * buffer would stall until the next inbound byte. Plain blocking reads and writes on two
     * platform threads do not depend on the virtual-thread poller.
     *
     * @param readSize the largest read
     */
    public static PipeIo pipe(ReadableByteChannel in, WritableByteChannel out, Framing framing, int readSize) {
        Objects.requireNonNull(framing, "framing");
        if (readSize < 1) throw new IllegalArgumentException("readSize below 1");
        Shared shared = in != null && in == out ? new Shared(in) : null;
        class Reader implements PipeReader, io.zmux.internal.spi.AdapterPorts.ReleasingHalf {
            private final java.util.concurrent.CompletableFuture<Void> released = new java.util.concurrent.CompletableFuture<>();
            private boolean claimed;
            private final ReadableByteChannel source;
            Reader(ReadableByteChannel source) { this.source = source; }
            @Override public java.util.concurrent.CompletionStage<Void> released() { return released.minimalCompletionStage(); }
            // The read thread ends on any failure, an interrupt included, and reports it to the sink.
            @SuppressWarnings("InterruptedExceptionSwallowed")
            @Override public synchronized void start(Sink sink) {
                if (claimed) throw new IllegalStateException("reader already started");
                claimed = true;
                threads(source).name("zmux-pipe-channel-read").start(() -> {
                    try {
                        while (true) {
                            ByteBuffer b = ByteBuffer.allocate(readSize);
                            int n = source.read(b);
                            if (n < 0) { sink.onEnd(); return; }
                            if (n == 0) continue;
                            var consumed = new java.util.concurrent.CountDownLatch(1);
                            sink.onChunk(Chunk.of(b.flip()), consumed::countDown);
                            consumed.await(); // the backpressure
                        }
                    } catch (Throwable e) {
                        if (source.isOpen()) sink.onError(e); else sink.onEnd();
                    } finally { released.complete(null); }
                });
            }
            @Override public void close() {
                boolean unused;
                synchronized (this) { unused = !claimed; claimed = true; }
                if (shared != null) shared.closeHalf(false); else closeQuietly(source);
                if (unused) released.complete(null);
            }
        }
        class Writer implements PipeWriter, io.zmux.internal.spi.AdapterPorts.ReleasingHalf {
            private final WriteThread run = new WriteThread(threads(out));
            @Override public java.util.concurrent.CompletionStage<Void> released() {
                return run.released.minimalCompletionStage();
            }
            @Override public void write(OutboundBatch batch, Completion done) {
                if (!run.offer(() -> {
                    int confirmed = 0;
                    try {
                        if (framing == Framing.MESSAGES) {
                            for (int m = 0, from = 0; m < batch.messageCount(); m++) {
                                int to = batch.messageEnd(m);
                                ByteBuffer message = gather(batch, from, to);
                                int size = message.remaining();
                                writeFully(out, message);
                                confirmed += size;
                                from = to;
                            }
                        } else if (batch.bytes() > 0) {
                            if (out instanceof GatheringByteChannel g) {
                                ByteBuffer[] v = batch.segments(null);
                                while (confirmed < batch.bytes()) confirmed += (int) g.write(v, 0, batch.segmentCount());
                            } else {
                                ByteBuffer bytes = gather(batch, 0, batch.segmentCount());
                                while (bytes.hasRemaining()) confirmed += out.write(bytes);
                            }
                        }
                        done.completed();
                    } catch (Throwable e) {
                        done.failed(e, confirmed);
                    }
                })) done.failed(new java.nio.channels.ClosedChannelException());
            }
            @Override public void close() {
                if (shared != null) shared.closeHalf(true); else closeQuietly(out);
                run.stop();
            }
            @Override public void abort(Throwable cause) {
                closeQuietly(out);
                run.stop();
            }
        }
        return new PipeIo(in == null ? null : new Reader(in), out == null ? null : new Writer(), framing, null, null);
    }

    /**
     * The two halves of one channel: each close shuts its direction where the channel can (a socket channel), the
     * second closes the channel.
     */
    private record Shared(java.nio.channels.Channel ch, java.util.concurrent.atomic.AtomicInteger open) {
        Shared(java.nio.channels.Channel ch) { this(ch, new java.util.concurrent.atomic.AtomicInteger(2)); }

        void closeHalf(boolean output) {
            if (ch instanceof SocketChannel sc) {
                try {
                    if (output) sc.shutdownOutput(); else sc.shutdownInput();
                } catch (IOException ignored) {
                    // The socket is gone already; closing it below is all that is left.
                }
            }
            if (open.decrementAndGet() == 0) closeQuietly(ch);
        }
    }

    /** Virtual threads, or a platform thread for a selectable channel (see {@link #pipe}). */
    private static Thread.Builder threads(java.nio.channels.Channel c) {
        return c instanceof SelectableChannel ? Thread.ofPlatform().daemon(true) : Thread.ofVirtual();
    }

    /** At most one write is submitted at a time. The same lazy worker drains it through close. */
    private static final class WriteThread {
        private static final Runnable STOP = () -> {};
        private final AtomicReference<Runnable> next = new AtomicReference<>();
        private final AtomicBoolean launched = new AtomicBoolean();
        private final java.util.concurrent.CompletableFuture<Void> released = new java.util.concurrent.CompletableFuture<>();
        private final Thread worker;
        private volatile boolean closing;

        WriteThread(Thread.Builder threads) {
            worker = threads.name("zmux-pipe-channel-write").unstarted(this::run);
        }

        boolean offer(Runnable write) {
            if (!next.compareAndSet(null, write)) return false;
            if (!launched.get() && launched.compareAndSet(false, true)) worker.start();
            else LockSupport.unpark(worker);
            return true;
        }

        void stop() {
            closing = true;
            if (next.compareAndSet(null, STOP) && launched.compareAndSet(false, true)) released.complete(null);
            else LockSupport.unpark(worker);
        }

        private void run() {
            try {
                while (true) {
                    Runnable work = next.get();
                    if (work == STOP) return;
                    if (work != null) {
                        if (next.compareAndSet(work, null)) work.run();
                    } else if (closing && next.compareAndSet(null, STOP)) {
                        return;
                    } else {
                        LockSupport.park(this);
                    }
                }
            } finally { released.complete(null); }
        }
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
