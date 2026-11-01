package io.zmux.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.util.ReferenceCountUtil;
import io.zmux.Chunk;
import io.zmux.carrier.Framing;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * The halves of one HTTP carrier over a Netty channel (an HTTP/1.1 connection or an HTTP/2
 * stream): body bytes (or WebSocket messages) arriving on the channel go to zmux as owned chunks,
 * without copying, and zmux's batches go out as wrapped buffers, without copying. Every field is
 * owned by the channel's event loop; zmux's calls hop there.
 */
final class BodyCarrier {
    /** What a carrier kind writes and how it ends. */
    interface Out {
        /** Writes the head of the outbound half (a response, a handshake); null: nothing to write. */
        default ChannelFuture head() { return null; }
        /** The outbound object carrying data (a message with message framing). */
        Object content(ByteBuf data);
        /** The graceful end of the outbound half; null: none. */
        Object end();
    }

    /** The bytes a carrier delivers ahead of zmux's releases; the queue beyond it pauses reading. */
    static final int RECEIVE_MEMORY = 1 << 20;

    final Channel ch;
    private final EventLoop loop;
    private final Framing framing;
    private final Out out;
    /** Runs once both halves are done (closed or aborted). */
    private final Runnable done;
    private final Function<Throwable, Void> kill;

    // Inbound.
    private PipeReader.Sink sink;
    private final ArrayDeque<ByteBuf> queue = new ArrayDeque<>();
    private long outstanding;
    private final boolean inHalf;
    private boolean inEnded, inClosed;
    private Throwable inError;

    // Outbound.
    private final boolean outHalf;
    private boolean outClosed, started, starting, aborted, finished;
    /** A graceful end before anything was written leaves the head unwritten (see {@link #holdEmptyEnd}). */
    private boolean holdEmptyEnd;
    /** zmux closed the outbound half gracefully before anything was written. */
    private boolean endedUnstarted;
    /** The carrier went to the host's handler: zmux's calls no longer touch the channel. */
    private boolean detached;
    /** Duplicates of every inbound buffer while a verdict that may replay them is pending; null: none kept. */
    private ArrayList<ByteBuf> log;
    private final List<Runnable> pending = new ArrayList<>(0);
    /** The latest write on the channel. */
    private ChannelFuture lastWrite;
    /** Why the carrier was lost (a refusing response, say); later writes fail with it. */
    private Throwable lostCause;

    /**
     * A carrier on ch; reader and writer say which halves zmux gets (an absent half counts as
     * closed). kill aborts the channel's carrier (closes the connection or resets the stream).
     */
    BodyCarrier(Channel ch, Framing framing, boolean reader, boolean writer, Out out, Runnable done, Function<Throwable, Void> kill) {
        this.ch = ch;
        this.loop = ch.eventLoop();
        this.framing = framing;
        this.out = out;
        this.done = done;
        this.kill = kill;
        inHalf = reader;
        inClosed = !reader;
        outHalf = writer;
        outClosed = !writer;
    }

    PipeReader reader() { return inHalf ? new Reader() : null; }

    PipeWriter writer() { return outHalf ? new Writer() : null; }

    /** Keeps what arrives for a replay until {@link #keepNoMore}. */
    void keepForReplay() { if (log == null) log = new ArrayList<>(); }

    /** The verdict accepted the carrier: nothing is replayed. */
    void keepNoMore() {
        if (log == null) return;
        for (ByteBuf b : log) b.release();
        log = null;
    }

    /**
     * Gives the carrier up for the host's handler: every inbound buffer kept (in order) is
     * returned, the carrier's own references are dropped, and zmux's later calls are ignored.
     */
    List<ByteBuf> detach() {
        detached = true;
        finished = true;
        inClosed = outClosed = true;
        for (ByteBuf b; (b = queue.pollFirst()) != null; ) b.release();
        sink = null;
        List<ByteBuf> r = log == null ? List.of() : log;
        log = null;
        return r;
    }

    boolean detached() { return detached; }

    private void run(Runnable r) {
        if (loop.inEventLoop()) r.run();
        else loop.execute(r);
    }

    // ---- inbound (event loop) ----

    /** A body piece or message arrived; the carrier owns data's reference. */
    void in(ByteBuf data) {
        if (log != null) log.add(data.retainedDuplicate());
        if (inClosed || inEnded) { data.release(); return; }
        if (!data.isReadable() && framing != Framing.MESSAGES) { data.release(); return; }
        queue.add(data);
        drain();
    }

    /** The inbound body (or message stream) ended gracefully. */
    void inEnd() {
        if (inClosed || inEnded) return;
        inEnded = true;
        drain();
    }

    /** The inbound half failed. */
    void inFail(Throwable cause) {
        if (inClosed || inEnded) return;
        inEnded = true;
        inError = cause;
        drain();
    }

    private void drain() {
        if (sink == null) {
            if (!queue.isEmpty()) ch.config().setAutoRead(false);
            return;
        }
        while (!queue.isEmpty() && (outstanding == 0 || outstanding + queue.getFirst().readableBytes() <= RECEIVE_MEMORY)) {
            ByteBuf b = queue.removeFirst();
            int n = b.readableBytes();
            outstanding += n;
            ByteBuffer view = b.nioBufferCount() == 1 ? b.nioBuffer() : copy(b);
            sink.onChunk(Chunk.of(view, b::release), () -> run(() -> consumed(n)));
            if (inClosed) return;
        }
        ch.config().setAutoRead(queue.isEmpty() && outstanding < RECEIVE_MEMORY / 2);
        if (queue.isEmpty() && inEnded) {
            PipeReader.Sink s = sink;
            sink = null;
            if (inError == null) s.onEnd();
            else s.onError(inError);
        }
    }

    /** A buffer of several components as one: the copy only a composite needs. */
    private static ByteBuffer copy(ByteBuf b) {
        ByteBuffer c = ByteBuffer.allocate(b.readableBytes());
        b.getBytes(b.readerIndex(), c);
        return c.flip();
    }

    private void consumed(int n) {
        outstanding -= n;
        if (!inClosed) drain();
    }

    private final class Reader implements PipeReader {
        @Override public void start(Sink s) {
            run(() -> {
                if (inClosed || detached) return;
                sink = s;
                drain();
            });
        }

        @Override public void close() {
            run(() -> {
                if (inClosed) return;
                inClosed = true;
                sink = null;
                for (ByteBuf b; (b = queue.pollFirst()) != null; ) b.release();
                ch.config().setAutoRead(true); // discard what follows
                check();
            });
        }
    }

    // ---- outbound (event loop) ----

    /** Writes the head now, if not yet (an uplink's empty response, an accepted carrier's header). */
    void start() {
        if (started || starting) return;
        ChannelFuture f = out.head();
        if (f == null) {
            started = true;
            return;
        }
        starting = true;
        lastWrite = f;
        f.addListener(r -> {
            starting = false;
            started = true;
            if (!r.isSuccess()) fail(r.cause());
            List<Runnable> p = new ArrayList<>(pending);
            pending.clear();
            for (Runnable x : p) x.run();
        });
    }

    boolean started() { return started || starting; }

    /**
     * zmux's graceful end of the outbound half before anything was written writes nothing,
     * not even the head: the adapter ends it with {@link #endOut} or gives the request away.
     */
    void holdEmptyEnd() { holdEmptyEnd = true; }

    /** Whether zmux closed the outbound half gracefully before anything was written on it. */
    boolean endedUnstarted() { return endedUnstarted; }

    /** After the head: run r, at once when it went out already. */
    private void afterHead(Runnable r) {
        start();
        if (started) r.run();
        else pending.add(r);
    }

    private void fail(Throwable cause) {
        if (aborted || detached) return;
        aborted = true;
        kill.apply(cause);
        inFail(cause);
    }

    private final class Writer implements PipeWriter {
        @Override public void write(OutboundBatch batch, Completion c) {
            ByteBuffer[] segs = batch.segments(null);
            run(() -> {
                if (detached) { c.failed(new ClosedChannelException()); return; }
                afterHead(() -> {
                if (aborted || outClosed || !ch.isActive()) { c.failed(lostCause != null ? lostCause : new ClosedChannelException()); return; }
                ChannelFuture last;
                if (framing == Framing.MESSAGES) {
                    last = null;
                    for (int m = 0, from = 0; m < batch.messageCount(); m++) {
                        int to = batch.messageEnd(m);
                        ByteBuffer[] part = java.util.Arrays.copyOfRange(segs, from, to);
                        last = lastWrite = ch.write(out.content(Unpooled.wrappedBuffer(part)));
                        from = to;
                    }
                    ch.flush();
                    if (last == null) { c.completed(); return; }
                } else {
                    last = lastWrite = ch.writeAndFlush(out.content(Unpooled.wrappedBuffer(segs)));
                }
                last.addListener(r -> {
                    if (r.isSuccess()) c.completed();
                    else c.failed(r.cause());
                });
                });
            });
        }

        @Override public void close() {
            run(() -> {
                if (outClosed || detached) return;
                outClosed = true;
                if (holdEmptyEnd && !started && !starting) {
                    // Nothing was written: the head stays unwritten, so that the adapter may
                    // still give the request to the host; see endOut.
                    endedUnstarted = true;
                    check();
                    return;
                }
                afterHead(() -> {
                    Object end = aborted ? null : out.end();
                    if (end != null && ch.isActive()) lastWrite = ch.writeAndFlush(end);
                    else ReferenceCountUtil.release(end);
                    check();
                });
            });
        }

        @Override public void abort(Throwable cause) {
            run(() -> {
                if (detached) return;
                outClosed = true;
                fail(cause == null ? new ClosedChannelException() : cause);
                check();
            });
        }
    }

    /** The adapter ends an outbound half that zmux does not own (an uplink's response). */
    void endOut(Runnable then) {
        afterHead(() -> {
            Object end = aborted ? null : out.end();
            if (end != null && ch.isActive()) lastWrite = ch.writeAndFlush(end);
            else ReferenceCountUtil.release(end);
            then.run();
        });
    }

    /** Runs r once every write so far has gone out (or failed). */
    void afterWrites(Runnable r) {
        if (lastWrite == null || lastWrite.isDone()) r.run();
        else lastWrite.addListener(f -> r.run());
    }

    /** The channel's carrier was lost (the connection closed, the stream reset). */
    void lost(Throwable cause) {
        keepNoMore();
        if (detached) return;
        if (lostCause == null && !(cause instanceof ClosedChannelException)) lostCause = cause;
        aborted = true;
        inFail(cause);
        outClosed = true;
        check();
    }

    private void check() {
        if (finished || !inClosed || !outClosed || starting) return;
        finished = true;
        done.run();
    }

    /** Whether zmux has closed both halves. */
    boolean finished() { return finished; }

}
