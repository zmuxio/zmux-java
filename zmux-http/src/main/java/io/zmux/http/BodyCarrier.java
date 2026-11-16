package io.zmux.http;

import io.zmux.internal.spi.LentInput;
import io.zmux.internal.spi.Loops;
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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * The halves of one HTTP carrier over a Netty channel (an HTTP/1.1 connection or an HTTP/2
 * stream): body bytes (or WebSocket messages) arriving on the channel become owned core chunks
 * as they arrive, without copying, and wait for zmux's reader as such, and zmux's batches go out as wrapped buffers, without copying. Every field is
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
    /** What arrived and zmux has not taken: chunks from their arrival, so the carrier keeps no bare buffer. */
    private final LentInput input;
    private final boolean inHalf;
    /** zmux closed the inbound half (or it has none, or it went to the host). */
    private boolean inClosed;

    // Outbound.
    private final boolean outHalf;
    private boolean outClosed, started, starting, aborted, finished;
    /** A graceful end before anything was written leaves the head unwritten (see {@link #holdEmptyEnd()}). */
    private boolean holdEmptyEnd;
    /** zmux closed the outbound half gracefully before anything was written. */
    private boolean endedUnstarted;
    /** The carrier went to the host's handler: zmux's calls no longer touch the channel. */
    private boolean detached;
    /**
     * Duplicates of every inbound buffer while a verdict that may replay them is pending; null:
     * none kept. The carrier's only buffers outside chunks: {@link #keepNoMore} releases them
     * (the verdict, or {@link #lost}), and {@link #detach} hands them to the host.
     */
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
        input = new LentInput(RECEIVE_MEMORY, loop, loop::inEventLoop, more -> ch.config().setAutoRead(more));
        // The channel's close, which its loop's shutdown does too, notifies this on the loop as
        // it happens; the handlers' channelInactive is a later task, which a terminating loop may
        // never run. So the carrier is lost here, whatever its handlers see afterwards; a loop
        // that is shutting down runs no more of zmux's calls, so its input fails outright.
        ch.closeFuture().addListener(f -> {
            if (loop.isShuttingDown()) input.fail(new ClosedChannelException());
            lost(new ClosedChannelException());
        });
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
     * The channel reads again on its own (autoRead), as zmux's backpressure no longer applies:
     * the host's handler reads the rest of the request.
     */
    List<ByteBuf> detach() {
        detached = true;
        finished = true;
        inClosed = outClosed = true;
        input.close();
        List<ByteBuf> r = log == null ? List.of() : log;
        log = null;
        return r;
    }

    boolean detached() { return detached; }

    /**
     * Runs a zmux call's task on the loop. A loop that refuses it is shutting down and closes the
     * channel, whose {@link #lost} releases what the carrier holds; onRejected fails only what
     * the call brought (its completion, its sink).
     */
    private void run(Runnable task, Runnable onRejected) {
        Loops.dispatch(loop, loop.inEventLoop(), task, onRejected);
    }

    private static final Runnable NOTHING = () -> {};

    // ---- inbound (event loop) ----

    /** A body piece or message arrived; the carrier owns data's reference. */
    void in(ByteBuf data) {
        if (log != null) log.add(data.retainedDuplicate());
        if (inClosed || (!data.isReadable() && framing != Framing.MESSAGES)) { data.release(); return; }
        int n = data.readableBytes();
        if (data.nioBufferCount() == 1) {
            input.offer(Chunk.of(data.nioBuffer(), data::release), n);
            return;
        }
        // A buffer of several components as one: the copy only a composite needs; it goes at once.
        ByteBuffer c = ByteBuffer.allocate(n);
        data.getBytes(data.readerIndex(), c);
        data.release();
        input.offer(Chunk.of(c.flip()), n);
    }

    /** The inbound body (or message stream) ended gracefully. */
    void inEnd() { input.end(); }

    /**
     * The inbound half failed: what is still queued is never delivered, and the sink, once there
     * is one, gets the error; an input that ended gracefully keeps its end for a reader taking it.
     */
    void inFail(Throwable cause) { input.lost(cause); }

    /** Bounds a first message, for a carrier of messages (its frames set it). */
    java.util.function.IntConsumer firstLimit = n -> {};

    private final class Reader implements PipeReader {
        @Override public void limitFirstMessage(int limit) { firstLimit.accept(limit); }

        @Override public void start(Sink s) {
            run(() -> {
                if (inClosed || detached) return;
                input.start(s);
            }, () -> s.onError(new ClosedChannelException()));
        }

        @Override public void close() {
            run(() -> {
                if (inClosed) return;
                inClosed = true;
                input.close(); // reading goes on, and what follows is discarded
                check();
            }, NOTHING);
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
            }, () -> c.failed(new ClosedChannelException()));
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
            }, NOTHING);
        }

        @Override public void abort(Throwable cause) {
            run(() -> {
                if (detached) return;
                outClosed = true;
                fail(cause == null ? new ClosedChannelException() : cause);
                check();
            }, NOTHING);
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
