package io.zmux.http;

import io.zmux.internal.spi.LentInput;
import io.zmux.internal.spi.Loops;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.PromiseCombiner;
import io.zmux.Chunk;
import io.zmux.carrier.Framing;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.ArrayDeque;
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
    private final io.zmux.internal.spi.ProviderBackings backings = new io.zmux.internal.spi.ProviderBackings();
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
    private final ArrayDeque<Runnable> pending = new ArrayDeque<>();
    private boolean releasingPending;
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
            channelEnded = true;
            if (loop.isShuttingDown()) { input.close(); inClosed = true; }
            checkRelease();
        });
    }

    /** Real completion of this HTTP stream, including the last write and input-buffer cleanup. */
    private final java.util.concurrent.CompletableFuture<Void> release = new java.util.concurrent.CompletableFuture<>();
    private boolean channelEnded;

    private void checkRelease() {
        if (channelEnded && inClosed && !starting && !releasingPending) afterWrites(() -> release.complete(null));
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
        release.complete(null); // Buffer ownership passed to the host.
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
        Chunk chunk;
        try { chunk = ownedChunk(data, n); }
        finally { data.release(); }
        input.offer(chunk, n);
    }

    private Chunk ownedChunk(ByteBuf data, int length) {
        // Keep the Netty dependency in this adapter, independent of the QUIC module.
        ByteBuf root = data;
        for (ByteBuf parent; (parent = root.unwrap()) != null;) root = parent;
        int capacity = root.writerIndex() + root.maxFastWritableBytes();
        if (data.nioBufferCount() == 1 && !(root instanceof io.netty.buffer.CompositeByteBuf) && capacity < root.maxCapacity()
                && (root.alloc() instanceof io.netty.buffer.PooledByteBufAllocator
                    || root.alloc() instanceof io.netty.buffer.AdaptiveByteBufAllocator)) {
            // The pooled allocators report their allocation size here unless clipped by maxCapacity.
            return backings.chunk(root, capacity, data.nioBuffer(), data::retain, data::release);
        }
        // Composite messages must stay contiguous. An unattested backing becomes one exact owned copy.
        ByteBuffer copy = ByteBuffer.allocate(length);
        data.getBytes(data.readerIndex(), copy);
        return Chunk.of(copy.flip());
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

    private final class Reader implements PipeReader, Requests.CarrierHalf, io.zmux.internal.spi.AdapterPorts.ReleasingHalf {
        @Override public Channel carrierRequest() { return ch; }
        @Override public java.util.concurrent.CompletionStage<Void> released() { return release.minimalCompletionStage(); }
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
                checkRelease();
            }, () -> {
                // The channel may have closed before its loop started shutting down.
                // After termination the final cleanup has exclusive access to queued input.
                loop.terminationFuture().addListener(f -> {
                    input.close();
                    inClosed = true;
                    checkRelease();
                });
            });
        }
    }

    // ---- outbound (event loop) ----

    /** Writes the head now, if not yet (an uplink's empty response, an accepted carrier's header). */
    void start() {
        if (started || starting) return;
        starting = true;
        ChannelFuture f;
        try {
            f = aborted || detached ? null : out.head();
        } catch (RuntimeException failure) {
            headFinished(failure);
            return;
        }
        if (f == null) {
            headFinished(null);
            return;
        }
        lastWrite = f;
        f.addListener(r -> headFinished(r.cause()));
    }

    private void headFinished(Throwable cause) {
        started = true;
        starting = false;
        releaseOutput(cause == null ? null : () -> fail(cause));
    }

    private void releaseOutput(Runnable first) {
        releasingPending = true;
        try {
            if (first != null) first.run();
            for (Runnable task; (task = pending.pollFirst()) != null; ) task.run();
        } finally {
            releasingPending = false;
        }
        check();
        checkRelease();
    }

    boolean started() { return started || starting; }

    /**
     * {@link #start}, then a stage that completes once the head went out, failed when it could
     * not; on the channel's loop.
     */
    ResponseHead opening() {
        return new ResponseHead(head -> run(() -> {
            if (head.cancelled()) { head.written(new ClosedChannelException()); return; }
            afterHead(() -> head.written(aborted || detached
                    ? lostCause != null ? lostCause : new ClosedChannelException() : null));
        }, () -> head.written(new ClosedChannelException())),
                () -> run(() -> fail(new ClosedChannelException()), NOTHING));
    }

    /**
     * zmux's graceful end of the outbound half before anything was written writes nothing,
     * not even the head: the adapter ends it with {@link #endOut} or gives the request away.
     */
    void holdEmptyEnd() { holdEmptyEnd = true; }

    /** Whether zmux closed the outbound half gracefully before anything was written on it. */
    boolean endedUnstarted() { return endedUnstarted; }

    /** After the head: run r, at once when it went out already. */
    private void afterHead(Runnable r) {
        if (started && !releasingPending) {
            releaseOutput(r);
        } else {
            pending.addLast(r);
            start();
        }
    }

    private void fail(Throwable cause) {
        if (aborted || detached) return;
        aborted = true;
        lostCause = cause;
        kill.apply(cause);
        inFail(cause);
    }

    private final class Writer implements Requests.CarrierWriter, io.zmux.internal.spi.AdapterPorts.ReleasingHalf {
        @Override public Channel carrierRequest() { return ch; }
        @Override public java.util.concurrent.CompletionStage<Void> released() { return release.minimalCompletionStage(); }
        @Override public void write(OutboundBatch batch, Completion completion) {
            run(() -> {
                if (outClosed || detached || aborted) {
                    completion.failed(lostCause != null ? lostCause : new ClosedChannelException());
                } else {
                    afterHead(() -> writeBody(batch, completion));
                }
            }, () -> completion.failed(new ClosedChannelException()));
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

    private void writeBody(OutboundBatch batch, PipeWriter.Completion completion) {
        if (detached || aborted || !ch.isActive()) {
            completion.failed(lostCause != null ? lostCause : new ClosedChannelException());
            return;
        }
        if (framing == Framing.MESSAGES && batch.messageCount() == 0) { completion.completed(); return; }
        var segments = batch.segments(null);
        var written = ch.newPromise();
        lastWrite = written;
        boolean multi = framing == Framing.MESSAGES && batch.messageCount() > 1;
        java.util.List<ChannelFuture> messages = multi ? new java.util.ArrayList<>(batch.messageCount()) : java.util.List.of();
        if (multi) {
            var writes = new PromiseCombiner(loop);
            for (int message = 0, from = 0; message < batch.messageCount(); message++) {
                if (aborted) {
                    writes.add(ch.newFailedFuture(lostCause != null ? lostCause : new ClosedChannelException()));
                    break;
                }
                int to = batch.messageEnd(message);
                var part = java.util.Arrays.copyOfRange(segments, from, to);
                ChannelFuture sent = ch.write(out.content(Unpooled.wrappedBuffer(part)));
                messages.add(sent);
                writes.add(sent);
                from = to;
            }
            writes.finish(written);
            ch.flush();
        } else {
            ch.writeAndFlush(out.content(Unpooled.wrappedBuffer(segments)), written);
        }
        written.addListener(result -> {
            if (result.isSuccess()) completion.completed();
            else {
                int confirmed = 0;
                int from = 0;
                for (int i = 0; i < messages.size() && messages.get(i).isSuccess(); i++) {
                    int to = batch.messageEnd(i);
                    for (; from < to; from++) confirmed += batch.segment(from).remaining();
                }
                completion.failed(result.cause(), confirmed);
            }
        });
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
        if (finished || !inClosed || !outClosed || starting || releasingPending) return;
        finished = true;
        done.run();
    }

    /** Whether zmux has closed both halves. */
    boolean finished() { return finished; }

}
