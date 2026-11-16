package io.zmux.internal.spi;

import io.zmux.Chunk;
import io.zmux.carrier.PipeReader;
import java.util.ArrayDeque;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

/**
 * The input a carrier lends to zmux's reader: owned chunks, queued from their arrival, handed to
 * the sink in order within a receive budget, then exactly one end or error. Failing drops what is
 * queued. Every method runs on the owner's thread (a carrier's event loop), or after the owner's
 * loop terminated; the sink's consumed callbacks come back through the owner's executor.
 */
public final class LentInput {
    /** The carrier's source of input (its channel's reads, its requests): asked for more, or for a pause. */
    public interface Source {
        /** more: the input has room for further input; otherwise the source pauses. */
        void demand(boolean more);
    }

    private record Held(Chunk chunk, int size) {}

    private final long budget;
    private final Executor owner;
    private final BooleanSupplier onOwner;
    private final Source source;
    private final ArrayDeque<Held> queue = new ArrayDeque<>();
    private long queued, outstanding;
    private PipeReader.Sink sink;
    private boolean started, ended, closed, draining;
    private Throwable error;

    /**
     * An input that hands the sink more only while what it has not consumed is within budget
     * (one chunk at a time with a budget of 1); onOwner tells whether the caller is on the owner.
     */
    public LentInput(long budget, Executor owner, BooleanSupplier onOwner, Source source) {
        this.budget = budget;
        this.owner = owner;
        this.onOwner = onOwner;
        this.source = source;
    }

    /** Takes chunk (of size bytes); an input that ended or closed closes it. */
    public void offer(Chunk chunk, int size) {
        if (ended || closed) { chunk.close(); return; }
        queue.add(new Held(chunk, size));
        queued += size;
        drain();
    }

    /** The input ended gracefully: the sink gets the end after what is queued. */
    public void end() {
        if (ended || closed) return;
        ended = true;
        drain();
    }

    /**
     * The carrier was lost: a complete input that a started reader is taking keeps its end;
     * anything else fails.
     */
    public void lost(Throwable cause) {
        if (ended && error == null && started && sink != null) return;
        fail(cause);
    }

    /** The input failed: what is queued goes, and the sink gets cause (at once, or at its start). */
    public void fail(Throwable cause) {
        if (closed || error != null || (ended && started && sink == null)) return; // closed, failed, or its end delivered
        ended = true;
        error = cause;
        dropQueue();
        drain();
    }

    /** zmux's reader starts; a failure that came first reaches the sink at once. */
    public void start(PipeReader.Sink s) {
        if (started || closed) return;
        started = true;
        sink = s;
        drain();
    }

    /** zmux's reader closed: nothing more is delivered, and the source is free to go on (its input is discarded). */
    public void close() {
        if (closed) return;
        closed = true;
        sink = null;
        dropQueue();
        source.demand(true);
    }

    public boolean isClosed() { return closed; }

    /** Whether the reader started and the input has neither ended nor closed. */
    public boolean taking() { return started && !ended && !closed; }

    /** Whether n more bytes would be handed to the sink at once. */
    public boolean hasRoom(long n) {
        return started && !closed && queue.isEmpty() && (outstanding == 0 || outstanding + n <= budget);
    }

    /** The bytes queued and those handed to the sink and not consumed. */
    public long bytes() { return queued + outstanding; }

    private void dropQueue() {
        for (Held h; (h = queue.pollFirst()) != null; ) h.chunk().close();
        queued = 0;
    }

    private void drain() {
        if (draining) return; // a consumed callback run inline: the loop below goes on
        draining = true;
        try {
            while (sink != null && !queue.isEmpty() && (outstanding == 0 || outstanding + queue.getFirst().size() <= budget)) {
                Held h = queue.removeFirst();
                int n = h.size();
                queued -= n;
                outstanding += n;
                sink.onChunk(h.chunk(), () -> Loops.dispatch(owner, onOwner.getAsBoolean(), () -> consumed(n), () -> {}));
            }
        } finally {
            draining = false;
        }
        if (sink != null && queue.isEmpty() && ended) {
            PipeReader.Sink s = sink;
            sink = null;
            if (error == null) s.onEnd();
            else s.onError(error);
        }
        if (!closed) source.demand(queue.isEmpty() && (outstanding == 0 || outstanding < budget / 2));
    }

    private void consumed(int n) {
        outstanding -= n;
        if (!closed) drain();
    }
}
