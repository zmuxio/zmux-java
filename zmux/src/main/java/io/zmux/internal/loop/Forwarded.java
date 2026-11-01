package io.zmux.internal.loop;

import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import io.zmux.internal.core.Batch;
import io.zmux.internal.core.ReadBuf;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A carrier an admission did not accept, handed to a fallback target as a byte-stream
 * {@link PipeIo}: the target reads what arrives, the bytes
 * read before the decision first, and writes the carrier. The target plays the part zmux
 * plays for a pipe input: it starts the reader once, has at most one write in flight, and
 * closes each half once; the carrier closes when both halves are closed. Sink callbacks and
 * write completions run on the carrier's loop and must not block.
 *
 * <p>Input is copied out of the loop's read buffers, so a slow target pins no pooled buffer;
 * reading pauses while more than {@link #AHEAD} bytes are unreleased. A fallback carrier
 * serves other protocols, off the zmux hot path.
 */
final class Forwarded implements Inbound {
    /** Unreleased input bytes beyond which reading pauses. */
    static final long AHEAD = BufferPool.SIZE;

    private final Transport carrier;
    /** The batch of the target's write, reused. */
    private final Batch batch = new Batch();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicLong ahead = new AtomicLong();
    private final Runnable shutdown = () -> abort(new IOException("event loop closed"));
    /** Bytes read before the decision once the input ended (otherwise they are in the read buffer). */
    private byte[] replay;
    private PipeReader.Sink sink;
    private boolean ended, graceful, endDelivered;
    private boolean readerClosed, writerClosed, closed;
    private boolean creditPaused;
    private PipeWriter.Completion pending;

    /** Message carrier: input arrives as length-prefixed records, delivered one message per call. */
    private final boolean messages;
    /** Message mode: the start of a message not complete yet. */
    private byte[] partial = new byte[0];

    /** On the carrier's loop: takes the carrier over; its input waits until the target starts the reader. */
    Forwarded(Transport c, byte[] replay, boolean ended, boolean graceful) { this(c, replay, ended, graceful, false); }

    /** As above; messages: the carrier is a message carrier, its pipe input of message framing. */
    Forwarded(Transport c, byte[] replay, boolean ended, boolean graceful, boolean messages) {
        this.messages = messages;
        carrier = c;
        this.replay = replay;
        this.ended = ended;
        this.graceful = graceful;
        c.rebind(this);
        c.pauseInput();
        c.loop().onShutdown(shutdown);
    }

    /** The carrier as a byte-stream pipe input. */
    PipeIo pipe(String applicationProtocol) {
        boolean in = (carrier.directions() & io.zmux.internal.core.Action.DIR_IN) != 0;
        boolean out = (carrier.directions() & io.zmux.internal.core.Action.DIR_OUT) != 0;
        if (!in) readerClosed = true;
        if (!out) writerClosed = true;
        PipeReader reader = !in ? null : new PipeReader() {
            @Override public void start(Sink s) {
                if (!started.compareAndSet(false, true)) throw new IllegalStateException("reader already started");
                if (!carrier.loop().tryExecute(() -> begin(s))) s.onError(new IOException("event loop closed"));
            }

            @Override public void close() { carrier.loop().tryExecute(Forwarded.this::closeReader); }
        };
        PipeWriter writer = !out ? null : new PipeWriter() {
            @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) {
                if (!carrier.loop().tryExecute(() -> write0(data, done))) done.failed(new IOException("event loop closed"));
            }

            @Override public void close() { carrier.loop().tryExecute(Forwarded.this::closeWriter); }

            @Override public void abort(Throwable cause) { Forwarded.this.abort(cause); }
        };
        PipeIo pio = PipeIo.of(reader, writer);
        if (messages) pio = pio.withFraming(io.zmux.carrier.Framing.MESSAGES);
        return applicationProtocol.isEmpty() ? pio : pio.withProtocol(applicationProtocol);
    }

    private void begin(PipeReader.Sink s) {
        if (closed || readerClosed) {
            s.onError(new IOException("carrier closed"));
            return;
        }
        sink = s;
        byte[] r = replay;
        replay = null;
        if (r != null && r.length > 0) push(r);
        if (ended) {
            end();
            return;
        }
        carrier.start(this);
        if (!creditPaused) carrier.resumeInput();
    }

    @Override
    public int bytes(Transport c, ReadBuf buf, ByteBuffer data, int pos, int lim) {
        if (readerClosed || closed) return lim - pos;
        if (sink == null) return 0; // Paused: kept in the read buffer until the target starts reading.
        byte[] b = new byte[lim - pos];
        data.get(pos, b);
        push(b);
        return lim - pos;
    }

    private void push(byte[] b) {
        if (!messages) { deliver(b); return; }
        // Splits the length-prefixed records back into messages.
        byte[] all = partial.length == 0 ? b : java.util.Arrays.copyOf(partial, partial.length + b.length);
        if (all != b) System.arraycopy(b, 0, all, partial.length, b.length);
        int at = 0;
        while (at < all.length) {
            int sn = io.zmux.carrier.VarInt.lengthOf(all[at]);
            if (all.length - at < sn) break;
            long len = io.zmux.carrier.VarInt.get(ByteBuffer.wrap(all), at, all.length);
            if (len > all.length - at - sn) break;
            deliver(java.util.Arrays.copyOfRange(all, at + sn, at + sn + (int) len));
            at += sn + (int) len;
        }
        partial = java.util.Arrays.copyOfRange(all, at, all.length);
    }

    private void deliver(byte[] b) {
        int n = b.length;
        if (ahead.addAndGet(n) > AHEAD && !creditPaused) {
            creditPaused = true;
            carrier.pauseInput();
        }
        AtomicBoolean once = new AtomicBoolean();
        try {
            sink.onData(ByteBuffer.wrap(b), () -> {
                if (once.compareAndSet(false, true) && ahead.addAndGet(-n) <= AHEAD) carrier.loop().tryExecute(this::credit);
            });
        } catch (RuntimeException e) {
            abort(e);
        }
    }

    /** On the loop: reads again once the target released enough. */
    private void credit() {
        if (!creditPaused || ahead.get() > AHEAD || closed) return;
        creditPaused = false;
        if (sink != null && !ended && !readerClosed) carrier.resumeInput();
    }

    @Override
    public void ended(Transport c, boolean graceful, boolean partial) {
        if (ended) return;
        ended = true;
        this.graceful = graceful;
        if (sink != null) end();
    }

    private void end() {
        if (endDelivered || readerClosed) return;
        endDelivered = true;
        try {
            if (graceful) sink.onEnd();
            else sink.onError(new IOException("carrier failed"));
        } catch (RuntimeException ignored) {
            // The target's sink failed; it still closes the reader.
        }
    }

    private void write0(io.zmux.carrier.OutboundBatch data, PipeWriter.Completion done) {
        if (closed || writerClosed) {
            done.failed(new IOException("carrier closed"));
            return;
        }
        if (pending != null) {
            done.failed(new IllegalStateException("a write is in flight"));
            return;
        }
        pending = done;
        batch.wrap(data);
        carrier.write(batch);
    }

    /** The carrier's write completion, on its loop. */
    @Override
    public void written(Transport c, Batch b, Throwable cause) {
        b.discard();
        PipeWriter.Completion done = pending;
        pending = null;
        if (done != null) {
            try {
                if (cause == null) done.completed();
                else done.failed(cause);
            } catch (RuntimeException ignored) {
                // The target's completion failed; the carrier is unaffected.
            }
        }
        if (readerClosed && writerClosed) finish(null);
    }

    private void closeReader() {
        if (readerClosed) return;
        readerClosed = true;
        if (!ended && !closed) carrier.shutdownInput();
        if (writerClosed) finish(null);
    }

    private void closeWriter() {
        if (writerClosed) return;
        writerClosed = true;
        if (!closed) carrier.shutdownOutput();
        if (readerClosed) finish(null);
    }

    /** Whether the target has not started reading and no input was set aside (the input is all in the read buffer). */
    boolean untouched() { return !started.get() && replay == null && !ended; }

    /** On the loop, untouched: gives the carrier back without closing it; the caller rebinds it. */
    void release() {
        closed = true;
        carrier.loop().removeShutdownHook(shutdown);
    }

    /** Ends the carrier abortively; the sink, if reading, sees a failure. From any thread. */
    void abort(Throwable cause) {
        Runnable r = () -> {
            if (closed) return;
            if (sink != null && !endDelivered && !readerClosed) {
                endDelivered = true;
                try { sink.onError(cause); } catch (RuntimeException ignored) { /* Closing anyway. */ }
            }
            readerClosed = writerClosed = true;
            finish(cause);
        };
        // A loop that is gone has closed the carrier through the shutdown hook.
        if (carrier.loop().inLoop()) r.run();
        else carrier.loop().tryExecute(r);
    }

    /** Closes the carrier once no write is in flight (abortive with a cause). */
    private void finish(Throwable cause) {
        if (closed || cause == null && pending != null) return;
        closed = true;
        carrier.loop().removeShutdownHook(shutdown);
        PipeWriter.Completion done = pending;
        pending = null;
        carrier.close(cause);
        if (done != null) {
            try { done.failed(cause != null ? cause : new IOException("carrier closed")); } catch (RuntimeException ignored) { /* Closed. */ }
        }
    }
}
