package io.zmux;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;

/** The sending part of a stream. */
public sealed interface ZmuxSendStream extends AutoCloseable permits ZmuxStream {
    /**
     * Writes all of src, waiting for send-buffer room. It returns once the bytes are in the
     * stream's send buffer: a STOP_SENDING, reset or session end that races it may drop them,
     * and that error is reported by the next call or by {@link #finish()}, as with TCP.
     */
    void write(ByteBuffer src) throws IOException;

    /** Writes owned bytes without copying. Takes ownership even on failure or interruption. */
    void writeChunk(Chunk chunk) throws IOException;

    /** Transfers ownership of src's remaining bytes without copying. */
    default void writeChunk(ByteBuffer src) throws IOException { writeChunk(Chunk.of(src)); }

    void write(byte[] b, int off, int len) throws IOException;

    default void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    /**
     * Writes as much of src as the send buffer takes now, without waiting; returns the count.
     * Returns 0 at once while another write of this stream is in progress.
     */
    int tryWrite(ByteBuffer src) throws IOException;

    /**
     * Finishes the sending part cleanly: FIN after the written data, so the peer reads every
     * byte and then end of stream. This is the only way to end a stream successfully. It
     * queues behind writes in progress, waiting at most the write timeout for them.
     */
    void finish() throws IOException;

    /**
     * Aborts the sending part with an application error code: RESET_STREAM (RESET_STREAM_AT
     * after {@link #markReliableBoundary()}). Unlike {@link #finish()} this is not a clean
     * end: unsent data is discarded and the peer sees the reset, never end of stream.
     */
    void reset(long code);

    /** The data written so far is delivered even if the stream is reset later (RESET_STREAM_AT). */
    void markReliableBoundary();

    /** Bytes the part could transmit now within the peer's credit, minus what it holds unsent. */
    long sendCapacity() throws IOException;

    /** Waits until the capacity is at least min, or above 0 while nothing is held unsent; returns it. */
    long awaitSendCapacity(long min) throws IOException;

    /** Like {@link #awaitSendCapacity(long)}, waiting at most timeout; {@link java.net.SocketTimeoutException} after it. */
    long awaitSendCapacity(long min, Duration timeout) throws IOException;

    /**
     * Local scheduling priority. Like every setter that cannot fail, it is applied on the
     * session's loop without waiting; it has no effect on a released stream or a closed
     * session.
     */
    void setPriority(Priority priority);

    /** Bounds this direction's written-but-unreleased bytes (at least 16 KiB); queued like {@link #setPriority}. */
    void setSendBuffer(long bytes);

    /** Limits blocking writes; null for none. */
    void setWriteTimeout(Duration timeout);

    /** An OutputStream view: close() finishes the sending part. */
    OutputStream outputStream();

    /**
     * The stream identifier. Reading it before the first frame pins it and sends the opening
     * frame at once, so set the preamble when opening.
     */
    long id() throws IOException;

    /** Pins the identifier asynchronously, with the same effect as {@link #id()}. */
    ZmuxFuture<Long> idAsync();
    /**
     * Like {@link #write(ByteBuffer)} without blocking: src is borrowed until the future
     * completes or a {@link ZmuxFuture#cancel cancel} returns true.
     */
    ZmuxFuture<Void> writeAsync(ByteBuffer src);
    /** Like {@link #writeChunk(Chunk)} without blocking; the chunk is released on every outcome. */
    ZmuxFuture<Void> writeChunkAsync(Chunk chunk);
    /** Like {@link #finish()} without blocking: FIN behind all earlier writes. */
    ZmuxFuture<Void> finishAsync();

    /**
     * Releases the stream: an unfinished sending part is <b>reset</b> and an unread receiving
     * part stopped, code 0.
     *
     * <p><b>Unlike {@link java.net.Socket#close()}, this does not finish the stream.</b> Data
     * still unsent is discarded and the peer sees RESET_STREAM, never end of stream. To end a
     * stream cleanly call {@link #finish()} after the last write, then close.
     */
    @Override
    void close();
}
