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
     * Waits until the send buffer has room for a write and returns the bytes
     * {@link #tryWrite} takes now (above 0); throws the error that ended the part. Room, not
     * credit, admits a write: it may be there while {@link #sendCapacity()} is 0, and the
     * other way round. It consumes nothing, and the write timeout bounds the wait.
     * <p>
     * The wait holds the stream's write turn, as a {@link #write write} that waits for room
     * does: a write in progress finishes first, and every other write, {@link #tryWrite} and
     * {@link #finish()} of this stream queues behind the wait until it returns or fails
     * (a {@code tryWrite} meanwhile returns 0). Do not wait here for room another thread of
     * the same stream is to make by writing. The turn is released when the call returns,
     * so the room is a promise of readiness, not a reservation: {@code tryWrite} usually
     * takes the returned bytes, but returns 0 when another write of the stream took the
     * turn meanwhile or the session took the room back for a stream that needed it more;
     * wait again then.
     */
    long awaitWritable() throws IOException;

    /** Like {@link #awaitWritable()}, waiting at most timeout; {@link java.net.SocketTimeoutException} after it. */
    long awaitWritable(Duration timeout) throws IOException;

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

    /**
     * Waits until the capacity is at least min, or above 0 while nothing is held unsent;
     * returns it. It waits for credit, not for what a write takes: see {@link #awaitWritable()}.
     */
    long awaitSendCapacity(long min) throws IOException;

    /** Like {@link #awaitSendCapacity(long)}, waiting at most timeout; {@link java.net.SocketTimeoutException} after it. */
    long awaitSendCapacity(long min, Duration timeout) throws IOException;

    /**
     * Local scheduling priority. Like every setter that cannot fail, it is applied on the
     * session's loop without waiting; it has no effect on a released stream or a closed
     * session.
     */
    void setPriority(Priority priority);

    /**
     * The local scheduling priority this part holds: the last {@link #setPriority}, or the one the peer requested
     * since (the PRIORITY TLV of its preamble or a PRIORITY_UPDATE), whichever came last; a new part holds
     * {@link Priority#LOCAL_DEFAULT}. Local state: it never waits or fails, and answers after the stream or session
     * ended with the value held last.
     */
    Priority priority();

    /** This direction's own scheduling policy; queued like {@link #setPriority}. */
    void setScheduler(Scheduler s);

    /** This direction follows the session's scheduling policy again. */
    void clearScheduler();

    /** This direction's own scheduling policy; empty while it follows the session's. */
    java.util.Optional<Scheduler> scheduler();

    /** Whether this direction's data is sent on two pipes when possible, over the session's redundancy. */
    void setRedundant(boolean on);

    /** This direction follows the session's redundancy again. */
    void clearRedundant();

    /** This direction's own redundancy; empty while it follows the session's. */
    java.util.Optional<Boolean> redundant();

    /**
     * Bounds this direction's written-but-unreleased bytes (at least 16 KiB); queued like {@link #setPriority}.
     *
     * @throws IllegalStateException the stream has no sending part (a unidirectional stream the peer opened)
     */
    void setSendBuffer(long bytes);

    /**
     * The send buffer this direction holds in bytes: what {@link #setSendBuffer} set last (raised to 16 KiB at
     * least, as the setter applies it), else the session configuration's value copied when the part was created
     * (it does not follow the configuration afterwards); clamped to {@link Integer#MAX_VALUE}. Local state: it
     * never waits or fails, and answers after the stream or session ended with the value held last.
     */
    int sendBuffer();

    /**
     * Bounds every blocking write, the {@link #outputStream()} and channel views included, as a socket's timeout
     * does: a positive duration; {@link java.net.SocketTimeoutException} after it. The JDK fixes those views'
     * signatures, so this runtime setting is the one exception to bounding a wait by its own argument.
     */
    void setWriteTimeout(Duration timeout);

    /** Blocking writes wait without bound again (the default). */
    void clearWriteTimeout();

    /** The write timeout; empty when writes wait without bound. */
    java.util.Optional<Duration> writeTimeout();

    /** An OutputStream view: close() finishes the sending part. */
    OutputStream outputStream();

    /**
     * The stream identifier. Reading it before the first frame pins it and sends the opening
     * frame at once, so set the preamble when opening.
     */
    long id() throws IOException;

    /**
     * Sends what was written now: the opening frame at once when the stream has none yet, with
     * the preamble and the data written before it in that one frame, and the rest of the buffered
     * data, as flow control allows; waits until they were sent, bounded by the write timeout, and
     * fails as a write does on a reset or closed stream or session. It counts as the first frame:
     * preamble settings fail afterwards. Before establishment the opening frame goes as soon as
     * the stream may open (in early data when in use); while the first flight is held the call
     * returns at once and the frames leave with the flight. The explicit way to open a stream whose
     * peer speaks first; {@link #id()} pins the same way but does not wait.
     */
    void flush() throws IOException;

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
     * Releases the stream. After {@link #finish()} it does nothing to the sending part, so
     * {@code write}, {@code finish}, {@code close} ends the stream cleanly; an unfinished sending
     * part is <b>reset</b> with code 0.
     *
     * <p><b>Unlike {@link java.net.Socket#close()}, this does not finish the stream.</b> Without
     * a finish, data still unsent is discarded and the peer sees RESET_STREAM, never end of
     * stream. To end a stream cleanly call {@link #finish()} after the last write, then close.
     */
    @Override
    void close();
}
