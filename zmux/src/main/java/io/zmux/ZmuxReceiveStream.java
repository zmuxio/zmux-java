package io.zmux;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;

/** The receiving part of a stream. */
public sealed interface ZmuxReceiveStream extends AutoCloseable permits ZmuxStream {
    /** Reads at least one byte, waiting; -1 at the end of a finished stream; a reset fails with {@link StreamResetException}. */
    int read(ByteBuffer dst) throws IOException;

    /**
     * Takes up to max bytes without copying; null at FIN. Close or forward the returned chunk.
     * @throws ChunkBudgetException if the next backing cannot be leased. The bytes remain available
     *         for a normal read, or for a retry after other chunks are released.
     */
    Chunk readChunk(int max) throws IOException;

    int read(byte[] b, int off, int len) throws IOException;

    /** Stops reading with an application error code (STOP_SENDING); unread data is discarded. */
    void stop(long code);

    /**
     * Reads to the end of the stream; once more than maxBytes arrived it stops the receiving part
     * with code 0 and throws {@link LimitExceededException}.
     */
    byte[] readAll(long maxBytes) throws IOException;

    /**
     * Reads what is readable now into dst without waiting: advances it by the bytes taken and returns their count;
     * 0 when nothing is readable now (also while a read of this stream waits for data) or when dst has no room;
     * -1 at the end of a finished stream, as {@link java.nio.channels.ReadableByteChannel#read} does. "Nothing now"
     * and "ended" are always distinct answers. A reset, a stop by this endpoint or the session's end fails with the
     * stream's error, as {@link #read(ByteBuffer)} does. Pair it with {@link #awaitReadable()}.
     */
    int tryRead(ByteBuffer dst) throws IOException;

    /**
     * Waits, at most the {@linkplain #setReadTimeout read timeout}, until the next read returns at once, without
     * taking anything: data is readable, or the receiving part has ended (FIN, a reset, a {@link #stop} by this
     * endpoint, the session's end). It returns normally in every one of these cases and reports neither how much is
     * readable nor the end: the read that follows ({@link #tryRead} or {@link #read(ByteBuffer)}) reports the data or
     * the end, once, in its own form. It fails only with its own bound, {@link java.net.SocketTimeoutException}; any
     * number of threads may wait at once, none of them queued behind a read.
     */
    void awaitReadable() throws IOException;

    /** {@link #awaitReadable()} bounded by timeout instead of the read timeout. */
    void awaitReadable(Duration timeout) throws IOException;

    /** Declines the stream before reading (ZMUX_REJECTED on both parts, retryable for the peer). */
    void reject();

    /**
     * Requests a priority for the peer's sending part (the PRIORITY TLV or PRIORITY_UPDATE).
     * Unlike the queued setters it waits for the session's answer, because it can fail: with
     * {@link NotNegotiatedException} after the first frame when the peer lacks priorities.
     */
    void setPeerPriority(Priority priority) throws IOException;

    /**
     * The receive window: initial is advertised at once when it is above the window in force, max
     * caps auto-tuning, 0 making the cap automatic whatever the session configuration says. Credit
     * already advertised is never withdrawn: an
     * initial below the window in force changes only what {@link #receiveWindow} reads back. Queued
     * like {@link ZmuxSendStream#setPriority}.
     *
     * @throws IllegalArgumentException a negative value, an {@code initial} below 4096 (the floor of
     *     {@link ZmuxConfig.Builder#initialStreamReceiveWindow}), or a nonzero {@code max} below
     *     {@code initial} (nothing changed)
     * @throws IllegalStateException the stream has no receiving part (a unidirectional stream this endpoint opened)
     */
    void setReceiveWindow(long initial, long max);

    /**
     * The receive window this part holds, as set: what {@link #setReceiveWindow} set last, else the session configuration's
     * value copied when the part was created (it does not follow the configuration afterwards); {@code max} is 0
     * while the cap is automatic. Local state: it never waits or fails, and answers after the stream or session
     * ended with the value held last.
     */
    ReceiveWindow receiveWindow();

    /** The opening information the peer sent (empty for local streams). */
    Preamble preamble();

    /**
     * The priority in the PRIORITY TLV of the opener's preamble; empty when it sent none, for a
     * stream opened locally, and without the preamble extension. Only the opening preamble's
     * value: a later PRIORITY_UPDATE is not reflected.
     */
    java.util.Optional<Priority> preamblePriority();

    /**
     * Whether the opening frame arrived as carrier early data (replayable); it stays true for the
     * stream's life. Always false: no Java carrier accepts TLS 1.3 early data or QUIC 0-RTT.
     */
    boolean isReplayable();

    /**
     * Bounds every blocking read, the {@link #inputStream()} and channel views included, as a socket's timeout does:
     * a positive duration; {@link java.net.SocketTimeoutException} after it.
     */
    void setReadTimeout(Duration timeout);

    /** Blocking reads wait without bound again (the default). */
    void clearReadTimeout();

    /** The read timeout; empty when reads wait without bound. */
    java.util.Optional<Duration> readTimeout();

    /**
     * The priority last requested for the peer's sending part ({@link #setPeerPriority}); empty when none was.
     * Local state: it never waits or fails, and answers after the stream or session ended.
     */
    java.util.Optional<Priority> peerPriority();

    /**
     * An InputStream view; close() stops only the receiving part ({@code stop(0)},
     * STOP_SENDING), leaving the sending part to {@link ZmuxSendStream#outputStream()}.
     */
    InputStream inputStream();

    long id() throws IOException;

    /** Pins the identifier asynchronously, with the same effect as {@link #id()}. */
    ZmuxFuture<Long> idAsync();
    /** Like {@link #read(ByteBuffer)} without blocking: dst is borrowed until completion or a successful cancel; -1 at FIN. */
    ZmuxFuture<Integer> readAsync(ByteBuffer dst);
    /** Like {@link #readChunk(int)} without blocking; null at FIN. */
    ZmuxFuture<Chunk> readChunkAsync(int max);

    /**
     * Releases the stream: a receiving part read to its end is left as it is; one not read to
     * its end is stopped (STOP_SENDING) with code 0.
     */
    @Override
    void close();
}
