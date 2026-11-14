package io.zmux;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;

/** The receiving part of a stream. */
public sealed interface ZmuxReceiveStream extends AutoCloseable permits ZmuxStream {
    /** Reads at least one byte, waiting; -1 at the end of a finished stream; a reset fails with {@link StreamResetException}. */
    int read(ByteBuffer dst) throws IOException;

    /** Takes up to max bytes without copying; null at FIN. Close or forward the returned chunk. */
    Chunk readChunk(int max) throws IOException;

    int read(byte[] b, int off, int len) throws IOException;

    /** Stops reading with an application error code (STOP_SENDING); unread data is discarded. */
    void stop(long code);

    /**
     * Reads to the end of the stream; once more than maxBytes arrived it stops the receiving part
     * with code 0 and throws {@link LimitExceededException}.
     */
    byte[] readAll(long maxBytes) throws IOException;

    /** Declines the stream before reading (ZMUX_REJECTED on both parts, retryable for the peer). */
    void reject();

    /**
     * Requests a priority for the peer's sending part (the PRIORITY TLV or PRIORITY_UPDATE).
     * Unlike the queued setters it waits for the session's answer, because it can fail: with
     * {@link NotNegotiatedException} after the first frame when the peer lacks priorities.
     */
    void setPeerPriority(Priority priority) throws IOException;

    /**
     * The receive window: initial raises the advertised limit at once, max caps auto-tuning
     * (0 leaves a value). Queued like {@link ZmuxSendStream#setPriority}.
     */
    void setReceiveWindow(long initial, long max);

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

    /** Limits blocking reads; null for none. */
    void setReadTimeout(Duration timeout);

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
