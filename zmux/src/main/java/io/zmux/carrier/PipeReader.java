package io.zmux.carrier;

import io.zmux.Chunk;
import java.nio.ByteBuffer;

/**
 * The receiving half of a pipe input. zmux calls {@link #start} once and then {@link #close}
 * exactly once; the reader pushes what arrives into the sink.
 */
public interface PipeReader {
    /** Receives a pipe's bytes; calls must not overlap (one at a time, in order). */
    interface Sink {
        /**
         * Transfers an owned chunk without copying; with {@link Framing#MESSAGES} each call is one
         * complete message. zmux may keep the payload after parsing and closes the chunk once it
         * no longer needs it. consumed runs once parsing is done, independently of payload
         * retention: it is the carrier's backpressure. A reader delivers more before earlier
         * chunks were consumed only within the receive memory it declared
         * ({@link PipeIo.Capabilities#receiveMemory()}). The queue charges the backing capacity
         * and a descriptor per delivery. Declare receive memory for backing larger than a
         * reader batch, including when the chunk exposes only a small range of it.
         */
        void onChunk(Chunk chunk, Runnable consumed);

        /**
         * Lends data, a buffer the reader keeps: zmux copies it before returning and never touches
         * data afterwards, so the reader may reuse the buffer at once. As for {@link #onChunk},
         * consumed runs once parsing is done. Until then, the copy counts against the reader's
         * declared receive memory. Use {@link #onChunk} to transfer a buffer without a copy.
         */
        default void onLent(ByteBuffer data, Runnable consumed) {
            ByteBuffer copy = ByteBuffer.allocate(data.remaining()).put(data).flip();
            onChunk(Chunk.of(copy), consumed);
        }

        /** The half ended gracefully (FIN, end of body, WebSocket close). */
        void onEnd();

        /** The half failed. */
        void onError(Throwable cause);
    }

    /**
     * Before {@link #start}: a reader of messages may fail a first message longer than limit
     * rather than hold it whole (the admission reads the first message as its prefix), with
     * {@link MessageTooLargeException}; later messages have the carrier's own limit. No effect by
     * default: the message is then read whole and checked.
     */
    default void limitFirstMessage(int limit) {}

    /** Starts delivering into sink; called once. */
    void start(Sink sink);

    /** Stops reading and releases the half; called once, may interrupt a delivery in progress. */
    void close();
}
