package io.zmux.carrier;

/**
 * The receiving half of a pipe input. zmux calls {@link #start} once and then {@link #close}
 * exactly once; the reader pushes what arrives into the sink.
 */
public interface PipeReader {
    /** Receives a pipe's bytes; calls must not overlap (one at a time, in order). */
    interface Sink {
        /**
         * Lends a batch of bytes; with {@link Framing#MESSAGES} each call is one complete
         * message. zmux copies or parses what it needs and then runs release exactly once
         * (also when it discards the batch). This is the carrier's backpressure: a reader
         * delivers more before earlier batches are released only within the receive memory
         * it declared ({@link PipeIo.Capabilities#receiveMemory()}); zmux fails a carrier
         * that queues beyond that.
         */
        void onData(java.nio.ByteBuffer data, Runnable release);

        /**
         * Transfers an owned chunk without copying; zmux may keep the payload after parsing.
         * consumed runs once parsing is done, independently of payload retention, and plays
         * the role of release for backpressure.
         */
        default void onChunk(io.zmux.Chunk chunk, Runnable consumed) {
            onData(chunk.buffer(), () -> { chunk.close(); consumed.run(); });
        }

        /** The half ended gracefully (FIN, end of body, WebSocket close). */
        void onEnd();

        /** The half failed. */
        void onError(Throwable cause);
    }

    /** Starts delivering into sink; called once. */
    void start(Sink sink);

    /** Stops reading and releases the half; called once, may interrupt a delivery in progress. */
    void close();
}
