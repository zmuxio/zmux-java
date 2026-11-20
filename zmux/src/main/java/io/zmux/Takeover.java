package io.zmux;

import io.zmux.carrier.PipeIo;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.Optional;

/**
 * A carrier the admission hands to the host ({@link ListenerConfig#fallback()}): on {@link AdmissionVerdict#FALLBACK},
 * or on a prefix {@link Admission.Builder#fallbackOnPeekTimeout} gave up on. What was read of it is replayed: the
 * bytes of a byte stream, the first message of a message-framed carrier (always a whole binary message: a carrier
 * whose first message fails is closed without a decision). The carrier is the host's until the stage its
 * {@link Admission.Fallback} returned completes; zmux then closes it.
 *
 * <p>Two views share one replay, and the host uses one of them: {@link #pipe()} for every carrier, {@link #socket()}
 * for a plain TCP carrier. Once the pipe's reader started or it wrote, the socket view is no longer offered; once the
 * socket view was taken, the pipe's halves fail.
 */
public interface Takeover {
    /** The carrier's metadata, as the decider saw it last. */
    Admission.CarrierInfo info();

    /**
     * The carrier as a pipe input with what was read replayed in front of its input: with
     * {@link io.zmux.carrier.Framing#MESSAGES} its reader delivers the first message again first, then the
     * messages that follow. The host uses it as zmux uses a pipe input: it starts the reader once, has at most one
     * write in flight, and closes (or aborts) each half once. Its sink and write completions run on a zmux event
     * loop and must not block. The application protocol, and the identity of the carrier's multi-stream connection
     * ({@link PipeIo#carrierConn()}, a QUIC connection), are the carrier's. The same view on every call.
     */
    PipeIo pipe();

    /**
     * The carrier as its socket, for a plain TCP carrier whose pipe view is untouched: the channel, in blocking mode
     * and no longer on any zmux event loop, and the bytes read before the decision, which the host reads before the
     * channel's own. Empty for other carriers (TLS, adapters' carriers) and once the pipe view was used; taken once:
     * a second call is empty.
     */
    default Optional<Socket> socket() { return Optional.empty(); }

    /**
     * The socket view of a {@link Takeover}.
     *
     * @param channel the carrier's socket, blocking; zmux closes it when the fallback's stage completes
     * @param replay the bytes read before the decision, read-only, to be read before the channel's own
     */
    record Socket(SocketChannel channel, ByteBuffer replay) {
        public Socket {
            Objects.requireNonNull(channel, "channel");
            replay = Objects.requireNonNull(replay, "replay").asReadOnlyBuffer();
        }
    }
}
