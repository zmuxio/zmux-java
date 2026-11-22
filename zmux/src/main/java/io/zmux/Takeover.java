package io.zmux;

import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.Optional;

/**
 * A carrier the admission hands to the host ({@link ListenerConfig#fallback()}): on {@link AdmissionVerdict#TAKEOVER}, on byte-stream {@link AdmissionVerdict#FALLBACK},
 * or on a prefix {@link Admission.Builder#fallbackOnPeekTimeout} gave up on. What was read of it is replayed: the
 * bytes of a byte stream, the first message of a message-framed carrier (always a whole, non-empty binary message: a
 * carrier whose first read fails or returns an empty message is closed without a decision).
 *
 * <p>The host may use the carrier until its fallback stage completes (or {@link Admission.Fallback#serve}
 * returns a null stage). Withdrawal during setup or shutdown of the carrier’s event loop can close it earlier.
 * Withdrawal leaves the host’s stage running. Calling {@link #detach()} before completion or withdrawal gives the
 * host ownership of the carrier and its adapter’s QUIC connection or HTTP request, to close once done.
 *
 * <p>Two views share one replay, and the host uses one of them: {@link #pipe()} for every carrier (with
 * {@link #messages()} as its typed form on a message-framed carrier), {@link #socket()} for a plain TCP carrier. Once
 * the pipe's reader started or it wrote, the socket view is no longer offered; once the socket view was taken, the
 * pipe's halves fail. Each half is closed once, whoever closes it: the host, or zmux when the stage completes without
 * a detach; a half the host closed is not closed again.
 */
public interface Takeover {
    /** The carrier's metadata, as the decider saw it last. */
    Admission.CarrierInfo info();

    /**
     * The {@link Admission.Decision#value()} of the {@link AdmissionVerdict#FALLBACK} that handed the carrier over
     * ({@link Admission.Decision#fallback(Object)}), null without one or after
     * {@link Admission.Builder#fallbackOnPeekTimeout}: the host's routing of the carrier, as an accepted carrier's value
     * is its session's.
     */
    Object value();

    /**
     * The carrier as a pipe input with what was read replayed in front of its input: with
     * {@link io.zmux.carrier.Framing#MESSAGES} its reader delivers the first message again first, then the
     * messages that follow. The host uses it as zmux uses a pipe input: it starts the reader once, has at most one
     * write in flight, and closes (or aborts) each half once; closing the sending half alone half-closes the
     * carrier, and its receiving half reads on to the end. Its sink and write completions run on a zmux event loop
     * and must not block. It carries the carrier's own properties: its label, kind, variant, the identity of its
     * multi-stream connection ({@link PipeIo#carrierConn()}, a QUIC connection), and every capability of
     * {@link PipeIo.Capabilities} but the lane opener (lanes run zmux sessions); its writer reports the carrier's
     * {@link PipeWriter#unsent()}. The same view on every call.
     */
    PipeIo pipe();

    /**
     * The halves of a message-framed carrier ({@link io.zmux.carrier.Framing#MESSAGES}, a WebSocket), the halves of
     * {@link #pipe()}: each chunk the reader delivers is one whole message, the first message again first, and each
     * record of a write is one message. Empty for a byte stream.
     */
    Optional<Messages> messages();

    /**
     * The carrier as its socket, for a plain TCP carrier whose pipe view is untouched: the channel, in blocking mode
     * and no longer on any zmux event loop, and the bytes read before the decision, which the host reads before the
     * channel's own. Empty for other carriers (TLS, adapters' carriers) and once the pipe view was used; taken once:
     * a second call is empty.
     */
    default Optional<Socket> socket() { return Optional.empty(); }

    /**
     * Makes the carrier the host's: zmux does not close it when the fallback's stage completes and never touches it
     * again, and its adapter leaves what it runs on to the host; the host must close it once done. It is called during
     * the fallback, which may then hand the carrier to another thread and complete its stage. A second call returns
     * true.
     *
     * @return true when the carrier is the host's; false when the host already closed it (every half of the pipe view,
     *     or the socket view's channel), so there is nothing to detach
     * @throws IllegalStateException once zmux reclaimed the carrier on withdrawal or fallback completion
     */
    boolean detach();

    /**
     * The message halves of a {@link Takeover}; a half the carrier does not have is null.
     *
     * @param reader delivers one whole message per chunk
     * @param writer writes each record of a batch as one message
     */
    record Messages(PipeReader reader, PipeWriter writer) {}

    /**
     * The socket view of a {@link Takeover}.
     *
     * @param channel the carrier's socket, blocking; zmux closes it when the fallback's stage completes without a detach
     * @param replay the bytes read before the decision, read-only, to be read before the channel's own
     */
    record Socket(SocketChannel channel, ByteBuffer replay) {
        public Socket {
            Objects.requireNonNull(channel, "channel");
            replay = Objects.requireNonNull(replay, "replay").asReadOnlyBuffer();
        }
    }
}
