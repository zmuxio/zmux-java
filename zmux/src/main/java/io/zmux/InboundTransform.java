package io.zmux;

import io.zmux.carrier.PipeIo;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * A listener's inbound transform ({@link ListenerConfig.Builder#inboundTransform}): it takes
 * each carrier the listener takes, after the admission and before any session processing, and
 * returns the pipe input the listener continues with, a layer over it or the carrier itself.
 *
 * <p>It is called once for every carrier, whatever entry fed it (the listener's sockets, {@link
 * ZmuxListener#handle}, the HTTP, WebSocket, exchange and QUIC carriers), each one-way half of a
 * split bootstrap included ({@link JoinHint#pairing()} is set, the carrier has one half only),
 * on the listener's event loop: it must not block, and returns a stage that completes later
 * when the layer takes time (a handshake run on a virtual thread, say). The carrier keeps its
 * slot of {@link ListenerConfig#maxPendingPerSource()} meanwhile, and the session's handshake
 * timeout bounds it: a stage not complete by the deadline is cancelled ({@code
 * toCompletableFuture().cancel(false)}) and the carrier closed, as when the listener closes.
 * The first-record timeout runs from its completion.
 *
 * <p>The carrier given is a pipe input of the carrier's framing, with every byte the
 * admission read and did not skip in front of its input; it is the listener's to close when
 * the transform fails, and the transform's (or the returned layer's) otherwise. The result must
 * have the same halves (directions) and framing; the listener sets the carrier's kind, label
 * and fate group on it, and keeps its other properties ({@link PipeIo#withWriteOverhead}, the
 * exporter of the layer's own handshake). A transform that throws, a stage that fails, a null
 * result or one of another kind closes the carrier and counts in {@link
 * ListenerStats#setupFailures()}. Once a transform was called the carrier counts as taken
 * ({@link ZmuxListener.Handled#taken()}): an HTTP entry no longer hands a refused carrier to the
 * host, as the layer may have read and written on it.
 *
 * <p>A layer must map every write on the pipe input it returns to exactly one write on the
 * carrier, never merging writes: on a message carrier one message each. {@link
 * io.zmux.carrier.PipeChannel} presents a pipe input as a blocking channel that keeps this.
 */
@FunctionalInterface
public interface InboundTransform {
    /**
     * Transforms a carrier.
     *
     * @param carrier the carrier, as described above
     * @param context the carrier's hint and admission value, and the deadline
     */
    CompletionStage<PipeIo> transform(PipeIo carrier, Context context);

    /**
     * What a transform knows of its carrier.
     *
     * @param hint the carrier's hint: its addresses and request, and, for one half of a split
     *     bootstrap, its pairing
     * @param admitted the value of the admission that accepted the carrier ({@link
     *     Admission.Decision#value()}); empty without one
     * @param deadline when the transform is due
     */
    record Context(JoinHint hint, Optional<Object> admitted, Admission.Deadline deadline) {
        public Context {
            Objects.requireNonNull(hint, "hint");
            Objects.requireNonNull(admitted, "admitted");
            Objects.requireNonNull(deadline, "deadline");
        }
    }
}
