package io.zmux.internal.loop;

import io.zmux.carrier.PipeIo;

/**
 * A pipe-input half whose carrier can run as a {@link Transport} on its own host: a session
 * there reads and writes it directly, without the PipeIo callbacks and their hand-offs.
 * Implemented by the halves of carriers that live on a session host's thread (QUIC streams
 * on their connection's loop).
 */
public interface NativePipe {
    /**
     * The transport of io, on its host and not started; null when io's halves are not this
     * carrier's alone or the transport was taken already (io then works through its halves).
     */
    Transport claim(PipeIo io);
}
