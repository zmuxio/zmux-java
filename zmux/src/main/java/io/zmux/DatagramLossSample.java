package io.zmux;

/**
 * Finalized outcomes attributable to original datagrams on one outbound path and representation.
 *
 * @param observedAt observation time in the local {@link System#nanoTime()} clock domain
 * @param sourceGeneration positive provider generation for this path and representation.
 *                         Announce replacement with a higher generation and zero finalized attempts
 *                         if outcomes are not yet available.
 * @param finalized original attempts with known final outcomes, excluding unresolved attempts and copies
 * @param lost finalized attempts known to be lost
 */
public record DatagramLossSample(long observedAt, long sourceGeneration, long finalized, long lost) {
    public DatagramLossSample {
        if (sourceGeneration < 0 || lost < 0 || lost > finalized) {
            throw new IllegalArgumentException("invalid finalized datagram outcomes");
        }
    }
}
