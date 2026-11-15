package io.zmux;

/**
 * A stream preamble (OPEN_INFO, application TLVs, the priority, the carrier mark) larger than
 * the peer accepts: its Maximum Preamble Size once its parameters arrived, 1024 bytes
 * before, never more than 4096 bytes (and always within the carrier record floor, so it fits
 * one record of any pipe). {@link #max()} is that bound. An open or setting that would exceed it is
 * refused at once and changes nothing; a stream whose preamble no longer fits when it takes its
 * identifier fails alone with it, without sending anything.
 */
public final class PreambleTooLargeException extends IllegalArgumentException {
    private final int max;

    /** A preamble beyond max bytes, the largest accepted when it was refused. */
    public PreambleTooLargeException(int max) {
        super("stream preamble too large (max " + max + " bytes)");
        this.max = max;
    }

    /** The largest preamble, in bytes, accepted when this one was refused. */
    public int max() { return max; }
}
