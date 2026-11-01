package io.zmux;

/**
 * A stream preamble (OPEN_INFO, application TLVs, the priority, the carrier mark) larger than
 * the peer accepts: its Maximum Preamble Size once its parameters arrived, the remembered one
 * or 1024 bytes before, never more than 4096 bytes. An open or setting that would exceed it is
 * refused at once and changes nothing; a stream whose preamble no longer fits when it takes its
 * identifier fails alone with it, without sending anything.
 */
public final class PreambleTooLargeException extends IllegalArgumentException {
    public PreambleTooLargeException() {
        super("stream preamble too large");
    }
}
