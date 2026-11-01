package io.zmux;

/**
 * The session refused a pipe: its carrier runs inside this session (directly or through
 * nesting), or its reservation was retired before the carrier attached.
 */
public final class PipeRefusedException extends PipeException {
    public PipeRefusedException() {
        super("pipe refused", -1);
    }
}
