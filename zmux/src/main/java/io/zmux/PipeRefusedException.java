package io.zmux;

/**
 * The session refused a pipe: its carrier runs inside this session (directly or through
 * nesting).
 */
public final class PipeRefusedException extends PipeException {
    public PipeRefusedException() {
        super("pipe refused", null);
    }
}
