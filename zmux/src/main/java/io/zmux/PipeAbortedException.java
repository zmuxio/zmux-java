package io.zmux;

/**
 * The pipe was aborted by this endpoint, so its wait ends with this rather than {@link PipeGoneException}: by
 * {@link ZmuxPipe#abort()}, at the bound of its join, or to make room for another pipe or stay within a limit.
 */
public final class PipeAbortedException extends PipeException {
    public PipeAbortedException(PipeId id) {
        super("pipe " + id + " was aborted by this endpoint");
    }
}
