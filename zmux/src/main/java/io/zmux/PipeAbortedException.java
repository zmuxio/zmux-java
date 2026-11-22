package io.zmux;

/**
 * A pipe-gone error identifying a local abort. The endpoint aborted the pipe by
 * {@link ZmuxPipe#abort()}, at the bound of its join, or to make room for another pipe or stay within a limit.
 */
public final class PipeAbortedException extends PipeGoneException {
    public PipeAbortedException(PipeId id) {
        super("pipe " + id + " was aborted by this endpoint");
    }
}
