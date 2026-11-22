package io.zmux;

/** The pipe was retired (or never joined); its handle no longer controls anything. */
public sealed class PipeGoneException extends PipeException permits PipeAbortedException {
    PipeGoneException(String message) { super(message); }
    public PipeGoneException(PipeId id) {
        super("pipe " + id + " is gone");
    }
}
