package io.zmux;

/** The pipe was retired (or never joined); its handle no longer controls anything. */
public final class PipeGoneException extends PipeException {
    public PipeGoneException(long id) {
        super("pipe " + id + " is gone", id);
    }
}
