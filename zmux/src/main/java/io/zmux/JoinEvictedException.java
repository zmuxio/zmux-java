package io.zmux;

/** The join evicted by a newer join. */
public final class JoinEvictedException extends PipeException {
    public JoinEvictedException() { super("join evicted by a newer join"); }
}
