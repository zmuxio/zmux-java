package io.zmux.shaping;

/**
 * Returns the {@link Shaper} of an outbound pipe, called when the pipe sends its first record under the
 * current factory; null: the pipe is not shaped. May be called by several sessions at once.
 */
@FunctionalInterface
public interface ShaperFactory {
    Shaper create(ShapedPipe pipe);
}
