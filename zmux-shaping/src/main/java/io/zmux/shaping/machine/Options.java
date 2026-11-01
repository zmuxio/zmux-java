package io.zmux.shaping.machine;

import java.util.Objects;
import java.util.function.Supplier;
import java.util.random.RandomGenerator;

/**
 * Configuration of a machine factory.
 *
 * @param newSource returns the random source of one pipe; null: a generator seeded from
 *     {@link java.security.SecureRandom}
 * @param limits bounds of the definition
 */
public record Options(Supplier<RandomGenerator> newSource, SizeLimits limits) {
    private static final Options DEFAULTS = new Options(null, SizeLimits.NONE);

    public Options { Objects.requireNonNull(limits, "limits"); }

    public static Options defaults() { return DEFAULTS; }

    public Options withNewSource(Supplier<RandomGenerator> s) { return new Options(s, limits); }
    public Options withLimits(SizeLimits l) { return new Options(newSource, l); }
}
