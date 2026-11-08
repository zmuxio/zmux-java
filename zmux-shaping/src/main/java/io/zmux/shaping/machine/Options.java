package io.zmux.shaping.machine;

import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.random.RandomGenerator;

/**
 * Configuration of the readers.
 *
 * @param newSource returns a random source: of one pipe, of one policy, of one kind of a bundle; null: a generator seeded
 *     from {@link java.security.SecureRandom}
 * @param limits the bound of the input
 * @param features the {@code requires} names a bundle's candidates may use
 * @param allowUnshaped a bundle's kind without a usable candidate is reported instead of failing the bundle
 */
public record Options(Supplier<RandomGenerator> newSource, SizeLimits limits, Set<String> features, boolean allowUnshaped) {
    private static final Options DEFAULTS = new Options(null, SizeLimits.NONE, Set.of(), false);

    public Options {
        Objects.requireNonNull(limits, "limits");
        features = Set.copyOf(features);
    }

    public static Options defaults() { return DEFAULTS; }

    public Options withNewSource(Supplier<RandomGenerator> s) { return new Options(s, limits, features, allowUnshaped); }
    public Options withLimits(SizeLimits l) { return new Options(newSource, l, features, allowUnshaped); }
    public Options withFeatures(Set<String> f) { return new Options(newSource, limits, f, allowUnshaped); }
    public Options withAllowUnshaped(boolean on) { return new Options(newSource, limits, features, on); }
}
