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
public record MachineOptions(Supplier<RandomGenerator> newSource, SizeLimits limits, Set<String> features, boolean allowUnshaped) {
    private static final MachineOptions DEFAULTS = new MachineOptions(null, SizeLimits.NONE, Set.of(), false);

    public MachineOptions {
        Objects.requireNonNull(limits, "limits");
        features = Set.copyOf(features);
    }

    public static MachineOptions defaults() { return DEFAULTS; }

    public MachineOptions withNewSource(Supplier<RandomGenerator> s) { return new MachineOptions(s, limits, features, allowUnshaped); }
    public MachineOptions withLimits(SizeLimits l) { return new MachineOptions(newSource, l, features, allowUnshaped); }
    public MachineOptions withFeatures(Set<String> f) { return new MachineOptions(newSource, limits, f, allowUnshaped); }
    public MachineOptions withAllowUnshaped(boolean on) { return new MachineOptions(newSource, limits, features, on); }
}
