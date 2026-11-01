package io.zmux.shaping.machine;

import io.zmux.shaping.machine.Definition.Dist;
import io.zmux.shaping.machine.Definition.Value;
import java.security.SecureRandom;
import java.util.IdentityHashMap;
import java.util.random.RandomGenerator;
import java.util.random.RandomGeneratorFactory;

/** Draws values from a random source; {@link #sample} keeps them per scope. */
abstract class Sampler {
    final RandomGenerator r;
    private IdentityHashMap<Value, double[]> bags;
    private final IdentityHashMap<Value, Integer> bagLen = new IdentityHashMap<>();

    Sampler(RandomGenerator r) { this.r = r; }

    private static final RandomGeneratorFactory<RandomGenerator> DEFAULT = RandomGeneratorFactory.of("L64X256MixRandom");
    /** Bytes covering the whole L64X256MixRandom state: a, s and the 256-bit xoshiro part. */
    static final int SEED_BYTES = (64 + 64 + 256) / 8;
    private static final SecureRandom SEEDS = new SecureRandom();

    /** The options' source, or a generator whose full state is seeded from {@link SecureRandom}. */
    static RandomGenerator source(Options o) {
        RandomGenerator r = o.newSource() == null ? null : o.newSource().get();
        return r != null ? r : seeded(freshSeed());
    }

    /** A fresh full-state seed from {@link SecureRandom}. */
    static byte[] freshSeed() {
        byte[] seed = new byte[SEED_BYTES];
        SEEDS.nextBytes(seed);
        return seed;
    }

    /** The default generator built from seed. */
    static RandomGenerator seeded(byte[] seed) { return DEFAULT.create(seed); }

    /** x rounded to an integral value, halves away from zero. */
    static double round(double x) {
        double a = Math.abs(x), f = Math.floor(a);
        return Math.copySign(a - f >= 0.5 ? f + 1 : f, x);
    }

    /** v, kept per its scope; mixture components are sampled through this. */
    abstract double sample(Value v);

    /** Samples v and applies its bounds. */
    double draw(Value v) {
        double x;
        if (v.constant() != null) x = v.constant();
        else if (v.range() != null) x = v.range()[0] + (v.range()[1] - v.range()[0]) * r.nextDouble();
        else x = dist(v);
        if (v.min() != null) x = Math.max(x, v.min());
        if (v.max() != null) x = Math.min(x, v.max());
        return x;
    }

    private double u() { return 1 - r.nextDouble(); } // (0, 1]

    private double dist(Value v) {
        Dist d = v.dist();
        double[] p = d.params();
        switch (d.kind()) {
            case UNIFORM: return p[0] + (p[1] - p[0]) * r.nextDouble();
            case LOGUNIFORM: return Math.exp(Math.log(p[0]) + (Math.log(p[1]) - Math.log(p[0])) * r.nextDouble());
            case EXPONENTIAL: return r.nextExponential() / p[0];
            case NORMAL: return p[0] + p[1] * r.nextGaussian();
            case LOGNORMAL: return Math.exp(p[0] + p[1] * r.nextGaussian());
            case WEIBULL: return p[0] * Math.pow(-Math.log(u()), 1 / p[1]);
            case PARETO: return p[0] / Math.pow(u(), 1 / p[1]);
            case GAMMA: return gamma(p[0]) * p[1];
            case BETA: {
                double[] x = gammaLog(p[0]), y = gammaLog(p[1]);
                double t = x[0] + y[0];
                if (t > 0 && !Double.isInfinite(t)) return x[0] / t;
                // Both variates underflowed (small shapes): their ratio from the logarithms.
                return 1 / (1 + Math.exp(y[1] - x[1]));
            }
            case GEOMETRIC: return p[0] == 1 ? 1 : Math.ceil(Math.log(u()) / Math.log(1 - p[0]));
            case BINOMIAL: return binomial(p[0], p[1]);
            case POISSON: return poisson(p[0]);
            case EMPIRICAL: return d.points()[pick(d.weights())];
            case BAG: {
                if (bags == null) bags = new IdentityHashMap<>();
                double[] b = bags.computeIfAbsent(v, k -> d.points().clone());
                int len = bagLen.getOrDefault(v, 0);
                if (len == 0) { System.arraycopy(d.points(), 0, b, 0, b.length); len = b.length; }
                int i = r.nextInt(len);
                double x = b[i];
                b[i] = b[len - 1];
                bagLen.put(v, len - 1);
                return x;
            }
            default: return sample(d.of().get(pick(d.weights()))); // mixture
        }
    }

    /** Trials a binomial sample takes one by one at most. */
    private static final double DIRECT_TRIALS = 64;
    /** The largest mean a Poisson sample takes by the product of uniforms. */
    private static final double DIRECT_MEAN = 30;

    /**
     * Binomial(n, p) for an integral n in [0, 2^53] (Knuth, TAOCP 3.4.1 F): while n is large, the median
     * order statistic of n uniforms, a Beta variate, halves the trials; O(log n) steps, then at most
     * DIRECT_TRIALS Bernoulli trials.
     */
    private double binomial(double n, double p) {
        double k = 0;
        while (n > DIRECT_TRIALS) {
            // Exact at the ends, where a rounded Beta variate could skip trials.
            if (p <= 0) return k;
            if (p >= 1) return k + n;
            double a = 1 + Math.floor(n / 2), b = n + 1 - a;
            double x = gamma(a);
            x = x / (x + gamma(b));
            if (x >= p) { n = a - 1; p /= x; }
            else { k += a; n = b - 1; p = (p - x) / (1 - x); }
        }
        for (long i = 0; i < (long) n; i++) if (r.nextDouble() < p) k++;
        return k;
    }

    /**
     * Poisson(mean) for a mean in (0, 2^53] (Knuth, TAOCP 3.4.1 F): while the mean is large, a Gamma variate
     * takes 7/8 of it at once, or the rest is binomial; O(log mean) steps, then the product of uniforms for a
     * mean of at most DIRECT_MEAN.
     */
    private double poisson(double mean) {
        double k = 0;
        while (mean > DIRECT_MEAN) {
            double m = Math.floor(mean * 7 / 8);
            double x = gamma(m);
            if (x >= mean) return k + binomial(m - 1, mean / x);
            k += m;
            mean -= x;
        }
        double l = Math.exp(-mean), t = r.nextDouble();
        while (t > l) { k++; t *= r.nextDouble(); }
        return k;
    }

    private int pick(double[] w) {
        double t = 0;
        for (double x : w) t += x;
        t *= r.nextDouble();
        for (int i = 0; i < w.length; i++) {
            if (t < w[i]) return i;
            t -= w[i];
        }
        return w.length - 1;
    }

    /** Gamma(a, 1) (Marsaglia and Tsang). */
    private double gamma(double a) {
        if (a < 1) return gamma(a + 1) * Math.pow(1 - r.nextDouble(), 1 / a);
        return gamma1(a);
    }

    /**
     * Gamma(a, 1) and its logarithm, which stays finite for a shape below 1 where the variate
     * underflows to 0.
     */
    private double[] gammaLog(double a) {
        if (a < 1) {
            double g = gamma(a + 1), u = 1 - r.nextDouble();
            return new double[] {g * Math.pow(u, 1 / a), Math.log(g) + Math.log(u) / a};
        }
        double x = gamma1(a);
        return new double[] {x, Math.log(x)};
    }

    /** Gamma(a, 1) for a shape of at least 1. */
    private double gamma1(double a) {
        double dd = a - 1.0 / 3, c = 1 / Math.sqrt(9 * dd);
        while (true) {
            double x = r.nextGaussian(), v = 1 + c * x;
            if (v <= 0) continue;
            v = v * v * v;
            double uu = 1 - r.nextDouble();
            if (Math.log(uu) < 0.5 * x * x + dd - dd * v + dd * Math.log(v)) return dd * v;
        }
    }
}
