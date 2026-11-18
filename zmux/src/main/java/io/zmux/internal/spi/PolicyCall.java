package io.zmux.internal.spi;

import io.zmux.PoolDecision;
import io.zmux.PoolPolicy;
import io.zmux.PoolView;
import java.util.Objects;

/**
 * The one place both pools ask their policy. A look whose policy throws anything but a {@link VirtualMachineError}, or
 * returns null, is rejected as a whole: the pool applies {@link PoolDecision#KEEP} for it and reports the error.
 */
public final class PolicyCall {
    private PolicyCall() {}

    /**
     * What a look came to.
     *
     * @param decision the policy's decision, {@link PoolDecision#KEEP} when it was rejected
     * @param rejected why it was rejected; null when the decision stands
     */
    public record Outcome(PoolDecision decision, Throwable rejected) {}

    /** Asks policy about view. */
    public static Outcome decide(PoolPolicy policy, PoolView view) {
        try {
            return new Outcome(Objects.requireNonNull(policy.decide(view), "the pool policy returned null"), null);
        } catch (VirtualMachineError fatal) {
            throw fatal;
        } catch (Throwable rejected) {
            return new Outcome(PoolDecision.KEEP, rejected);
        }
    }

    /** Hands a rejected look to callback; a failure of the callback itself changes nothing (a VM error still propagates). */
    public static void report(java.util.function.Consumer<Throwable> callback, Throwable rejected) {
        try { callback.accept(rejected); }
        catch (VirtualMachineError fatal) { throw fatal; }
        catch (Throwable ignored) { /* the callback's own failure changes nothing */ }
    }
}
