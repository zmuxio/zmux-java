package io.zmux.internal.exchange;

import io.zmux.Admission;
import io.zmux.JoinHint;
import io.zmux.adapter.Handled;
import io.zmux.adapter.Slot;
import io.zmux.adapter.Upgrade;
import io.zmux.carrier.PipeIo;
import java.util.function.UnaryOperator;

/** The authenticated request handoff, qualified to the HTTP module. */
public final class ExchangeAccess {
    private ExchangeAccess() {}

    /** An internal capability authorizing a paired exchange. */
    public static final class Proof {
        private Proof() {}
    }

    private static final Proof VERIFIED = new Proof();

    /** Hands an authenticated split request to its paired exchange slot. */
    @FunctionalInterface
    public interface Handoff {
        Handled handle(Slot slot, PipeIo io, JoinHint hint, Admission.Fallback fallback, Upgrade response, Proof proof);
    }

    private static final java.util.concurrent.atomic.AtomicReference<Handoff> SLOT =
            new java.util.concurrent.atomic.AtomicReference<>();
    private static final java.util.concurrent.atomic.AtomicReference<UnaryOperator<Admission.CarrierInfo>> INFO =
            new java.util.concurrent.atomic.AtomicReference<>();

    public static void installSlot(Handoff handoff) {
        if (!SLOT.compareAndSet(null, handoff)) throw new IllegalStateException("exchange slot access already installed");
    }

    public static void installInfo(UnaryOperator<Admission.CarrierInfo> verified) {
        if (!INFO.compareAndSet(null, verified)) throw new IllegalStateException("exchange metadata access already installed");
    }

    /** Call only after the complete exchange envelope has been verified. */
    public static Handled handle(Slot slot, PipeIo io, JoinHint hint, Admission.Fallback fallback, Upgrade response) {
        return SLOT.get().handle(slot, io, hint, fallback, response, VERIFIED);
    }

    public static Admission.CarrierInfo verified(Admission.CarrierInfo info, Proof proof) {
        if (proof != VERIFIED) throw new IllegalArgumentException("missing exchange proof");
        return INFO.get().apply(info);
    }
}
