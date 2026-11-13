package io.zmux.carrier;

import java.time.Duration;

/**
 * Asked for the size and timing of each handshake message a side writes on an Establish or a Join. Called outside the
 * session, from concurrent handshakes; it does not block and sees no sizes. A runtime exception, or no plan, leaves the
 * message as it is.
 */
@FunctionalInterface
public interface HandshakeShaper {
    /**
     * One message.
     *
     * @param kind the carrier's kind; null when it declares none
     * @param handshake {@code establish} or {@code join}
     * @param message {@code hello}, {@code hello_early}, {@code reply}, {@code finish} or {@code confirm}
     */
    record View(io.zmux.CarrierKind kind, String handshake, String message) {}

    /**
     * The size and timing of one unit written to a carrier, in bytes on the carrier.
     *
     * @param padTo pad a shorter unit up to this length; 0: none
     * @param padMultiple round up to a multiple of this; 0: none
     * @param pad bytes added after both; 0: none
     * @param delay the wait before the write; null: none
     */
    record UnitPlan(int padTo, int padMultiple, int pad, Duration delay) {
        /** No padding and no delay. */
        public static final UnitPlan NONE = new UnitPlan(0, 0, 0, null);
    }

    /** The plan of a message; null when it has none. */
    UnitPlan plan(View view);
}
