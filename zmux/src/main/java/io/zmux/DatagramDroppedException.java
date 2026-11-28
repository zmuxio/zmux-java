package io.zmux;

import io.zmux.internal.core.CoreException.DatagramDrop;
import java.io.IOException;
import java.util.Objects;

/** A local refusal before a datagram entered the send queue. */
public final class DatagramDroppedException extends IOException {
    @java.io.Serial
    private static final long serialVersionUID = 1L;

    /** The local condition that prevented queueing the datagram. */
    public enum Reason {
        DEADLINE(DatagramDrop.DEADLINE),
        NO_NATIVE_PATH(DatagramDrop.NO_NATIVE_PATH),
        LIMITER(DatagramDrop.LIMITER),
        QUEUE_OVERFLOW(DatagramDrop.QUEUE_OVERFLOW),
        SEQUENCE_EXHAUSTED(DatagramDrop.SEQUENCE_EXHAUSTED);

        private final DatagramDrop outcome;

        Reason(DatagramDrop outcome) { this.outcome = outcome; }

        /** The stable code shared by the language bindings. */
        public String code() { return outcome.code(); }

        static Reason from(DatagramDrop outcome) {
            return switch (outcome) {
                case DEADLINE -> DEADLINE;
                case NO_NATIVE_PATH -> NO_NATIVE_PATH;
                case LIMITER -> LIMITER;
                case QUEUE_OVERFLOW -> QUEUE_OVERFLOW;
                case SEQUENCE_EXHAUSTED -> SEQUENCE_EXHAUSTED;
            };
        }
    }

    private final Reason reason;

    public DatagramDroppedException(Reason reason) {
        super("datagram dropped: " + Objects.requireNonNull(reason).code());
        this.reason = reason;
    }

    public Reason reason() { return reason; }
}
