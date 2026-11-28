package io.zmux;

import java.time.Duration;
import java.util.Objects;

/** Options for one logical datagram on the local hop. A null component inherits its default. */
public record DatagramSendOptions(Deadline deadline, Carriage carriage, Copies copies, Boolean urgent, Boolean distinct) {
    public static final DatagramSendOptions INHERIT = new DatagramSendOptions(null, null, null, null, null);

    /** Selects whether records can carry the datagram when there is no suitable native path. */
    public enum Carriage { PREFER_NATIVE, REQUIRE_NATIVE }

    /** Controls how many copies the sender may put on eligible paths. */
    public sealed interface Copies {
        /** Sends at most one copy. */
        enum Single implements Copies { INSTANCE }
        /** Lets the sender choose the number of copies from path feedback. */
        enum Adaptive implements Copies { INSTANCE }
        /** Requests up to count copies. */
        record Duplicate(int count) implements Copies {
            public Duplicate {
                if (count < 1) throw new IllegalArgumentException("Duplicate needs a positive copy count");
            }
        }
    }

    /** The local expiry of a datagram awaiting transmission. */
    public sealed interface Deadline {
        /** No application deadline. The queue still applies its normal sojourn policy. */
        enum None implements Deadline { INSTANCE }

        /** A lifetime starting when the local sender first submits the datagram. */
        record After(Duration ttl) implements Deadline {
            public After {
                Objects.requireNonNull(ttl);
                if (ttl.isNegative()) throw new IllegalArgumentException("negative datagram lifetime");
                try {
                    if (ttl.toNanos() == Long.MAX_VALUE) throw new IllegalArgumentException("datagram lifetime out of range");
                } catch (ArithmeticException overflow) {
                    throw new IllegalArgumentException("datagram lifetime out of range", overflow);
                }
            }
        }

        /** An absolute deadline measured by the sending session's {@link ClockDomain#now()}. */
        record At(long nanoTime) implements Deadline {
            public At {
                if (nanoTime < 0 || nanoTime == Long.MAX_VALUE) throw new IllegalArgumentException("datagram deadline out of range");
            }
        }
    }

    /** Applies a less specific set of options. */
    public DatagramSendOptions inheriting(DatagramSendOptions defaults) {
        if (this == INHERIT) return defaults;
        if (defaults == INHERIT) return this;
        return new DatagramSendOptions(deadline != null ? deadline : defaults.deadline,
                carriage != null ? carriage : defaults.carriage,
                copies != null ? copies : defaults.copies,
                urgent != null ? urgent : defaults.urgent,
                distinct != null ? distinct : defaults.distinct);
    }
}
