package io.zmux;

import io.zmux.internal.wire.Capsule;
import java.util.function.BiConsumer;
import io.zmux.helper.Limiter;
import java.util.Objects;
import java.util.Optional;

/**
 * Relay policy of {@link Zmux#splice} and {@link Zmux#splicePackets}, built with {@link #builder()};
 * {@link #DEFAULT} sets nothing.
 */
public final class SpliceOptions {
    /** The default chunk, 64 KiB. */
    public static final int DEFAULT_CHUNK = 65536;

    /** No limiter, no abort handle and the default chunk. */
    public static final SpliceOptions DEFAULT = builder().build();

    private final int chunk, maxPacketPayload, maxOpaqueValue;
    private final Limiter aToB, bToA;
    private final SpliceAbort abort;
    private final DatagramSendOptions packetsAToB, packetsBToA;

    private final SpliceHooks hooksAToB, hooksBToA;
    private final SpliceLimits limits;
    private final SpliceMirror mirrorAToB, mirrorBToA;
    private final String aTag, bTag;
    private final SpliceClock clock;
    final BiConsumer<SpliceUnit.Direction, SpliceController.Event> events;
    enum Work { BOUNDARY_READ, HOOK_VIEW, HOOK_DISPATCH, TIMER }
    final BiConsumer<SpliceUnit.Direction, Work> probe;

    private SpliceOptions(Builder b) {
        maxPacketPayload = b.maxPacketPayload;
        maxOpaqueValue = b.maxOpaqueValue;
        chunk = b.chunk; aToB = b.aToB; bToA = b.bToA; abort = b.abort;
        packetsAToB = b.packetsAToB; packetsBToA = b.packetsBToA;
        hooksAToB = b.hooksAToB;
        hooksBToA = b.hooksBToA;
        limits = b.limits == null ? SpliceLimits.defaults(chunk, maxPacketPayload, maxOpaqueValue) : b.limits;
        mirrorAToB = b.mirrorAToB;
        mirrorBToA = b.mirrorBToA;
        aTag = b.aTag;
        bTag = b.bTag;
        clock = b.clock;
        events = b.events;
        probe = b.probe;
    }

    public static Builder builder() { return new Builder(); }

    public int maxPacketPayload() { return maxPacketPayload; }
    public int maxOpaqueValue() { return maxOpaqueValue; }

    /** The maximum reliable byte block. */
    public int chunk() { return chunk; }

    /** The limiter of the direction from a to b; empty: none. */
    public Optional<Limiter> aToB() { return Optional.ofNullable(aToB); }

    /** The limiter of the direction from b to a; empty: none. */
    public Optional<Limiter> bToA() { return Optional.ofNullable(bToA); }

    /** The handle that may end the splice with an application code; empty: none. */
    public Optional<SpliceAbort> abort() { return Optional.ofNullable(abort); }

    public DatagramSendOptions packetsAToB() { return packetsAToB; }
    public DatagramSendOptions packetsBToA() { return packetsBToA; }

    public SpliceHooks hooksAToB() { return hooksAToB; }
    public SpliceHooks hooksBToA() { return hooksBToA; }
    public SpliceLimits limits() { return limits; }
    public Optional<SpliceMirror> mirrorAToB() { return Optional.ofNullable(mirrorAToB); }
    public Optional<SpliceMirror> mirrorBToA() { return Optional.ofNullable(mirrorBToA); }
    public String aTag() { return aTag; }
    public String bTag() { return bTag; }
    SpliceClock clock() { return clock; }

    /** Builds {@link SpliceOptions}; every setting not called keeps its default. */
    public static final class Builder {
        private int chunk = DEFAULT_CHUNK;
        private int maxPacketPayload = Capsule.DEFAULT_MAX_DATAGRAM_PAYLOAD;
        private int maxOpaqueValue = Capsule.MAX_OPAQUE_VALUE;

        /** Sets the largest DATAGRAM payload and opaque capsule Value, independently of record boundaries. */
        public Builder packetLimits(int payload, int opaque) {
            if (payload <= 0 || opaque <= 0) throw new IllegalArgumentException("invalid packet limits");
            maxPacketPayload = payload;
            maxOpaqueValue = opaque;
            return this;
        }
        private Limiter aToB, bToA;
        private SpliceAbort abort;
        private DatagramSendOptions packetsAToB = DatagramSendOptions.INHERIT;
        private DatagramSendOptions packetsBToA = DatagramSendOptions.INHERIT;

        private SpliceHooks hooksAToB = SpliceHooks.NONE, hooksBToA = SpliceHooks.NONE;
        private SpliceLimits limits;
        private SpliceMirror mirrorAToB, mirrorBToA;
        private String aTag = "", bTag = "";
        private SpliceClock clock;
        private BiConsumer<SpliceUnit.Direction, SpliceController.Event> events;

        /** Receives ordered control events on a callback context outside the session owners. */
        public Builder events(BiConsumer<SpliceUnit.Direction, SpliceController.Event> observer) {
            events = Objects.requireNonNull(observer);
            return this;
        }


        private Builder() {}

        /** The most bytes a direction holds between reading and writing, at least 1; default {@link #DEFAULT_CHUNK}. */
        public Builder chunk(int n) {
            if (n < 1) throw new IllegalArgumentException("chunk must be positive");
            chunk = n;
            return this;
        }

        /** The limiter from a to b, charged at the first complete carrier handoff. */
        public Builder aToB(Limiter l) { aToB = Objects.requireNonNull(l, "l"); return this; }

        /** The limiter from b to a, charged at the first complete carrier handoff. */
        public Builder bToA(Limiter l) { bToA = Objects.requireNonNull(l, "l"); return this; }

        /** The handle that may end the splice with an application code; one handle may serve several splices. */
        public Builder abort(SpliceAbort a) { abort = Objects.requireNonNull(a, "a"); return this; }

        /** Sets the local-hop datagram policy for packets flowing from a to b. */
        public Builder packetsAToB(DatagramSendOptions options) {
            packetsAToB = Objects.requireNonNull(options);
            return this;
        }

        /** Sets the local-hop datagram policy for packets flowing from b to a. */
        public Builder packetsBToA(DatagramSendOptions options) {
            packetsBToA = Objects.requireNonNull(options);
            return this;
        }

        public Builder hooksAToB(SpliceHooks hooks) { hooksAToB = Objects.requireNonNull(hooks); return this; }
        public Builder hooksBToA(SpliceHooks hooks) { hooksBToA = Objects.requireNonNull(hooks); return this; }
        public Builder limits(SpliceLimits limits) { this.limits = Objects.requireNonNull(limits); return this; }
        public Builder mirrorAToB(SpliceMirror mirror) { mirrorAToB = Objects.requireNonNull(mirror); return this; }
        public Builder mirrorBToA(SpliceMirror mirror) { mirrorBToA = Objects.requireNonNull(mirror); return this; }

        /** Sets application-provided labels for the two legs. */
        public Builder tags(String a, String b) {
            aTag = Objects.requireNonNull(a);
            bTag = Objects.requireNonNull(b);
            return this;
        }

        private BiConsumer<SpliceUnit.Direction, Work> probe;
        Builder probe(BiConsumer<SpliceUnit.Direction, Work> observer) {
            probe = observer;
            return this;
        }

        Builder clock(SpliceClock clock) { this.clock = Objects.requireNonNull(clock); return this; }

        public SpliceOptions build() { return new SpliceOptions(this); }
    }
}
