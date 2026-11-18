package io.zmux;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The opening information of a stream, given once when the stream is opened and read on the
 * accepting side (the stream preamble), built with {@link #builder()}. Byte arrays are copied in and out and
 * compared by content.
 */
public final class Preamble {
    /** No opening information. */
    public static final Preamble EMPTY = new Preamble(null, List.of(), false);

    private final byte[] openInfo;
    private final List<Tlv> tlvs;
    private final boolean pipeCarrier;

    /** An application TLV; the value is copied in and out and compared by content. */
    public record Tlv(long type, byte[] value) {
        public Tlv {
            value = Objects.requireNonNull(value, "value").clone();
            if (!io.zmux.internal.wire.Preamble.isApplication(type)) {
                throw new IllegalArgumentException("application TLV types are 0x1000 to 0x3fff, not GREASE: 0x" + Long.toHexString(type));
            }
        }

        @Override public byte[] value() { return value.clone(); }

        /** The value without a copy, for the session, which copies what it keeps. */
        byte[] raw() { return value; }

        @Override
        public boolean equals(Object o) { return o instanceof Tlv t && type == t.type && Arrays.equals(value, t.value); }

        @Override
        public int hashCode() { return Long.hashCode(type) * 31 + Arrays.hashCode(value); }

        @Override
        public String toString() { return "Tlv[type=0x" + Long.toHexString(type) + ", " + value.length + " bytes]"; }
    }

    Preamble(byte[] openInfo, List<Tlv> tlvs, boolean pipeCarrier) {
        this.openInfo = openInfo == null ? null : openInfo.clone();
        this.tlvs = List.copyOf(tlvs);
        this.pipeCarrier = pipeCarrier;
    }

    public static Builder builder() { return new Builder(); }

    /** The OPEN_INFO value (an address, a request line), or null without one. */
    public byte[] openInfo() { return openInfo == null ? null : openInfo.clone(); }

    /** The application TLVs, types 0x1000 to 0x3fff, in order. */
    public List<Tlv> tlvs() { return tlvs; }

    /** Whether the stream carries a pipe of a nested session (PIPE_CARRIER). */
    public boolean isPipeCarrier() { return pipeCarrier; }

    /** OPEN_INFO without a copy, for the session, which copies what it keeps; null without one. */
    byte[] rawOpenInfo() { return openInfo; }

    /** Builds a {@link Preamble}; nothing set is {@link #EMPTY}. */
    public static final class Builder {
        private byte[] openInfo;
        private final List<Tlv> tlvs = new java.util.ArrayList<>();
        private boolean pipeCarrier;

        private Builder() {}

        /** The OPEN_INFO value (an address, a request line); copied. */
        public Builder openInfo(byte[] info) {
            openInfo = Objects.requireNonNull(info, "info").clone();
            return this;
        }

        /** Adds an application TLV after those added before; repeatable. */
        public Builder tlv(long type, byte[] value) {
            tlvs.add(new Tlv(type, value));
            return this;
        }

        /** Adds an application TLV after those added before. */
        public Builder tlv(Tlv t) {
            tlvs.add(Objects.requireNonNull(t, "t"));
            return this;
        }

        /** Marks the stream as the carrier of a nested session's pipe (PIPE_CARRIER). */
        public Builder pipeCarrier() {
            pipeCarrier = true;
            return this;
        }

        public Preamble build() { return new Preamble(openInfo, tlvs, pipeCarrier); }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Preamble p && Arrays.equals(openInfo, p.openInfo) && tlvs.equals(p.tlvs) && pipeCarrier == p.pipeCarrier;
    }

    @Override
    public int hashCode() { return (Arrays.hashCode(openInfo) * 31 + tlvs.hashCode()) * 31 + Boolean.hashCode(pipeCarrier); }

    @Override
    public String toString() {
        return "Preamble[openInfo=" + (openInfo == null ? "none" : openInfo.length + " bytes") + ", tlvs=" + tlvs + ", pipeCarrier=" + pipeCarrier + "]";
    }
}
