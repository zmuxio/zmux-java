package io.zmux;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The opening information of a stream, given once when the stream is opened and read on the
 * accepting side (the stream preamble). Byte arrays are copied in and out and compared by content.
 *
 * @param openInfo the OPEN_INFO value (an address, a request line), or null
 * @param tlvs application TLVs, types 0x1000 to 0x3fff, in order
 * @param pipeCarrier the stream carries a pipe of a nested session (PIPE_CARRIER)
 */
public record Preamble(byte[] openInfo, List<Tlv> tlvs, boolean pipeCarrier) {
    /** No opening information. */
    public static final Preamble EMPTY = new Preamble(null, List.of(), false);

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

    public Preamble {
        openInfo = openInfo == null ? null : openInfo.clone();
        tlvs = List.copyOf(tlvs);
    }

    /** A preamble with only OPEN_INFO. */
    public static Preamble openInfo(byte[] info) {
        return new Preamble(Objects.requireNonNull(info, "info"), List.of(), false);
    }

    @Override public byte[] openInfo() { return openInfo == null ? null : openInfo.clone(); }

    /** OPEN_INFO without a copy, for the session, which copies what it keeps; null without one. */
    byte[] rawOpenInfo() { return openInfo; }

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
