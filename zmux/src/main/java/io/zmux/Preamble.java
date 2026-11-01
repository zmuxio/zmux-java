package io.zmux;

import java.util.List;
import java.util.Objects;

/**
 * The opening information of a stream, given once when the stream is opened and read on the
 * accepting side (the stream preamble, EXTENSIONS.md Section 4).
 *
 * @param openInfo the OPEN_INFO value (an address, a request line), or null
 * @param tlvs application TLVs, types 0x1000 to 0x3fff, in order
 * @param pipeCarrier the stream carries a pipe of a nested session (PIPE_CARRIER)
 */
public record Preamble(byte[] openInfo, List<Tlv> tlvs, boolean pipeCarrier) {
    /** No opening information. */
    public static final Preamble EMPTY = new Preamble(null, List.of(), false);

    /** An application TLV. */
    public record Tlv(long type, byte[] value) {
        public Tlv {
            Objects.requireNonNull(value, "value");
            if (!io.zmux.internal.wire.Preamble.isApplication(type)) {
                throw new IllegalArgumentException("application TLV types are 0x1000 to 0x3fff, not GREASE: 0x" + Long.toHexString(type));
            }
        }
    }

    public Preamble {
        tlvs = List.copyOf(tlvs);
    }

    /** A preamble with only OPEN_INFO. */
    public static Preamble openInfo(byte[] info) {
        return new Preamble(info.clone(), List.of(), false);
    }
}
