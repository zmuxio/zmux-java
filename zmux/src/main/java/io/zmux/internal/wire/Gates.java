package io.zmux.internal.wire;

import io.zmux.carrier.VarInt;

/**
 * Which frames a receiver accepts in a session (SPEC.md Sections 4.2 and 5.5):
 * features in use and unilateral announcements of the receiver.
 *
 * @param maxDatagram the receiver's max_datagram_frame_size; 0 = none
 * @param lanes lanes available toward the receiver
 */
public record Gates(long maxDatagram, boolean resetStreamAt, boolean goAway, boolean priority,
                    boolean multipath, boolean pipeAck, boolean lanes) {

    /** Returns normally when the decoded frame's feature is in use (SPEC.md Section 4.1 rule 3, RFC 9221 Section 3). */
    public void check(Frame f) throws WireException {
        boolean ok = switch (f.kind) {
            case RESET_STREAM_AT -> resetStreamAt;
            case DATAGRAM -> {
                if (maxDatagram == 0) {
                    throw new WireException(TransportError.PROTOCOL_VIOLATION, f.type, "DATAGRAM not advertised", "");
                }
                long size = 1 + f.dataLen + (f.hasLength ? VarInt.length(f.dataLen) : 0);
                if (Long.compareUnsigned(size, maxDatagram) > 0) {
                    throw new WireException(TransportError.PROTOCOL_VIOLATION, f.type, "DATAGRAM larger than advertised", "");
                }
                yield true;
            }
            case GOAWAY -> goAway;
            case PRIORITY_UPDATE -> priority;
            case PIPE_JOIN, PIPE_PING, PIPE_ABANDON, PIPE_STATUS -> multipath;
            case PIPE_ACK -> multipath || pipeAck;
            case PIPE_LANE -> lanes;
            default -> true;
        };
        if (!ok) throw WireException.frame(f.type, f.kind.wireName() + " not in use");
    }
}
