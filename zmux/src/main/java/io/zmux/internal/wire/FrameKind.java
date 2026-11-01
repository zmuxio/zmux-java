package io.zmux.internal.wire;

/** Classification of a decoded frame. */
public enum FrameKind {
    PADDING("PADDING"), STREAM("STREAM"), RESET_STREAM("RESET_STREAM"), RESET_STREAM_AT("RESET_STREAM_AT"),
    STOP_SENDING("STOP_SENDING"), MAX_DATA("MAX_DATA"), MAX_STREAM_DATA("MAX_STREAM_DATA"),
    MAX_STREAMS("MAX_STREAMS"), DATA_BLOCKED("DATA_BLOCKED"), STREAM_DATA_BLOCKED("STREAM_DATA_BLOCKED"),
    STREAMS_BLOCKED("STREAMS_BLOCKED"), CONNECTION_CLOSE("CONNECTION_CLOSE"), DATAGRAM("DATAGRAM"),
    TRANSPORT_PARAMETERS("QX_TRANSPORT_PARAMETERS"), PING("QX_PING"), GOAWAY("GOAWAY"),
    PRIORITY_UPDATE("PRIORITY_UPDATE"), PIPE_JOIN("PIPE_JOIN"), PIPE_ACK("PIPE_ACK"), PIPE_LANE("PIPE_LANE"),
    PIPE_PING("PIPE_PING"), PIPE_ABANDON("PIPE_ABANDON"), PIPE_STATUS("PIPE_STATUS");

    private final String wireName;

    FrameKind(String wireName) {
        this.wireName = wireName;
    }

    /** The registry name. */
    public String wireName() {
        return wireName;
    }

    /** Whether a frame of this kind elicits a PIPE_ACK (SPEC.md Section 1.4). */
    public boolean ackEliciting() {
        return this != PADDING && this != PIPE_ACK && this != CONNECTION_CLOSE;
    }
}
