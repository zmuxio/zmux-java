package io.zmux;

public enum ErrorCode {
    NO_ERROR(0),
    PROTOCOL(1),
    FLOW_CONTROL(2),
    STREAM_LIMIT(3),
    REFUSED_STREAM(4),
    STREAM_STATE(5),
    STREAM_CLOSED(6),
    SESSION_CLOSING(7),
    CANCELLED(8),
    IDLE_TIMEOUT(9),
    FRAME_SIZE(10),
    UNSUPPORTED_VERSION(11),
    ROLE_CONFLICT(12),
    INTERNAL(13);

    private final long code;

    ErrorCode(long code) {
        this.code = code;
    }

    public static ErrorCode fromCode(long code) {
        for (ErrorCode value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown zmux error code: " + code);
    }

    public long code() {
        return code;
    }
}
