package io.zmux;

public enum FrameType {
    DATA(1),
    MAX_DATA(2),
    STOP_SENDING(3),
    PING(4),
    PONG(5),
    BLOCKED(6),
    RESET(7),
    ABORT(8),
    GOAWAY(9),
    CLOSE(10),
    EXT(11);

    private final int code;

    FrameType(int code) {
        this.code = code;
    }

    public static FrameType fromCode(int code) {
        for (FrameType value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        throw new IllegalArgumentException("invalid frame type: " + code);
    }

    public int code() {
        return code;
    }
}
