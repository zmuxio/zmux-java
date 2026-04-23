package io.zmux;

public enum Role {
    INITIATOR(0),
    RESPONDER(1),
    AUTO(2);

    private final int code;

    Role(int code) {
        this.code = code;
    }

    public static Role fromCode(int code) {
        for (Role value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        throw new IllegalArgumentException("invalid role: " + code);
    }

    public int code() {
        return code;
    }
}
