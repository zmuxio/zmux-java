package io.zmux;

/** The session-local identity of a group of attachments. */
public record MemberId(long value) {
    public MemberId {
        if (value == 0) throw new IllegalArgumentException("member ID zero is reserved");
    }
}
