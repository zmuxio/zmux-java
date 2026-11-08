package io.zmux.internal.join;

/** The content of a Reply. {@code decision} is 0 accept, 1 refuse; {@code frames} may be null. */
public record ReplyPayload(int decision, byte[] frames, int padding) {}
