package io.zmux.seal;

/** An opaque credential; its type selects how it is verified. */
public record Credential(long type, byte[] data) {}
