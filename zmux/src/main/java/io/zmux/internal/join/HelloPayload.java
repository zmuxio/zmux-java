package io.zmux.internal.join;

/** The content of a Hello. {@code firstBatch} is opaque and may be null. */
public record HelloPayload(JoinPipe pipe, byte[] firstBatch, int padding) {}
