package io.zmux;

/** A datagram of a stream's datagram flow, with its context ID (EXTENSIONS.md Section 6.3). */
public record StreamDatagram(long context, byte[] payload) {}
