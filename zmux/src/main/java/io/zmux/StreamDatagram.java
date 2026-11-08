package io.zmux;

/** A datagram of a stream's datagram flow, with its context ID. */
public record StreamDatagram(long context, byte[] payload) {}
