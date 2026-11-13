package io.zmux;

/**
 * A datagram of a stream's datagram flow, with its context ID. The payload array is not copied: a received datagram's
 * array belongs to the caller, and the library no longer holds it.
 */
public record StreamDatagram(long context, byte[] payload) {}
