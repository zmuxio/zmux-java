package io.zmux;

/**
 * One direction of a splice: what it forwarded and how it ended.
 *
 * @param bytes stream bytes forwarded
 * @param datagramBytes payload bytes of the datagrams forwarded, those a packet relay wrote as capsules included; with
 *     bytes, exactly what the direction's limiter granted and was not refunded
 * @param datagramsDropped every datagram received and not forwarded
 * @param datagramsTooLarge those of datagramsDropped for their size: above the next hop's {@link ZmuxStream#maxDatagramPayload(long)} ({@link Zmux#splice}), or, in a packet splice, which writes such a datagram as a capsule, above {@link io.zmux.helper.PacketStream#DEFAULT_MAX_PACKET}; a capsule not written for another reason (the next hop ended, the splice ended) counts only in datagramsDropped
 * @param datagramsLimited those of datagramsDropped the direction's or the next hop's datagram limiter refused
 * @param datagramsAsCapsules datagrams a packet relay forwarded as DATAGRAM capsules, counted once written whole;
 *     always 0 for {@link Zmux#splice}
 * @param end how the direction ended
 */
public record SpliceDirection(long bytes, long datagramBytes, long datagramsDropped, long datagramsTooLarge,
                              long datagramsLimited, long datagramsAsCapsules, SpliceEnd end) {}
