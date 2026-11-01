package io.zmux;

/**
 * Peer-facing counts and datagram queues.
 *
 * @param incomingStreams concurrent peer bidirectional streams: 0, or at least 4
 * @param incomingUniStreams concurrent peer unidirectional streams advertised at once; -1:
 *     16, raised to 256 on the first accept
 * @param lanes lanes accepted on each inbound pipe, at least 1; 1 = no lanes toward this endpoint
 * @param datagramSendQueue queued outgoing datagrams, at least 1
 * @param datagramReceiveQueue queued incoming session datagrams, at least 1
 * @param datagramStreamQueue queued incoming datagrams per stream flow, at least 1
 * @param datagramReceiveBytes queued incoming datagram bytes, at least 1
 */
public record Limits(long incomingStreams, long incomingUniStreams, long lanes, int datagramSendQueue,
                     int datagramReceiveQueue, int datagramStreamQueue, long datagramReceiveBytes) {
    private static final Limits DEFAULTS = new Limits(256, -1, 258, 64, 128, 32, 1 << 20);

    public Limits {
        if (incomingStreams != 0 && incomingStreams < 4) throw new IllegalArgumentException("incomingStreams must be 0 or at least 4");
        if (incomingUniStreams < -1) throw new IllegalArgumentException("incomingUniStreams below -1");
        if (lanes < 1) throw new IllegalArgumentException("lanes below 1");
        if (datagramSendQueue < 1 || datagramReceiveQueue < 1 || datagramStreamQueue < 1 || datagramReceiveBytes < 1) {
            throw new IllegalArgumentException("datagram queues below 1");
        }
    }

    public static Limits defaults() { return DEFAULTS; }

    public Limits withIncomingStreams(long n) { return new Limits(n, incomingUniStreams, lanes, datagramSendQueue, datagramReceiveQueue, datagramStreamQueue, datagramReceiveBytes); }
    public Limits withIncomingUniStreams(long n) { return new Limits(incomingStreams, n, lanes, datagramSendQueue, datagramReceiveQueue, datagramStreamQueue, datagramReceiveBytes); }
    public Limits withLanes(long n) { return new Limits(incomingStreams, incomingUniStreams, n, datagramSendQueue, datagramReceiveQueue, datagramStreamQueue, datagramReceiveBytes); }
    public Limits withDatagramQueues(int send, int receive, int perStream, long receiveBytes) { return new Limits(incomingStreams, incomingUniStreams, lanes, send, receive, perStream, receiveBytes); }
}
