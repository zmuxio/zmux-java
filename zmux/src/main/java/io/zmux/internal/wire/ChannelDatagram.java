package io.zmux.internal.wire;

import io.zmux.carrier.VarInt;

import java.nio.ByteBuffer;

/**
 * Channel datagrams (EXTENSIONS.md Section 6.8): one DATAGRAM frame sent on a carrier's own
 * datagram channel instead of in a record, after the Pipe Stream ID on channels shared by the
 * pipes of a connection (QUIC, WebTransport) and alone where the channel belongs to the pipe
 * (HTTP/3). The only place channel datagrams are encoded and decoded.
 */
public final class ChannelDatagram {
    /** No Pipe Stream ID: the channel belongs to the pipe. */
    public static final long NO_ID = -1;

    private static final String NOT_ONE_FRAME = "channel datagram is not one DATAGRAM frame without Length";

    private ChannelDatagram() {}

    /** The length of a channel datagram carrying n bytes of DATAGRAM data, after Pipe Stream ID id unless {@link #NO_ID}. */
    public static int length(long id, int n) {
        return (id == NO_ID ? 0 : VarInt.length(id)) + 1 + n;
    }

    /** A channel datagram: the Pipe Stream ID id unless {@link #NO_ID}, then a DATAGRAM frame of data without Length. */
    public static byte[] encode(long id, byte[] data) {
        byte[] b = new byte[length(id, data.length)];
        int i = id == NO_ID ? 0 : VarInt.put(b, 0, id);
        b[i++] = (byte) FrameType.DATAGRAM;
        System.arraycopy(data, 0, b, i, data.length);
        return b;
    }

    /** The Pipe Stream ID at the start of b[pos, end), or -1 when it is truncated; the frame follows its {@link VarInt#lengthOf} bytes. */
    public static long pipeStreamId(ByteBuffer b, int pos, int end) {
        return VarInt.get(b, pos, end);
    }

    /**
     * Decodes the frame of a channel datagram, b[pos, end), into f: it must be exactly one
     * DATAGRAM frame of type 0x30; anything else is a PROTOCOL_VIOLATION. f's data is a range
     * of b.
     */
    public static void decodeFrame(FrameDecoder decoder, ByteBuffer b, int pos, int end, Frame f) throws WireException {
        if (pos >= end || (b.get(pos) & 0xff) != FrameType.DATAGRAM) throw WireException.protocol(NOT_ONE_FRAME);
        int n;
        try {
            n = decoder.decode(b, pos, end, f);
        } catch (WireException e) {
            throw new WireException(TransportError.PROTOCOL_VIOLATION, f.type, e.reason(), e.details());
        }
        if (n != end - pos || f.kind != FrameKind.DATAGRAM) throw WireException.protocol(NOT_ONE_FRAME);
    }
}
