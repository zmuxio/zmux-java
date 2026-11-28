package io.zmux.quic.internal;

import io.netty.buffer.ByteBuf;
import io.zmux.internal.wire.VarInt;

/** Netty buffer access to the shared QUIC integer codec. */
public final class QuicVarInts {
    public static final long MAX = VarInt.MAX;

    private QuicVarInts() { }

    /** Takes an integer, leaving the buffer in place if it is incomplete. */
    public static long read(ByteBuf in) {
        if (!in.isReadable()) return -1;
        int index = in.readerIndex();
        int length = VarInt.lengthOf(in.getByte(index));
        if (in.readableBytes() < length) return -1;
        // A composite prefix can straddle components; only the integer is gathered.
        long value = VarInt.get(in.nioBuffer(index, length), 0, length);
        in.skipBytes(length);
        return value;
    }

    public static void write(ByteBuf out, long value) {
        int length = VarInt.length(value);
        out.ensureWritable(length);
        int index = out.writerIndex();
        VarInt.put(out.internalNioBuffer(index, length), value);
        out.writerIndex(index + length);
    }

    public static int width(byte first) { return VarInt.lengthOf(first); }

    public static long read(byte[] bytes, int length) { return VarInt.getKnownWidth(bytes, 0, length); }
}
