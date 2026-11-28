package io.zmux.http3;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http3.DefaultHttp3DataFrame;
import io.netty.handler.codec.http3.Http3DataFrame;
import io.zmux.quic.internal.QuicVarInts;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;

/** Incremental CONNECT DATA decoder. Unknown capsule payloads are skipped as they arrive. */
final class WebTransportCapsules {
    static final long CLOSE = 0x2843;
    static final long DRAIN = 0x78ae;
    private static final int MAX_REASON = 1024;

    interface Receiver {
        void peerClosed(long code, String reason);
        void peerDraining();
    }

    private final Receiver receiver;
    private final byte[] integer = new byte[Long.BYTES];
    private int used;
    private long type = -1, remaining = -1;
    private ByteBuffer value;
    private boolean closed;

    WebTransportCapsules(Receiver receiver) {
        this.receiver = receiver;
    }

    static Http3DataFrame close(ByteBufAllocator alloc, long code, String reason) {
        if (code < 0 || code > 0xffff_ffffL) throw new IllegalArgumentException("WebTransport close code exceeds uint32");
        if (reason.length() > MAX_REASON) throw new IllegalArgumentException("WebTransport close reason too long");
        ByteBuffer utf8;
        try {
            utf8 = StandardCharsets.UTF_8.newEncoder().encode(CharBuffer.wrap(reason));
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException("invalid WebTransport close reason", malformed);
        }
        if (utf8.remaining() > MAX_REASON) throw new IllegalArgumentException("WebTransport close reason too long");
        var out = alloc.buffer(8 + Integer.BYTES + utf8.remaining());
        QuicVarInts.write(out, CLOSE);
        QuicVarInts.write(out, Integer.BYTES + utf8.remaining());
        return new DefaultHttp3DataFrame(out.writeInt((int) code).writeBytes(utf8));
    }

    static Http3DataFrame drain(ByteBufAllocator alloc) {
        var out = alloc.buffer(5);
        QuicVarInts.write(out, DRAIN);
        QuicVarInts.write(out, 0);
        return new DefaultHttp3DataFrame(out);
    }

    void read(ByteBuf in) throws IOException {
        while (in.isReadable()) {
            if (closed) throw new IOException("data after WebTransport CLOSE");
            if (type < 0) {
                type = integer(in);
                if (type < 0) return;
            }
            if (remaining < 0) {
                remaining = integer(in);
                if (remaining < 0) return;
                start();
            }
            int count = (int) Math.min(in.readableBytes(), remaining);
            if (value == null) in.skipBytes(count);
            else {
                in.readBytes(value.limit(value.position() + count));
                value.limit(value.capacity());
            }
            remaining -= count;
            if (remaining == 0) finish();
        }
    }

    private void start() throws IOException {
        if (type == CLOSE) {
            if (remaining < Integer.BYTES || remaining > Integer.BYTES + MAX_REASON) {
                throw new IOException("invalid WebTransport CLOSE length");
            }
            value = ByteBuffer.allocate((int) remaining);
        } else if (type == DRAIN && remaining != 0) {
            throw new IOException("nonempty WebTransport DRAIN");
        }
    }

    private void finish() throws IOException {
        if (type == CLOSE) {
            value.flip();
            long code = Integer.toUnsignedLong(value.getInt());
            String reason = StandardCharsets.UTF_8.newDecoder().decode(value).toString();
            closed = true;
            receiver.peerClosed(code, reason);
        } else if (type == DRAIN) receiver.peerDraining();
        value = null;
        type = remaining = -1;
    }

    void end() throws IOException {
        if (type >= 0 || used != 0) throw new IOException("incomplete WebTransport capsule");
        if (!closed) {
            closed = true;
            receiver.peerClosed(0, "");
        }
    }

    private long integer(ByteBuf in) {
        if (used == 0) {
            if (!in.isReadable()) return -1;
            integer[used++] = in.readByte();
        }
        int width = QuicVarInts.width(integer[0]);
        int count = Math.min(in.readableBytes(), width - used);
        in.readBytes(integer, used, count);
        used += count;
        if (used < width) return -1;
        used = 0;
        return QuicVarInts.read(integer, width);
    }
}
