package io.zmux.http3;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http3.Http3ErrorCode;
import io.netty.handler.codec.quic.QuicChannel;
import io.zmux.quic.internal.QuicVarInts;
import java.util.List;

/** Filters unused H3 frame types before Netty buffers them or applies its frame length limit. */
final class WebTransportHttpFrames extends ByteToMessageDecoder {
    private int preamble;
    private boolean expectSettings;
    private long remaining;
    private boolean discard, failed;

    WebTransportHttpFrames(int preamble) {
        this.preamble = preamble;
        expectSettings = preamble == 1;
    }

    @Override protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (failed) {
            in.skipBytes(in.readableBytes());
            return;
        }
        int start = in.readerIndex();
        if (preamble != 0) {
            if (QuicVarInts.read(in) < 0) return;
            preamble--;
            forward(in, start, out);
            return;
        }
        if (remaining == 0) {
            long type = QuicVarInts.read(in);
            if (type < 0) return;
            if (type == WebTransportConnection.BIDI_STREAM) {
                fail(ctx, in, Http3ErrorCode.H3_FRAME_UNEXPECTED);
                return;
            }
            // Known HTTP/3 and reserved HTTP/2 types still go through Netty's stream validators.
            discard = !forwardType(type);
            if (discard && expectSettings) {
                fail(ctx, in, Http3ErrorCode.H3_MISSING_SETTINGS);
                return;
            }
            remaining = QuicVarInts.read(in);
            if (remaining < 0) {
                remaining = 0;
                in.readerIndex(start);
                return;
            }
            expectSettings = false;
        }
        int count = (int) Math.min(remaining, in.readableBytes());
        remaining -= count;
        in.skipBytes(count);
        if (!discard) forward(in, start, out);
    }

    private static boolean forwardType(long type) {
        if (type > 0x0d) return false;
        return switch ((int) type) {
            case 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0x0d -> true;
            default -> false;
        };
    }

    private static void forward(ByteBuf in, int start, List<Object> out) {
        out.add(in.retainedSlice(start, in.readerIndex() - start));
    }

    private void fail(ChannelHandlerContext ctx, ByteBuf in, Http3ErrorCode error) {
        failed = true;
        in.skipBytes(in.readableBytes());
        ((QuicChannel) ctx.channel().parent()).close(true, error.code(), Unpooled.EMPTY_BUFFER);
    }
}
