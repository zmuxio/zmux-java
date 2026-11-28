package io.zmux.internal.wire;

import io.zmux.internal.buffer.ReceiveSpans;
import java.nio.ByteBuffer;

/**
 * Decodes one frame. Bounds-checked: every field is read
 * within the record; nothing allocates.
 */
public final class FrameDecoder {
    public static final int MAX_DATAGRAM_ACK_RANGES = 4090;
    public static final int MAX_DATAGRAM_ACK_SIZE = 8192;

    private ByteBuffer b;
    private ReceiveSpans spans;
    private int i;
    private int limit;
    private boolean bad;

    private long varint() {
        if (bad) return 0;
        long v = VarInt.get(b, spans, i, limit);
        if (v < 0) {
            bad = true;
            return 0;
        }
        i += VarInt.lengthOf(get(i));
        return v;
    }

    private int u8() {
        if (bad || i >= limit) {
            bad = true;
            return 0;
        }
        return get(i++) & 0xff;
    }

    private byte get(int index) { return spans == null ? b.get(index) : spans.get(index); }

    /** Skips {@code n} bytes and returns their start, or -1 when they are not there. */
    private int bytes(long n) {
        if (bad || n > limit - i) {
            bad = true;
            return -1;
        }
        int p = i;
        i += (int) n;
        return p;
    }

    /**
     * Decodes the frame at {@code pos}; {@code limit} is the end of the record.
     * Returns the frame length. Decode errors take precedence over a non-minimal
     * type, which is reported only after the frame decoded.
     */
    public int decode(ByteBuffer buf, int pos, int limit, Frame f) throws WireException {
        return decode(buf, null, pos, limit, f);
    }

    public int decode(ByteBuffer buf, ReceiveSpans ranges, int pos, int limit, Frame f) throws WireException {
        f.reset();
        f.src = buf;
        f.spans = ranges;
        this.b = buf;
        this.spans = ranges;
        this.i = pos;
        this.limit = limit;
        this.bad = false;
        long t = VarInt.get(buf, ranges, pos, limit);
        if (t < 0) throw WireException.frame("truncated frame type");
        int tn = VarInt.lengthOf(get(pos));
        i = pos + tn;
        f.type = t;
        if (t == FrameType.PADDING) {
            f.kind = FrameKind.PADDING;
            int n = i;
            while (n < limit && get(n) == 0) n++;
            f.count = n - pos;
            i = n;
        } else if (t >= 0x08 && t <= 0x0f) {
            f.kind = FrameKind.STREAM;
            f.streamId = varint();
            if ((t & FrameType.STREAM_OFF) != 0) {
                f.hasOffset = true;
                f.offset = varint();
            }
            f.fin = (t & FrameType.STREAM_FIN) != 0;
            if ((t & FrameType.STREAM_LEN) != 0) {
                f.hasLength = true;
                long n = varint();
                f.dataPos = bytes(n);
                f.dataLen = (int) n;
            } else if (!bad) {
                f.dataPos = i;
                f.dataLen = limit - i;
                i = limit;
            }
            if (!bad && f.offset + f.dataLen > VarInt.MAX) {
                throw WireException.frame(t, "stream offset plus length above 2^62-1");
            }
        } else if (t == FrameType.RESET_STREAM) {
            f.kind = FrameKind.RESET_STREAM;
            f.streamId = varint();
            f.code = varint();
            f.finalSize = varint();
        } else if (t == FrameType.RESET_STREAM_AT) {
            f.kind = FrameKind.RESET_STREAM_AT;
            f.streamId = varint();
            f.code = varint();
            f.finalSize = varint();
            f.reliableSize = varint();
            if (!bad && f.reliableSize > f.finalSize) throw WireException.frame(t, "reliable size above final size");
        } else if (t == FrameType.STOP_SENDING) {
            f.kind = FrameKind.STOP_SENDING;
            f.streamId = varint();
            f.code = varint();
        } else if (t == FrameType.MAX_DATA) {
            f.kind = FrameKind.MAX_DATA;
            f.value = varint();
        } else if (t == FrameType.MAX_STREAM_DATA) {
            f.kind = FrameKind.MAX_STREAM_DATA;
            f.streamId = varint();
            f.value = varint();
        } else if (t == FrameType.MAX_STREAMS_BIDI || t == FrameType.MAX_STREAMS_UNI) {
            f.kind = FrameKind.MAX_STREAMS;
            f.bidi = t == FrameType.MAX_STREAMS_BIDI;
            f.value = varint();
            if (!bad && f.value > VarInt.MAX_STREAM_COUNT) throw WireException.frame(t, "MAX_STREAMS above 2^60");
        } else if (t == FrameType.DATA_BLOCKED) {
            f.kind = FrameKind.DATA_BLOCKED;
            f.value = varint();
        } else if (t == FrameType.STREAM_DATA_BLOCKED) {
            f.kind = FrameKind.STREAM_DATA_BLOCKED;
            f.streamId = varint();
            f.value = varint();
        } else if (t == FrameType.STREAMS_BLOCKED_BIDI || t == FrameType.STREAMS_BLOCKED_UNI) {
            f.kind = FrameKind.STREAMS_BLOCKED;
            f.bidi = t == FrameType.STREAMS_BLOCKED_BIDI;
            f.value = varint();
            if (!bad && f.value > VarInt.MAX_STREAM_COUNT) throw WireException.frame(t, "STREAMS_BLOCKED above 2^60");
        } else if (t == FrameType.CLOSE_TRANSPORT || t == FrameType.CLOSE_APPLICATION) {
            f.kind = FrameKind.CONNECTION_CLOSE;
            f.application = t == FrameType.CLOSE_APPLICATION;
            f.code = varint();
            if (!f.application) f.errorFrameType = varint();
            long n = varint();
            f.dataPos = bytes(n);
            f.dataLen = (int) n;
        } else if (t == FrameType.DATAGRAM || t == FrameType.DATAGRAM_LEN) {
            f.kind = FrameKind.DATAGRAM;
            if (t == FrameType.DATAGRAM_LEN) {
                f.hasLength = true;
                long n = varint();
                f.dataPos = bytes(n);
                f.dataLen = (int) n;
            } else {
                f.dataPos = i;
                f.dataLen = limit - i;
                i = limit;
            }
        } else if (t == FrameType.DATAGRAM_ACK) {
            datagramAck(f, pos);
        } else if (t == FrameType.TRANSPORT_PARAMETERS) {
            f.kind = FrameKind.TRANSPORT_PARAMETERS;
            long n = varint();
            f.dataPos = bytes(n);
            f.dataLen = (int) n;
        } else if (t == FrameType.PING_REQUEST || t == FrameType.PING_RESPONSE) {
            f.kind = FrameKind.PING;
            f.response = t == FrameType.PING_RESPONSE;
            f.value = varint();
        } else if (t == FrameType.GOAWAY) {
            f.kind = FrameKind.GOAWAY;
            f.maxBidi = varint();
            f.maxUni = varint();
            f.code = varint();
            if (!bad && (f.maxBidi > VarInt.MAX_STREAM_COUNT || f.maxUni > VarInt.MAX_STREAM_COUNT)) throw WireException.frame(t, "GOAWAY watermark above 2^60");
        } else if (t == FrameType.PRIORITY_UPDATE) {
            f.kind = FrameKind.PRIORITY_UPDATE;
            f.streamId = varint();
            f.seq = varint();
            int p = u8();
            if (!bad) {
                if ((p & 0x0f) != 0) throw WireException.frame(t, "PRIORITY_UPDATE reserved bits");
                f.urgency = p >>> 5;
                f.incremental = (p & 0x10) != 0;
            }
        } else if (t == FrameType.PIPE_ACK) {
            f.kind = FrameKind.PIPE_ACK;
            long cnt = varint();
            if (!bad && cnt == 0) throw WireException.frame(t, "PIPE_ACK without entries");
            int start = i;
            long pp = 0, pl = 0;
            for (long k = 0; k < cnt && !bad; k++) {
                long p = varint(), l = varint();
                varint();
                varint();
                if (!bad && k > 0 && (p < pp || (p == pp && l <= pl))) throw WireException.frame(t, "PIPE_ACK entries not increasing");
                pp = p;
                pl = l;
            }
            if (!bad) {
                f.count = (int) cnt;
                f.entriesPos = start;
                f.entriesEnd = i;
            }
        } else if (t == FrameType.PIPE_LANE) {
            f.kind = FrameKind.PIPE_LANE;
            f.laneKeyPos = bytes(16);
            f.pipeId = varint();
            f.lane = varint();
        } else if (t == FrameType.PIPE_PING) {
            f.kind = FrameKind.PIPE_PING;
        } else if (t == FrameType.PIPE_ABANDON) {
            f.kind = FrameKind.PIPE_ABANDON;
            f.pipeId = varint();
            f.code = varint();
        } else if (t == FrameType.SESSION_EPOCH) {
            f.kind = FrameKind.SESSION_EPOCH;
            f.bodyPos = i;
            f.phase = u8();
            if (!bad && f.phase > 1) throw new WireException(TransportError.PROTOCOL_VIOLATION, t, "SESSION_EPOCH phase", "");
            f.value = varint();
            long sl = varint();
            f.dataPos = bytes(sl);
            f.dataLen = (int) (bad ? 0 : sl);
            f.locatorLen = u8();
            f.locatorPos = bytes(f.locatorLen);
            f.bodyEnd = i;
        } else if (t == FrameType.PIPE_STATUS) {
            f.kind = FrameKind.PIPE_STATUS;
            f.pipeId = varint();
            f.seq = varint();
            f.status = varint();
            f.priority = varint();
            if (!bad && Long.compareUnsigned(f.status, 1) > 0) throw WireException.frame(t, "PIPE_STATUS status");
        } else {
            throw WireException.frame(t, "unknown or prohibited frame type");
        }
        if (bad) throw WireException.frame(t, "truncated " + f.kind.wireName());
        if (tn != VarInt.length(t)) {
            throw new WireException(TransportError.PROTOCOL_VIOLATION, t, "frame type not minimally encoded", "");
        }
        return i - pos;
    }

    private void datagramAck(Frame f, int start) throws WireException {
        f.kind = FrameKind.DATAGRAM_ACK;
        f.pipeId = varint();
        f.datagramChannel = u8();
        f.value = varint();
        f.datagramAckDelay = varint();
        long count = varint();
        f.firstDatagramRange = varint();
        if (bad) return;
        if (f.datagramChannel > 1 || count > MAX_DATAGRAM_ACK_RANGES || f.firstDatagramRange > f.value) {
            throw WireException.frame(f.type, "invalid DATAGRAM_ACK header");
        }
        f.entriesPos = i;
        long low = f.value - f.firstDatagramRange;
        for (int k = 0; k < count; k++) {
            long gap = varint(), length = varint();
            if (bad) return;
            if (low < gap + 2 || length > low - gap - 2) {
                throw WireException.frame(f.type, "DATAGRAM_ACK range below zero");
            }
            if (i - start > MAX_DATAGRAM_ACK_SIZE) throw WireException.frame(f.type, "DATAGRAM_ACK above size bound");
            low -= gap + 2 + length;
        }
        f.entriesEnd = i;
        f.count = (int) count;
    }

}
