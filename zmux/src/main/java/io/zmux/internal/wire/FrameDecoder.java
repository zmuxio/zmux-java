package io.zmux.internal.wire;

import io.zmux.carrier.VarInt;

import java.nio.ByteBuffer;

/**
 * Decodes one frame (SPEC.md Section 4). Bounds-checked: every field is read
 * within the record; nothing allocates.
 */
public final class FrameDecoder {
    private static final long MAX_60 = 1L << 60;

    private ByteBuffer b;
    private int i;
    private int limit;
    private boolean bad;

    private long varint() {
        if (bad) return 0;
        long v = VarInt.get(b, i, limit);
        if (v < 0) {
            bad = true;
            return 0;
        }
        i += VarInt.lengthOf(b.get(i));
        return v;
    }

    private int u8() {
        if (bad || i >= limit) {
            bad = true;
            return 0;
        }
        return b.get(i++) & 0xff;
    }

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
     * type, which is reported only after the frame decoded (SPEC.md Section 10.3).
     */
    public int decode(ByteBuffer buf, int pos, int limit, Frame f) throws WireException {
        f.reset();
        f.src = buf;
        this.b = buf;
        this.i = pos;
        this.limit = limit;
        this.bad = false;
        long t = VarInt.get(buf, pos, limit);
        if (t < 0) throw WireException.frame("truncated frame type");
        int tn = VarInt.lengthOf(buf.get(pos));
        i = pos + tn;
        f.type = t;
        if (t == FrameType.PADDING) {
            f.kind = FrameKind.PADDING;
            int n = i;
            while (n < limit && buf.get(n) == 0) n++;
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
            if (!bad && f.value > MAX_60) throw WireException.frame(t, "MAX_STREAMS above 2^60");
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
            if (!bad && f.value > MAX_60) throw WireException.frame(t, "STREAMS_BLOCKED above 2^60");
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
            if (!bad && (f.maxBidi > MAX_60 || f.maxUni > MAX_60)) throw WireException.frame(t, "GOAWAY watermark above 2^60");
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
        } else if (t == FrameType.PIPE_JOIN) {
            f.kind = FrameKind.PIPE_JOIN;
            decodeJoin(pos, f.join);
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
                if (!bad && k > 0 && (p < pp || p == pp && l <= pl)) throw WireException.frame(t, "PIPE_ACK entries not increasing");
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

    private void decodeJoin(int start, Frame.Join j) throws WireException {
        int ll = u8();
        if (!bad && (ll == 0 || ll > 16)) throw WireException.frame(FrameType.PIPE_JOIN, "PIPE_JOIN locator length");
        j.locatorLen = ll;
        j.locatorPos = bytes(ll);
        j.pipeId = varint();
        int fl = u8();
        if (!bad) {
            if ((fl & 0xf0) != 0 || (fl & 0x03) == 3) throw WireException.frame(FrameType.PIPE_JOIN, "PIPE_JOIN flags");
            j.direction = fl & 3;
            j.carrierBound = (fl & 0x04) != 0;
            j.standby = (fl & 0x08) != 0;
        }
        j.fateGroup = varint();
        j.priority = varint();
        j.macLength = i - start;
        j.tokenPos = bytes(16);
        if (!bad && i - start > 64) throw WireException.frame(FrameType.PIPE_JOIN, "PIPE_JOIN longer than 64 bytes");
    }
}
