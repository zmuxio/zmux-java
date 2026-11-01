package io.zmux.internal.wire;

import io.zmux.carrier.VarInt;

import java.nio.ByteBuffer;

/**
 * One decoded frame, reused across decodes (no allocation per frame). Byte fields
 * are ranges of {@link #src}, valid until the buffer is reused; fields not used by
 * the kind hold 0.
 */
public final class Frame {
    public FrameKind kind;
    /** Wire frame type. */
    public long type;
    public ByteBuffer src;
    /** STREAM and DATAGRAM data, CONNECTION_CLOSE reason, QX_TRANSPORT_PARAMETERS block. */
    public int dataPos;
    public int dataLen;
    public long streamId;
    public long offset;
    public boolean fin;
    public boolean hasOffset;
    public boolean hasLength;
    public long code;
    public long finalSize;
    public long reliableSize;
    /** MAX_*, *_BLOCKED limit; QX_PING sequence. */
    public long value;
    public boolean bidi;
    public boolean application;
    /** CONNECTION_CLOSE 0x1c Frame Type field. */
    public long errorFrameType;
    public boolean response;
    public long maxBidi;
    public long maxUni;
    public int urgency;
    public boolean incremental;
    public long pipeId;
    public long lane;
    public int laneKeyPos;
    /** PRIORITY_UPDATE and PIPE_STATUS sequence number. */
    public long seq;
    public long status;
    public long priority;
    /** PADDING run length; PIPE_ACK entry count. */
    public int count;
    /** PIPE_ACK entries, validated: [entriesPos, entriesEnd). */
    public int entriesPos;
    public int entriesEnd;
    /** PIPE_JOIN fields. */
    public final Join join = new Join();

    /** Decoded PIPE_JOIN (MULTIPATH.md Section 5.1). */
    public static final class Join {
        public int locatorPos;
        public int locatorLen;
        public long pipeId;
        /** 0 up, 1 down, 2 bidirectional. */
        public int direction;
        public boolean carrierBound;
        public boolean standby;
        public long fateGroup;
        public long priority;
        public int tokenPos;
        /** Bytes from the frame start through Priority: the MAC input. */
        public int macLength;
    }

    void reset() {
        kind = null;
        type = 0;
        dataPos = dataLen = 0;
        streamId = offset = code = finalSize = reliableSize = value = 0;
        fin = hasOffset = hasLength = bidi = application = response = incremental = false;
        errorFrameType = maxBidi = maxUni = pipeId = lane = seq = status = priority = 0;
        urgency = 0;
        laneKeyPos = count = entriesPos = entriesEnd = 0;
    }

    /** A copy of the data range. */
    public byte[] dataBytes() {
        byte[] a = new byte[dataLen];
        src.get(dataPos, a);
        return a;
    }

    /** A read-only view of the data range. */
    public ByteBuffer data() {
        return src.slice(dataPos, dataLen).asReadOnlyBuffer();
    }

    /** Iterator over the PIPE_ACK entries; reuse one instance per caller. */
    public AckEntries entries(AckEntries it) {
        it.reset(src, entriesPos, entriesEnd);
        return it;
    }

    /** Allocation-free cursor over PIPE_ACK entries. */
    public static final class AckEntries {
        private ByteBuffer b;
        private int pos;
        private int end;
        public long pipeId;
        public long lane;
        public long position;
        public long delay;

        void reset(ByteBuffer b, int pos, int end) {
            this.b = b;
            this.pos = pos;
            this.end = end;
        }

        /** Advances to the next entry; false at the end. */
        public boolean next() {
            if (pos >= end) return false;
            pipeId = VarInt.get(b, pos, end);
            pos += VarInt.lengthOf(b.get(pos));
            lane = VarInt.get(b, pos, end);
            pos += VarInt.lengthOf(b.get(pos));
            position = VarInt.get(b, pos, end);
            pos += VarInt.lengthOf(b.get(pos));
            delay = VarInt.get(b, pos, end);
            pos += VarInt.lengthOf(b.get(pos));
            return true;
        }
    }
}
