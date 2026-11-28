package io.zmux.internal.wire;

import io.zmux.internal.buffer.ReceiveSpans;
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
    public ReceiveSpans spans;

    public byte get(int pos) {
        return ReceiveSpans.get(src, spans, pos);
    }

    public long varint(int pos, int end) {
        return VarInt.get(src, spans, pos, end);
    }

    public void copy(int pos, byte[] dst) {
        copy(pos, dst, 0, dst.length);
    }

    public void copy(int pos, byte[] dst, int offset, int length) {
        ReceiveSpans.copy(src, spans, pos, dst, offset, length);
    }
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
    /** DATAGRAM_ACK: channel and microsecond delay, with largest Sequence in {@link #value}. */
    public int datagramChannel;
    public long datagramAckDelay;
    public long firstDatagramRange;
    /** PADDING run length; PIPE_ACK entry count. */
    public int count;
    /** PIPE_ACK entries, validated: [entriesPos, entriesEnd). */
    public int entriesPos;
    public int entriesEnd;
    /** SESSION_EPOCH: the phase (0 offer, 1 answer), the epoch in {@link #value}, the share in data, and the Locator. */
    public int phase;
    public int locatorPos;
    public int locatorLen;
    /** SESSION_EPOCH: where its body, from Phase through Locator, starts and ends. */
    public int bodyPos;
    public int bodyEnd;
    void reset() {
        kind = null;
        type = 0;
        dataPos = dataLen = 0;
        streamId = offset = code = finalSize = reliableSize = value = 0;
        fin = hasOffset = hasLength = bidi = application = response = incremental = false;
        errorFrameType = maxBidi = maxUni = pipeId = lane = seq = status = priority = 0;
        urgency = 0;
        laneKeyPos = count = entriesPos = entriesEnd = 0;
        phase = locatorPos = locatorLen = bodyPos = bodyEnd = 0;
        datagramChannel = 0;
        datagramAckDelay = firstDatagramRange = 0;
    }

    /** A copy of the data range. */
    public byte[] dataBytes() {
        byte[] a = new byte[dataLen];
        copy(dataPos, a);
        return a;
    }

    /** The Locator of a SESSION_EPOCH; empty keeps the sender's current one. */
    public byte[] locatorBytes() {
        byte[] a = new byte[locatorLen];
        copy(locatorPos, a);
        return a;
    }

    /** The body of a SESSION_EPOCH, from Phase through Locator. */
    public byte[] bodyBytes() {
        byte[] a = new byte[bodyEnd - bodyPos];
        copy(bodyPos, a);
        return a;
    }

    void writeData(ByteBuffer destination) {
        if (spans == null) {
            destination.put(destination.position(), src, dataPos, dataLen);
            destination.position(destination.position() + dataLen);
        } else {
            spans.write(dataPos, destination, dataLen);
        }
    }

    /** Iterator over the PIPE_ACK entries; reuse one instance per caller. */
    public AckEntries entries(AckEntries it) {
        it.reset(this, entriesPos, entriesEnd);
        return it;
    }

    /** Allocation-free cursor over PIPE_ACK entries. */
    public static final class AckEntries {
        private Frame frame;
        private int pos;
        private int end;
        public long pipeId;
        public long lane;
        public long position;
        public long delay;

        void reset(Frame frame, int pos, int end) {
            this.frame = frame;
            this.pos = pos;
            this.end = end;
        }

        /** Advances to the next entry; false at the end. */
        public boolean next() {
            if (pos >= end) return false;
            pipeId = read();
            lane = read();
            position = read();
            delay = read();
            return true;
        }

        private long read() {
            long value = frame.varint(pos, end);
            pos += VarInt.lengthOf(frame.get(pos));
            return value;
        }
    }
}
