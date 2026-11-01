package io.zmux.internal.wire;

import io.zmux.carrier.VarInt;

import java.nio.ByteBuffer;

/**
 * Frame encoders writing at the buffer's position, every field in its shortest
 * encoding (IMPLEMENTATION.md Section 4, compact frames). The caller checks room.
 */
public final class FrameEncoder {
    private FrameEncoder() {}

    /** Length of a STREAM frame header; the OFF field is present only for a non-zero offset. */
    public static int streamHeaderLength(long id, long off, int n, boolean withLength) {
        int l = 1 + VarInt.length(id);
        if (off > 0) l += VarInt.length(off);
        if (withLength) l += VarInt.length(n);
        return l;
    }

    /** STREAM frame header; {@code n} bytes of data follow it. */
    public static void streamHeader(ByteBuffer b, long id, long off, int n, boolean fin, boolean withLength) {
        int t = (int) FrameType.STREAM;
        if (off > 0) t |= FrameType.STREAM_OFF;
        if (withLength) t |= FrameType.STREAM_LEN;
        if (fin) t |= FrameType.STREAM_FIN;
        b.put((byte) t);
        VarInt.put(b, id);
        if (off > 0) VarInt.put(b, off);
        if (withLength) VarInt.put(b, n);
    }

    public static void resetStream(ByteBuffer b, long id, long code, long finalSize) {
        b.put((byte) FrameType.RESET_STREAM);
        VarInt.put(b, id);
        VarInt.put(b, code);
        VarInt.put(b, finalSize);
    }

    public static void resetStreamAt(ByteBuffer b, long id, long code, long finalSize, long reliable) {
        b.put((byte) FrameType.RESET_STREAM_AT);
        varints(b, id, code, finalSize, reliable);
    }

    public static void stopSending(ByteBuffer b, long id, long code) {
        b.put((byte) FrameType.STOP_SENDING);
        VarInt.put(b, id);
        VarInt.put(b, code);
    }

    public static void maxData(ByteBuffer b, long v) {
        b.put((byte) FrameType.MAX_DATA);
        VarInt.put(b, v);
    }

    public static void maxStreamData(ByteBuffer b, long id, long v) {
        b.put((byte) FrameType.MAX_STREAM_DATA);
        VarInt.put(b, id);
        VarInt.put(b, v);
    }

    public static void maxStreams(ByteBuffer b, boolean bidi, long v) {
        b.put((byte) (bidi ? FrameType.MAX_STREAMS_BIDI : FrameType.MAX_STREAMS_UNI));
        VarInt.put(b, v);
    }

    public static void dataBlocked(ByteBuffer b, long v) {
        b.put((byte) FrameType.DATA_BLOCKED);
        VarInt.put(b, v);
    }

    public static void streamDataBlocked(ByteBuffer b, long id, long v) {
        b.put((byte) FrameType.STREAM_DATA_BLOCKED);
        VarInt.put(b, id);
        VarInt.put(b, v);
    }

    public static void streamsBlocked(ByteBuffer b, boolean bidi, long v) {
        b.put((byte) (bidi ? FrameType.STREAMS_BLOCKED_BIDI : FrameType.STREAMS_BLOCKED_UNI));
        VarInt.put(b, v);
    }

    /** CONNECTION_CLOSE; {@code frameType} is written only for type 0x1c. */
    public static void close(ByteBuffer b, boolean application, long code, long frameType, byte[] reason) {
        b.put((byte) (application ? FrameType.CLOSE_APPLICATION : FrameType.CLOSE_TRANSPORT));
        VarInt.put(b, code);
        if (!application) VarInt.put(b, frameType);
        VarInt.put(b, reason.length);
        b.put(reason);
    }

    public static int closeLength(boolean application, long code, long frameType, int reasonLength) {
        int l = 1 + VarInt.length(code) + VarInt.length(reasonLength) + reasonLength;
        return application ? l : l + VarInt.length(frameType);
    }

    /** DATAGRAM header for {@code n} bytes of data. */
    public static void datagramHeader(ByteBuffer b, int n, boolean withLength) {
        if (withLength) {
            b.put((byte) FrameType.DATAGRAM_LEN);
            VarInt.put(b, n);
        } else {
            b.put((byte) FrameType.DATAGRAM);
        }
    }

    public static void transportParameters(ByteBuffer b, byte[] block) {
        VarInt.put(b, FrameType.TRANSPORT_PARAMETERS);
        VarInt.put(b, block.length);
        b.put(block);
    }

    public static void ping(ByteBuffer b, boolean response, long seq) {
        VarInt.put(b, response ? FrameType.PING_RESPONSE : FrameType.PING_REQUEST);
        VarInt.put(b, seq);
    }

    public static void goAway(ByteBuffer b, long maxBidi, long maxUni, long code) {
        VarInt.put(b, FrameType.GOAWAY);
        VarInt.put(b, maxBidi);
        VarInt.put(b, maxUni);
        VarInt.put(b, code);
    }

    public static void priorityUpdate(ByteBuffer b, long id, long seq, int urgency, boolean incremental) {
        VarInt.put(b, FrameType.PRIORITY_UPDATE);
        VarInt.put(b, id);
        VarInt.put(b, seq);
        b.put(Preamble.priorityByte(urgency, incremental));
    }

    /** PIPE_JOIN from Type through Priority: the MAC input (MULTIPATH.md Section 5.1). */
    public static void joinMacInput(ByteBuffer b, byte[] locator, long pipeId, int direction, boolean carrierBound,
                                    boolean standby, long fateGroup, long priority) {
        VarInt.put(b, FrameType.PIPE_JOIN);
        b.put((byte) locator.length);
        b.put(locator);
        VarInt.put(b, pipeId);
        b.put((byte) ((direction & 3) | (carrierBound ? 0x04 : 0) | (standby ? 0x08 : 0)));
        VarInt.put(b, fateGroup);
        VarInt.put(b, priority);
    }

    /** PIPE_ACK type and entry count; the entries follow. */
    public static void pipeAckHeader(ByteBuffer b, int count) {
        VarInt.put(b, FrameType.PIPE_ACK);
        VarInt.put(b, count);
    }

    public static void ackEntry(ByteBuffer b, long pipeId, long lane, long position, long delay) {
        varints(b, pipeId, lane, position, delay);
    }

    private static void varints(ByteBuffer b, long v0, long v1, long v2, long v3) {
        VarInt.put(b, v0);
        VarInt.put(b, v1);
        VarInt.put(b, v2);
        VarInt.put(b, v3);
    }

    public static int ackEntryLength(long pipeId, long lane, long position, long delay) {
        return VarInt.length(pipeId) + VarInt.length(lane) + VarInt.length(position) + VarInt.length(delay);
    }

    public static void pipeLane(ByteBuffer b, byte[] laneKey, long pipeId, long lane) {
        VarInt.put(b, FrameType.PIPE_LANE);
        b.put(laneKey);
        VarInt.put(b, pipeId);
        VarInt.put(b, lane);
    }

    public static void pipePing(ByteBuffer b) {
        VarInt.put(b, FrameType.PIPE_PING);
    }

    public static void pipeAbandon(ByteBuffer b, long pipeId, long code) {
        VarInt.put(b, FrameType.PIPE_ABANDON);
        VarInt.put(b, pipeId);
        VarInt.put(b, code);
    }

    public static void pipeStatus(ByteBuffer b, long pipeId, long seq, long status, long priority) {
        VarInt.put(b, FrameType.PIPE_STATUS);
        varints(b, pipeId, seq, status, priority);
    }

    /**
     * Re-encodes a decoded frame in its shortest form (tests and tools; the core
     * builds frames with the encoders above).
     */
    public static void encode(ByteBuffer b, Frame f) {
        ByteBuffer s = f.src;
        switch (f.kind) {
            case PADDING -> {
                for (int k = 0; k < f.count; k++) b.put((byte) 0);
            }
            case STREAM -> {
                if (f.hasOffset && f.offset == 0) {
                    int t = (int) FrameType.STREAM | FrameType.STREAM_OFF | (f.hasLength ? FrameType.STREAM_LEN : 0)
                            | (f.fin ? FrameType.STREAM_FIN : 0);
                    b.put((byte) t);
                    VarInt.put(b, f.streamId);
                    b.put((byte) 0);
                    if (f.hasLength) VarInt.put(b, f.dataLen);
                } else {
                    streamHeader(b, f.streamId, f.offset, f.dataLen, f.fin, f.hasLength);
                }
                b.put(s.slice(f.dataPos, f.dataLen));
            }
            case RESET_STREAM -> resetStream(b, f.streamId, f.code, f.finalSize);
            case RESET_STREAM_AT -> resetStreamAt(b, f.streamId, f.code, f.finalSize, f.reliableSize);
            case STOP_SENDING -> stopSending(b, f.streamId, f.code);
            case MAX_DATA -> maxData(b, f.value);
            case MAX_STREAM_DATA -> maxStreamData(b, f.streamId, f.value);
            case MAX_STREAMS -> maxStreams(b, f.bidi, f.value);
            case DATA_BLOCKED -> dataBlocked(b, f.value);
            case STREAM_DATA_BLOCKED -> streamDataBlocked(b, f.streamId, f.value);
            case STREAMS_BLOCKED -> streamsBlocked(b, f.bidi, f.value);
            case CONNECTION_CLOSE -> close(b, f.application, f.code, f.errorFrameType, f.dataBytes());
            case DATAGRAM -> {
                datagramHeader(b, f.dataLen, f.hasLength);
                b.put(s.slice(f.dataPos, f.dataLen));
            }
            case TRANSPORT_PARAMETERS -> transportParameters(b, f.dataBytes());
            case PING -> ping(b, f.response, f.value);
            case GOAWAY -> goAway(b, f.maxBidi, f.maxUni, f.code);
            case PRIORITY_UPDATE -> priorityUpdate(b, f.streamId, f.seq, f.urgency, f.incremental);
            case PIPE_JOIN -> {
                Frame.Join j = f.join;
                byte[] loc = new byte[j.locatorLen];
                s.get(j.locatorPos, loc);
                joinMacInput(b, loc, j.pipeId, j.direction, j.carrierBound, j.standby, j.fateGroup, j.priority);
                b.put(s.slice(j.tokenPos, 16));
            }
            case PIPE_ACK -> {
                pipeAckHeader(b, f.count);
                Frame.AckEntries it = f.entries(new Frame.AckEntries());
                while (it.next()) ackEntry(b, it.pipeId, it.lane, it.position, it.delay);
            }
            case PIPE_LANE -> {
                byte[] key = new byte[16];
                s.get(f.laneKeyPos, key);
                pipeLane(b, key, f.pipeId, f.lane);
            }
            case PIPE_PING -> pipePing(b);
            case PIPE_ABANDON -> pipeAbandon(b, f.pipeId, f.code);
            case PIPE_STATUS -> pipeStatus(b, f.pipeId, f.seq, f.status, f.priority);
        }
    }
}
