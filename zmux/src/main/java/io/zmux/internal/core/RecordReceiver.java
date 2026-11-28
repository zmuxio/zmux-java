package io.zmux.internal.core;

import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.buffer.ReceiveSpans;

import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameDecoder;
import io.zmux.internal.wire.FrameKind;
import io.zmux.internal.wire.Gates;
import io.zmux.internal.wire.Records;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.TransportParams;
import io.zmux.internal.wire.VarInt;
import io.zmux.internal.wire.WireException;
import java.nio.ByteBuffer;

/**
 * Splits the bytes of inbound lanes into records and records into frames: record sizes, the first record of the bootstrap pipe, the PIPE_LANE
 * record of a lane, gating, and the per-lane positions. Decoded frames go to the session
 * through {@link Port#dispatch}; owned by the session's loop.
 */
final class RecordReceiver {
    /** What the receiver asks of the session. */
    interface Port {
        SessionState state();

        boolean established();
        boolean familyConfirmed();

        Gates gates();

        /** The peer's QX_TRANSPORT_PARAMETERS arrived. */
        void peerParams(TransportParams tp) throws WireException;

        /** Applies one decoded frame. */
        void dispatch(Pipe p, Frame f, ReadBuf buf) throws WireException;

        /**
         * A transport frame during the close wait (PIPE_ACK, PIPE_PING, PIPE_ABANDON, PIPE_STATUS,
         * MAX_DATA, MAX_STREAM_DATA): applied as at any other time, an error ignored.
         */
        void closingFrame(Pipe p, Frame f, ReadBuf buf);

        void failLocal(WireException e);

        /** The peer's CONNECTION_CLOSE was read on an inbound pipe of a session that lingers: its linger may end. */
        void closeRead();

        /** The peer's close releases the local CONNECTION_CLOSE from its coverage wait. */
        void closeWaitDone();

        /** Charges one no-effect frame or record. */
        void charge();

        /**
         * A record was applied and counted into its lane's position. pingOnly: PIPE_PING is its only ack-eliciting
         * frame.
         */
        void recorded(Pipe p, Pipe.InLane il, long posBytes, boolean elicit, boolean pipePing, boolean pingOnly, boolean traffic,
                      boolean activity);
    }

    private final CoreConfig cfg;
    private final SessionStats stats;
    private final Lifecycle life;
    private final Port port;
    private final FrameDecoder decoder = new FrameDecoder();
    private final Frame frame = new Frame();
    /** The pipe of the record being processed. */
    private Pipe rxPipe;

    RecordReceiver(CoreConfig cfg, SessionStats stats, Lifecycle life, Port port) {
        this.cfg = cfg;
        this.stats = stats;
        this.life = life;
        this.port = port;
    }

    /** The pipe of the last record processed, or null. */
    Pipe rxPipe() {
        return rxPipe;
    }

    private boolean closing() {
        return port.state().compareTo(SessionState.CLOSING) >= 0;
    }

    /**
     * Processes the complete records in data[pos, limit) of an inbound lane and returns how
     * many bytes it consumed; the caller keeps the rest (a partial record).
     */
    int receive(Pipe p, long lane, ReadBuf buf, ByteBuffer data, ReceiveSpans spans, int pos, int limit, long now, boolean carrierEarly) {
        int all = limit - pos;
        if (p == null || p.in() == null) return all;
        Pipe.InLane il = p.in().lane(lane);
        SessionState state = port.state();
        if (il == null || il.rx.ended || state == SessionState.CLOSED) return all;
        if (state == SessionState.CLOSING && !life.waitingLanes()) return lingerRead(p, il, data, spans, pos, limit);
        int consumed = 0;
        while (pos + consumed < limit && (!closing() || life.waitingLanes())) {
            int at = pos + consumed;
            if (il.rx.expectTp) {
                WireException e = checkFirstRecord(data, spans, at, limit);
                if (e != null) {
                    port.failLocal(e);
                    return all;
                }
            }
            long size = VarInt.get(data, spans, at, limit);
            if (size < 0) break;
            int n = VarInt.lengthOf(ReceiveSpans.get(data, spans, at));
            long lim = cfg.local().maxRecordSize;
            if (size == 0 || size > lim) {
                port.failLocal(WireException.withDetails(TransportError.FRAME_ENCODING_ERROR, "record size", "value", size,
                        "limit", lim, "pipe", p.id, "pos", il.rx.pos));
                return all;
            }
            if (limit - at - n < size) break;
            int payloadAt = at + n;
            int recordSize = n + (int) size;
            consumed += recordSize;
            if (spans == null) {
                record(p, il, buf, data, null, payloadAt, payloadAt + (int) size, recordSize, now, carrierEarly);
                continue;
            }
            var head = spans.span(payloadAt);
            int from = spans.position(head, payloadAt);
            if (size <= head.to() - from) {
                record(p, il, head.owner(), head.data(), null, from, from + (int) size, recordSize, now, carrierEarly);
            } else {
                record(p, il, null, null, spans, payloadAt, payloadAt + (int) size, recordSize, now, carrierEarly);
            }
        }
        return consumed;
    }

    /** Processes one message of a message-framed carrier. */
    void receiveMessage(Pipe p, long lane, ReadBuf buf, ByteBuffer data, int pos, int limit, long now) {
        if (p == null || p.in() == null) return;
        Pipe.InLane il = p.in().lane(lane);
        if (il == null || il.rx.ended) return;
        // During the close wait records are still processed, as by receive: acknowledgements and the peer's close.
        if (closing() && !(port.state() == SessionState.CLOSING && life.waitingLanes())) {
            if (port.state() == SessionState.CLOSING && lingerWatch(p, il) && holdsClose(data, null, pos, limit)) markClose(p);
            return;
        }
        int len = limit - pos;
        if (len > cfg.local().maxRecordSize) {
            port.failLocal(WireException.frame("message above max_record_size"));
            return;
        }
        if (il.rx.expectTp) {
            boolean ok = len >= 9;
            for (int i = 0; ok && i < 8; i++) {
                ok = data.get(pos + i) == Records.TRANSPORT_PARAMETERS_PREFIX[i];
            }
            if (!ok) {
                port.failLocal(WireException.protocol("first message is not QX_TRANSPORT_PARAMETERS"));
                return;
            }
        }
        record(p, il, buf, data, null, pos, limit, VarInt.length(len) + len, now, false);
    }

    /** Whether a lane of a lingering session is still read for the peer's CONNECTION_CLOSE (lane 0 carries it, once). */
    private static boolean lingerWatch(Pipe p, Pipe.InLane il) {
        return il.lane == 0 && !il.rx.expectTp && !il.rx.expectLane && !p.in().closeRead();
    }

    /**
     * Reads a lingering session's lane: what it holds is discarded without processing, apart from the one thing the
     * linger waits for, the peer's CONNECTION_CLOSE (nothing follows it on the pipe). Returns the bytes consumed;
     * a partial record stays with the caller.
     */
    private int lingerRead(Pipe p, Pipe.InLane il, ByteBuffer data, ReceiveSpans spans, int pos, int limit) {
        int all = limit - pos;
        if (!lingerWatch(p, il)) return all;
        int consumed = 0;
        while (pos + consumed < limit) {
            int at = pos + consumed;
            long size = VarInt.get(data, spans, at, limit);
            if (size < 0) break;
            int n = VarInt.lengthOf(ReceiveSpans.get(data, spans, at));
            if (size == 0 || size > cfg.local().maxRecordSize) return all;
            if (limit - at - n < size) break;
            consumed += n + (int) size;
            if (holdsClose(data, spans, at + n, at + n + (int) size)) {
                markClose(p);
                return all;
            }
        }
        return consumed;
    }

    /** Whether the record in data[pos, end) holds a CONNECTION_CLOSE; undecodable bytes hold none. */
    private boolean holdsClose(ByteBuffer data, ReceiveSpans spans, int pos, int end) {
        Frame f = frame;
        while (pos < end) {
            try {
                pos += decoder.decode(data, spans, pos, end, f);
            } catch (WireException e) {
                return false;
            }
            if (f.kind == FrameKind.CONNECTION_CLOSE) return true;
        }
        return false;
    }

    private void markClose(Pipe p) {
        p.in().readClose();
        port.closeRead();
    }

    /** Validates the prefix of a bootstrap pipe's first record byte by byte. */
    private WireException checkFirstRecord(ByteBuffer b, ReceiveSpans spans, int pos, int limit) {
        if (pos >= limit) return null;
        long size = VarInt.get(b, spans, pos, limit);
        if (size < 0) return null;
        int n = VarInt.lengthOf(ReceiveSpans.get(b, spans, pos));
        if (size < 9 || size > cfg.local().maxRecordSize) {
            return WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "not a zmux or QMux peer", "value", size);
        }
        for (int i = 0; i < 8 && pos + n + i < limit; i++) {
            if (ReceiveSpans.get(b, spans, pos + n + i) != Records.TRANSPORT_PARAMETERS_PREFIX[i]) {
                return WireException.protocol("not a zmux or QMux peer");
            }
        }
        return null;
    }

    /** Applies every frame of one complete record, then counts it into the lane's position. */
    private void record(Pipe p, Pipe.InLane il, ReadBuf buf, ByteBuffer data, ReceiveSpans spans, int pos, int end, long posBytes, long now, boolean carrierEarly) {
        rxPipe = p;
        boolean first = il.rx.expectTp;
        boolean elicit = false, otherElicit = false, activity = false, pipePing = false, traffic = false;
        Frame f = frame;
        int idx = 0;
        while (pos < end) {
            int n;
            try {
                n = decoder.decode(data, spans, pos, end, f);
                if (f.kind == FrameKind.TRANSPORT_PARAMETERS && (!first || idx != 0)) {
                    TransportParams.decode(f.src, f.spans(), f.dataPos, f.dataLen);
                    throw new WireException(TransportError.PROTOCOL_VIOLATION, f.type, "QX_TRANSPORT_PARAMETERS out of place", "");
                }
            } catch (WireException e) {
                port.failLocal(e.frameType() != 0 || f.type == 0 ? e
                        : new WireException(e.code(), f.type, e.reason(), e.details()));
                return;
            }
            if (il.rx.expectLane && idx == 0) {
                if (f.kind != FrameKind.PIPE_LANE) {
                    port.failLocal(WireException.protocol("lane does not start with PIPE_LANE"));
                    return;
                }
                elicit = otherElicit = true;
                pos += n;
                idx++;
                continue;
            }
            if (il.rx.expectLane && f.kind != FrameKind.PADDING) {
                port.failLocal(WireException.protocol("a lane's first record holds a frame other than PIPE_LANE"));
                return;
            }
            if (first && idx == 0) {
                if (f.kind != FrameKind.TRANSPORT_PARAMETERS) {
                    port.failLocal(WireException.protocol("first frame is not QX_TRANSPORT_PARAMETERS"));
                    return;
                }
                try {
                    TransportParams tp = TransportParams.decode(f.src, f.spans(), f.dataPos, f.dataLen);
                    il.rx.expectTp = false;
                    port.peerParams(tp);
                } catch (WireException e) {
                    port.failLocal(e);
                    return;
                }
                elicit = otherElicit = true;
                pos += n;
                idx++;
                continue;
            }
            if (!port.established()) {
                port.failLocal(WireException.protocol("frame before transport parameters"));
                return;
            }
            if (carrierEarly && port.familyConfirmed() && switch (f.kind) {
                case STREAM, RESET_STREAM, RESET_STREAM_AT, STOP_SENDING, DATAGRAM -> true;
                default -> false;
            }) {
                port.failLocal(WireException.protocol("application frame in carrier early data"));
                return;
            }
            try {
                port.gates().check(f);
            } catch (WireException e) {
                port.failLocal(e);
                return;
            }
            if (f.kind.ackEliciting()) {
                elicit = true;
                if (f.kind != FrameKind.PIPE_PING) otherElicit = true;
            }
            switch (f.kind) {
                case STREAM, RESET_STREAM, RESET_STREAM_AT, STOP_SENDING, DATAGRAM -> {
                    activity = true;
                    traffic = true;
                }
                case PING -> activity = true;
                case PIPE_PING -> pipePing = true;
                default -> { }
            }
            if (f.kind == FrameKind.CONNECTION_CLOSE && il.lane == 0) p.in().readClose();
            if (closing()) {
                // The close wait: the record counts (it is acknowledged), acknowledgements and the peer's close apply;
                // the streams failed at the close, so their frames are not applied.
                switch (f.kind) {
                    case PIPE_ACK, PIPE_PING, PIPE_ABANDON, PIPE_STATUS, MAX_DATA, MAX_STREAM_DATA -> port.closingFrame(p, f, buf);
                    default -> { }
                }
                if (f.kind == FrameKind.CONNECTION_CLOSE) port.closeWaitDone();
                pos += n;
                idx++;
                continue;
            }
            try {
                port.dispatch(p, f, buf);
            } catch (WireException e) {
                port.failLocal(e);
                return;
            }
            pos += n;
            idx++;
        }
        il.rx.expectLane = false;
        il.rx.pos += posBytes;
        il.rx.recs++;
        il.rx.posAt = now;
        stats.received(posBytes);
        port.recorded(p, il, posBytes, elicit, pipePing, pipePing && !otherElicit, traffic, activity);
    }

    /** The inbound lane ended; false when it had ended already. */
    boolean laneEnded(Pipe.InLane il) {
        if (il.rx.ended) return false;
        il.rx.ended = true;
        return true;
    }

    /** The receiver's state of one inbound lane. */
    static final class LaneRx {
        private long pos;
        private long posAt;
        private long recs;
        private boolean expectTp;
        private boolean expectLane;
        private boolean ended;

        LaneRx(boolean expectTp, boolean expectLane) {
            this.expectTp = expectTp;
            this.expectLane = expectLane;
        }

        long pos() {
            return pos;
        }

        long posAt() {
            return posAt;
        }

        long recs() {
            return recs;
        }

        boolean expectTp() {
            return expectTp;
        }

        boolean expectLane() {
            return expectLane;
        }

        boolean ended() {
            return ended;
        }
    }
}
