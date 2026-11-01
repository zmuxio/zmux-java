package io.zmux.internal.core;

import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameDecoder;
import io.zmux.internal.wire.FrameKind;
import io.zmux.internal.wire.Gates;
import io.zmux.internal.wire.Records;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.TransportParams;
import io.zmux.carrier.VarInt;
import io.zmux.internal.wire.WireException;
import java.nio.ByteBuffer;

/**
 * Splits the bytes of inbound lanes into records and records into frames (SPEC.md
 * Sections 3 and 5): record sizes, the first record of the bootstrap pipe, the PIPE_LANE
 * record of a lane, gating, and the per-lane positions. Decoded frames go to the session
 * through {@link Port#dispatch}; owned by the session's loop.
 */
final class RecordReceiver {
    /** What the receiver asks of the session. */
    interface Port {
        SessionState state();

        boolean established();

        Gates gates();

        /** The peer's QX_TRANSPORT_PARAMETERS arrived. */
        void peerParams(TransportParams tp) throws WireException;

        /** Applies one decoded frame. */
        void dispatch(Pipe p, Frame f, ReadBuf buf) throws WireException;

        /**
         * Whether a CONNECTION_CLOSE leading the record would answer an unconfirmed local join
         * of p (MULTIPATH.md 5.4 step 3): the peer holds no session for its locator.
         */
        boolean unconfirmedJoin(Pipe p);

        /** The peer answered an unconfirmed local join of p with CONNECTION_CLOSE. */
        void joinAnswered(Pipe p, boolean application, long code, String reason);

        /** A PIPE_ACK while the session waits for its lanes before CONNECTION_CLOSE. */
        void closingAck(Frame f);

        void failLocal(WireException e);

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
    int receive(Pipe p, long lane, ReadBuf buf, ByteBuffer data, int pos, int limit, long now) {
        int all = limit - pos;
        if (p == null || p.in() == null) return all;
        Pipe.InLane il = p.in().lane(lane);
        SessionState state = port.state();
        if (il == null || il.rx.ended || state == SessionState.CLOSED) return all;
        if (state == SessionState.CLOSING && !life.waitingLanes()) return all;
        int consumed = 0;
        while (pos + consumed < limit && (!closing() || life.waitingLanes())) {
            int at = pos + consumed;
            if (il.rx.expectTp) {
                WireException e = checkFirstRecord(data, at, limit);
                if (e != null) {
                    port.failLocal(e);
                    return all;
                }
            }
            long size = VarInt.get(data, at, limit);
            if (size < 0) break;
            int n = VarInt.lengthOf(data.get(at));
            long lim = cfg.local().maxRecordSize;
            if (size == 0 || size > lim) {
                port.failLocal(WireException.withDetails(TransportError.FRAME_ENCODING_ERROR, "record size", "value", size,
                        "limit", lim, "pipe", p.id, "pos", il.rx.pos));
                return all;
            }
            if (limit - at - n < size) break;
            consumed += n + (int) size;
            record(p, il, buf, data, at + n, at + n + (int) size, n + size, now);
        }
        return consumed;
    }

    /** Processes one message of a message-framed carrier (SPEC.md Section 3.4). */
    void receiveMessage(Pipe p, long lane, ReadBuf buf, ByteBuffer data, int pos, int limit, long now) {
        if (p == null || p.in() == null || closing()) return;
        Pipe.InLane il = p.in().lane(lane);
        if (il == null || il.rx.ended) return;
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
        record(p, il, buf, data, pos, limit, VarInt.length(len) + len, now);
    }

    /** Validates the prefix of a bootstrap pipe's first record byte by byte (SPEC.md Section 6.2). */
    private WireException checkFirstRecord(ByteBuffer b, int pos, int limit) {
        if (pos >= limit) return null;
        long size = VarInt.get(b, pos, limit);
        if (size < 0) return null;
        int n = VarInt.lengthOf(b.get(pos));
        if (size < 9 || size > cfg.local().maxRecordSize) {
            return WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "not a zmux or QMux peer", "value", size);
        }
        for (int i = 0; i < 8 && pos + n + i < limit; i++) {
            if (b.get(pos + n + i) != Records.TRANSPORT_PARAMETERS_PREFIX[i]) {
                return WireException.protocol("not a zmux or QMux peer");
            }
        }
        return null;
    }

    /** Applies every frame of one complete record, then counts it into the lane's position (MULTIPATH.md 6.1). */
    private void record(Pipe p, Pipe.InLane il, ReadBuf buf, ByteBuffer data, int pos, int end, long posBytes, long now) {
        rxPipe = p;
        int frames = end - pos;
        if (il.rx.discardTp) {
            il.rx.discardTp = false;
            boolean tp = end - pos >= 8;
            for (int i = 0; tp && i < 8; i++) tp = data.get(pos + i) == Records.TRANSPORT_PARAMETERS_PREFIX[i];
            // A shared listener's parameters leading a join carrier: discarded, not counted (MULTIPATH.md 5.7 step 3).
            if (tp) return;
        }
        if (il.lane == 0 && port.unconfirmedJoin(p)) {
            Frame c = frame;
            try {
                decoder.decode(data, pos, end, c);
                if (c.kind == FrameKind.CONNECTION_CLOSE) {
                    port.joinAnswered(p, c.application, c.code, new String(c.dataBytes(), java.nio.charset.StandardCharsets.UTF_8));
                    return;
                }
            } catch (WireException ignored) {
                // Decoded again below, where the failure is reported.
            }
        }
        boolean first = il.rx.expectTp;
        boolean elicit = false, otherElicit = false, activity = false, pipePing = false, traffic = false, onlyPad = true;
        Frame f = frame;
        int idx = 0;
        while (pos < end) {
            int n;
            try {
                n = decoder.decode(data, pos, end, f);
                if (f.kind == FrameKind.TRANSPORT_PARAMETERS && (!first || idx != 0)) {
                    TransportParams.decode(f.src, f.dataPos, f.dataLen);
                    throw new WireException(TransportError.PROTOCOL_VIOLATION, f.type, "QX_TRANSPORT_PARAMETERS out of place", "");
                }
            } catch (WireException e) {
                port.failLocal(e.frameType() != 0 || f.type == 0 ? e
                        : new WireException(e.code(), f.type, e.reason(), e.details()));
                return;
            }
            if (il.rx.skipJoin && idx == 0 && f.kind == FrameKind.PIPE_JOIN) {
                il.rx.skipJoin = false;
                elicit = otherElicit = true;
                pos += n;
                idx++;
                continue;
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
                    TransportParams tp = TransportParams.decode(f.src, f.dataPos, f.dataLen);
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
            try {
                port.gates().check(f);
            } catch (WireException e) {
                port.failLocal(e);
                return;
            }
            if (f.kind != FrameKind.PADDING) onlyPad = false;
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
            if (closing()) {
                // Waiting for lanes before CONNECTION_CLOSE: only acknowledgements and the peer's close matter.
                if (f.kind == FrameKind.PIPE_ACK) port.closingAck(f);
                if (f.kind == FrameKind.CONNECTION_CLOSE) life.lanesDone();
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
        if (p.shape() != null) Shaping.recordIn(p, il.lane, frames, onlyPad);
    }

    /** The first record of an in-band join starts with the PIPE_JOIN the listener processed. */
    static void skipJoin(Pipe.InLane il) {
        il.rx.skipJoin = true;
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
        private boolean skipJoin;
        private boolean discardTp;
        private boolean ended;

        LaneRx(boolean expectTp, boolean expectLane, boolean discardTp) {
            this.expectTp = expectTp;
            this.expectLane = expectLane;
            this.discardTp = discardTp;
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

        boolean skipJoin() {
            return skipJoin;
        }

        boolean discardTp() {
            return discardTp;
        }

        boolean ended() {
            return ended;
        }
    }
}
