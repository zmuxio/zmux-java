package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.VarInt;
import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameDecoder;
import io.zmux.internal.wire.FrameKind;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Stream-level control frames of a local stream wait for its opening frame (SPEC.md 7.2,
 * EXTENSIONS.md 4.4): before it they would name an identifier the peer has not seen, or
 * none at all.
 */
@Timeout(30)
class HeldControlTest {
    record Sent(FrameKind kind, long id, long off, int len) {}

    static Session session(CoreConfig.Role role, boolean early) {
        return session(role, early, 0);
    }

    static Session session(CoreConfig.Role role, boolean early, long maxData) {
        CoreConfig.Builder c = CoreConfig.builder().role(role).transportRole(role);
        // Carrier 0-RTT under remembered parameters: the early sends are kept until the carrier decides.
        if (early) c.earlyRemembered(CoreConfig.builder().role(CoreConfig.Role.RESPONDER).build().local()).earlyReplayable(true);
        if (maxData > 0) c.local().initialMaxData = maxData;
        Session s = new Session(c.build(), 0);
        CarrierInfo info = new CarrierInfo(Framing.BYTES, false, true, false, true);
        s.attachBootstrap(info, info, null);
        return s;
    }

    /** Moves bytes both ways until quiet; returns the frames each side sent ([0] by a, [1] by b). */
    @SuppressWarnings("unchecked")
    static List<Sent>[] pump(Session a, Session b) throws Exception {
        List<Sent>[] out = new List[] {new ArrayList<>(), new ArrayList<>()};
        Batch batch = new Batch();
        for (int round = 0; round < 50; round++) {
            boolean moved = false;
            for (Session from : b == null ? new Session[] {a} : new Session[] {a, b}) {
                Session to = from == a ? b : a;
                while (from.pollTransmit(0, 0, batch, Session.DEFAULT_BATCH, 0)) {
                    byte[] bytes = BatchFixture.flat(batch);
                    from.writeDone(batch, null, 0);
                    decode(bytes, out[from == a ? 0 : 1]);
                    if (to == null) continue;
                    ByteBuffer bb = ByteBuffer.wrap(bytes);
                    assertEquals(bytes.length, to.receive(0, 0, new ReadBuf(bb, null), bb, 0, bytes.length, 0));
                    moved = true;
                }
            }
            if (!moved) return out;
        }
        fail("pump does not converge");
        return out;
    }

    static void decode(byte[] data, List<Sent> out) throws Exception {
        ByteBuffer bb = ByteBuffer.wrap(data);
        FrameDecoder dec = new FrameDecoder();
        int i = 0;
        while (i < data.length) {
            long size = VarInt.get(bb, i, data.length);
            int n = VarInt.lengthOf(data[i]);
            int end = i + n + (int) size;
            for (int j = i + n; j < end; ) {
                Frame f = new Frame();
                j += dec.decode(bb, j, end, f);
                out.add(new Sent(f.kind, f.streamId, f.offset, f.dataLen));
            }
            i = end;
        }
    }

    static int index(List<Sent> l, FrameKind k, long id) {
        for (int i = 0; i < l.size(); i++) if (l.get(i).kind() == k && l.get(i).id() == id) return i;
        return -1;
    }

    /** The stream's first frame is its opening STREAM frame; the STOP_SENDING follows with the real identifier. */
    static void assertOpenedFirst(List<Sent> sent, long id, FrameKind ctl) {
        int open = -1;
        for (int i = 0; i < sent.size(); i++) {
            Sent f = sent.get(i);
            if (f.id() != id || f.kind() == FrameKind.MAX_DATA || f.kind() == FrameKind.MAX_STREAMS) continue;
            if (f.kind() != FrameKind.STREAM && f.kind() != FrameKind.STREAM_DATA_BLOCKED) continue;
            open = i;
            break;
        }
        int c = index(sent, ctl, id);
        assertTrue(open >= 0, "stream " + id + " opened: " + sent);
        assertEquals(FrameKind.STREAM, sent.get(open).kind(), "opening frame: " + sent);
        assertEquals(0, sent.get(open).off());
        assertTrue(sent.get(open).len() > 0, "with the preamble");
        assertTrue(c > open, ctl + " after the opening frame: " + sent);
        for (int i = 0; i < open; i++) {
            Sent f = sent.get(i);
            assertFalse(f.kind() == ctl, "no " + ctl + " before the opening frame: " + sent);
        }
    }

    /** The peer's stream 0 is open; the responder stops reading a new stream before writing it. */
    @Test void responderStopBeforeOpeningLeavesThePeersStreamZeroAlone() throws Exception {
        Session a = session(CoreConfig.Role.INITIATOR, false), b = session(CoreConfig.Role.RESPONDER, false);
        Stream s0 = a.streams().openStream(true);
        a.sending().write(s0, ByteBuffer.wrap(new byte[] {1}), false);
        pump(a, b);
        assertNotNull(b.streams().acceptStream(true));
        Stream st = b.streams().openStream(true);
        b.receiving().stop(st, 7);
        b.sending().write(st, ByteBuffer.wrap(new byte[] {2}), true);
        List<Sent> sent = pump(a, b)[1];
        assertNull(a.error(), "initiator alive");
        assertNull(b.error(), "responder alive");
        assertNull(a.sending().writeError(s0), "the peer's stream 0 is untouched");
        assertEquals(-1, index(sent, FrameKind.STOP_SENDING, 0), "nothing names stream 0: " + sent);
        assertOpenedFirst(sent, 1, FrameKind.STOP_SENDING);
    }

    /** The initiator's second stream is stopped before it opens: its own stream 0 stays whole. */
    @Test void initiatorStopBeforeOpeningUsesTheRealIdentifier() throws Exception {
        Session a = session(CoreConfig.Role.INITIATOR, false), b = session(CoreConfig.Role.RESPONDER, false);
        pump(a, b);
        Stream first = a.streams().openStream(true);
        a.receiving().stop(first, 7);
        a.sending().write(first, ByteBuffer.wrap(new byte[] {1}), false);
        List<Sent> sent = pump(a, b)[0];
        assertOpenedFirst(sent, 0, FrameKind.STOP_SENDING);
        Stream st = a.streams().openStream(true);
        a.receiving().stop(st, 7);
        a.sending().write(st, ByteBuffer.wrap(new byte[] {2}), true);
        sent = pump(a, b)[0];
        assertNull(a.error());
        assertNull(b.error());
        assertEquals(-1, index(sent, FrameKind.STOP_SENDING, 0), "nothing names stream 0 again: " + sent);
        assertOpenedFirst(sent, 4, FrameKind.STOP_SENDING);
    }

    /** A larger window set before the stream opens is advertised after its opening frame. */
    @Test void receiveWindowBeforeOpeningFollowsTheOpeningFrame() throws Exception {
        Session a = session(CoreConfig.Role.INITIATOR, false), b = session(CoreConfig.Role.RESPONDER, false);
        Stream s0 = a.streams().openStream(true);
        a.sending().write(s0, ByteBuffer.wrap(new byte[] {1}), false);
        pump(a, b);
        assertNotNull(b.streams().acceptStream(true));
        Stream st = b.streams().openStream(true);
        b.receiving().setReceiveWindow(st, 8 << 20, 0);
        List<Sent> quiet = pump(a, b)[1];
        assertEquals(-1, index(quiet, FrameKind.MAX_STREAM_DATA, 0), "nothing before the stream opens: " + quiet);
        b.sending().write(st, ByteBuffer.wrap(new byte[] {2}), false);
        List<Sent> sent = pump(a, b)[1];
        assertNull(a.error());
        assertNull(b.error());
        assertOpenedFirst(sent, 1, FrameKind.MAX_STREAM_DATA);
    }

    /** A preamble beyond the session credit takes its identifier without opening; STOP_SENDING waits. */
    @Test void stopOnAStreamWhosePreambleExceedsTheCreditWaits() throws Exception {
        Session a = session(CoreConfig.Role.INITIATOR, false, 1000), b = session(CoreConfig.Role.RESPONDER, false);
        Stream s0 = a.streams().openStream(true);
        a.sending().write(s0, ByteBuffer.wrap(new byte[] {1}), false);
        pump(a, b);
        Stream in = b.streams().acceptStream(true);
        assertNotNull(in);
        // Use up the responder's session credit on stream 0 so the next preamble cannot fit.
        b.sending().write(in, ByteBuffer.wrap(new byte[1000]), false);
        pump(a, b);
        Stream st = b.streams().openStream(true);
        b.streams().setOpenInfo(st, new byte[200]);
        b.sending().write(st, ByteBuffer.wrap(new byte[] {2}), true);
        b.receiving().stop(st, 7);
        List<Sent> sent = pump(a, b)[1];
        assertNull(a.error(), "initiator alive");
        assertNull(b.error(), "responder alive");
        assertNull(a.sending().writeError(s0), "the peer's stream 0 is untouched");
        assertEquals(-1, index(sent, FrameKind.STOP_SENDING, 0), "nothing names stream 0: " + sent);
        // Reading frees the credit: the stream opens, and only then its STOP_SENDING goes.
        ByteBuffer sink = ByteBuffer.allocate(1 << 16);
        while (a.receiving().readable(s0) > 0) {
            sink.clear();
            a.receiving().read(s0, sink);
        }
        sent.addAll(pump(a, b)[1]);
        assertNull(a.error());
        assertNull(b.error());
        assertOpenedFirst(sent, 1, FrameKind.STOP_SENDING);
    }

    /** A rejected 0-RTT sends the opening frame again; the STOP_SENDING queued meanwhile waits for it. */
    @Test void stopWaitsForTheOpeningFrameAgainWhenEarlyDataIsRejected() throws Exception {
        Session a = session(CoreConfig.Role.INITIATOR, true);
        Stream st = a.streams().openStream(true);
        a.sending().write(st, ByteBuffer.wrap(new byte[] {1}), false);
        a.receiving().stop(st, 7);
        List<Sent> lost = pump(a, null)[0];
        assertEquals(FrameKind.STREAM, lost.get(index(lost, FrameKind.STREAM, 0)).kind(), "opened early: " + lost);
        assertEquals(-1, index(lost, FrameKind.STOP_SENDING, 0), "early data carries no control: " + lost);
        CarrierInfo info = new CarrierInfo(Framing.BYTES, false, true, false, true);
        assertTrue(a.restartEarly(info, 0));
        Session b = session(CoreConfig.Role.RESPONDER, false);
        List<Sent> sent = pump(a, b)[0];
        assertNull(a.error());
        assertNull(b.error());
        assertOpenedFirst(sent, 0, FrameKind.STOP_SENDING);
    }
}
