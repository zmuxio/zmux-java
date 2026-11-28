package io.zmux.internal.core;

import io.zmux.internal.buffer.ReadBuf;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.internal.wire.VarInt;
import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameDecoder;
import io.zmux.internal.wire.FrameKind;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Stream-level control frames of a local stream wait for its opening frame: before it they would name an identifier the peer has not seen, or
 * none at all.
 */
@Timeout(30)
class HeldControlTest {
    record Sent(FrameKind kind, long id, long off, int len) {}

    static Session session(CoreConfig.Role role) {
        return session(role, 0);
    }

    static Session session(CoreConfig.Role role, long maxData) {
        CoreConfig.Builder c = CoreConfig.builder().role(role).transportRole(role);
        if (maxData > 0) c.local().initialMaxData = maxData;
        Session s = new Session(c.build(), 0);
        CarrierInfo info = new CarrierInfo(Framing.BYTES, false, true, false, true);
        s.attachBootstrap(info, info);
        return s;
    }

    /** Moves bytes both ways until quiet; returns the frames each side sent ([0] by a, [1] by b). */
    @SuppressWarnings({"unchecked", "rawtypes"}) // an array of the two sides' lists: generic arrays are created raw
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
        Session a = session(CoreConfig.Role.INITIATOR), b = session(CoreConfig.Role.RESPONDER);
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
        Session a = session(CoreConfig.Role.INITIATOR), b = session(CoreConfig.Role.RESPONDER);
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
        Session a = session(CoreConfig.Role.INITIATOR), b = session(CoreConfig.Role.RESPONDER);
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

    /** A preamble beyond the session credit takes its identifier without opening; STOP_SENDING goes, naming it. */
    @Test void stopOnAStreamWhosePreambleExceedsTheCreditGoesAtOnce() throws Exception {
        Session a = session(CoreConfig.Role.INITIATOR, 1000), b = session(CoreConfig.Role.RESPONDER);
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
        int stop = index(sent, FrameKind.STOP_SENDING, 1);
        assertTrue(stop >= 0, "STOP_SENDING before the blocked opening frame: " + sent);
        // Reading frees the credit: the stream opens.
        ByteBuffer sink = ByteBuffer.allocate(1 << 16);
        while (a.receiving().readable(s0) > 0) {
            sink.clear();
            a.receiving().read(s0, sink);
        }
        sent.addAll(pump(a, b)[1]);
        assertNull(a.error());
        assertNull(b.error());
        assertTrue(index(sent, FrameKind.STREAM, 1) >= 0, "opened: " + sent);
    }

    /** Scenario A: an identifier without an opening frame (no session credit), then a reset: RESET_STREAM goes at once. */
    @Test void resetAfterAllocationWithoutOpening() throws Exception {
        Session a = session(CoreConfig.Role.INITIATOR), b = session(CoreConfig.Role.RESPONDER, 1);
        pump(a, b);
        Stream fill = a.streams().openStream(true);
        a.sending().write(fill, ByteBuffer.wrap(new byte[] {1}), false);
        pump(a, b);
        Stream st = a.streams().openStream(true);
        a.sending().write(st, ByteBuffer.wrap(new byte[] {2, 3}), false);
        pump(a, b);
        assertTrue(st.idSet(), "the identifier is allocated");
        a.sending().reset(st, 9);
        List<Sent> sent = pump(a, b)[0];
        assertTrue(index(sent, FrameKind.RESET_STREAM_AT, st.id()) >= 0, "RESET_STREAM_AT: " + sent);
        assertEquals(0, ControlQueue.bits(st), "no bits left");
        assertNull(a.error());
        assertNull(b.error());
    }

    /** Scenario B: a preamble beyond the stream credit reports STREAM_DATA_BLOCKED at once, and the peer's credit opens it. */
    @Test void preambleBeyondStreamCreditReportsBlocked() throws Exception {
        Session a = session(CoreConfig.Role.INITIATOR), b = session(CoreConfig.Role.RESPONDER);
        pump(a, b);
        a.peer().maxStreamDataBidiRemote = 8;
        Stream st = a.streams().openStream(true);
        a.streams().setOpenInfo(st, new byte[64]);
        a.sending().write(st, ByteBuffer.wrap(new byte[] {1}), true);
        List<Sent> sent = pump(a, b)[0];
        assertTrue(st.idSet());
        assertTrue(index(sent, FrameKind.STREAM_DATA_BLOCKED, st.id()) >= 0, "blocked report: " + sent);
        assertEquals(-1, index(sent, FrameKind.STREAM, st.id()), "the opening frame waits: " + sent);
        a.send.onSendCredit(st, 1 << 16);
        sent = pump(a, b)[0];
        assertTrue(index(sent, FrameKind.STREAM, st.id()) >= 0, "credit lets it finish: " + sent);
        assertNotNull(b.streams().acceptStream(true), "the opening frame delivers the preamble");
        assertNull(a.error());
        assertNull(b.error());
    }

    /** Data before flush goes in the opening frame; a stream opened with nothing written sends an empty one. */
    @Test void flushSendsTheOpeningFrame() throws Exception {
        Session a = session(CoreConfig.Role.INITIATOR), b = session(CoreConfig.Role.RESPONDER);
        pump(a, b);
        Stream empty = a.streams().openStream(true);
        Waiter none = new Waiter() { @Override public void wake() { } };
        long target = a.streams().flush(empty);
        assertFalse(a.sending().flushDone(empty, target, none), "not sent yet");
        List<Sent> sent = pump(a, b)[0];
        assertTrue(a.sending().flushDone(empty, target, none), "sent");
        assertTrue(index(sent, FrameKind.STREAM, empty.id()) >= 0, "the opening frame: " + sent);
        assertNotNull(b.streams().acceptStream(true));
        Stream full = a.streams().openStream(true);
        a.sending().write(full, ByteBuffer.wrap(new byte[] {1, 2}), false);
        target = a.streams().flush(full);
        sent = pump(a, b)[0];
        Sent first = sent.get(index(sent, FrameKind.STREAM, full.id()));
        assertEquals(0, first.off());
        assertEquals(3, first.len(), "the preamble and the data written before go in the opening frame");
        assertTrue(a.sending().flushDone(full, target, none));
    }
    /** A STOP_SENDING after the flushed data left does not fail the flush; for data not yet sent it does. */
    @Test void aStopAfterTheDataWasSentLeavesTheFlushDone() throws Exception {
        Session a = session(CoreConfig.Role.INITIATOR), b = session(CoreConfig.Role.RESPONDER);
        pump(a, b);
        Waiter none = new Waiter() { @Override public void wake() { } };
        Stream st = a.streams().openStream(true);
        a.sending().write(st, ByteBuffer.wrap(new byte[] {1, 2}), false);
        long target = a.streams().flush(st);
        pump(a, b);
        assertTrue(a.sending().flushDone(st, target, none), "sent");
        Stream peer = b.streams().acceptStream(true);
        assertNotNull(peer);
        b.receiving().stop(peer, 9);
        pump(a, b);
        assertTrue(a.sending().flushDone(st, target, none), "the data left before the STOP_SENDING");
        assertThrows(CoreException.class, () -> a.sending().flushDone(st, target + 1, none), "data not sent reports the stop");
    }
}
