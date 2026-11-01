package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.wire.ChannelDatagram;
import io.zmux.internal.wire.FrameDecoder;
import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameKind;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.TransportParams;
import io.zmux.carrier.VarInt;
import io.zmux.internal.wire.WireException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Datagrams on a carrier's datagram channel (EXTENSIONS.md Section 6.8): wire format, choice, receiving, holding. */
@Timeout(30)
class ChannelDatagramTest {
    /** A fake channel: a Pipe Stream ID and the current maximum. */
    static final class FakeChannel implements DatagramChannel {
        final long id;
        int max;
        FakeChannel(long id, int max) { this.id = id; this.max = max; }
        @Override public long channelStreamId() { return id; }
        @Override public int maxChannelDatagram() { return max; }
    }

    /** Two cores over one in-memory pipe, each with an optional datagram channel. */
    static final class Pair {
        final Session a, b;
        final FakeChannel ca, cb;
        long now;
        /** Channel datagrams a sent, as they went out. */
        final List<byte[]> fromA = new ArrayList<>();
        final List<byte[]> recordsA = new ArrayList<>();

        Pair(FakeChannel ca, FakeChannel cb) { this(ca, cb, 1 << 20); }

        Pair(FakeChannel ca, FakeChannel cb, long receiveBytes) {
            this.ca = ca;
            this.cb = cb;
            a = session(CoreConfig.Role.INITIATOR, ca, receiveBytes);
            b = session(CoreConfig.Role.RESPONDER, cb, receiveBytes);
        }

        static Session session(CoreConfig.Role role, DatagramChannel ch, long receiveBytes) {
            CoreConfig.Builder c = CoreConfig.builder().role(role).transportRole(role).datagrams(true).datagramReceiveBytes(receiveBytes);
            TransportParams l = c.local();
            l.present |= TransportParams.DATAGRAM | TransportParams.FLOWS;
            l.maxDatagramFrameSize = 65535;
            Session s = new Session(c.build(), 0);
            CarrierInfo info = new CarrierInfo(Framing.BYTES, false, true, false, true).withChannel(ch);
            s.attachBootstrap(info, info, null);
            return s;
        }

        /** Moves records both ways until quiet; channel datagrams of a are collected, of b dropped. */
        void pump() {
            Batch batch = new Batch();
            for (int round = 0; round < 50; round++) {
                boolean moved = false;
                for (Session from : new Session[] {a, b}) {
                    Session to = from == a ? b : a;
                    while (from.pollTransmit(0, 0, batch, Session.DEFAULT_BATCH, now)) {
                        byte[] bytes = BatchFixture.flat(batch);
                        from.writeDone(batch, null, now);
                        if (from == a) recordsA.add(bytes);
                        ByteBuffer bb = ByteBuffer.wrap(bytes);
                        assertEquals(bytes.length, to.receive(0, 0, new ReadBuf(bb, null), bb, 0, bytes.length, now));
                        moved = true;
                    }
                    List<byte[]> out = new ArrayList<>();
                    from.pollChannel(0, out, new ArrayList<>(), now);
                    if (from == a) fromA.addAll(out);
                    moved |= !out.isEmpty();
                }
                if (!moved) return;
            }
            fail("pump does not converge");
        }

        /** Delivers a's channel datagrams to b, Pipe Stream ID removed. */
        void deliverToB() {
            for (byte[] d : fromA) toB(d, ca.id);
            fromA.clear();
        }

        void toB(byte[] d, long id) {
            ByteBuffer bb = ByteBuffer.wrap(d);
            int from = id == ChannelDatagram.NO_ID ? 0 : VarInt.lengthOf(d[0]);
            b.receiveChannelDatagram(0, bb, from, d.length, now);
        }
    }

    static byte[] bytes(int n, int seed) {
        byte[] p = new byte[n];
        for (int i = 0; i < n; i++) p[i] = (byte) (seed + i);
        return p;
    }

    /** Whether any of the records a sent carries a DATAGRAM frame. */
    static boolean datagramInRecords(List<byte[]> records) {
        FrameDecoder d = new FrameDecoder();
        Frame f = new Frame();
        for (byte[] batch : records) {
            ByteBuffer b = ByteBuffer.wrap(batch);
            for (int i = 0; i < batch.length; ) {
                int size = (int) VarInt.get(b, i, batch.length);
                int start = i + VarInt.lengthOf(batch[i]), end = start + size;
                for (int p = start; p < end; ) {
                    try { p += d.decode(b, p, end, f); } catch (WireException e) { throw new AssertionError(e); }
                    if (f.kind == FrameKind.DATAGRAM) return true;
                }
                i = end;
            }
        }
        return false;
    }

    // ---- wire ----

    @Test void wireEncodesAndDecodesOneDatagramFrame() throws Exception {
        byte[] data = {0, 'h', 'i'};
        byte[] d = ChannelDatagram.encode(70, data);
        assertEquals(ChannelDatagram.length(70, data.length), d.length);
        assertArrayEquals(new byte[] {0x40, 70, 0x30, 0, 'h', 'i'}, d);
        ByteBuffer b = ByteBuffer.wrap(d);
        assertEquals(70, ChannelDatagram.pipeStreamId(b, 0, d.length));
        Frame f = new Frame();
        ChannelDatagram.decodeFrame(new FrameDecoder(), b, 2, d.length, f);
        assertEquals(FrameKind.DATAGRAM, f.kind);
        assertEquals(3, f.dataLen);
        assertArrayEquals(new byte[] {0x30, 1}, ChannelDatagram.encode(ChannelDatagram.NO_ID, new byte[] {1}));
        assertEquals(-1, ChannelDatagram.pipeStreamId(ByteBuffer.wrap(new byte[] {0x40}), 0, 1));
        assertEquals(-1, ChannelDatagram.pipeStreamId(ByteBuffer.wrap(new byte[0]), 0, 0));
    }

    @Test void wireRejectsAnythingButOneDatagramFrameWithoutLength() {
        byte[][] bad = {
            {}, // truncated
            {0x31, 2, 'h', 'i'}, // DATAGRAM with Length
            {0x01}, // PING
            {0x40, 0x30, 'x'}, // a non-minimal type
            {0x00, 0x30, 'x'}, // PADDING first
        };
        for (byte[] d : bad) {
            WireException e = assertThrows(WireException.class,
                    () -> ChannelDatagram.decodeFrame(new FrameDecoder(), ByteBuffer.wrap(d), 0, d.length, new Frame()));
            assertEquals(TransportError.PROTOCOL_VIOLATION, e.code());
        }
    }

    @Test void parameterRoundTrips() throws Exception {
        TransportParams p = new TransportParams();
        p.present = TransportParams.CARRIER_DATAGRAMS | TransportParams.DATAGRAM;
        p.maxDatagramFrameSize = 1200;
        byte[] enc = p.toBytes();
        TransportParams q = TransportParams.decode(ByteBuffer.wrap(enc), 0, enc.length);
        assertTrue(q.has(TransportParams.CARRIER_DATAGRAMS));
        assertTrue(q.isZmux());
        byte[] nonEmpty = {(byte) 0x80, 0x3d, 0x27, 0x4c, 0x5f, 1, 0};
        assertThrows(WireException.class, () -> TransportParams.decode(ByteBuffer.wrap(nonEmpty), 0, nonEmpty.length));
    }

    // ---- choice ----

    @Test void negotiatedChannelCarriesDatagramsThatFit() throws Exception {
        Pair p = new Pair(new FakeChannel(0, 1200), new FakeChannel(0, 1200));
        p.pump();
        assertTrue(p.a.features().carrierDatagrams());
        assertTrue(p.a.localParams().has(TransportParams.CARRIER_DATAGRAMS));
        p.a.sendDatagram(null, 0, bytes(100, 1));
        p.recordsA.clear();
        p.pump();
        assertEquals(1, p.fromA.size());
        assertFalse(datagramInRecords(p.recordsA));
        byte[] d = p.fromA.get(0);
        assertEquals(0, d[0]); // Pipe Stream ID 0
        assertEquals(0x30, d[1]);
        assertEquals(0, d[2]); // flow 0: a session datagram
        p.deliverToB();
        assertArrayEquals(bytes(100, 1), p.b.datagrams().receive());
        assertEquals(1, p.a.stats().datagramsSent());
        assertEquals(1, p.a.stats().datagramsSentChannel());
        assertEquals(1, p.b.stats().datagramsReceived());
        assertEquals(1, p.b.stats().datagramsReceivedChannel());
        p.a.channelSendFailed(new Datagrams.ChannelSent(null, 100));
        assertEquals(0, p.a.stats().datagramBytesSent());
        assertEquals(0, p.a.stats().datagramsSent());
        assertEquals(0, p.a.stats().datagramsSentChannel());
        assertEquals(1, p.a.stats().datagramsDroppedSend());
    }

    @Test void datagramsGoInRecordsWithoutTheChannel() throws Exception {
        record Case(String name, FakeChannel a, FakeChannel b, int size, boolean shaped) {}
        for (Case c : List.of(
                new Case("peer has no channel", new FakeChannel(0, 1200), null, 100, false),
                new Case("carrier did not negotiate datagrams", new FakeChannel(0, 0), new FakeChannel(0, 1200), 100, false),
                new Case("too large", new FakeChannel(0, 1200), new FakeChannel(0, 1200), 1198, false),
                new Case("shaped", new FakeChannel(0, 1200), new FakeChannel(0, 1200), 100, true))) {
            Pair p = new Pair(c.a, c.b);
            p.pump();
            if (c.shaped) p.a.setPipeShaper(0, new io.zmux.shaping.Shaper() {
                @Override public Set<io.zmux.shaping.Capability> capabilities() { return Set.of(); }
                @Override public void plan(io.zmux.shaping.RecordView view, io.zmux.shaping.RecordPlan plan) {}
                @Override public void onEvent(io.zmux.shaping.Event event, io.zmux.shaping.Reply reply) {}
            });
            p.recordsA.clear();
            p.a.sendDatagram(null, 0, bytes(c.size, 2));
            p.pump();
            assertTrue(p.fromA.isEmpty(), c.name);
            assertTrue(datagramInRecords(p.recordsA), c.name);
            assertArrayEquals(bytes(c.size, 2), p.b.datagrams().receive(), c.name);
            assertEquals(0, p.a.stats().datagramsSentChannel(), c.name);
            assertEquals(1, p.a.stats().datagramsSent(), c.name);
            assertEquals(0, p.b.stats().datagramsReceivedChannel(), c.name);
        }
    }

    @Test void theLargestThatFitsGoesOnTheChannel() throws Exception {
        Pair p = new Pair(new FakeChannel(0, 1200), new FakeChannel(0, 1200));
        p.pump();
        p.a.sendDatagram(null, 0, bytes(1197, 3)); // ID 1 + type 1 + flow 1 + 1197 = 1200
        p.pump();
        assertEquals(1, p.fromA.size());
        assertEquals(1200, p.fromA.get(0).length);
    }

    // ---- receiving ----

    @Test void malformedChannelDatagramIsAProtocolViolation() throws Exception {
        Pair p = new Pair(new FakeChannel(0, 1200), new FakeChannel(0, 1200));
        p.pump();
        p.toB(new byte[] {0x31, 1, 0}, ChannelDatagram.NO_ID);
        assertEquals(SessionState.CLOSING, p.b.state());
        assertEquals(TransportError.PROTOCOL_VIOLATION, p.b.error().code());
    }

    @Test void datagramForNoPipeOrWithoutNegotiationIsDropped() throws Exception {
        Pair p = new Pair(new FakeChannel(0, 1200), new FakeChannel(0, 1200));
        p.pump();
        p.b.receiveChannelDatagram(7, ByteBuffer.wrap(new byte[] {0x30, 0, 1}), 0, 3, 0);
        assertEquals(1, p.b.stats().datagramsDroppedRecv());
        Pair q = new Pair(new FakeChannel(0, 1200), null);
        q.pump();
        assertFalse(q.b.features().carrierDatagrams());
        q.toB(new byte[] {0x30, 0, 1}, ChannelDatagram.NO_ID);
        assertEquals(1, q.b.stats().datagramsDroppedRecv());
        assertNull(q.b.datagrams().receive());
        assertNull(q.b.error());
    }

    @Test void channelDatagramIsNotARecord() throws Exception {
        Pair p = new Pair(new FakeChannel(0, 1200), new FakeChannel(0, 1200));
        p.pump();
        long records = p.b.stats().recordsReceived();
        p.toB(new byte[] {0x30, 0, 9}, ChannelDatagram.NO_ID);
        assertEquals(records, p.b.stats().recordsReceived());
        assertArrayEquals(new byte[] {9}, p.b.datagrams().receive());
    }

    // ---- holding ----

    /** A channel datagram for a's first bidirectional stream (ID 0): flow 1, context ctx. */
    static byte[] streamDatagram(long ctx, byte[] payload) {
        ByteBuffer b = ByteBuffer.allocate(16 + payload.length);
        VarInt.put(b, 1);
        VarInt.put(b, ctx);
        b.put(payload);
        byte[] data = java.util.Arrays.copyOf(b.array(), b.position());
        return ChannelDatagram.encode(ChannelDatagram.NO_ID, data);
    }

    static Stream openAndWrite(Pair p) throws Exception {
        Stream st = p.a.streams().openStream(true);
        p.a.sending().write(st, ByteBuffer.wrap(new byte[] {1}), false);
        p.pump();
        return p.b.streams.lookup(0, true);
    }

    @Test void datagramsOvertakingTheStreamOpeningAreHeldAndDeliveredInOrder() throws Exception {
        Pair p = new Pair(new FakeChannel(0, 1200), new FakeChannel(0, 1200));
        p.pump();
        p.toB(streamDatagram(5, new byte[] {'a'}), ChannelDatagram.NO_ID);
        p.toB(streamDatagram(6, new byte[] {'b'}), ChannelDatagram.NO_ID);
        // Counted as received once delivered.
        assertEquals(0, p.b.stats().datagramsReceived());
        assertEquals(0, p.b.stats().datagramsDroppedRecv());
        assertEquals(2 * (Datagrams.HELD_COST + 2L), p.b.datagrams().receivedBytes());
        assertNotEquals(Time.NEVER, p.b.nextTimeout());
        Stream st = openAndWrite(p);
        assertEquals(2, p.b.stats().datagramsReceived());
        assertEquals(2, p.b.stats().datagramsReceivedChannel());
        assertNotNull(st);
        var first = p.b.datagrams().receive(st);
        var second = p.b.datagrams().receive(st);
        assertEquals(5, first.context());
        assertArrayEquals(new byte[] {'a'}, first.payload());
        assertEquals(6, second.context());
        assertNull(p.b.datagrams().receive(st));
        assertEquals(0, p.b.datagrams().receivedBytes());
    }

    @Test void heldDatagramsExpireAfterOneSmoothedRtt() throws Exception {
        Pair p = new Pair(new FakeChannel(0, 1200), new FakeChannel(0, 1200));
        p.pump();
        p.toB(streamDatagram(0, new byte[] {'a'}), ChannelDatagram.NO_ID);
        long due = p.now + p.b.srtt();
        assertTrue(p.b.nextTimeout() <= due);
        p.b.handleTimeout(due - 1);
        assertEquals(0, p.b.stats().datagramsDroppedRecv());
        p.now = due;
        p.b.handleTimeout(due);
        assertEquals(1, p.b.stats().datagramsDroppedRecv());
        assertEquals(0, p.b.stats().datagramsReceived());
        assertEquals(0, p.b.datagrams().receivedBytes());
        Stream st = openAndWrite(p);
        assertNull(p.b.datagrams().receive(st));
    }

    @Test void heldDatagramsGiveWayUnderMemoryPressure() throws Exception {
        // Each held datagram costs its 2 bytes and the fixed overhead: two fit, a third pushes out the oldest.
        Pair p = new Pair(new FakeChannel(0, 1200), new FakeChannel(0, 1200), 2 * (Datagrams.HELD_COST + 2));
        p.pump();
        for (int i = 0; i < 3; i++) p.toB(streamDatagram(i, new byte[] {'x'}), ChannelDatagram.NO_ID);
        assertEquals(1, p.b.stats().datagramsDroppedRecv());
        Stream st = openAndWrite(p);
        assertEquals(1, p.b.datagrams().receive(st).context());
        assertEquals(2, p.b.datagrams().receive(st).context());
        assertNull(p.b.datagrams().receive(st));
        assertEquals(2, p.b.stats().datagramsReceived());
    }

    @Test void inRecordDatagramsOvertakingTheOpeningAreHeldToo() throws Exception {
        Pair p = new Pair(null, null);
        p.pump();
        // A record with only a DATAGRAM frame for stream 0, before the stream's opening frame.
        byte[] frame = streamDatagram(3, new byte[] {'r'});
        ByteBuffer rec = ByteBuffer.allocate(8 + frame.length);
        VarInt.put(rec, frame.length);
        rec.put(frame);
        byte[] r = java.util.Arrays.copyOf(rec.array(), rec.position());
        ByteBuffer bb = ByteBuffer.wrap(r);
        assertEquals(r.length, p.b.receive(0, 0, new ReadBuf(bb, null), bb, 0, r.length, p.now));
        assertEquals(0, p.b.stats().datagramsDroppedRecv());
        Stream st = openAndWrite(p);
        assertEquals(3, p.b.datagrams().receive(st).context());
    }
}
