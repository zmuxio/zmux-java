package io.zmux.internal.core;

import io.zmux.internal.buffer.ReadBuf;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.CarrierChannel.Outcome;
import io.zmux.internal.wire.ChannelDatagram;
import io.zmux.internal.wire.FrameDecoder;
import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameKind;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.TransportParams;
import io.zmux.internal.wire.VarInt;
import io.zmux.internal.wire.WireException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Datagrams on a carrier's datagram channel: wire format, choice, receiving, holding. */
@Timeout(30)
class ChannelDatagramTest {
    /** A fake channel: a Pipe Stream ID and the current maximum. */
    static final class FakeChannel implements DatagramChannel {
        long id;
        int max;
        Integer limit;
        boolean defaultLimit;
        FakeChannel(long id, int max) { this.id = id; this.max = max; }
        @Override public long channelStreamId() { return id; }
        @Override public int maxChannelDatagram() { return max; }
        @Override public int channelDatagramLimit(int room) {
            return defaultLimit ? DatagramChannel.super.channelDatagramLimit(room) : limit == null ? room : limit;
        }
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
            this(ca, cb, receiveBytes, CoreConfig.DEFAULT_DATAGRAM_STREAM_BYTES);
        }

        Pair(FakeChannel ca, FakeChannel cb, long receiveBytes, long streamReceiveBytes) {
            this.ca = ca;
            this.cb = cb;
            a = session(CoreConfig.Role.INITIATOR, ca, receiveBytes);
            b = session(CoreConfig.Role.RESPONDER, cb, receiveBytes, streamReceiveBytes);
        }

        static Session session(CoreConfig.Role role, DatagramChannel ch, long receiveBytes) {
            return session(role, ch, receiveBytes, CoreConfig.DEFAULT_DATAGRAM_STREAM_BYTES);
        }

        private static Session session(CoreConfig.Role role, DatagramChannel ch, long receiveBytes, long streamReceiveBytes) {
            CoreConfig.Builder c = CoreConfig.builder().role(role).transportRole(role).datagrams(true)
                    .datagramReceiveBytes(receiveBytes).datagramStreamBytes(streamReceiveBytes);
            TransportParams l = c.local();
            l.present |= TransportParams.DATAGRAM;
            l.maxDatagramFrameSize = 65535;
            Session s = new Session(c.build(), 0);
            CarrierInfo info = new CarrierInfo(Framing.BYTES, false, true, false, true).withChannel(ch);
            s.attachBootstrap(info, info);
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
                    var tickets = new ArrayList<Datagrams.ChannelSent>();
                    from.pollChannel(0, out, tickets, now);
                    for (var ticket : tickets) from.channelDone(ticket, Outcome.ACCEPTED, now);
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

    @Test void legacyPeerKeepsUnpaddedNativeDatagrams() throws Exception {
        var p = new Pair(new FakeChannel(0, 1200), new FakeChannel(0, 1200));
        p.b.localParams().present &= ~TransportParams.CARRIER_DATAGRAM_PADDING;
        p.pump();
        assertTrue(p.a.features().carrierDatagrams());
        assertFalse(p.a.features().carrierDatagramPadding());
        assertFalse(p.b.features().carrierDatagramPadding());
        p.a.sendDatagram(null, 0, new byte[] {0x61, 0});
        p.pump();
        assertArrayEquals(new byte[] {0, 0x30, 0, 0, 0x61, 0}, p.fromA.getFirst());
        p.deliverToB();
        assertArrayEquals(new byte[] {0x61, 0}, p.b.datagrams().receive());
        p.toB(new byte[] {0, 0, 0x30, 1, 0, 0x61}, 0);
        assertEquals(TransportError.PROTOCOL_VIOLATION, p.b.error().code());
    }

    @Test void negotiatedPaddingScanIsBoundedAndPreservesTheTail() throws Exception {
        int cap = ChannelDatagram.MAX_PADDED_FRAME;
        byte[] bytes = ChannelDatagram.encode(0, new byte[] {0, 0, 0x61, 0}, cap - 5);
        Frame f = new Frame();
        ChannelDatagram.decodeFrame(new FrameDecoder(), ByteBuffer.wrap(bytes), 1, bytes.length, f, true);
        assertEquals(4, f.dataLen);
        assertEquals(0, bytes[bytes.length - 1]);
        var oversized = java.util.Arrays.copyOf(bytes, bytes.length + 1);
        assertThrows(WireException.class, () -> ChannelDatagram.decodeFrame(new FrameDecoder(),
                ByteBuffer.wrap(oversized), 1, oversized.length, f, true));
        var pure = ByteBuffer.wrap(new byte[cap]);
        assertThrows(WireException.class, () -> ChannelDatagram.decodeFrame(new FrameDecoder(), pure, 0, cap, f, true));
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
        p.present = TransportParams.FAMILY | TransportParams.CARRIER_DATAGRAMS | TransportParams.DATAGRAM;
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
        assertEquals(0, d[2]); // sequence 0
        assertEquals(0, d[3]); // flow 0: a session datagram
        p.deliverToB();
        assertArrayEquals(bytes(100, 1), p.b.datagrams().receive());
        assertEquals(1, p.a.stats().datagramsSent());
        assertEquals(1, p.a.stats().datagramsSentChannel());
        assertEquals(1, p.b.stats().datagramsReceived());
        assertEquals(1, p.b.stats().datagramsReceivedChannel());
        p.a.sendDatagram(null, 0, bytes(100, 1));
        var tickets = new ArrayList<Datagrams.ChannelSent>();
        p.a.pollChannel(0, new ArrayList<>(), tickets, p.now);
        p.a.channelDone(tickets.getFirst(), Outcome.REJECTED, p.now);
        assertEquals(100, p.a.stats().datagramBytesSent());
        assertEquals(1, p.a.stats().datagramsSent());
        assertEquals(1, p.a.stats().datagramsSentChannel());
        assertEquals(1, p.a.stats().datagramsDroppedSend());
    }

    @Test void datagramsGoInRecordsWithoutTheChannel() throws Exception {
        record Case(String name, FakeChannel a, FakeChannel b, int size) {}
        for (Case c : List.of(
                new Case("peer has no channel", new FakeChannel(0, 1200), null, 100),
                new Case("carrier did not negotiate datagrams", new FakeChannel(0, 0), new FakeChannel(0, 1200), 100),
                new Case("too large", new FakeChannel(0, 1200), new FakeChannel(0, 1200), 1198))) {
            Pair p = new Pair(c.a, c.b);
            p.pump();
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

    @Test void peerFrameLimitExcludesThePipeStreamIdAndLengthField() throws Exception {
        var p = new Pair(new FakeChannel(0, 1200), new FakeChannel(0, 1200));
        p.b.localParams().maxDatagramFrameSize = 64;
        p.pump();
        assertEquals(61, p.a.datagrams().nativePayload(1));
        byte[] payload = new byte[61];
        p.a.sendDatagram(null, 0, payload);
        p.pump();
        assertEquals(65, p.fromA.getFirst().length);
        p.deliverToB();
        assertArrayEquals(payload, p.b.datagrams().receive());
    }

    @Test void theLargestThatFitsGoesOnTheChannel() throws Exception {
        Pair p = new Pair(new FakeChannel(0, 1200), new FakeChannel(0, 1200));
        p.pump();
        p.a.sendDatagram(null, 0, bytes(1196, 3)); // ID, type, sequence and flow each take one byte.
        p.pump();
        assertEquals(1, p.fromA.size());
        assertEquals(1200, p.fromA.get(0).length);
    }

    /** The path datagram payloads a reported, in order. */
    static List<Long> nativePathEvents(Session s) {
        List<Long> got = new ArrayList<>();
        for (Event e; (e = s.outbox().pollEvent()) != null; ) if (e.kind() == Event.Kind.DATAGRAM_PATH) got.add(e.code());
        return got;
    }

    /**
     * The path datagram payload is what fits one channel datagram: a datagram of it goes on the channel, one byte more
     * in a record; a smaller channel lowers it and DATAGRAM_PATH reports it once.
     */
    @Test void nativePayloadIsWhatOneChannelDatagramTakes() throws Exception {
        Pair p = new Pair(new FakeChannel(0, 1200), new FakeChannel(0, 1200));
        p.pump();
        int path = p.a.datagrams().nativePayload(1);
        assertEquals(1200 - 4, path); // Pipe Stream ID, type and flow 0
        assertTrue(path < p.a.datagrams().maxPayload(1));
        assertEquals(List.of((long) path), nativePathEvents(p.a));
        p.a.sendDatagram(null, 0, bytes(path, 1));
        p.recordsA.clear();
        p.pump();
        assertEquals(1, p.fromA.size());
        assertFalse(datagramInRecords(p.recordsA));
        p.fromA.clear();
        p.a.sendDatagram(null, 0, bytes(path + 1, 2));
        p.pump();
        assertTrue(p.fromA.isEmpty());
        assertTrue(datagramInRecords(p.recordsA));
        p.ca.max = 800;
        p.pump();
        assertEquals(800 - 4, p.a.datagrams().nativePayload(1));
        assertEquals(List.of(800L - 4), nativePathEvents(p.a));
        p.ca.max = 0; // the carrier takes no datagrams now: they ride the records
        p.pump();
        assertEquals(-1, p.a.datagrams().nativePayload(1));
        assertEquals(p.a.datagrams().maxPayload(1), p.a.datagrams().pathPayload(1));
        assertEquals(List.of(-1L), nativePathEvents(p.a));
    }

    @Test void emptyNativePacketRequiresRealOverheadAndWaitsForOpenAndChannelRoom() throws Exception {
        var channel = new FakeChannel(0, 5);
        var p = new Pair(channel, new FakeChannel(0, 5));
        p.pump();
        var st = p.a.streams().openStream(true);
        var nativeOnly = new io.zmux.DatagramSendOptions(null, io.zmux.DatagramSendOptions.Carriage.REQUIRE_NATIVE, null, null, null);
        var options = p.a.resolveDatagram(st, nativeOnly, 0);
        assertEquals(0, p.a.datagrams().nativePayload(2), "an empty Flow payload fits exactly");
        channel.limit = channel.max;
        channel.max = 0;
        assertEquals(-1, p.a.datagrams().nativePayload(2));
        assertEquals(Session.DatagramAdmission.QUEUED, p.a.admitDatagram(st, 0, new byte[0], 0, options, 0,
                Session.DatagramMode.DATAGRAM_OR_CAPSULE, false, false, null));
        assertTrue(st.idSet());
        p.pump();
        assertTrue(p.fromA.isEmpty(), "unwritable native carriage does not turn into a capsule or a record");
        assertFalse(datagramInRecords(p.recordsA));
        channel.max = channel.limit;
        p.pump();
        assertEquals(1, p.fromA.size(), "the original was queued through the OPEN fence");
        assertEquals(0, p.a.datagrams().nativePayload(2));
    }

    @Test void rejectsADeadlineAndExhaustedSequenceBeforePinningOrChoosingCapsules() throws Exception {
        var p = new Pair(null, null);
        p.pump();
        var st = p.a.streams().openStream(true);
        p.a.setNow(10);
        var zero = new io.zmux.DatagramSendOptions(new io.zmux.DatagramSendOptions.Deadline.At(10), null, null, null, null);
        var deadline = assertThrows(CoreException.class, () -> p.a.admitDatagram(st, 0, new byte[0], 0,
                p.a.resolveDatagram(st, zero, 10), 10, Session.DatagramMode.DATAGRAM_OR_CAPSULE, false, false, null));
        assertEquals(CoreException.DatagramDrop.DEADLINE, deadline.datagramDrop());
        assertFalse(st.idSet());

        var tooLong = new io.zmux.DatagramSendOptions(new io.zmux.DatagramSendOptions.Deadline.After(
                java.time.Duration.ofNanos(Long.MAX_VALUE - 10)), null, null, null, null);
        assertEquals(CoreException.Reason.INVALID_DATAGRAM_OPTIONS,
                assertThrows(CoreException.class, () -> p.a.sendDatagram(st, 0, new byte[0], tooLong, 10)).reason());
        assertFalse(st.idSet());

        var inherited = p.a.resolveDatagram(st, io.zmux.DatagramSendOptions.INHERIT, 10);
        assertEquals(Session.DatagramAdmission.CAPSULE, p.a.planDatagram(st, 0, 65536, 1, inherited,
                Session.DatagramMode.DATAGRAM_OR_CAPSULE));
        Poke.set(p.a.datagrams(), "nextSequence", VarInt.MAX + 1);
        var exhausted = assertThrows(CoreException.class, () -> p.a.planDatagram(st, 0, 65536, 1, inherited,
                Session.DatagramMode.DATAGRAM_OR_CAPSULE));
        assertEquals(CoreException.DatagramDrop.SEQUENCE_EXHAUSTED, exhausted.datagramDrop());
        assertFalse(st.idSet());
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
        p.toB(new byte[] {0x30, 0, 0, 9}, ChannelDatagram.NO_ID);
        assertEquals(records, p.b.stats().recordsReceived());
        assertArrayEquals(new byte[] {9}, p.b.datagrams().receive());
    }

    // ---- holding ----

    /** A channel datagram for a's first bidirectional stream (ID 0): flow 1, context ctx. */
    static byte[] streamDatagram(long ctx, byte[] payload) {
        ByteBuffer b = ByteBuffer.allocate(24 + payload.length);
        VarInt.put(b, ctx);
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
        assertNotEquals(Time.NEVER, DeadlineFixture.next(p.b));
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
        assertTrue(DeadlineFixture.next(p.b) <= due);
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
        // Each held datagram costs its 2 bytes and the fixed overhead: two fit, a third is refused.
        Pair p = new Pair(new FakeChannel(0, 1200), new FakeChannel(0, 1200), 2 * (Datagrams.HELD_COST + 2));
        p.pump();
        for (int i = 0; i < 3; i++) p.toB(streamDatagram(i, new byte[] {'x'}), ChannelDatagram.NO_ID);
        assertEquals(1, p.b.stats().datagramsDroppedRecv());
        Stream st = openAndWrite(p);
        assertEquals(0, p.b.datagrams().receive(st).context());
        assertEquals(1, p.b.datagrams().receive(st).context());
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
    @Test void datagramsQueuedBeforeTheOpeningGoAheadOfLaterStreamData() throws Exception {
        Pair p = new Pair(null, null);
        p.pump();
        Stream st = p.a.streams().openStream(true);
        p.a.sendDatagram(st, 7, new byte[] {'d'});
        p.a.sending().write(st, ByteBuffer.wrap(bytes(100, 1)), true);
        p.recordsA.clear();
        p.pump();
        // The opening frame, then the datagram queued before the data, then the data and its FIN.
        List<String> order = new ArrayList<>();
        FrameDecoder d = new FrameDecoder();
        Frame f = new Frame();
        for (byte[] batch : p.recordsA) {
            ByteBuffer b = ByteBuffer.wrap(batch);
            for (int i = 0; i < batch.length; ) {
                int size = (int) VarInt.get(b, i, batch.length);
                int start = i + VarInt.lengthOf(batch[i]), end = start + size;
                for (int q = start; q < end; ) {
                    q += d.decode(b, q, end, f);
                    if (f.kind == FrameKind.DATAGRAM) order.add("D");
                    else if (f.kind == FrameKind.STREAM) order.add(f.fin ? "F" : "S");
                }
                i = end;
            }
        }
        assertEquals(List.of("S", "D", "F"), order);
        Stream peer = p.b.streams.lookup(0, true);
        assertEquals(7, p.b.datagrams().receive(peer).context());
    }
    /**
     * MULTIPATH.md S7: while its opening frame is unacknowledged, a stream's datagrams go only on the pipe that
     * carried it or its latest reinjected copy; the acknowledgement of the frame itself, empty or not, ends that.
     */
    @Test void streamDatagramsFollowTheUnacknowledgedOpeningPipe() throws Exception {
        Pair p = new Pair(null, null);
        p.pump();
        Stream st = p.a.streams().openStream(true);
        p.a.streams().pinId(st);
        p.a.streams().markOpened(st); // its empty opening frame went out
        Pipe opening = p.a.pipes().get(0), other = p.b.pipes().get(0);
        assertNull(Session.followPipe(st), "outside multipath no pipe is followed");
        PathStreamState m = st.snd.mp();
        m.openedOn(opening);
        assertSame(opening, m.openingPipe());

        Datagrams dg = new Datagrams(CoreConfig.builder().datagrams(true).build(), new SessionStats(), new Outbox(), () -> 0L);
        dg.recordCap(() -> 65535);
        dg.peer(65535, 65535, true);
        dg.channels(new Datagrams.Channels() {
            @Override public Pipe datagramPipe() { return null; }
            @Override public Datagrams.ChannelUse of(Pipe x) { return Datagrams.ChannelUse.NONE; }
            @Override public boolean carries(Stream s, Pipe x) { Pipe o = s == null ? null : s.snd.mps().openingPipe(); return o == null || o == x; }
        });
        dg.enqueueSingle(st, 1, new byte[] {9}, 0, Time.NEVER, io.zmux.DatagramSendOptions.INHERIT, null);
        assertFalse(dg.sendable(other, 65535), "another pipe could let it overtake the opening frame");
        assertTrue(dg.sendable(opening, 65535));
        RecordWriter w = new RecordWriter();
        w.init(new Batch(), Framing.BYTES, 65535, Session.DEFAULT_BATCH);
        dg.write(w, other);
        assertTrue(dg.queued());

        // A reinjected copy moves it.
        m.openedOn(other);
        assertFalse(dg.sendable(opening, 65535));
        dg.write(w, other);
        assertFalse(dg.queued());

        // The empty opening frame acknowledged: any pipe.
        m.openingAcked();
        assertNull(m.openingPipe());
        m.openedOn(opening);
        assertNull(m.openingPipe(), "a late copy does not pin it again");
        dg.enqueueSingle(st, 1, new byte[] {9}, 0, Time.NEVER, io.zmux.DatagramSendOptions.INHERIT, null);
        assertTrue(dg.sendable(other, 65535));
    }
}
