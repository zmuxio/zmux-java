package io.zmux.internal.core;

import io.zmux.internal.buffer.ReadBuf;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.wire.ChannelDatagram;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.TransportParams;
import io.zmux.internal.wire.VarInt;
import io.zmux.internal.wire.WireException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class NextBatchE1Test {
    @org.junit.jupiter.api.TestFactory java.util.stream.Stream<org.junit.jupiter.api.DynamicTest> specVectors() throws Exception {
        return DatagramCheckpoints.tests("E1");
    }

    @Test void familyValidationWorksWithDatagramsDisabled() throws Exception {
        var local = CoreConfig.defaults().local();
        assertTrue(local.isZmux());
        assertFalse(local.has(TransportParams.DATAGRAM));
        byte[] proper = local.toBytes();
        assertTrue(TransportParams.decode(ByteBuffer.wrap(proper), 0, proper.length).isZmux());
        local.present &= ~TransportParams.FAMILY;
        byte[] incompatible = local.toBytes();
        assertEquals(TransportError.TRANSPORT_PARAMETER_ERROR, assertThrows(WireException.class,
                () -> TransportParams.decode(ByteBuffer.wrap(incompatible), 0, incompatible.length)).code());
        ByteBuffer retired = ByteBuffer.allocate(16);
        VarInt.put(retired, TransportParams.ID_RETIRED_DATAGRAM_FLOWS);
        VarInt.put(retired, 0);
        assertEquals(TransportError.TRANSPORT_PARAMETER_ERROR, assertThrows(WireException.class,
                () -> TransportParams.decode(retired, 0, retired.position())).code());
        assertFalse(TransportParams.decode(ByteBuffer.allocate(0), 0, 0).isZmux());
    }

    @Test void dispatchUsesEveryVarintWidthAndEndsAfterTheMaximum() throws Exception {
        var carrier = new ChannelDatagramTest.FakeChannel(ChannelDatagram.NO_ID, 1200);
        var pair = new ChannelDatagramTest.Pair(carrier, carrier);
        pair.pump();
        for (long seq : new long[] {63, 64, 16383, 16384, (1L << 30) - 1, 1L << 30, VarInt.MAX}) {
            Poke.set(pair.a.datagrams(), "nextSequence", seq);
            assertEquals(1198 - VarInt.length(seq), pair.a.datagrams().nativePayload(1));
            pair.a.sendDatagram(null, 0, new byte[] {7});
            pair.pump();
            assertEquals(1, pair.fromA.size());
            var packet = ByteBuffer.wrap(pair.fromA.removeFirst());
            assertEquals(seq, VarInt.get(packet, 1, packet.limit()));
            assertEquals(VarInt.length(seq), VarInt.lengthOf(packet.get(1)));
            assertEquals(0, packet.get(1 + VarInt.length(seq)));
        }
        assertThrows(CoreException.class, () -> pair.a.sendDatagram(null, 0, new byte[] {8}));
    }

    @Test void onePositionCoversRecordAndNativeCopiesAfterDelivery() {
        var carrier = new ChannelDatagramTest.FakeChannel(ChannelDatagram.NO_ID, 1200);
        var pair = new ChannelDatagramTest.Pair(carrier, carrier);
        pair.pump();
        byte[] frame = new byte[] {0x30, 3, 0, 42};
        pair.toB(frame, ChannelDatagram.NO_ID);
        assertArrayEquals(new byte[] {42}, pair.b.datagrams().receive());
        var record = ByteBuffer.wrap(new byte[] {4, 0x30, 3, 0, 42});
        pair.b.receive(0, 0, new ReadBuf(record, null), record, 0, record.limit(), pair.now);
        assertNull(pair.b.datagrams().receive());
        assertEquals(1, pair.b.statsSnapshot().datagramDuplicatesDropped());
    }

    @Test void holdExpiryReleasesItsPositionWithoutRefreshingOnDuplicate() throws Exception {
        var carrier = new ChannelDatagramTest.FakeChannel(ChannelDatagram.NO_ID, 1200);
        var pair = new ChannelDatagramTest.Pair(carrier, carrier);
        pair.pump();
        byte[] frame = ChannelDatagramTest.streamDatagram(1, new byte[] {42});
        pair.toB(frame, ChannelDatagram.NO_ID);
        long expiry = pair.b.datagrams().heldDeadline();
        pair.now = expiry - 1;
        pair.toB(frame, ChannelDatagram.NO_ID);
        assertEquals(expiry, pair.b.datagrams().heldDeadline());
        pair.now = expiry;
        pair.b.handleTimeout(expiry);
        var stream = ChannelDatagramTest.openAndWrite(pair);
        assertNull(pair.b.datagrams().receive(stream));
        pair.toB(frame, ChannelDatagram.NO_ID);
        assertArrayEquals(new byte[] {42}, pair.b.datagrams().receive(stream).payload());
    }
}
