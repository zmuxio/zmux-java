package io.zmux.internal.core;

import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class NextBatchE2Test {
    @TestFactory Stream<DynamicTest> specVectors() throws Exception {
        return DatagramCheckpoints.tests("E2");
    }

    @org.junit.jupiter.api.Test void receiveQueueEnforcesItsByteLimitAtAdmission() throws Exception {
        assertQueue(16 << 10, 10, 4 << 10, 6);
        assertQueue(-1, 10, 4 << 10, 0);
        assertQueue(CoreConfig.DEFAULT_DATAGRAM_STREAM_BYTES, 32, 9000, 0);
        assertQueue(1000, 3, 2000, 3);
    }

    private static void assertQueue(long limit, int count, int size, int dropped) throws Exception {
        var channel = new ChannelDatagramTest.FakeChannel(0, 65535);
        var pair = new ChannelDatagramTest.Pair(channel, channel, 8 << 10, limit);
        pair.pump();
        var stream = ChannelDatagramTest.openAndWrite(pair);
        for (int i = 0; i < count; i++) {
            var wire = java.nio.ByteBuffer.allocate(size + 32);
            wire.put((byte) 0x30);
            io.zmux.internal.wire.VarInt.put(wire, i);
            io.zmux.internal.wire.VarInt.put(wire, stream.tab.id() + 1);
            io.zmux.internal.wire.VarInt.put(wire, 0);
            byte[] payload = new byte[size];
            payload[0] = (byte) i;
            wire.put(payload).flip();
            pair.b.receiveChannelDatagram(0, wire, 0, wire.limit(), pair.now);
        }
        var stats = pair.b.statsSnapshot();
        org.junit.jupiter.api.Assertions.assertEquals(dropped, stats.datagramsDroppedRecv());
        for (int i = dropped; i < count; i++) {
            org.junit.jupiter.api.Assertions.assertEquals(i, pair.b.datagrams().receive(stream).payload()[0]);
        }
        org.junit.jupiter.api.Assertions.assertNull(pair.b.datagrams().receive(stream));
    }
}
