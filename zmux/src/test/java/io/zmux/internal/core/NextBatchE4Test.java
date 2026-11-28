package io.zmux.internal.core;

import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import static org.junit.jupiter.api.Assertions.*;

class NextBatchE4Test {
    @TestFactory Stream<DynamicTest> specVectors() throws Exception {
        return DatagramCheckpoints.tests("E4");
    }

    @org.junit.jupiter.api.Test void anOversizedDatagramWaitsForAPathUntilItsLocalDeadline() throws Exception {
        long[] now = {0};
        int[] room = {65535};
        var stats = new SessionStats();
        var datagrams = new Datagrams(CoreConfig.defaults(), stats, new Outbox(), () -> now[0]);
        datagrams.recordCap(() -> room[0]);
        datagrams.channels(new Datagrams.Channels() {
            @Override public Pipe datagramPipe() { return null; }
            @Override public Datagrams.ChannelUse of(Pipe pipe) { return Datagrams.ChannelUse.NONE; }
            @Override public boolean fixedRecordPath() { return false; }
        });
        datagrams.peer(65535, 65535, true);
        long expiry = 5 * Time.MILLISECOND;
        datagrams.enqueueSingle(null, 0, new byte[3000], 0, expiry, io.zmux.DatagramSendOptions.INHERIT, null);
        room[0] = 2000;
        assertFalse(datagrams.sendable(null, room[0]));
        now[0] = expiry - 1;
        datagrams.expireSend(now[0]);
        assertTrue(datagrams.queued());
        now[0] = expiry;
        datagrams.expireSend(now[0]);
        assertFalse(datagrams.queued());
        datagrams.enqueueSingle(null, 0, new byte[7], 0, Time.NEVER, io.zmux.DatagramSendOptions.INHERIT, null);
        assertTrue(datagrams.sendable(null, room[0]));
    }
}
