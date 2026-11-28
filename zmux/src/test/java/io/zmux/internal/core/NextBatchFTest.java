package io.zmux.internal.core;

import io.zmux.DatagramSendOptions;
import io.zmux.internal.join.Opener;
import io.zmux.internal.join.Sealer;
import io.zmux.internal.join.Suite;
import io.zmux.seal.SecureCarrierException;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NextBatchFTest {
    @Test void queuedDatagramExpiresWithNoWriteOpportunity() throws Exception {
        var pair = new ChannelDatagramTest.Pair(null, null);
        pair.pump();
        var session = pair.a;
        session.setWritable(0, 0, false, pair.now);
        long start = session.now();
        var after = new DatagramSendOptions.Deadline.After(Duration.ofMillis(5));
        var options = new DatagramSendOptions(after, null, null, null, null);
        session.sendDatagram(null, 0, new byte[] {1}, options, start);
        long deadline = start + 5 * Time.MILLISECOND;
        assertEquals(deadline, session.datagrams().sendDeadline());
        session.handleTimeout(deadline - 1);
        assertTrue(session.datagrams().queued());
        session.handleTimeout(deadline);
        assertFalse(session.datagrams().queued());
        assertEquals(Time.NEVER, session.datagrams().sendDeadline());
        assertEquals(1, session.statsSnapshot().datagramDeadlineDropped());
    }

    @Test void aRejectedBufferLeavesTheRekeyRecordInSequence() throws Exception {
        byte[] key = new byte[Suite.AES.keyLen()];
        long boundary = (1L << 20) - 1;
        var writer = Sealer.create(Suite.AES, key, boundary);
        var reader = Opener.create(Suite.AES, key, boundary);
        var before = writer.seal(new byte[] {1}, 0, 1);
        var after = ByteBuffer.wrap(new byte[] {2, 3});
        assertThrows(IllegalArgumentException.class, () -> writer.seal(after, ByteBuffer.allocate(1)));
        assertEquals(0, after.position());
        var retry = ByteBuffer.allocate(after.remaining() + writer.overhead());
        assertEquals(retry.capacity(), writer.seal(after, retry));
        assertArrayEquals(new byte[] {1}, reader.open(before, 0, before.length));
        assertArrayEquals(new byte[] {2, 3}, reader.open(retry.array(), 0, retry.capacity()));
    }

    @Test void aCorruptedRecordLeavesTheReaderPermanentlyClosed() throws Exception {
        byte[] key = new byte[Suite.AES.keyLen()];
        var writer = Sealer.create(Suite.AES, key, 0);
        var reader = Opener.create(Suite.AES, key, 0);
        byte[] authentic = writer.seal(new byte[] {42}, 0, 1);
        byte[] corrupt = authentic.clone();
        corrupt[0] ^= 0x01;
        var fault = assertThrows(SecureCarrierException.class, () -> reader.open(corrupt, 0, corrupt.length));
        assertSame(fault, assertThrows(SecureCarrierException.class, () -> reader.open(authentic, 0, authentic.length)));
    }
}
