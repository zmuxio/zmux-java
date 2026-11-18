package io.zmux.internal.join;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.core.HandshakePlans;
import org.junit.jupiter.api.Test;

/** A Join Hello stays within the 8192-byte handshake message floor: the first batch takes only the room left. */
class JoinFloorTest {
    @Test
    void aGroup2HelloWithTheLargestLocatorTrimsItsBatchToTheFloor() throws Exception {
        byte[] psk = new byte[32], locator = new byte[255];
        for (Framing f : Framing.values()) {
            Joiner j = Joiner.create(Suite.AES, Mode.SEALED, f, locator, psk, null, Group.X25519_MLKEM768, 0);
            JoinPipe pipe = new JoinPipe(1, 2, 0, 0, false);
            HelloPayload empty = new HelloPayload(pipe, null, 0);
            // What Joins does: the default 8 KiB batch, cut to the room of the floor.
            int batch = Math.clamp(HandshakePlans.fit(Sealer.MESSAGE_FLOOR - j.helloLength(empty)), 0, 8 << 10);
            assertTrue(batch > 0 && batch < 8 << 10, f + ": batch " + batch);
            byte[] hello = j.writeHello(new HelloPayload(pipe, new byte[batch], 0));
            assertTrue(hello.length <= Sealer.MESSAGE_FLOOR, f + ": Hello of " + hello.length);
            Hello h = Hello.parseExact(hello, 0, hello.length, f);
            Acceptor a = Acceptor.accept(h, f, psk, null, new long[] {Group.X25519_MLKEM768.id()});
            assertEquals(batch, a.payload().firstBatch().length);
            byte[] reply = a.writeReply(new ReplyPayload(0, null, 0));
            assertEquals(0, j.readReply(reply, 0, reply.length).decision());
            // The whole 8 KiB batch would not fit: too_large before anything is written.
            Joiner big = Joiner.create(Suite.AES, Mode.SEALED, f, locator, psk, null, Group.X25519_MLKEM768, 0);
            var e = assertThrows(io.zmux.seal.SecureCarrierException.class, () -> big.writeHello(new HelloPayload(pipe, new byte[8 << 10], 0)));
            assertEquals(io.zmux.seal.SecureCarrierException.Kind.TOO_LARGE, e.kind());
        }
    }
}
