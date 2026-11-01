package io.zmux.internal.wire;

import io.zmux.carrier.VarInt;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

class WireTest {
    @Test
    void varintBoundaries() {
        long[] vs = {0, 63, 64, 16383, 16384, (1L << 30) - 1, 1L << 30, VarInt.MAX};
        int[] lens = {1, 1, 2, 2, 4, 4, 8, 8};
        for (int k = 0; k < vs.length; k++) {
            ByteBuffer b = ByteBuffer.allocate(8);
            VarInt.put(b, vs[k]);
            assertEquals(lens[k], b.position());
            assertEquals(vs[k], VarInt.get(b, 0, b.position()));
            assertEquals(-1, VarInt.get(b, 0, b.position() - 1));
            byte[] a = new byte[8];
            assertEquals(lens[k], VarInt.put(a, 0, vs[k]));
            assertArrayEquals(java.util.Arrays.copyOf(b.array(), lens[k]), java.util.Arrays.copyOf(a, lens[k]));
        }
    }

    @Test
    void transportParametersRoundTrip() throws Exception {
        TransportParams p = new TransportParams();
        p.present = TransportParams.IDLE | TransportParams.MAX_DATA | TransportParams.BIDI_REMOTE | TransportParams.UNI
                | TransportParams.RESET_AT | TransportParams.PREAMBLE | TransportParams.PIPE_ACK | TransportParams.LANES
                | TransportParams.MULTIPATH | TransportParams.ROLE | TransportParams.RECORD_SIZE;
        p.maxIdleTimeoutMillis = 30000;
        p.initialMaxData = 1 << 20;
        p.maxStreamDataBidiRemote = 262144;
        p.maxStreamDataUni = 262144;
        p.maxPreambleSize = 4096;
        p.maxLanes = 258;
        p.role = TransportParams.ROLE_AUTO;
        p.roleNonce = 0x0102030405060708L;
        p.maxRecordSize = 65535;
        p.secretFlags = TransportParams.SECRET_PSK;
        p.locator = new byte[] {1, 2, 3};
        p.greaseId = 27 + 31 * 5;
        p.greaseValue = new byte[] {9};
        byte[] enc = p.toBytes();
        TransportParams q = TransportParams.decode(ByteBuffer.wrap(enc), 0, enc.length);
        assertEquals(p.present, q.present);
        assertEquals(0x0102030405060708L, q.roleNonce);
        assertArrayEquals(new byte[] {1, 2, 3}, q.locator);
        assertTrue(q.isZmux());
        assertTrue(TransportParams.isGrease(p.greaseId));
        byte[] dup = new byte[enc.length * 2];
        System.arraycopy(enc, 0, dup, 0, enc.length);
        System.arraycopy(enc, 0, dup, enc.length, enc.length);
        WireException x = assertThrows(WireException.class, () -> TransportParams.decode(ByteBuffer.wrap(dup), 0, dup.length));
        assertEquals(TransportError.TRANSPORT_PARAMETER_ERROR, x.code());
    }

    @Test
    void preambleRoundTripAndViolations() throws Exception {
        Preamble p = new Preamble(0, "dst".getBytes(), 1, true, true,
                List.of(new Preamble.Tlv(0x1000, new byte[] {7})), new Preamble.Tlv(0x21, new byte[0]));
        ByteBuffer b = ByteBuffer.allocate(64);
        p.encode(b);
        assertEquals(p.encodedSize(), b.position());
        Preamble q = Preamble.parse(b, 0, b.position(), 1024);
        assertArrayEquals("dst".getBytes(), q.openInfo());
        assertEquals(1, q.urgency());
        assertTrue(q.incremental() && q.pipeCarrier());
        assertEquals(1, q.applicationTlvs().size());
        assertEquals(p.encodedSize(), q.size());
        assertThrows(WireException.class, () -> Preamble.parse(b, 0, b.position(), 4));
        assertThrows(WireException.class, () -> Preamble.parse(ByteBuffer.wrap(HexFormat.of().parseHex("040100" + "0100")), 0, 5, 1024));
    }

    @Test
    void firstRecordCheck() {
        Records.FirstRecordCheck c = new Records.FirstRecordCheck(65535, false, false);
        ByteBuffer legacy = ByteBuffer.wrap("ZMUX".getBytes());
        assertEquals(Records.Verdict.REJECT, c.feed(legacy, 0, 4));
        // The legacy magic fails at its third byte, without waiting for more (SPEC.md Section 6.2).
        assertEquals(Records.Verdict.REJECT, new Records.FirstRecordCheck(65535, false, false).feed(legacy, 0, 3));
        ByteBuffer tp = ByteBuffer.wrap(HexFormat.of().parseHex("0dff5153300d0a0d0a0404024400"));
        Records.FirstRecordCheck ok = new Records.FirstRecordCheck(65535, false, false);
        assertEquals(Records.Verdict.NEED_MORE, ok.feed(tp, 0, 5));
        assertEquals(Records.Verdict.TRANSPORT_PARAMETERS, ok.feed(tp, 5, tp.limit()));
        ByteBuffer join = ByteBuffer.wrap(HexFormat.of().parseHex("1a9a24de36"));
        assertEquals(Records.Verdict.PIPE_JOIN, new Records.FirstRecordCheck(65535, true, false).feed(join, 0, 5));
        assertEquals(Records.Verdict.REJECT, new Records.FirstRecordCheck(65535, false, false).feed(join, 0, 5));
        ByteBuffer small = ByteBuffer.wrap(HexFormat.of().parseHex("08ff"));
        assertEquals(Records.Verdict.REJECT, new Records.FirstRecordCheck(65535, true, true).feed(small, 0, 1));
        ByteBuffer http = ByteBuffer.wrap("GET / HTTP/1.1".getBytes());
        assertEquals(Records.Verdict.REJECT, new Records.FirstRecordCheck(65535, true, true).feed(http, 0, 3));
    }

    @Test
    void joinTokenVerifies() {
        byte[] secret = JoinCrypto.sessionSecret(new byte[16], new byte[16], new byte[0], "psk".getBytes());
        assertEquals(32, secret.length);
        byte[][] keys = JoinCrypto.joinKeys(secret);
        byte[] tok = JoinCrypto.token(keys[0], new byte[] {1, 2}, new byte[0]);
        assertEquals(16, tok.length);
        assertTrue(JoinCrypto.verify(keys[0], new byte[] {1, 2}, new byte[0], tok));
        assertTrue(!JoinCrypto.verify(keys[1], new byte[] {1, 2}, new byte[0], tok));
    }

    @Test
    void phraseIsBounded() {
        WireException e = WireException.withDetails(TransportError.FLOW_CONTROL_ERROR, "x".repeat(2000), "stream", 4);
        assertTrue(e.phrase().getBytes().length <= 1024);
        assertEquals("stream=4", e.details());
    }
}
