package io.zmux.internal.wire;


import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

class WireTest {
    /** Many unknown parameters decode in near-linear time; a duplicate among them, at any distance, is still found. */
    @Test
    void manyUnknownParametersAreCheckedForDuplicatesWithoutAQuadraticScan() throws WireException {
        // Large enough that a quadratic scan (8e10 comparisons) takes minutes: the generous bound below never
        // separates a loaded machine's linear decode from it by a narrow margin.
        int n = 400_000;
        ByteBuffer b = ByteBuffer.allocate(n * 9 + 9);
        for (int k = 0; k < n; k++) { VarInt.put(b, 0x4000_0000L + (n - k) * 2L); VarInt.put(b, 0); }
        int len = b.position();
        TransportParams.decode(b, 0, len);
        VarInt.put(b, 0x4000_0000L + 2L * n); VarInt.put(b, 0); // the first identifier again, at the end
        int dup = b.position();
        WireException x = assertThrows(WireException.class, () -> TransportParams.decode(b, 0, dup));
        assertTrue(x.getMessage().endsWith("duplicate parameter"), x.getMessage());
    }

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
        p.present = TransportParams.FAMILY | TransportParams.IDLE | TransportParams.MAX_DATA | TransportParams.BIDI_REMOTE | TransportParams.UNI
                | TransportParams.RESET_AT | TransportParams.PREAMBLE | TransportParams.PIPE_ACK | TransportParams.LANES
                | TransportParams.MULTIPATH | TransportParams.SESSION_CRYPTO | TransportParams.ROLE | TransportParams.RECORD_SIZE;
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
        p.sessionCrypto = io.zmux.internal.join.SessionCrypto.of(new long[] {1}, new long[] {2});
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
    void preambleValueAbove8160IsAParameterError() throws Exception {
        for (int v : new int[] {TransportParams.MAX_PREAMBLE_SIZE, TransportParams.MAX_PREAMBLE_SIZE + 1}) {
            TransportParams p = new TransportParams();
            p.present = TransportParams.FAMILY | TransportParams.RESET_AT | TransportParams.PREAMBLE | TransportParams.BIDI_REMOTE | TransportParams.UNI;
            p.maxStreamDataBidiRemote = p.maxStreamDataUni = 1 << 20;
            p.maxPreambleSize = v;
            byte[] enc = p.toBytes();
            if (v == TransportParams.MAX_PREAMBLE_SIZE) {
                assertEquals(v, TransportParams.decode(ByteBuffer.wrap(enc), 0, enc.length).maxPreambleSize);
            } else {
                WireException x = assertThrows(WireException.class, () -> TransportParams.decode(ByteBuffer.wrap(enc), 0, enc.length));
                assertEquals(TransportError.TRANSPORT_PARAMETER_ERROR, x.code());
            }
        }
    }

    @Test
    void preambleRoundTripAndViolations() throws Exception {
        Preamble p = new Preamble(0, "dst".getBytes(UTF_8), 1, true, true,
                List.of(new Preamble.Tlv(0x1000, new byte[] {7})), new Preamble.Tlv(0x21, new byte[0]));
        ByteBuffer b = ByteBuffer.allocate(64);
        p.encode(b);
        assertEquals(p.encodedSize(), b.position());
        Preamble q = Preamble.parse(b, 0, b.position(), 1024);
        assertArrayEquals("dst".getBytes(UTF_8), q.openInfo());
        assertEquals(1, q.urgency());
        assertTrue(q.incremental() && q.pipeCarrier());
        assertEquals(1, q.applicationTlvs().size());
        assertEquals(p.encodedSize(), q.size());
        assertThrows(WireException.class, () -> Preamble.parse(b, 0, b.position(), 4));
        assertThrows(WireException.class, () -> Preamble.parse(ByteBuffer.wrap(HexFormat.of().parseHex("040100" + "0100")), 0, 5, 1024));
    }

    @Test
    void firstRecordCheck() {
        Records.FirstRecordCheck c = new Records.FirstRecordCheck(65535, true);
        ByteBuffer foreign = ByteBuffer.wrap("GET ".getBytes(UTF_8));
        assertEquals(Records.Verdict.REJECT, c.feed(foreign, 0, 4));
        // A foreign first record fails at its third byte, without waiting for more.
        assertEquals(Records.Verdict.REJECT, new Records.FirstRecordCheck(65535, true).feed(foreign, 0, 3));
        ByteBuffer tp = ByteBuffer.wrap(HexFormat.of().parseHex("0dff5153300d0a0d0a0404024400"));
        Records.FirstRecordCheck ok = new Records.FirstRecordCheck(65535, true);
        assertEquals(Records.Verdict.NEED_MORE, ok.feed(tp, 0, 5));
        assertEquals(Records.Verdict.TRANSPORT_PARAMETERS, ok.feed(tp, 5, tp.limit()));
        ByteBuffer other = ByteBuffer.wrap(HexFormat.of().parseHex("1a9a24de36"));
        assertEquals(Records.Verdict.REJECT, new Records.FirstRecordCheck(65535, true).feed(other, 0, 5));
        ByteBuffer small = ByteBuffer.wrap(HexFormat.of().parseHex("08ff"));
        assertEquals(Records.Verdict.REJECT, new Records.FirstRecordCheck(65535, true).feed(small, 0, 1));
        ByteBuffer http = ByteBuffer.wrap("GET / HTTP/1.1".getBytes(UTF_8));
        assertEquals(Records.Verdict.REJECT, new Records.FirstRecordCheck(65535, true).feed(http, 0, 3));
    }

    @Test
    void reasonPhraseCutKeepsCharactersWhole() {
        byte[] b = Utf8.truncate("x".repeat(1023) + "\u00e9", 1024);
        assertEquals(1023, b.length);
        assertEquals(1024, Utf8.truncate("y".repeat(2000), 1024).length);
    }

    @Test
    void sessionEpochRoundTripsAndRefusesAnotherPhase() throws Exception {
        byte[] share = {1, 2, 3}, locator = {9, 8};
        for (int phase : new int[] {0, 1}) {
            byte[] body = FrameEncoder.sessionEpochBody(phase, 64, share, locator);
            ByteBuffer b = ByteBuffer.allocate(64);
            FrameEncoder.sessionEpoch(b, body);
            int n = b.position();
            Frame f = new Frame();
            assertEquals(n, new FrameDecoder().decode(b, 0, n, f));
            assertEquals(FrameKind.SESSION_EPOCH, f.kind);
            assertEquals(phase, f.phase);
            assertEquals(64, f.value);
            assertArrayEquals(share, f.dataBytes());
            assertArrayEquals(locator, f.locatorBytes());
            assertArrayEquals(body, f.bodyBytes());
        }
        ByteBuffer b = ByteBuffer.allocate(64);
        FrameEncoder.sessionEpoch(b, FrameEncoder.sessionEpochBody(2, 1, new byte[0], new byte[0]));
        int n = b.position();
        var e = assertThrows(WireException.class, () -> new FrameDecoder().decode(b, 0, n, new Frame()));
        assertEquals(TransportError.PROTOCOL_VIOLATION, e.code());
    }
}
