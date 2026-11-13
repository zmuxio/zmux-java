package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

/** Fixed wire vectors of the exchange carrier: keys, authenticated UP and DOWN requests and their responses. */
class ExchangeWireTest {
    static final HexFormat HEX = HexFormat.of();
    static final byte[] CID = HEX.parseHex("000102030405060708090a0b0c0d0e0f");
    static final int TIME = 1767225600;
    static final byte[] NONCE = HEX.parseHex("0102030405060708");

    static String hex(ByteBuffer... bs) {
        var sb = new StringBuilder();
        for (ByteBuffer b : bs) { byte[] a = new byte[b.remaining()]; b.duplicate().get(a); sb.append(HEX.formatHex(a)); }
        return sb.toString();
    }

    static ByteBuffer ascii(String s) { return ByteBuffer.wrap(s.getBytes(StandardCharsets.US_ASCII)); }

    /** The vectors of HTTP-EXCHANGE.md Section 8. */
    static final String K_PLAIN = "8e08f7810b828921459c8d8c4f1be67963269408f5e7179f7f0df94d417723ef",
            UP_PLAIN = "000102030405060708090a0b0c0d0e0f0100000000000000000000000000000000006955b90001020304050607080000000500178e3775404d528a25094a591bf36468656c6c6fdd1ab741da654edc8e365e51e9f7ebe6",
            UP_RESP_PLAIN = "cf0b72020d0c5d8827e1131a6ad3ff8300",
            DOWN_PLAIN = "000102030405060708090a0b0c0d0e0f0200000000000000012c000000000000012b6955b9000102030405060708000000000f8a9597942e2aeca56c93dd9cd643a2ba220cca8f1bbd9f8cdb4b22b0265607",
            DOWN_RESP_PLAIN = "19df1f7aab49142d7b4919e2cd68942701776f726c64",
            K_PSK = "68489b10ee27b348be274432395cb2107dcfc8cf3f9f69f2cd4cf7cd7a0f8c95",
            UP_PSK = "000102030405060708090a0b0c0d0e0f0100000000000000000000000000000000006955b9000102030405060708000000055a69b4f32b1258b63968abe73831a16d68656c6c6fa9d13425b6115dfcf7677ee22d70535c",
            UP_RESP_PSK = "58696a7a658e71acfa63777f9655fbdb00",
            DOWN_PSK = "000102030405060708090a0b0c0d0e0f0200000000000000012c000000000000012b6955b900010203040506070800000000b3f9c1b72105e61b0331705b811d47d29afea042a48e16233bfc597f178a49c5",
            DOWN_RESP_PSK = "52774f4b737d7bd0a5f69989ff1fafd001776f726c64";

    static void vectors(byte[] psk, String k, String up, String upResp, String down, String downResp) {
        var key = Exchange.key(psk, CID);
        assertEquals(k, HEX.formatHex(key.k()));
        ByteBuffer hello = ascii("hello");
        var upSealed = Exchange.seal(key, CID, Exchange.KIND_UP, 0, 0, 0, TIME, NONCE, hello);
        assertEquals(up, hex(upSealed.header(), hello, upSealed.dataMac()));
        var downSealed = Exchange.seal(key, CID, Exchange.KIND_DOWN, 0, 300, 299, TIME, NONCE, ByteBuffer.allocate(0));
        assertEquals(down, hex(downSealed.header(), downSealed.dataMac()));

        ByteBuffer upBody = ByteBuffer.wrap(HEX.parseHex(up));
        var q = Exchange.parseHeader(upBody);
        assertEquals(5, q.dataLength());
        assertTrue(q.valid(5) && Exchange.verifyHeader(key.newMac(), q));
        ByteBuffer data = upBody.slice(Exchange.HDR, 5), dataMac = upBody.slice(Exchange.HDR + 5, Exchange.MAC);
        assertEquals("hello", StandardCharsets.US_ASCII.decode(data.duplicate()).toString());
        assertTrue(Exchange.verifyData(key.newMac(), q, data, dataMac));
        assertEquals(upResp, hex(Exchange.responseHeader(key, q, 0, ByteBuffer.allocate(0))));

        ByteBuffer downBody = ByteBuffer.wrap(HEX.parseHex(down));
        var d = Exchange.parseHeader(downBody);
        var m = key.newMac();
        assertTrue(d.valid(0) && Exchange.verifyHeader(m, d));
        assertTrue(Exchange.verifyData(m, d, ByteBuffer.allocate(0), downBody.slice(Exchange.HDR, Exchange.MAC)), "one MAC for both");
        assertEquals(300, d.seq());
        assertEquals(299, d.ack());
        assertEquals(TIME, d.time());
        ByteBuffer world = ascii("world");
        assertEquals(downResp, hex(Exchange.responseHeader(key, d, Exchange.FLAG_FIN, world), world));

        // A changed byte anywhere fails a MAC: in the header its MAC, in the Data or its MAC the Data MAC.
        for (int i = 0; i < upBody.capacity(); i++) {
            byte[] bad = HEX.parseHex(up);
            bad[i] ^= 1;
            ByteBuffer b = ByteBuffer.wrap(bad);
            var h = Exchange.parseHeader(b);
            boolean ok = Exchange.verifyHeader(key.newMac(), h)
                    && Exchange.verifyData(key.newMac(), h, b.slice(Exchange.HDR, 5), b.slice(Exchange.HDR + 5, Exchange.MAC));
            assertFalse(ok, "byte " + i);
        }
    }

    @Test void vectorsWithoutAPsk() {
        vectors(new byte[0], K_PLAIN, UP_PLAIN, UP_RESP_PLAIN, DOWN_PLAIN, DOWN_RESP_PLAIN);
    }

    @Test void vectorsWithAPsk() {
        byte[] psk = new byte[32];
        Arrays.fill(psk, (byte) 0x6b);
        vectors(psk, K_PSK, UP_PSK, UP_RESP_PSK, DOWN_PSK, DOWN_RESP_PSK);
    }

    @Test void requestFieldsFollowSectionThree() {
        var key = Exchange.key(new byte[0], CID);
        record Case(int kind, int flags, long seq, int data, boolean valid) {}
        for (Case c : new Case[] {new Case(1, 1, 5, 3, true), new Case(1, 2, 5, 0, false), new Case(2, 0, 7, 0, true),
                new Case(2, 0, 7, 1, false), new Case(2, 1, 7, 0, false), new Case(3, 0, 0, 256, true), new Case(3, 0, 0, 257, false),
                new Case(3, 0, 1, 0, false), new Case(4, 0, 0, 0, false), new Case(0, 0, 0, 0, false)}) {
            var h = Exchange.parseHeader(Exchange.seal(key, CID, c.kind, c.flags, c.seq, 0, TIME, NONCE, ByteBuffer.allocate(c.data)).header());
            assertEquals(c.valid, h.valid(c.data), c.toString());
            assertEquals(c.data, h.dataLength());
        }
        // The header is fixed: the largest fields take no more room.
        var full = Exchange.seal(key, CID, 1, 0, -1L, -1L, -1, NONCE, ByteBuffer.allocate(0));
        assertEquals(66, Exchange.HDR);
        assertEquals(Exchange.HDR, full.header().remaining());
        var h = Exchange.parseHeader(full.header());
        assertEquals(-1L, h.seq());
        assertEquals(-1L, h.ack());
        assertEquals(1 << 20, h.dataLimit(1 << 20));
        assertEquals(0, Exchange.parseHeader(Exchange.seal(key, CID, 2, 0, 0, 0, TIME, NONCE, ByteBuffer.allocate(0)).header()).dataLimit(1 << 20));
        assertEquals(Exchange.REASON_MAX, Exchange.parseHeader(Exchange.seal(key, CID, 3, 0, 0, 0, TIME, NONCE, ByteBuffer.allocate(0)).header()).dataLimit(1 << 20));
    }

    @Test void responseFlagsFollowSectionFour() {
        assertTrue(Exchange.responseValid(Exchange.KIND_DOWN, Exchange.FLAG_FIN, 5));
        assertTrue(Exchange.responseValid(Exchange.KIND_UP, 0, 0));
        assertFalse(Exchange.responseValid(Exchange.KIND_UP, 0, 1));
        assertFalse(Exchange.responseValid(Exchange.KIND_UP, Exchange.FLAG_FIN, 0));
        assertTrue(Exchange.responseValid(Exchange.KIND_DOWN, Exchange.FLAG_BUSY, 0));
        assertFalse(Exchange.responseValid(Exchange.KIND_DOWN, Exchange.FLAG_BUSY | Exchange.FLAG_FIN, 0));
        assertFalse(Exchange.responseValid(Exchange.KIND_DOWN, Exchange.FLAG_GONE, 1));
        assertFalse(Exchange.responseValid(Exchange.KIND_CLOSE, 0x08, 0));
    }

    @Test void theDefaultPieceSizeKeepsEveryBodyWithinAMillionBytes() {
        for (boolean b64 : new boolean[] {false, true}) {
            int piece = ExchangeConfig.defaultPieceSize(b64);
            int body = Exchange.REQ_OVERHEAD + piece;
            assertTrue((b64 ? Exchange.base64Length(body) : body) <= 1_000_000, "request " + b64);
            assertTrue((b64 ? Exchange.base64Length(Exchange.RESP_HDR + piece) : Exchange.RESP_HDR + piece) <= 1_000_000, "response " + b64);
            assertTrue((b64 ? Exchange.base64Length(body + 3) : body + 1) > 1_000_000, "largest " + b64);
        }
    }

    @Test void base64BodiesAreDecodedStrictly() {
        assertEquals("hello", StandardCharsets.US_ASCII.decode(Exchange.decodeBase64(ascii("aGVsbG8="))).toString());
        assertEquals(0, Exchange.decodeBase64(ascii("")).remaining());
        for (String bad : new String[] {"aGVsbG8", "aGVsbG9=", "aGVs bG8=", "aGVsbG8=aGVs", "a===", "aGVsbA=="}) {
            if (bad.equals("aGVsbA==")) { assertNotNull(Exchange.decodeBase64(ascii(bad))); continue; }
            assertNull(Exchange.decodeBase64(ascii(bad)), bad);
        }
        assertNull(Exchange.decodeBase64(ascii("aGVsbB==")), "stray bits");
    }

    @Test void configNeedsAPsk() {
        assertThrows(IllegalArgumentException.class, () -> ExchangeConfig.builder().build());
        assertThrows(IllegalArgumentException.class, () -> ExchangeConfig.builder().psk(new byte[32]).upPieceSize(100).upStoreLimit(1000).build(),
                "store below twice the largest UP piece");
        assertThrows(IllegalArgumentException.class, () -> ExchangeConfig.builder().psk(new byte[32]).upPieceSize(3 << 20).upStoreLimit((6 << 20) - 1).build());
        assertEquals(2 << 20, ExchangeConfig.builder().psk(new byte[32]).upStoreLimit(2 << 20).build().upStoreLimit());
        assertEquals(8 << 20, ExchangeConfig.builder().psk(new byte[32]).upPieceSize(4 << 20).build().upStoreLimit(), "default raised");
        var zero = ExchangeConfig.builder().psk(new byte[32])
                .holdTime(java.time.Duration.ZERO).goneMemory(java.time.Duration.ZERO).build();
        assertEquals(java.time.Duration.ZERO, zero.holdTime());
        assertEquals(java.time.Duration.ZERO, zero.goneMemory());
        assertEquals(java.time.Duration.ofSeconds(20), ExchangeConfig.builder().psk(new byte[32]).build().holdTime());
        assertThrows(IllegalArgumentException.class, () -> ExchangeConfig.builder().downRequests(0), "0 is no default");
        assertThrows(IllegalArgumentException.class, () -> ExchangeConfig.builder().requestTimeout(java.time.Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> ExchangeConfig.builder().requestTimeout(java.time.Duration.ofNanos(-1)));
        assertEquals(java.time.Duration.ofMillis(2), ExchangeConfig.builder().psk(new byte[32]).build().coalesce());
        assertEquals(java.time.Duration.ZERO, ExchangeConfig.builder().psk(new byte[32]).coalesce(java.time.Duration.ZERO).build().coalesce(), "0: no wait");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> ExchangeConfig.builder().coalesce(java.time.Duration.ofMillis(-1)));
    }
}
