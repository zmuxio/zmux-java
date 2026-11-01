package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

/** Fixed wire vectors of the exchange carrier: keys, sealed UP and DOWN requests and their responses. */
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

    static void vectors(byte[] psk, String k, String up, String upResp, String down, String downResp) {
        var key = Exchange.key(psk, CID);
        assertEquals(k, HEX.formatHex(key.k()));
        ByteBuffer hello = ascii("hello");
        ByteBuffer upHdr = Exchange.requestHeader(key, CID, Exchange.KIND_UP, 0, 0, 0, TIME, NONCE, hello);
        assertEquals(up, hex(upHdr, hello));
        ByteBuffer downHdr = Exchange.requestHeader(key, CID, Exchange.KIND_DOWN, 0, 300, 299, TIME, NONCE, ByteBuffer.allocate(0));
        assertEquals(down, hex(downHdr));

        var q = Exchange.parseRequest(ByteBuffer.wrap(HEX.parseHex(up)));
        assertTrue(q.valid() && Exchange.verify(key, q));
        assertEquals("hello", StandardCharsets.US_ASCII.decode(q.data()).toString());
        assertEquals(upResp, hex(Exchange.responseHeader(key, q, 0, ByteBuffer.allocate(0))));

        var d = Exchange.parseRequest(ByteBuffer.wrap(HEX.parseHex(down)));
        assertTrue(d.valid() && Exchange.verify(key, d));
        assertEquals(300, d.seq());
        assertEquals(299, d.ack());
        assertEquals(TIME, d.time());
        ByteBuffer world = ascii("world");
        assertEquals(downResp, hex(Exchange.responseHeader(key, d, Exchange.FLAG_FIN, world), world));

        // A changed byte anywhere fails the MAC.
        byte[] bad = HEX.parseHex(up);
        bad[bad.length - 1] ^= 1;
        assertFalse(Exchange.verify(key, Exchange.parseRequest(ByteBuffer.wrap(bad))));
    }

    @Test void vectorsWithoutAPsk() {
        vectors(new byte[0], "8e08f7810b828921459c8d8c4f1be67963269408f5e7179f7f0df94d417723ef",
                "000102030405060708090a0b0c0d0e0f010000006955b9000102030405060708507a0b4c9f35406fc018ecb298ac332068656c6c6f",
                "477e5b04a29e7fc80fd53eb36fd0715400",
                "000102030405060708090a0b0c0d0e0f0200412c412b6955b9000102030405060708e285ca274bac052e7c0a98cf3174ee5b",
                "0ccf05f246c40f075271dd7e52d2420401776f726c64");
    }

    @Test void vectorsWithAPsk() {
        byte[] psk = new byte[32];
        Arrays.fill(psk, (byte) 0x6b);
        vectors(psk, "68489b10ee27b348be274432395cb2107dcfc8cf3f9f69f2cd4cf7cd7a0f8c95",
                "000102030405060708090a0b0c0d0e0f010000006955b90001020304050607089b83951cb9d54b75d8d0785f3eb2d3d668656c6c6f",
                "c5b18d1988c3d46dbfee70883026026000",
                "000102030405060708090a0b0c0d0e0f0200412c412b6955b9000102030405060708d48a0293326ba2533431172737bc96a7",
                "f900c6512f2edcc31275640df468edf201776f726c64");
    }

    @Test void requestFieldsFollowSectionThree() {
        var key = Exchange.key(new byte[0], CID);
        record Case(int kind, int flags, long seq, int data, boolean valid) {}
        for (Case c : new Case[] {new Case(1, 1, 5, 3, true), new Case(1, 2, 5, 0, false), new Case(2, 0, 7, 0, true),
                new Case(2, 0, 7, 1, false), new Case(2, 1, 7, 0, false), new Case(3, 0, 0, 256, true), new Case(3, 0, 0, 257, false),
                new Case(3, 0, 1, 0, false), new Case(4, 0, 0, 0, false), new Case(0, 0, 0, 0, false)}) {
            ByteBuffer data = ByteBuffer.allocate(c.data);
            ByteBuffer h = Exchange.requestHeader(key, CID, c.kind, c.flags, c.seq, 0, TIME, NONCE, data);
            ByteBuffer body = ByteBuffer.allocate(h.remaining() + c.data).put(h).put(data).flip();
            assertEquals(c.valid, Exchange.parseRequest(body).valid(), c.toString());
        }
        ByteBuffer full = Exchange.requestHeader(key, CID, 1, 0, 1L << 40, 1L << 20, TIME, NONCE, ByteBuffer.allocate(0));
        assertEquals(Exchange.REQ_HDR_MAX - 4, full.remaining());
        for (int n = 0; n < full.remaining(); n++) assertNull(Exchange.parseRequest(full.duplicate().limit(n)), "truncated at " + n);
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
            int body = Exchange.REQ_HDR_MAX + piece;
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

    @Test void configNeedsAPskOrInsecure() {
        assertThrows(IllegalArgumentException.class, () -> ExchangeConfig.builder().build());
        assertThrows(IllegalArgumentException.class, () -> ExchangeConfig.builder().psk(new byte[] {1}).allowInsecure(true).build());
        assertThrows(IllegalArgumentException.class, () -> ExchangeConfig.builder().allowInsecure(true).upPieceSize(100).upStoreLimit(1000).build(),
                "store below twice the largest UP piece");
        assertThrows(IllegalArgumentException.class, () -> ExchangeConfig.builder().allowInsecure(true).upPieceSize(3 << 20).upStoreLimit((6 << 20) - 1).build());
        assertEquals(2 << 20, ExchangeConfig.builder().allowInsecure(true).upStoreLimit(2 << 20).build().upStoreLimit());
        assertEquals(8 << 20, ExchangeConfig.builder().allowInsecure(true).upPieceSize(4 << 20).build().upStoreLimit(), "default raised");
        assertThrows(IllegalArgumentException.class, () -> ExchangeConfig.builder().allowInsecure(true)
                .backoff(java.time.Duration.ofSeconds(3), java.time.Duration.ofSeconds(1)).build(), "maximum below minimum");
        var zero = ExchangeConfig.builder().allowInsecure(true).backoff(java.time.Duration.ZERO, java.time.Duration.ZERO)
                .holdTime(java.time.Duration.ZERO).goneMemory(java.time.Duration.ZERO).build();
        assertEquals(java.time.Duration.ZERO, zero.backoffMax(), "0 as it is");
        assertEquals(java.time.Duration.ZERO, zero.holdTime());
        assertEquals(java.time.Duration.ZERO, zero.goneMemory());
        assertEquals(java.time.Duration.ofSeconds(20), ExchangeConfig.builder().allowInsecure(true).build().holdTime());
        assertThrows(IllegalArgumentException.class, () -> ExchangeConfig.builder().downRequests(-1));
        assertThrows(IllegalArgumentException.class, () -> ExchangeConfig.builder().requestTimeout(java.time.Duration.ofNanos(-1)));
        assertEquals(java.time.Duration.ofMillis(2), ExchangeConfig.builder().allowInsecure(true).build().coalesce());
        assertEquals(java.time.Duration.ZERO, ExchangeConfig.builder().allowInsecure(true).coalesce(java.time.Duration.ZERO).build().coalesce(), "0: no wait");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> ExchangeConfig.builder().coalesce(java.time.Duration.ofMillis(-1)));
    }
}
