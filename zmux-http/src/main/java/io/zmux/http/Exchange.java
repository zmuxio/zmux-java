package io.zmux.http;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** The wire format of the exchange carrier: key derivation, request authentication and response MACs. */
final class Exchange {
    private Exchange() {}

    static final int KIND_UP = 0x01, KIND_DOWN = 0x02, KIND_CLOSE = 0x03;
    static final int FLAG_FIN = 0x01, FLAG_BUSY = 0x02, FLAG_GONE = 0x04;
    static final int CID = 16, TIME = 4, NONCE = 8, MAC = 16;
    /** The authentication header: Carrier ID, Kind, Flags, Sequence, Down Ack, Time, Nonce, Data Length, Header MAC. */
    static final int HDR = CID + 2 + 8 + 8 + TIME + NONCE + 4 + MAC;
    /** Where the Header MAC starts: the length of the fields it covers. */
    static final int HDR_FIELDS = HDR - MAC;
    /** A request body's bytes besides its Data: the authentication header and the Data MAC. */
    static final int REQ_OVERHEAD = HDR + MAC;
    static final int RESP_HDR = MAC + 1;
    static final int REASON_MAX = 256;
    /** Every body of the default piece size stays at most this long, after the body encoding. */
    static final int BODY_MAX = 1_000_000;
    /** The default limit of piece data in one body. */
    static final int LIMIT = 1 << 20;

    private static final byte[] LABEL = "zmux-http-exchange/1".getBytes(StandardCharsets.US_ASCII);

    /** A carrier key K: immutable, safe for concurrent use. */
    static final class Key {
        private final byte[] k;
        private final Mac proto;

        Key(byte[] k) {
            this.k = k;
            proto = mac(k);
        }

        /** K, for tests. */
        byte[] k() { return k.clone(); }

        /** A MAC keyed with K, for one thread; doFinal leaves it keyed for the next MAC. */
        Mac newMac() { return copy(proto); }

        /** HMAC-SHA256(K, "Q" || fields), whose first 16 bytes are the Header MAC: fields are the header from Carrier ID through Data Length. */
        static byte[] headerMac(Mac m, ByteBuffer fields) {
            m.update((byte) 'Q');
            m.update(fields.duplicate());
            return m.doFinal();
        }

        /** HMAC-SHA256(K, "D" || Header MAC || data), whose first 16 bytes are the Data MAC. */
        static byte[] dataMac(Mac m, ByteBuffer headerMac, ByteBuffer data) {
            m.update((byte) 'D');
            m.update(headerMac.duplicate());
            m.update(data.duplicate());
            return m.doFinal();
        }

        /** The response MAC: "R" || Carrier ID || Kind || Sequence (8 bytes) || Nonce || Flags || Data. */
        byte[] responseMac(byte[] cid, int kind, long seq, byte[] nonce, int flags, ByteBuffer data) {
            Mac m = copy(proto);
            m.update((byte) 'R');
            m.update(cid);
            m.update((byte) kind);
            for (int i = 56; i >= 0; i -= 8) m.update((byte) (seq >>> i));
            m.update(nonce);
            m.update((byte) flags);
            m.update(data.duplicate());
            return java.util.Arrays.copyOf(m.doFinal(), MAC);
        }
    }

    /** The carrier key of cid under psk (empty: none). */
    static Key key(byte[] psk, byte[] cid) {
        Mac m = psk.length == 0 ? emptyKeyMac() : mac(psk);
        m.update(LABEL);
        m.update(cid);
        return new Key(m.doFinal());
    }

    static Mac mac(byte[] key) {
        try {
            Mac m = Mac.getInstance("HmacSHA256");
            m.init(new SecretKeySpec(key, "HmacSHA256"));
            return m;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    /** HMAC-SHA256 with the empty key, which SecretKeySpec does not take: HMAC pads keys with zeros, so one zero byte is the same key. */
    private static Mac emptyKeyMac() { return mac(new byte[1]); }

    private static Mac copy(Mac m) {
        try {
            return (Mac) m.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * A request's authentication header and Data MAC: header holds the header (Header MAC
     * included), dataMac the Data MAC over data. The body is header || data || dataMac.
     */
    record Sealed(ByteBuffer header, ByteBuffer dataMac) {}

    /** Seals a request: its authentication header and its Data MAC over data. */
    static Sealed seal(Key key, byte[] cid, int kind, int flags, long seq, long ack, int time, byte[] nonce, ByteBuffer data) {
        byte[] h = new byte[HDR];
        ByteBuffer b = ByteBuffer.wrap(h);
        b.put(cid).put((byte) kind).put((byte) flags).putLong(seq).putLong(ack).putInt(time).put(nonce).putInt(data.remaining());
        Mac m = key.newMac();
        System.arraycopy(Key.headerMac(m, ByteBuffer.wrap(h, 0, HDR_FIELDS)), 0, h, HDR_FIELDS, MAC);
        byte[] d = Key.dataMac(m, ByteBuffer.wrap(h, HDR_FIELDS, MAC), data);
        return new Sealed(ByteBuffer.wrap(h), ByteBuffer.wrap(d, 0, MAC));
    }

    /**
     * A parsed authentication header; its buffers are views of the header.
     *
     * @param dataLength Data Length, unsigned
     */
    record Header(byte[] cid, int kind, int flags, long seq, long ack, int time, byte[] nonce, long dataLength,
                  ByteBuffer fields, ByteBuffer mac) {
        boolean fin() { return (flags & FLAG_FIN) != 0; }

        /** The largest Data a request of this Kind may carry: upLimit for UP, 0 for DOWN, the reason's for CLOSE. */
        long dataLimit(int upLimit) {
            return kind == KIND_UP ? upLimit : kind == KIND_CLOSE ? REASON_MAX : 0;
        }

        /** Whether Kind, Flags, Sequence and a Data of length n follow the request rules. */
        boolean valid(int n) {
            return switch (kind) {
                case KIND_UP -> (flags & ~FLAG_FIN) == 0;
                case KIND_DOWN -> flags == 0 && n == 0;
                case KIND_CLOSE -> flags == 0 && seq == 0 && n <= REASON_MAX;
                default -> false;
            };
        }
    }

    /** Parses the {@link #HDR} bytes of an authentication header at b's position. */
    static Header parseHeader(ByteBuffer b) {
        ByteBuffer h = b.slice(b.position(), HDR);
        byte[] cid = new byte[CID];
        h.get(0, cid);
        byte[] nonce = new byte[NONCE];
        h.get(CID + 2 + 16 + TIME, nonce);
        return new Header(cid, h.get(CID) & 0xff, h.get(CID + 1) & 0xff, h.getLong(CID + 2), h.getLong(CID + 10),
                h.getInt(CID + 18), nonce, h.getInt(CID + 2 + 16 + TIME + NONCE) & 0xffffffffL,
                h.slice(0, HDR_FIELDS), h.slice(HDR_FIELDS, MAC));
    }

    /** Whether a header's MAC verifies with m (keyed with the carrier key), compared in constant time. */
    static boolean verifyHeader(Mac m, Header h) {
        return equal(Key.headerMac(m, h.fields()), h.mac());
    }

    /** Whether a Data MAC verifies with m for header h and data, compared in constant time. */
    static boolean verifyData(Mac m, Header h, ByteBuffer data, ByteBuffer mac) {
        return equal(Key.dataMac(m, h.mac(), data), mac);
    }

    private static boolean equal(byte[] want, ByteBuffer got) {
        int diff = got.remaining() ^ MAC;
        for (int i = 0; i < MAC && i < got.remaining(); i++) diff |= want[i] ^ got.get(got.position() + i);
        return diff == 0;
    }

    /** The response header (MAC and Flags) to the request h with flags and data. */
    static ByteBuffer responseHeader(Key key, Header h, int flags, ByteBuffer data) {
        ByteBuffer b = ByteBuffer.allocate(RESP_HDR);
        b.put(key.responseMac(h.cid(), h.kind(), h.seq(), h.nonce(), flags, data)).put((byte) flags);
        return b.flip();
    }

    /** Whether response flags and data are well formed for a request of kind; a malformed response fails the carrier. */
    static boolean responseValid(int kind, int flags, int dataLength) {
        if ((flags & ~(FLAG_FIN | FLAG_BUSY | FLAG_GONE)) != 0) return false;
        if ((flags & (FLAG_BUSY | FLAG_GONE)) != 0) return (flags == FLAG_BUSY || flags == FLAG_GONE) && dataLength == 0;
        if (kind != KIND_DOWN) return flags == 0 && dataLength == 0;
        return true;
    }

    /** The length of the padded base64 encoding of n bytes. */
    static int base64Length(int n) { return (n + 2) / 3 * 4; }

    /**
     * Decodes a padded base64 body strictly: whole quanta, canonical padding and trailing bits;
     * null when it is not one.
     */
    static ByteBuffer decodeBase64(ByteBuffer in) {
        int n = in.remaining();
        if (n % 4 != 0) return null;
        try {
            ByteBuffer out = Base64.getDecoder().decode(in.duplicate());
            if (n > 0) {
                // Re-encoding the last quantum's bytes must give it back: no stray bits, canonical padding.
                int tail = out.remaining() - ((n / 4 - 1) * 3);
                byte[] last = new byte[tail];
                out.get(out.limit() - tail, last);
                byte[] again = Base64.getEncoder().encode(last);
                for (int i = 0; i < 4; i++) if (again[i] != in.get(in.position() + n - 4 + i)) return null;
            }
            return out;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Makes room for n more bytes in buf (in write mode): the same buffer when they fit, else a
     * larger copy (twice the capacity, at least what is needed, at most max). The exchange
     * carriers keep their pieces and bodies in such heap buffers; they hold nothing to release.
     */
    static ByteBuffer room(ByteBuffer buf, int n, int max) {
        if (buf.remaining() >= n) return buf;
        int need = buf.position() + n;
        ByteBuffer grown = ByteBuffer.allocate((int) Math.min(max, Math.max(need, 2L * buf.capacity())));
        return grown.put(buf.flip());
    }

    /** Appends src's remaining bytes to buf, grown as {@link #room} does; returns the buffer holding them. */
    static ByteBuffer append(ByteBuffer buf, ByteBuffer src, int max) {
        return room(buf, src.remaining(), max).put(src);
    }

    /** Appends n bytes of src from index (src's indices unchanged) to buf, grown as {@link #room} does. */
    static ByteBuffer append(ByteBuffer buf, io.netty.buffer.ByteBuf src, int index, int n, int max) {
        ByteBuffer b = room(buf, n, max);
        src.getBytes(index, b.array(), b.arrayOffset() + b.position(), n);
        return b.position(b.position() + n);
    }
}
