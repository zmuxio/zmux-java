package io.zmux.http;

import io.zmux.carrier.VarInt;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** The wire format of the exchange carrier: key derivation, piece sealing and request bodies. */
final class Exchange {
    private Exchange() {}

    static final int KIND_UP = 0x01, KIND_DOWN = 0x02, KIND_CLOSE = 0x03;
    static final int FLAG_FIN = 0x01, FLAG_BUSY = 0x02, FLAG_GONE = 0x04;
    static final int CID = 16, TIME = 4, NONCE = 8, MAC = 16;
    /** A request header with the longest varints, MAC included. */
    static final int REQ_HDR_MAX = CID + 2 + 8 + 8 + TIME + NONCE + MAC;
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

        /** The first 16 bytes of HMAC-SHA256(K, "Q" || hdr || data): a request's MAC, hdr from its Carrier ID through its Nonce. */
        byte[] requestMac(ByteBuffer hdr, ByteBuffer data) {
            Mac m = copy(proto);
            m.update((byte) 'Q');
            m.update(hdr.duplicate());
            m.update(data.duplicate());
            return java.util.Arrays.copyOf(m.doFinal(), MAC);
        }

        /** The response MAC: "R" || Carrier ID || Kind || Sequence (as sent) || Nonce || Flags || Data. */
        byte[] responseMac(byte[] cid, int kind, ByteBuffer seqRaw, byte[] nonce, int flags, ByteBuffer data) {
            Mac m = copy(proto);
            m.update((byte) 'R');
            m.update(cid);
            m.update((byte) kind);
            m.update(seqRaw.duplicate());
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

    /** Appends a request from its Carrier ID through its Nonce, then its MAC over it and data. */
    static ByteBuffer requestHeader(Key key, byte[] cid, int kind, int flags, long seq, long ack, int time, byte[] nonce, ByteBuffer data) {
        ByteBuffer b = ByteBuffer.allocate(CID + 2 + VarInt.length(seq) + VarInt.length(ack) + TIME + NONCE + MAC);
        b.put(cid).put((byte) kind).put((byte) flags);
        VarInt.put(b, seq);
        VarInt.put(b, ack);
        b.putInt(time).put(nonce);
        ByteBuffer hdr = b.duplicate().flip();
        b.put(key.requestMac(hdr, data));
        return b.flip();
    }

    /**
     * A parsed request body (binary); its buffers are views of the body.
     *
     * @param valid whether Kind, Flags, Data and the reason follow the request rules
     */
    record Request(byte[] cid, int kind, int flags, long seq, long ack, ByteBuffer seqRaw, int time, byte[] nonce,
                   ByteBuffer hdr, byte[] mac, ByteBuffer data, boolean valid) {
        boolean fin() { return (flags & FLAG_FIN) != 0; }
    }

    /** Parses a binary request body; null without a complete header. */
    static Request parseRequest(ByteBuffer body) {
        ByteBuffer b = body.slice();
        int n = b.remaining();
        if (n < CID + 2) return null;
        byte[] cid = new byte[CID];
        b.get(0, cid);
        int kind = b.get(CID) & 0xff, flags = b.get(CID + 1) & 0xff;
        int off = CID + 2;
        long seq = VarInt.get(b, off, n);
        if (seq < 0) return null;
        int seqLen = VarInt.lengthOf(b.get(off));
        ByteBuffer seqRaw = b.slice(off, seqLen);
        off += seqLen;
        long ack = VarInt.get(b, off, n);
        if (ack < 0) return null;
        off += VarInt.lengthOf(b.get(off));
        if (n < off + TIME + NONCE + MAC) return null;
        int time = b.getInt(off);
        off += TIME;
        byte[] nonce = new byte[NONCE];
        b.get(off, nonce);
        off += NONCE;
        ByteBuffer hdr = b.slice(0, off);
        byte[] mac = new byte[MAC];
        b.get(off, mac);
        ByteBuffer data = b.slice(off + MAC, n - off - MAC);
        boolean valid = switch (kind) {
            case KIND_UP -> (flags & ~FLAG_FIN) == 0;
            case KIND_DOWN -> flags == 0 && !data.hasRemaining();
            case KIND_CLOSE -> flags == 0 && seq == 0 && data.remaining() <= REASON_MAX;
            default -> false;
        };
        return new Request(cid, kind, flags, seq, ack, seqRaw, time, nonce, hdr, mac, data, valid);
    }

    /** Whether a request's MAC verifies with key, compared in constant time. */
    static boolean verify(Key key, Request q) {
        return MessageDigest.isEqual(key.requestMac(q.hdr(), q.data()), q.mac());
    }

    /** The response header (MAC and Flags) to q with flags and data. */
    static ByteBuffer responseHeader(Key key, Request q, int flags, ByteBuffer data) {
        ByteBuffer b = ByteBuffer.allocate(RESP_HDR);
        b.put(key.responseMac(q.cid(), q.kind(), q.seqRaw(), q.nonce(), flags, data)).put((byte) flags);
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
}
