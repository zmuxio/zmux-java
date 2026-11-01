package io.zmux;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Base64;
import java.util.Objects;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Prefix authentication tokens: a carrier starts with {@link
 * #TOKEN_LENGTH} bytes, a random Nonce and a MAC over a time slot, an optional binding to the
 * carrier's TLS handshake, and the Nonce. A carrier decided on its HTTP request head
 * (WebSocket, WebTransport, a request without a body) carries the token in a request header
 * field instead, as {@link #HEADER_LENGTH} characters of base64url without padding. key,
 * slot, bindTls and headerField must be the same on both ends; skew and cacheSize are the
 * verifier's. As a {@link Credential} it presents a fresh token on every carrier.
 *
 * @param key the pre-shared key, not empty
 * @param slot the slot length, a whole number of seconds
 * @param bindTls tokens are bound to the carrier's TLS handshake through the exporter
 *     ({@link #binding}); a token is then valid on that carrier (or the connection carrying
 *     the request) only
 * @param skew slots on either side of the verifier's current slot it also accepts
 * @param cacheSize bound of the Nonces the verifier records while their slot is acceptable;
 *     0: 65536 (a record of no Nonce would reject every token); a token that finds it full is
 *     rejected
 * @param headerField the request field of the header form (null or empty: {@link
 *     #HEADER_FIELD}), another name only where an intermediary strips unknown fields
 */
public record PrefixAuth(byte[] key, Duration slot, boolean bindTls, int skew, int cacheSize, String headerField)
        implements Credential {
    public static final int TOKEN_LENGTH = 32;
    /** The length of a token in the header form: 32 bytes in base64url without padding. */
    public static final int HEADER_LENGTH = 43;
    /** The request header field that carries the token when a carrier is decided on its request head. */
    public static final String HEADER_FIELD = "Zmux-Prefix-Auth";
    /** The TLS exporter label of the binding. */
    public static final String EXPORTER_LABEL = "EXPORTER-zmux-prefix-auth";
    private static final int NONCE = 16;
    private static final byte[] LABEL = "zmux-prefix-auth/1".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    private static final SecureRandom RANDOM = new SecureRandom();

    public PrefixAuth {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(slot, "slot");
        if (key.length == 0) throw new IllegalArgumentException("empty key");
        if (slot.compareTo(Duration.ofSeconds(1)) < 0 || slot.toNanosPart() != 0) throw new IllegalArgumentException("slot must be whole seconds");
        if (skew < 0 || cacheSize < 0) throw new IllegalArgumentException("negative bound");
        if (headerField == null || headerField.isEmpty()) headerField = HEADER_FIELD;
        else if (!headerField.chars().allMatch(PrefixAuth::tokenChar)) throw new IllegalArgumentException("invalid header field name");
        key = key.clone();
    }

    /** With the default header field {@link #HEADER_FIELD}. */
    public PrefixAuth(byte[] key, Duration slot, boolean bindTls, int skew, int cacheSize) {
        this(key, slot, bindTls, skew, cacheSize, HEADER_FIELD);
    }

    /** This configuration with another header field name (agreed by both ends). */
    public PrefixAuth withHeaderField(String name) { return new PrefixAuth(key, slot, bindTls, skew, cacheSize, name); }

    @Override public byte[] key() { return key.clone(); }

    /** An RFC 9110 token character, as a field name consists of. */
    private static boolean tokenChar(int c) {
        return c > 0x20 && c < 0x7f && "\"(),/:;<=>?@[\\]{}".indexOf(c) < 0;
    }

    /**
     * A fresh token for the current time slot, bound to info's exporter with bindTls (a
     * carrier without TLS is then an error).
     */
    @Override
    public byte[] prefix(Admission.CarrierInfo info) throws java.io.IOException {
        byte[] b = null;
        if (bindTls) {
            b = info == null ? null : binding(info);
            if (b == null) throw new java.io.IOException("PrefixAuth.bindTls on a carrier without a TLS exporter");
        }
        return token(b);
    }

    /** The header form of a fresh token: {@link #headerField()} with the token in base64url without padding. */
    @Override
    public Credential.HeaderField header(Admission.CarrierInfo info) throws java.io.IOException {
        return new Credential.HeaderField(headerField, Base64.getUrlEncoder().withoutPadding().encodeToString(prefix(info)));
    }

    /**
     * The token of a header form value: exactly {@link #HEADER_LENGTH} base64url characters
     * without padding whose unused bits are zero; null otherwise.
     */
    public static byte[] decodeHeader(String value) {
        if (value == null || value.length() != HEADER_LENGTH) return null;
        for (int i = 0; i < HEADER_LENGTH; i++) {
            char c = value.charAt(i);
            if (!(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_')) return null;
        }
        // 43 characters carry 258 bits: the last one's low 2 bits are unused and must be zero.
        int last = value.charAt(HEADER_LENGTH - 1);
        int sextet = last >= 'A' && last <= 'Z' ? last - 'A' : last >= 'a' && last <= 'z' ? last - 'a' + 26
                : last >= '0' && last <= '9' ? last - '0' + 52 : last == '-' ? 62 : 63;
        if ((sextet & 3) != 0) return null;
        byte[] t = Base64.getUrlDecoder().decode(value);
        return t.length == TOKEN_LENGTH ? t : null;
    }

    /** The binding of a TLS carrier: 32 bytes of the exporter with {@link #EXPORTER_LABEL} and an empty context; null without TLS. */
    public static byte[] binding(Admission.CarrierInfo info) {
        return info.tls() ? info.exportKeyingMaterial(EXPORTER_LABEL, new byte[0], 32) : null;
    }

    /** A token for the current slot; binding is the carrier's {@link #binding} when bindTls, else null. */
    byte[] token(byte[] binding) {
        byte[] nonce = new byte[NONCE];
        RANDOM.nextBytes(nonce);
        return token(binding, System.currentTimeMillis() / 1000, nonce);
    }

    byte[] token(byte[] binding, long nowSeconds, byte[] nonce) {
        byte[] b = binding == null ? new byte[0] : binding;
        if (b.length > 255) throw new IllegalArgumentException("binding over 255 bytes");
        byte[] out = new byte[TOKEN_LENGTH];
        System.arraycopy(nonce, 0, out, 0, NONCE);
        System.arraycopy(tag(mac(derive()), Math.floorDiv(nowSeconds, slot.toSeconds()), b, nonce, 0), 0, out, NONCE, NONCE);
        return out;
    }

    /** The token key. */
    byte[] derive() {
        Mac m = mac(key);
        m.update(LABEL);
        return m.doFinal();
    }

    private static Mac mac(byte[] k) {
        try {
            Mac m = Mac.getInstance("HmacSHA256");
            m.init(new SecretKeySpec(k, "HmacSHA256"));
            return m;
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] tag(Mac m, long slot, byte[] binding, byte[] src, int nonceAt) {
        m.reset();
        m.update(ByteBuffer.allocate(9).putLong(slot).put((byte) binding.length).array());
        m.update(binding);
        m.update(src, nonceAt, NONCE);
        byte[] t = new byte[NONCE];
        System.arraycopy(m.doFinal(), 0, t, 0, NONCE);
        return t;
    }

    /** A verifier of this configuration. */
    public Verifier verifier() { return new Verifier(this); }

    /**
     * Verifies tokens and rejects each Nonce after its first use. Safe for concurrent use. As an
     * {@link Admission.Decider} (with peek {@link #TOKEN_LENGTH}) it accepts a carrier whose prefix
     * is a valid token, without the token; on a carrier decided on its request head
     * ({@link Admission.CarrierInfo#head()}) the token is the request's only header field
     * {@link PrefixAuth#headerField()} ({@link #decodeHeader}), and the accept skips nothing.
     * Otherwise, and with bindTls on a carrier without an exporter, it answers
     * {@link Admission.Decision#FALLBACK}.
     */
    public static final class Verifier implements Admission.Decider {
        private final boolean bind;
        private final String header;
        private final long slot, skew;
        private final byte[] k;
        private final ThreadLocal<Mac> macs;
        private final io.zmux.helper.ReplayRecord nonces;
        java.util.function.LongSupplier clock = () -> System.currentTimeMillis() / 1000;

        private Verifier(PrefixAuth a) {
            bind = a.bindTls;
            header = a.headerField;
            slot = a.slot.toSeconds();
            skew = a.skew;
            k = a.derive();
            nonces = new io.zmux.helper.ReplayRecord(a.cacheSize == 0 ? 1 << 16 : a.cacheSize);
            macs = ThreadLocal.withInitial(() -> mac(k));
        }

        /** Whether token is valid for binding (null: none) and its Nonce is new. */
        public boolean verify(byte[] token, byte[] binding) {
            byte[] b = binding == null ? new byte[0] : binding;
            if (token == null || token.length != TOKEN_LENGTH || b.length > 255) return false;
            long cur = Math.floorDiv(clock.getAsLong(), slot);
            Mac m = macs.get();
            byte[] got = java.util.Arrays.copyOfRange(token, NONCE, TOKEN_LENGTH);
            boolean found = false;
            long at = 0;
            for (long d = 0; d <= 2 * skew && !found; d++) {
                long s = d % 2 == 1 ? cur - (d + 1) / 2 : cur + (d + 1) / 2; // cur, cur-1, cur+1, ...
                if (MessageDigest.isEqual(tag(m, s, b, token, 0), got)) { found = true; at = s; }
            }
            if (!found) return false;
            return nonces.add(at, java.util.Arrays.copyOf(token, NONCE), cur - skew);
        }

        @Override public java.util.concurrent.CompletionStage<Admission.Decision> decide(Admission.CarrierInfo info, ByteBuffer prefix,
                                                                                    Admission.Deadline deadline) {
            return java.util.concurrent.CompletableFuture.completedFuture(decide(info, prefix));
        }

        /** The decision on a carrier, taken at once. */
        public Admission.Decision decide(Admission.CarrierInfo info, ByteBuffer prefix) {
            byte[] binding = null;
            if (bind) {
                binding = binding(info);
                if (binding == null) return Admission.Decision.FALLBACK;
            }
            if (info.head()) {
                var values = info.header(header);
                byte[] t = values.size() == 1 ? decodeHeader(values.getFirst()) : null;
                return t != null && verify(t, binding) ? Admission.Decision.accept(0) : Admission.Decision.FALLBACK;
            }
            if (prefix.remaining() != TOKEN_LENGTH) return Admission.Decision.FALLBACK;
            byte[] t = new byte[TOKEN_LENGTH];
            prefix.duplicate().get(t);
            return verify(t, binding) ? Admission.Decision.accept(TOKEN_LENGTH) : Admission.Decision.FALLBACK;
        }
    }
}
