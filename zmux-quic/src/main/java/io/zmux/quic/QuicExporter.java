package io.zmux.quic;

import io.netty.handler.codec.quic.BoringSSLKeylog;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HexFormat;
import java.util.Map;
import java.util.WeakHashMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLEngine;

/**
 * The TLS exporter of a QUIC connection (RFC 8446 Section 7.5). The stack has no call for it, and its key log
 * has the exporter secret: {@link QuicClient} and {@link QuicServer} install {@link #KEYLOG} on their TLS
 * builder, and a host that builds its own codec for {@link QuicServer#serve} passes it to
 * {@code QuicSslContextBuilder.keylog}, which replaces any other key log. The secret stays in memory
 * for the life of the connection's engine, as the TLS stack's own does. The stack's key log lines carry the
 * connection's traffic secrets too: they are not kept here, but stay on the heap as strings until collected.
 */
public final class QuicExporter {
    private static final String LINE = "EXPORTER_SECRET ";
    private static final Map<SSLEngine, byte[]> SECRETS = Collections.synchronizedMap(new WeakHashMap<>());

    /** Keeps each connection's exporter secret. */
    public static final BoringSSLKeylog KEYLOG = (engine, line) -> {
        if (!line.startsWith(LINE)) return;
        // EXPORTER_SECRET <client random> <secret>, in hex.
        SECRETS.put(engine, HexFormat.of().parseHex(line, line.lastIndexOf(' ') + 1, line.length()));
    };

    private QuicExporter() {}

    /** The keying material of a connection; null when its secret is not known (the handshake is incomplete, or no key log). */
    public static byte[] export(SSLEngine engine, String label, byte[] context, int length) {
        byte[] secret = engine == null ? null : SECRETS.get(engine);
        if (secret == null) return null;
        boolean sha384 = engine.getSession().getCipherSuite().endsWith("SHA384");
        return derive(secret, sha384 ? "SHA-384" : "SHA-256", label, context, length);
    }

    /** TLS-Exporter(label, context, length) from the exporter secret. */
    static byte[] derive(byte[] secret, String hash, String label, byte[] context, int length) {
        try {
            String mac = "Hmac" + hash.replace("-", "");
            MessageDigest d = MessageDigest.getInstance(hash);
            byte[] empty = d.digest();
            byte[] derived = expandLabel(mac, secret, label, empty, d.getDigestLength());
            byte[] h = d.digest(context == null ? new byte[0] : context);
            return expandLabel(mac, derived, "exporter", h, length);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    /** HKDF-Expand-Label (RFC 8446 Section 7.1) on the JDK's HMAC. */
    private static byte[] expandLabel(String mac, byte[] prk, String label, byte[] context, int length) throws GeneralSecurityException {
        byte[] full = ("tls13 " + label).getBytes(StandardCharsets.US_ASCII);
        ByteBuffer info = ByteBuffer.allocate(2 + 1 + full.length + 1 + context.length);
        info.putShort((short) length).put((byte) full.length).put(full).put((byte) context.length).put(context);
        Mac m = Mac.getInstance(mac);
        m.init(new SecretKeySpec(prk, mac));
        byte[] out = new byte[length], t = new byte[0];
        for (int at = 0, i = 1; at < length; i++) {
            m.update(t);
            m.update(info.array());
            m.update((byte) i);
            t = m.doFinal();
            int n = Math.min(t.length, length - at);
            System.arraycopy(t, 0, out, at, n);
            at += n;
        }
        return out;
    }
}
