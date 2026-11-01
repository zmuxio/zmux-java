package io.zmux;

/**
 * The secret sources of pipe joins (MULTIPATH.md 10.1); multipath and resume need one.
 *
 * @param psk a pre-shared key of at least 16 bytes, or null
 * @param tls asserts that the bootstrap carrier's TLS 1.3 handshake terminates at the peer
 * @param allowInsecure joins without a secret (tests, protected networks); not with a PSK
 */
public record PipeAuth(byte[] psk, boolean tls, boolean allowInsecure) {
    public PipeAuth {
        if (psk != null) {
            if (psk.length < 16) throw new IllegalArgumentException("PSK shorter than 16 bytes");
            if (allowInsecure) throw new IllegalArgumentException("allowInsecure with a PSK");
            psk = psk.clone();
        }
        if (psk == null && !tls && !allowInsecure) throw new IllegalArgumentException("no secret source");
    }

    public static PipeAuth psk(byte[] key) {
        return new PipeAuth(key, false, false);
    }

    public static PipeAuth tlsExporter() {
        return new PipeAuth(null, true, false);
    }

    public static PipeAuth insecure() {
        return new PipeAuth(null, false, true);
    }

    @Override
    public byte[] psk() {
        return psk == null ? null : psk.clone();
    }
}
