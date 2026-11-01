package io.zmux;

/**
 * The secret sources of pipe joins (MULTIPATH.md 10.1); multipath and resume need one.
 *
 * @param psk a pre-shared key of at least 16 bytes, or null
 * @param useExporter derives the secret from the exporter of the innermost end-to-end handshake of the
 *     bootstrap carrier; today its source is TLS 1.3 that this process terminates
 * @param allowInsecure joins without a secret (tests, protected networks); not with a PSK
 */
public record PipeAuth(byte[] psk, boolean useExporter, boolean allowInsecure) {
    public PipeAuth {
        if (psk != null) {
            if (psk.length < 16) throw new IllegalArgumentException("PSK shorter than 16 bytes");
            if (allowInsecure) throw new IllegalArgumentException("allowInsecure with a PSK");
            psk = psk.clone();
        }
        if (psk == null && !useExporter && !allowInsecure) throw new IllegalArgumentException("no secret source");
    }

    public static PipeAuth psk(byte[] key) {
        return new PipeAuth(key, false, false);
    }

    /** The exporter of the innermost end-to-end handshake of the bootstrap carrier (E). */
    public static PipeAuth exporter() {
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
