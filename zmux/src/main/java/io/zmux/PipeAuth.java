package io.zmux;

/**
 * The secret sources of pipe joins: a pre-shared key (static or deferred), the exporter, or both.
 * Multipath and resume need a source; without one ({@link #none()}, or no {@code PipeAuth} at all) a
 * session that enables them fails to start with {@link IllegalArgumentException}, before any byte,
 * unless its first pipe is on the secure carrier, whose Establish is then the only source: a key or
 * the exporter here is rejected for such a session, and {@link #none()}{@code .withEndToEnd()} is
 * accepted without effect, so that one configuration serves both kinds of first pipe. A session whose
 * first pipe is not on the secure carrier also needs the proof that it is end to end: {@link
 * #withEndToEnd()} when known before the session starts, else {@link ZmuxSession#declareEndToEnd()};
 * neither is a source. What is configured is what is used: a source is never dropped because the
 * peer did not offer it.
 *
 * @param psk a pre-shared key of at least 16 bytes, or null
 * @param useExporter derives the secret from the exporter of the innermost end-to-end handshake of the
 *     bootstrap carrier ({@link io.zmux.carrier.PipeIo.Capabilities#exporter()}); a session on a
 *     carrier without one fails to start
 * @param deferredPsk offers a PSK the application supplies after establishment with
 *     {@link ZmuxSession#setJoinSecret}; until then local joins fail with
 *     {@link JoinPendingException}, the peer's are held and the session cannot resume. Combines
 *     with the exporter as a static PSK does; the peer may have the same bytes configured as a static one
 * @param endToEnd the host knows pipe 0 is end to end before the session starts
 */
public record PipeAuth(byte[] psk, boolean useExporter, boolean deferredPsk, boolean endToEnd) {
    public PipeAuth {
        if (psk != null) {
            if (psk.length < 16) throw new IllegalArgumentException("PSK shorter than 16 bytes");
            psk = psk.clone();
        }
        if (deferredPsk && psk != null) throw new IllegalArgumentException("deferredPsk with a PSK");
    }

    /** No source, for {@link #withEndToEnd()} alone: valid only where the first pipe is on the secure carrier. */
    public static PipeAuth none() {
        return new PipeAuth(null, false, false, false);
    }

    /** Whether this names a source: a pre-shared key (static or deferred) or the exporter. */
    public boolean isConfigured() {
        return psk != null || useExporter || deferredPsk;
    }

    public static PipeAuth psk(byte[] key) {
        return new PipeAuth(key, false, false, false);
    }

    /** The exporter of the innermost end-to-end handshake of the bootstrap carrier (E). */
    public static PipeAuth exporter() {
        return new PipeAuth(null, true, false, false);
    }

    /** A PSK supplied later with {@link ZmuxSession#setJoinSecret}. */
    public static PipeAuth deferred() {
        return new PipeAuth(null, false, true, false);
    }

    /** This source together with a static pre-shared key of at least 16 bytes. */
    public PipeAuth withPsk(byte[] key) {
        return new PipeAuth(java.util.Objects.requireNonNull(key, "key"), useExporter, deferredPsk, endToEnd);
    }

    /** This source together with the exporter. */
    public PipeAuth withExporter() {
        return new PipeAuth(psk, true, deferredPsk, endToEnd);
    }

    /** This source on a pipe 0 the host knows is end to end. */
    public PipeAuth withEndToEnd() {
        return new PipeAuth(psk, useExporter, deferredPsk, true);
    }

    @Override
    public byte[] psk() {
        return psk == null ? null : psk.clone();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PipeAuth a && (psk == null ? a.psk == null : a.psk != null && java.security.MessageDigest.isEqual(psk, a.psk))
                && useExporter == a.useExporter && deferredPsk == a.deferredPsk && endToEnd == a.endToEnd;
    }

    @Override
    public int hashCode() { return java.util.Objects.hash(psk != null, useExporter, deferredPsk, endToEnd); }

    /** The key never appears in a string. */
    @Override
    public String toString() {
        return "PipeAuth[psk=" + (psk == null ? "none" : "set") + ", useExporter=" + useExporter + ", deferredPsk=" + deferredPsk + ", endToEnd=" + endToEnd + "]";
    }
}
