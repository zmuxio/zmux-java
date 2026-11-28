package io.zmux;

import io.zmux.carrier.PipeIo;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

/**
 * A carrier and its adapter-owned dialer and redirect target.
 *
 * @param variant a short label of the host for the transform the candidate's carriers run ("": none):
 *     a kind's variants are different candidates of the cache, ordered as listed
 * @param target for a kind dialed at a URL, where its carriers start and what redirects led them
 *     to, or null if unknown
 */
public record Candidate(CarrierKind kind, Dialer dialer, String variant, HttpTarget target) {
    public Candidate {
        Objects.requireNonNull(kind);
        Objects.requireNonNull(dialer);
        if (variant == null) variant = "";
    }

    /** A candidate of no variant and no target. */
    public Candidate(CarrierKind kind, Dialer dialer) { this(kind, dialer, "", null); }

    /** This candidate with variant v. */
    public Candidate withVariant(String v) { return new Candidate(kind, dialer, v, target); }

    /** This candidate with the target t its dialer starts from. */
    public Candidate withTarget(HttpTarget t) { return new Candidate(kind, dialer, variant, t); }

    /** A candidate whose carriers join through {@link ZmuxSession#addPipe(PipeIo, PipeOptions)}. */
    public static Candidate of(CarrierKind kind, Dialer dialer) { return new Candidate(kind, dialer); }

    /**
     * This candidate with every carrier its dialer opens passed through wrap before zmux uses
     * it, such as a host's own layer over the carrier.
     */
    public Candidate wrapDial(Wrap wrap) {
        Objects.requireNonNull(wrap);
        Dialer inner = dialer;
        return new Candidate(kind, (credential, timeout) -> Objects.requireNonNull(
                wrap.wrap(inner.dial(credential, timeout)), "wrap returned null"), variant, target);
    }

    /** Opens a carrier of the kind. */
    @FunctionalInterface
    public interface Dialer {
        /**
         * Opens a carrier, presenting credential (the session's {@link ZmuxConfig#carrierCredential()},
         * null: none) for the server's admission, within timeout. The carrier lives until its
         * pipe ends.
         */
        PipeIo dial(CarrierCredential credential, Duration timeout) throws IOException;
    }

    /** Wraps a freshly opened carrier ({@link #wrapDial}). */
    @FunctionalInterface
    public interface Wrap {
        /** Returns the carrier to use in place of io; it owns io, and closes it when it fails. */
        PipeIo wrap(PipeIo io) throws IOException;
    }
}
