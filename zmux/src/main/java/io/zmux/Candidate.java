package io.zmux;

import io.zmux.carrier.PipeIo;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * One carrier kind that {@link Zmux#dial(CarrierMode, ZmuxConfig, ModePolicy)} or a
 * {@link PipePool#mode mode pool} may use. Each adapter keeps its own dialer and offers a
 * candidate that wraps it.
 *
 * @param joiner joins carriers of the kind to an established session, for a kind whose dialer
 *     does not open a carrier that can join (a split bootstrap joins its uplink and downlink as
 *     two pipes); null: {@link Paths#add(PipeIo, PipeOptions)} on a carrier from the dialer
 */
public record Candidate(CarrierKind kind, Dialer dialer, Joiner joiner) {
    public Candidate {
        Objects.requireNonNull(kind);
        Objects.requireNonNull(dialer);
    }

    /** A candidate whose carriers join through {@link Paths#add(PipeIo, PipeOptions)}. */
    public static Candidate of(CarrierKind kind, Dialer dialer) { return new Candidate(kind, dialer, null); }

    /**
     * This candidate with every carrier its dialer opens passed through wrap before zmux uses
     * it, such as a host's own layer over the carrier. The joiner, if any, is kept as it is.
     */
    public Candidate wrapDial(Wrap wrap) {
        Objects.requireNonNull(wrap);
        Dialer inner = dialer;
        return new Candidate(kind, (credential, timeout) -> Objects.requireNonNull(
                wrap.wrap(inner.dial(credential, timeout)), "wrap returned null"), joiner);
    }

    /** Opens a carrier of the kind. */
    @FunctionalInterface
    public interface Dialer {
        /**
         * Opens a carrier, presenting credential (the session's {@link ZmuxConfig#credential()},
         * null: none) for the server's admission, within timeout. The carrier lives until its
         * pipe ends.
         */
        PipeIo dial(Credential credential, Duration timeout) throws IOException;
    }

    /** Joins carriers of the kind to an established session. */
    @FunctionalInterface
    public interface Joiner {
        /** Joins the kind's carriers to session with options within timeout and returns their pipes. */
        List<ZmuxPipe> join(ZmuxSession session, PipeOptions options, Credential credential, Duration timeout) throws IOException;
    }

    /** Wraps a freshly opened carrier ({@link #wrapDial}). */
    @FunctionalInterface
    public interface Wrap {
        /** Returns the carrier to use in place of io; it owns io, and closes it when it fails. */
        PipeIo wrap(PipeIo io) throws IOException;
    }
}
