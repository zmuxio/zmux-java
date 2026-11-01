package io.zmux;

import java.io.IOException;
import java.util.List;

/** {@link Zmux#dial(CarrierMode, ZmuxConfig, ModePolicy)} found no usable candidate: the outcome of every one it tried. */
public final class ModeException extends IOException {
    private final transient List<CandidateResult> results;

    public ModeException(List<CandidateResult> results) {
        super(message(results));
        this.results = List.copyOf(results);
        for (CandidateResult r : this.results) if (r.error() != null) addSuppressed(r.error());
    }

    private static String message(List<CandidateResult> results) {
        var b = new StringBuilder("no usable carrier");
        for (int i = 0; i < results.size(); i++) {
            CandidateResult r = results.get(i);
            b.append(i == 0 ? ": " : "; ").append(r.kind()).append(' ').append(r.outcome());
            if (r.error() != null) b.append(" (").append(r.error().getMessage()).append(')');
        }
        return b.toString();
    }

    public List<CandidateResult> results() { return results; }
}
