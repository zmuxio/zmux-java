package io.zmux;

import java.util.ArrayList;
import java.util.List;

/**
 * Ends splices with a chosen application code. One handle may serve several splices
 * ({@link SpliceOptions.Builder#abort}); the first {@link #abort(long)} decides the code. An abort resets
 * (sending) and stops (receiving) both streams of each splice with the code, in both directions, and each
 * splice's stage completes normally with a result whose still-running directions end in
 * {@link SpliceEnd.Cancelled} with that code; a direction that had already ended keeps its own end. A splice
 * started with a handle already aborted is aborted as it starts. Cancelling a splice's stage instead resets
 * its streams with ZMUX_CANCELLED (0x5a4d0001), stops them with code 0 and leaves no result.
 */
public final class SpliceAbort {
    private final List<Splice> splices = new ArrayList<>();
    private long code = -1;

    /** A handle that has not aborted. */
    public SpliceAbort() {}

    /**
     * Aborts every splice this handle serves, and those started with it later, with code (0 to 2^62-1). False when
     * the handle had already aborted (the first code stays).
     */
    public boolean abort(long code) {
        if (code < 0 || code > io.zmux.internal.wire.VarInt.MAX) throw new IllegalArgumentException("code out of range");
        List<Splice> serving;
        synchronized (this) {
            if (this.code >= 0) return false;
            this.code = code;
            serving = List.copyOf(splices);
            splices.clear();
        }
        for (Splice s : serving) s.cancelWith(code);
        return true;
    }

    /** Binds a splice that starts with this handle; it is aborted at once when the handle already was. */
    void attach(Splice s) {
        long pending;
        synchronized (this) {
            pending = code;
            if (pending < 0) splices.add(s);
        }
        if (pending >= 0) s.cancelWith(pending);
    }

    /** Unbinds a splice that ended, so a long-lived handle holds no finished splice. */
    synchronized void detach(Splice s) {
        splices.remove(s);
    }
}
