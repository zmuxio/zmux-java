package io.zmux;

/**
 * Ends a splice with a chosen application code. Given to one splice through {@link SpliceOptions#withControl},
 * {@link #cancel(long)} stops the relay: both streams are reset (sending) and stopped (receiving) with the code, in
 * both directions, and the splice's stage completes normally with a result whose still-running directions end in
 * {@link SpliceResult.End.Cancelled} with that code. A direction that had already ended keeps its own end.
 * Cancelling the stage itself instead resets the streams with ZMUX_CANCELLED (0x5a4d0001), stops them with code 0 and leaves no result.
 */
public final class SpliceControl {
    private Splice splice;
    private long code = -1;

    /** A control for one splice. */
    public SpliceControl() {}

    /**
     * Cancels the splice with code (0 to 2^62-1); before the splice starts, it is cancelled as it starts. False when
     * this control was already cancelled, the splice ended or its stage was cancelled.
     */
    public boolean cancel(long code) {
        if (code < 0 || code > io.zmux.internal.wire.VarInt.MAX) throw new IllegalArgumentException("code out of range");
        Splice s;
        synchronized (this) {
            if (this.code >= 0) return false;
            this.code = code;
            s = splice;
        }
        return s == null || s.cancelWith(code);
    }

    /** Binds the control to the splice that starts with it; a control serves one splice. */
    void attach(Splice s) {
        long pending;
        synchronized (this) {
            if (splice != null) throw new IllegalStateException("the control already serves a splice");
            splice = s;
            pending = code;
        }
        if (pending >= 0) s.cancelWith(pending);
    }
}
