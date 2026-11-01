package io.zmux;

/** Resolution is final even when the optional background rejoin fails or is skipped. */
public record DuplicateResult(ZmuxSession survivor, ZmuxSession loser, Rule rule, PipeMaintenance rejoin) {
    public enum Rule { ENDPOINT_IDENTITY, SESSION_NONCE, ROLE_NONCE }
    /** Whether a best-effort rejoin worker was started; it may still skip dialing if admission changes. */
    public boolean rejoinStarted() { return rejoin != null; }
}
