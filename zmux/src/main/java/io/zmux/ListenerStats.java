package io.zmux;

/**
 * The counts of a listener since it was made (API.md Section 7.2). The unit is the pipe input handed to the listener:
 * a carrier, a lane, or the two carriers of a split pipe once paired, which are one input (a half that is never
 * paired is an input of its own). Each input whose outcome is known counts exactly once, when that outcome is
 * decided, in one of the first five fields, so that {@code sessions + joins + rejected + refused + timedOut} is the
 * number of inputs handed to the listener whose outcome is known; {@code setupFailures} counts connections that
 * never became a pipe input. Read without a lock; two counts are not a consistent snapshot.
 *
 * @param sessions sessions started: bootstraps offered to {@link ZmuxListener#accept()}, a split bootstrap once
 * @param joins joins accepted into existing sessions, and lanes accepted into their pipes
 * @param setupFailures connections never made a pipe input: a failed TLS handshake or its timeout (with an admission:
 *     {@link AdmissionStats#handshakeFailed()} instead), a failed HTTP or WebSocket upgrade (adapters with their own
 *     accept loops report them through {@link ZmuxListener#countSetupFailure()})
 * @param rejected inputs closed without a distinguishable reason: unknown, unauthenticated or replayed joins, lanes
 *     that name no pipe, inputs that are not zmux; an admission's decisions are counted in {@code admission} only
 * @param refused inputs refused at a limit: of the listener (MaxPendingPerSource, the memory budget), of the session
 *     (a join closed with ZMUX_PIPE_RESOURCE_LIMIT), new sessions after the listener was shut down, and every carrier
 *     after it was closed
 * @param timedOut inputs without a first record, or join, within the first-record timeout, and split halves whose
 *     other half did not arrive in time
 * @param admission the outcomes of the listener's admission
 */
public record ListenerStats(long sessions, long joins, long setupFailures, long rejected, long refused, long timedOut,
                            AdmissionStats admission) {}
