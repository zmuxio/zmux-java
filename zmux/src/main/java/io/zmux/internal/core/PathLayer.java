package io.zmux.internal.core;

import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.WireException;
import java.util.List;

/**
 * The single fork between single-pipe and multipath sessions (IMPLEMENTATION.md Section 2.6).
 * A session holds exactly one: {@link SinglePath} until multipath is negotiated, then
 * {@link PathSession}. Everything that differs between the two goes through here; the session
 * contains no other single/multipath branch.
 */
sealed interface PathLayer permits SinglePath, PathSession {
    // ---- writing ----

    /** Whether the established session wants to write on lane ol of p now. */
    boolean wantsWrite(Pipe p, Pipe.OutLane ol);

    /** Writes the established session's frames for lane ol of p. */
    void write(RecordWriter w, Pipe p, Pipe.OutLane ol);

    /** PIPE_ACK rides with the session's control frames (single pipe) rather than the path layer's. */
    boolean acksInControl();

    /** MAX_DATA went out; peerWaiting: the peer is or may be blocked on it (MULTIPATH.md 6.5 copies). */
    void maxDataSent(boolean peerWaiting);

    /** MAX_STREAM_DATA of st went out; peerWaiting as for {@link #maxDataSent}. */
    void maxStreamDataSent(Stream st, boolean peerWaiting);

    /** st is blocked on stream credit. */
    void creditBlocked(Stream st);

    /** Data of st prefers the urgent (first) lane. */
    boolean urgentLane(Stream st);

    // ---- retention (MULTIPATH.md 7.4) ----

    /** The effective session send buffer, given the configured one. */
    long sendBuffer(long configured);

    /** New (never sent) stream bytes the retention budget still admits. */
    long retainRoom();

    /** n new stream bytes were sent. */
    void retain(long n);

    /** n sent stream bytes are no longer retained (their stream reached a terminal send state). */
    void unretain(long n);

    /** Sent stream data stays until acknowledged (reinjection), rather than being released once written. */
    boolean retainsSent();

    /** Bytes retained for reinjection. */
    long retained();

    // ---- input ----

    /** Before any input or timer: pending scheduling answers are applied. */
    void pickInput();

    /** PIPE_PING, PIPE_ABANDON or PIPE_STATUS on p. */
    void onPathFrame(Pipe p, Frame f) throws WireException;

    /** The entries of a PIPE_ACK. */
    void onPipeAck(Frame.AckEntries it) throws WireException;

    /** An inbound lane of p ended; failed: not gracefully, for cause (null: unknown); allEnded: every lane of p ended. */
    void inboundEnded(Pipe p, boolean failed, boolean allEnded, Throwable cause);

    /** A write on pipe failed. */
    void outboundFailed(long pipe, Throwable cause);

    // ---- timers ----

    /** The earliest path-layer deadline. */
    long deadline(long now);

    /** Runs the path-layer timers due at t. */
    void onTimeout(long t);

    // ---- queries and commands ----

    /** The largest smoothed RTT over outbound pipes with a sample. */
    long srttMax();

    PipeInfo.States pipeState(Pipe p);

    Session.JoinProgress joinProgress(Pipe p);

    /** Physical readiness of p, independent of the scheduler's tier and standby selection. */
    boolean pipeUsable(Pipe p);

    /** The ID the next local join takes; 0 without multipath. */
    long nextPipeId();

    /** Whether maintenance has target room and a local join may start now. */
    boolean joinReady();

    /** The scheduling policy changed. */
    void policyChanged();

    /** [live outbound, live inbound] directions; closing: the session is closing. */
    boolean[] liveDirections(boolean closing);

    boolean wantsPick();

    boolean takePick(PathScheduler.PickRequest req);

    void answerPick(PathScheduler.PickRequest req, List<PathScheduler.PickAnswer> answers, boolean failed);
}
