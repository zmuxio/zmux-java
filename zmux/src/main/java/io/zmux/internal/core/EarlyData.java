package io.zmux.internal.core;

import io.zmux.internal.core.CoreConfig.Role;
import io.zmux.internal.wire.FrameEncoder;
import io.zmux.internal.wire.TransportParams;

/**
 * Early data under an assumed parameter set (SPEC.md Section 6.1.2) and carrier early data
 * (the carrier's own 0-RTT). Owns the assumption, the fence and the outcome decision; the
 * session applies the assumption and adopts or rolls back its streams.
 */
final class EarlyData {
    /** What the peer's parameters mean for the data sent early. */
    enum Outcome { NONE, ADOPT, REJECTED, BELOW }

    /** The parameters assumed until the peer's arrive; null when not sending early. */
    private TransportParams assumed;
    private boolean floor;
    private boolean fenceSent;
    private boolean sentAny;
    /** The early data rides on the carrier's early data, not yet accepted by it. */
    private boolean carrier;
    /** Sends are kept: the carrier's early data was not (yet) accepted. */
    private boolean held;
    private boolean accepted;
    /** A server whose carrier took early data: peer streams are replayable until the handshake completes. */
    private boolean replayableIn;

    EarlyData(CoreConfig cfg) {
        if (cfg.earlyRemembered() != null && cfg.earlyReplayable() && allowed(cfg)) {
            assumed = cfg.earlyRemembered();
            carrier = true;
        } else if (cfg.earlyZmuxPeer() && allowed(cfg)) {
            assumed = floorParams();
            floor = true;
        }
    }

    /** The role must not depend on the peer (SPEC.md Section 6.1.2 rule 1). */
    private static boolean allowed(CoreConfig cfg) {
        TransportParams l = cfg.local();
        return cfg.role() != Role.AUTO && (!l.has(TransportParams.ROLE) || l.role != TransportParams.ROLE_AUTO);
    }

    /** The zmux floor every zmux endpoint advertises at least. */
    static TransportParams floorParams() {
        TransportParams p = new TransportParams();
        p.present = TransportParams.MAX_DATA | TransportParams.STREAMS_BIDI | TransportParams.BIDI_REMOTE
                | TransportParams.PREAMBLE | TransportParams.PIPE_ACK | TransportParams.RESET_AT;
        p.initialMaxData = 16384;
        p.maxStreamsBidi = 4;
        p.maxStreamDataBidiRemote = 4096;
        p.maxPreambleSize = 1024;
        p.maxRecordSize = TransportParams.DEFAULT_MAX_RECORD_SIZE;
        return p;
    }

    /** The assumed parameters while sending early, else null. */
    TransportParams assumed() {
        return assumed;
    }

    /** The assumption is the zmux floor (bidi streams only). */
    boolean floor() {
        return floor;
    }

    /** Early data may be sent: an assumption is in force. */
    boolean active() {
        return assumed != null;
    }

    /** Sent stream data must be kept: carrier early data not yet accepted. */
    boolean holdSends() {
        return assumed != null && carrier || held;
    }

    /** Stream data left under the assumption. */
    void sent() {
        if (assumed != null) sentAny = true;
    }

    /** The carrier accepted its early data: the early sends are final. */
    void carrierAccepted() {
        held = false;
        accepted = true;
        carrier = false;
    }

    /** The carrier rejected its early data and restarted: everything is resent, held until accepted. */
    void restarted() {
        held = true;
        assumed = null;
    }

    /** Writes the early fence (an empty PIPE_ACK, SPEC.md 6.1.2 rule 3) once; false when it did not fit. */
    boolean writeFence(RecordWriter w) {
        if (fenceSent || !assumed.isZmux()) return true;
        if (!w.frame(9, false, bb -> {
            FrameEncoder.pipeAckHeader(bb, 1);
            FrameEncoder.ackEntry(bb, 0, 0, 0, 0);
        })) return false;
        fenceSent = true;
        return true;
    }

    /** Decides the fate of early data when the peer's parameters p arrive (SPEC.md Section 6.1.2, Outcome). */
    Outcome outcome(TransportParams p, boolean acked) {
        TransportParams a = assumed;
        if (a == null) {
            if (!held) return Outcome.NONE;
            held = false;
            return Outcome.ADOPT;
        }
        if (carrier) {
            carrier = false;
            held = !accepted;
        }
        assumed = null;
        if (!sentAny) return Outcome.ADOPT;
        if (a.isZmux() && !acked || floor && p.maxStreamsBidi == 0) return Outcome.REJECTED;
        return belowAssumed(p, a) ? Outcome.BELOW : Outcome.ADOPT;
    }

    /** Whether the peer's parameters p grant less than the assumed a that early data relied on. */
    static boolean belowAssumed(TransportParams p, TransportParams a) {
        if (p.initialMaxData < a.initialMaxData || p.maxStreamsBidi < a.maxStreamsBidi
                || p.maxStreamDataBidiRemote < a.maxStreamDataBidiRemote || p.maxRecordSize < a.maxRecordSize) return true;
        if (a.has(TransportParams.PREAMBLE) && (!p.has(TransportParams.PREAMBLE) || p.maxPreambleSize < a.maxPreambleSize)) return true;
        return a.has(TransportParams.PIPE_ACK) && !p.has(TransportParams.PIPE_ACK)
                || a.has(TransportParams.RESET_AT) && !p.has(TransportParams.RESET_AT);
    }

    void carrierEarlyIn() {
        replayableIn = true;
    }

    /** The carrier handshake completed: nothing received is replayable any more. */
    void carrierHandshakeDone() {
        replayableIn = false;
    }

    /** A peer stream opened now is replayable. */
    boolean replayableIn() {
        return replayableIn;
    }
}
