package io.zmux.runtime;

import io.zmux.ErrorCode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class StreamHalfStateEffectiveRecvTest {
    @Test
    void localReadStopDominatesLaterPeerFin() {
        StreamHalfState halfState = new StreamHalfState(true, true);

        halfState.markLocalReadStop();
        halfState.finishReceiveIfActive();

        assertEquals(StreamHalfState.EffectiveRecvState.STOPPED, halfState.effectiveRecvState());
        assertEquals(StreamHalfState.TerminalErrorChoice.RECV_CLOSED, halfState.terminalErrorPriority());
        // Read-stop precedence only governs local read errors: DATA after the observed peer FIN is still a
        // stream-state violation answered with ABORT(STREAM_CLOSED) (SPEC 9.2/9.6).
        assertEquals(StreamRuntime.PeerDataAction.ABORT_STREAM_CLOSED, halfState.peerDataAction(true, false, false));
        assertEquals(StreamRuntime.PeerDataAction.ABORT_STREAM_CLOSED, halfState.peerDataAction(true, false, true));
        assertEquals(StreamRuntime.PeerDataAction.ABORT_STREAM_CLOSED, halfState.peerDataAction(true, true, false));
        assertTrue(halfState.receiveGraceful(), "peer FIN after read-stop should make the receive half graceful for tombstones");
    }

    @Test
    void peerDataAfterFinAbortsEvenWhenFullyTerminal() {
        StreamHalfState halfState = new StreamHalfState(true, true);

        halfState.markSendReset();
        halfState.finishReceiveIfActive();

        assertTrue(halfState.fullyTerminal(), "test requires a fully terminal stream");
        assertEquals(StreamRuntime.PeerDataAction.ABORT_STREAM_CLOSED, halfState.peerDataAction(true, true, false));
        assertEquals(StreamRuntime.PeerDataAction.ABORT_STREAM_CLOSED, halfState.peerDataAction(true, true, true));
    }

    @Test
    void stoppedDirectionBeforePeerFinStillDiscardsLateData() {
        StreamHalfState halfState = new StreamHalfState(true, true);

        halfState.markLocalReadStop();

        assertTrue(halfState.localReadStopTailOpen(), "read-stopped direction should still be waiting for its tail");
        assertEquals(StreamRuntime.PeerDataAction.IGNORE, halfState.peerDataAction(true, false, false));
        assertEquals(StreamRuntime.PeerDataAction.IGNORE_AND_FIN, halfState.peerDataAction(true, false, true));
        halfState.finishReceiveIfActive();
        assertFalse(halfState.localReadStopTailOpen(), "peer FIN completes the stopped direction's tail");
    }

    @Test
    void localReadStopDominatesLaterPeerReset() {
        StreamHalfState halfState = new StreamHalfState(true, true);

        halfState.markLocalReadStop();
        halfState.markRecvReset();

        assertEquals(StreamHalfState.EffectiveRecvState.STOPPED, halfState.effectiveRecvState());
        assertEquals(StreamHalfState.TerminalErrorChoice.RECV_CLOSED, halfState.terminalErrorPriority());

        StreamTerminalState terminalState = new StreamTerminalState();
        terminalState.recordLocalReadStop(ErrorCode.CANCELLED.code());
        assertEquals(
                io.zmux.ZmuxTerminationKind.STOPPED,
                ((io.zmux.ApplicationError) terminalState.operationError(halfState)).terminationKind()
        );
    }
}
