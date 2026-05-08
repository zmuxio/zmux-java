package io.zmux.runtime;

import io.zmux.ApplicationError;
import io.zmux.ErrorCode;
import io.zmux.ZmuxTerminationKind;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class StreamHalfStateEffectiveRecvTest {
    @Test
    void localReadStopDominatesLaterPeerFin() {
        StreamHalfState halfState = new StreamHalfState(true, true);

        halfState.markLocalReadStop();
        halfState.finishReceiveIfActive();

        assertEquals(StreamHalfState.EffectiveRecvState.STOPPED, halfState.effectiveRecvState());
        assertEquals(StreamHalfState.TerminalErrorChoice.RECV_CLOSED, halfState.terminalErrorPriority());
        assertEquals(StreamRuntime.PeerDataAction.IGNORE, halfState.peerDataAction(true, false, false));
        assertEquals(StreamRuntime.PeerDataAction.IGNORE_AND_FIN, halfState.peerDataAction(true, false, true));
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
