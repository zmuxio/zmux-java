package io.zmux;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class LocalOpenPhaseTest {
    @Test
    void phasePredicatesFollowVisibilityStateTransitions() {
        LocalOpenPhase needsCommit = LocalOpenPhase.from(true, false, false, false);
        assertEquals(LocalOpenPhase.NEEDS_COMMIT, needsCommit, "local stream without send commit should require an opener");
        assertTrue(needsCommit.needsLocalOpener(), "pre-commit local stream should require an opener");
        assertTrue(needsCommit.awaitingPeerVisibility(), "pre-commit local stream should await peer visibility");
        assertTrue(needsCommit.shouldEmitOpenerFrame(), "pre-commit local stream should emit an opener frame");
        assertTrue(needsCommit.shouldMarkPeerVisible(), "local unseen stream should be eligible to become peer-visible");
        assertFalse(needsCommit.canTakePendingPriorityUpdate(), "pre-commit local stream should defer priority updates");

        LocalOpenPhase needsEmit = LocalOpenPhase.from(true, true, false, false);
        assertEquals(LocalOpenPhase.NEEDS_EMIT, needsEmit, "committed stream without opener barrier should need emission");
        assertFalse(needsEmit.needsLocalOpener(), "committed stream should no longer require a local opener");
        assertTrue(needsEmit.awaitingPeerVisibility(), "committed local stream should still await peer visibility");
        assertTrue(needsEmit.shouldEmitOpenerFrame(), "committed unseen stream without queued opener should emit one");

        LocalOpenPhase queued = LocalOpenPhase.from(true, true, false, true);
        assertEquals(LocalOpenPhase.QUEUED, queued, "queued opener should suppress duplicate opener emission");
        assertTrue(queued.awaitingPeerVisibility(), "queued opener should keep the stream unseen");
        assertFalse(queued.shouldEmitOpenerFrame(), "queued opener should suppress duplicate opener emission");
        assertFalse(queued.canTakePendingPriorityUpdate(), "queued opener should still defer priority updates");

        LocalOpenPhase peerVisible = LocalOpenPhase.from(true, true, true, true);
        assertEquals(LocalOpenPhase.PEER_VISIBLE, peerVisible, "peer-visible stream should converge to the peer-visible phase");
        assertFalse(peerVisible.awaitingPeerVisibility(), "peer-visible stream should leave unseen-local tracking");
        assertFalse(peerVisible.shouldMarkPeerVisible(), "peer-visible stream should not be marked again");
        assertTrue(peerVisible.canTakePendingPriorityUpdate(), "peer-visible stream should accept pending priority updates");
        assertTrue(peerVisible.shouldQueueStreamBlocked(0L), "peer-visible open stream should queue BLOCKED at zero credit");

        assertEquals(LocalOpenPhase.NONE, LocalOpenPhase.from(false, false, false, false), "peer-opened stream should not enter the local-open phase machine");
    }
}
