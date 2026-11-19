package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.wire.FrameKind;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A stream end written before a graceful close is flushed ahead of CONNECTION_CLOSE: the
 * application's close waits while {@link Session#flushable()} reports something unsent.
 */
@Timeout(30)
class CloseFlushEndTest {
    private static Stream accepted(Session a, Session b) throws Exception {
        HeldControlTest.pump(a, b);
        Stream st = a.streams().openStream(true);
        a.sending().write(st, ByteBuffer.wrap(new byte[] {7}), false);
        HeldControlTest.pump(a, b);
        Stream in = b.streams().acceptStream(true);
        assertNotNull(in);
        return in;
    }

    private static boolean sent(List<HeldControlTest.Sent> frames, FrameKind k) {
        return frames.stream().anyMatch(f -> f.kind() == k);
    }

    @Test void aQueuedResetIsFlushedBeforeTheClose() throws Exception {
        Session a = HeldControlTest.session(CoreConfig.Role.INITIATOR), b = HeldControlTest.session(CoreConfig.Role.RESPONDER);
        Stream in = accepted(a, b);
        b.sending().reset(in, 9);
        assertTrue(b.flushable(), "the queued RESET_STREAM is unsent");
        assertTrue(sent(HeldControlTest.pump(a, b)[1], FrameKind.RESET_STREAM));
        assertFalse(b.flushable(), "the RESET_STREAM went out");
    }

    @Test void aQueuedFinishIsFlushedBeforeTheClose() throws Exception {
        Session a = HeldControlTest.session(CoreConfig.Role.INITIATOR), b = HeldControlTest.session(CoreConfig.Role.RESPONDER);
        Stream in = accepted(a, b);
        b.sending().finish(in);
        assertTrue(b.flushable(), "the queued FIN is unsent");
        HeldControlTest.pump(a, b);
        assertFalse(b.flushable(), "the FIN went out");
    }

    /** What a close finds when it does not wait: the queued reset never reaches the peer. */
    @Test void aCloseWithTheResetQueuedSendsOnlyTheClose() throws Exception {
        Session a = HeldControlTest.session(CoreConfig.Role.INITIATOR), b = HeldControlTest.session(CoreConfig.Role.RESPONDER);
        Stream in = accepted(a, b);
        b.sending().reset(in, 9);
        b.close(0, new byte[0]);
        var frames = HeldControlTest.pump(a, b)[1];
        assertTrue(sent(frames, FrameKind.CONNECTION_CLOSE));
        assertFalse(sent(frames, FrameKind.RESET_STREAM), "a close discards unsent frames (SPEC.md 6.5)");
    }
}
