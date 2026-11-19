package io.zmux.internal.core;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The drain of unread data after the peer's CONNECTION_CLOSE, at the core. */
@Timeout(30)
class CloseDrainTest {
    /** a writes n bytes on a stream; b receives them; returns b's stream, accepted when accept is set. */
    private static Stream[] setUp(Session a, Session b, int n, boolean accept) throws Exception {
        HeldControlTest.pump(a, b);
        Stream st = a.streams().openStream(true);
        a.sending().write(st, ByteBuffer.wrap(new byte[n]), false);
        HeldControlTest.pump(a, b);
        Stream in = accept ? b.streams().acceptStream(true) : null;
        if (accept) assertNotNull(in);
        return new Stream[] {st, in};
    }

    /** A session without pipe acknowledgement, whose peer may end it by ending the carrier. */
    private static Session unacked(CoreConfig.Role role) {
        CoreConfig.Builder c = CoreConfig.builder().role(role).transportRole(role);
        c.local().present &= ~(io.zmux.internal.wire.TransportParams.PIPE_ACK | io.zmux.internal.wire.TransportParams.ACK_THRESHOLD | io.zmux.internal.wire.TransportParams.ACK_DELAY | io.zmux.internal.wire.TransportParams.LANES);
        Session s = new Session(c.build(), 0);
        CarrierInfo info = new CarrierInfo(Framing.BYTES, false, true, false, true);
        s.attachBootstrap(info, info, null);
        return s;
    }

    private static Session a() { return HeldControlTest.session(CoreConfig.Role.INITIATOR); }
    private static Session b() { return HeldControlTest.session(CoreConfig.Role.RESPONDER); }

    /** a closes; b reads the close, then a's FIN arrives and b's write side is shut. */
    private static void peerCloses(Session a, Session b) throws Exception {
        a.close(0x2a, "bye".getBytes(UTF_8));
        HeldControlTest.pump(a, b);
        b.pipeEnded(0, 0, true, false, null, 0);
        b.outputShut(0, 0, 0);
    }

    @Test void unreadDataHoldsTheEndUntilItIsRead() throws Exception {
        Session a = a(), b = b();
        Stream in = setUp(a, b, 100, true)[1];
        peerCloses(a, b);
        assertEquals(SessionState.CLOSING, b.state(), "the accepted stream's data is still unread");
        ByteBuffer dst = ByteBuffer.allocate(200);
        assertEquals(100, b.receiving().read(in, dst).n());
        b.handleTimeout(b.nextTimeout());
        assertEquals(SessionState.CLOSED, b.state(), "the last read ends the drain");
    }

    /**
     * The hand-off bound ends the wait for a write side that never reports its shutdown, not the read drain: the
     * linger timeout bounds that (SPEC.md 6.5 step 3).
     */
    @Test void theHandOffBoundLeavesTheReadDrainToTheLinger() throws Exception {
        Session a = a(), b = b();
        Stream in = setUp(a, b, 100, true)[1];
        a.close(0x2a, "bye".getBytes(UTF_8));
        HeldControlTest.pump(a, b);
        b.pipeEnded(0, 0, true, false, null, 0); // b's write side never reports its shutdown
        assertFalse(b.closeHandedOff());
        long bound = Session.closeLimit;
        b.handleTimeout(bound);
        assertTrue(b.closeHandedOff(), "the hand-off is over at its bound");
        assertEquals(SessionState.CLOSING, b.state(), "the unread data still holds the session");
        assertEquals(bound + b.lingerTimeout(), b.nextTimeout(), "the linger bounds the drain from there");
        assertEquals(100, b.receiving().read(in, ByteBuffer.allocate(200)).n());
        b.handleTimeout(b.nextTimeout());
        assertEquals(SessionState.CLOSED, b.state());
    }

    /** Bytes pushed to the receive lane and not consumed there are unread too. */
    @Test void dataOnTheReceiveLaneIsUnreadUntilConsumed() throws Exception {
        Session a = a(), b = b();
        Stream in = setUp(a, b, 100, true)[1];
        assertNotNull(b.receiving().push(in, 1000));
        peerCloses(a, b);
        assertEquals(SessionState.CLOSING, b.state(), "the lane holds unconsumed bytes");
        b.receiving().laneConsumed(in, 100, 0);
        b.handleTimeout(b.nextTimeout());
        assertEquals(SessionState.CLOSED, b.state());
    }

    /** An unaccepted stream is dropped at the close, holds nothing, and cannot be accepted. */
    @Test void unacceptedStreamsAreDroppedAtTheClose() throws Exception {
        Session a = a(), b = b();
        setUp(a, b, 100, false);
        peerCloses(a, b);
        assertNull(b.streams().acceptStream(true), "a closing session offers no stream");
        assertEquals(SessionState.CLOSED, b.state(), "nothing held");
    }

    /** A peer that ends its carrier without CONNECTION_CLOSE starts no drain. */
    @Test void anEndWithoutTheCloseFrameStartsNoDrain() throws Exception {
        Session a = a(), b = unacked(CoreConfig.Role.RESPONDER);
        setUp(a, b, 100, true);
        b.pipeEnded(0, 0, true, false, null, 0);
        assertEquals(SessionError.Kind.IDLE, b.error().kind(), "the peer's end without CONNECTION_CLOSE");
        assertTrue(b.error().remote());
        b.outputShut(0, 0, 0);
        assertEquals(SessionState.CLOSED, b.state(), "unread data holds only after the peer's close");
    }

    /** Without a live outbound pipe the peer's close still drains, lingering from its processing. */
    @Test void aPeersCloseWithoutAnOutboundPipeStillDrains() throws Exception {
        Session a = a(), b = b();
        setUp(a, b, 100, true);
        b.pipes.get(0).out().fail(); // The outbound side is gone, the inbound one still reads.
        b.setNow(1_000);
        a.close(0x2a, "bye".getBytes(UTF_8));
        HeldControlTest.pump(a, b);
        assertEquals(SessionState.CLOSING, b.state(), "the close was read; the drain runs");
        assertTrue(b.nextTimeout() <= 1_000 + b.lingerTimeout(), "bounded by a linger from the close");
    }
}
