package io.zmux.internal.core;

import io.zmux.internal.loop.MemoryBudget;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class SendBufferContractTest {
    private Session highBDP() {
        return highBDP(CoreConfig.builder());
    }

    private Session highBDP(CoreConfig.Builder b) {
        Session s = PipeTargetTest.session(b);
        Pipe p = s.pipes.get(0);
        Poke.set(p.mp().rtt, "smoothed", Time.SECOND);
        Poke.set(p.out().snd.re(), "blockedNow", true);
        p.out().snd.re().sample(512L << 20, false, 0, 0);
        Poke.set(p.out().snd.re(), "blockedNow", false);
        return s;
    }
    @Test void multipathBufferGrowsWhileSinglePipeKeepsItsInitialSize() {
        Session s = highBDP(CoreConfig.builder().memoryBudget(256L << 20));
        assertEquals(256L << 20, s.mp().retention.retransmitBudget());
        assertEquals(256L << 20, s.send.sendRoom(new Stream(true, true)));
        Poke.set(s, "path", s.singlePath());
        assertEquals(s.cfg.sendBuffer(), s.send.sendRoom(new Stream(true, true)));
    }
    @Test void fixedMaximumCannotUndercutInitialBuffer() {
        Session s = highBDP(CoreConfig.builder().sendBuffer(8L << 20).sendBufferMax(2L << 20));
        assertEquals(8L << 20, s.mp().retention.retransmitBudget());
    }
    @Test void sharedHeadroomIncludesUnchargedGrowthAndDoesNotDoubleCountCharges() {
        var budget = new MemoryBudget(512L << 20, null); budget.add(400L << 20);
        Session s = highBDP(CoreConfig.builder().memoryBudget(1).budget(budget));
        s.conn.held(0, (8L << 20) << 2); s.mp().retention.retain(8L << 20);
        assertEquals(112L << 20, s.mp().retention.retransmitBudget());
        s.memory().charge(); assertEquals(112L << 20, s.mp().retention.retransmitBudget());
        budget.add(64L << 20); assertEquals(64L << 20, s.mp().retention.retransmitBudget());
    }
    @Test void unsentDataIsNotChargedTwiceWhenComputingTheAbsoluteMaximum() {
        Session s = highBDP(CoreConfig.builder().memoryBudget(128L << 20)); s.conn.held(0, (32L << 20) << 2);
        assertEquals(128L << 20, s.mp().retention.retransmitBudget());
    }
    @Test void fullSessionBufferKeepsOnlyTheSmallDirectionException() {
        Session s = highBDP(); Poke.set(s, "path", s.singlePath());
        Stream bulk = new Stream(true, true);
        long before = bulk.heldClass(); Poke.set(bulk.snd.q(), "end", s.cfg.sendBuffer()); s.conn.held(before, bulk.heldAfter());
        assertEquals(Stream.SMALL_HELD, s.send.sendRoom(new Stream(true, true)));
    }
    @Test void explicitStreamLimitCanBeSmallerThanTheSessionFloor() {
        Session s = highBDP(); Poke.set(s, "path", s.singlePath()); Stream stream = new Stream(true, true);
        s.send.setSendBuffer(stream, 32 << 10); assertEquals(32 << 10, s.send.sendRoom(stream));
    }
}
